import asyncio
import contextlib
import enum
import json
import os
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, Optional

import aiosqlite
import fastapi
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Type alias for an action function.
# Each action receives a Transaction instance and returns an awaitable.
ActionFunction = Callable[["Transaction"], Awaitable[Any]]


class TransactionStatus(enum.Enum):
    NOT_INITIALIZED = "not_initialized"
    RUNNING = "running"
    SUSPENDED = "suspended"
    SUCCESS = "success"
    ERROR = "error"


class EventType(enum.Enum):
    IO = "io"


TIMEOUT_IO_REQUEST = 10


# ------------------------------
# DATABASE SCHEMA INITIALIZATION
# ------------------------------


async def init_db(db: aiosqlite.Connection):
    db.row_factory = aiosqlite.Row

    # Create a table for transactions with UUID primary key
    await db.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            action_slug TEXT,
            status TEXT,
            result TEXT,
            update_count INTEGER DEFAULT 1,
            created_at REAL,
            updated_at REAL
        )
    """)

    # Create a table for all steps in a transaction
    # data is a JSON string of step data. For IO requests, this is the request body. If it been responded to, the response is put under the "response" key.
    await db.execute("""
        CREATE TABLE IF NOT EXISTS steps (
            transaction_id TEXT,
            step_index INTEGER,
            step_type TEXT,
            data TEXT,
            response TEXT,
            created_at REAL,
            updated_at REAL,
            FOREIGN KEY(transaction_id) REFERENCES transactions(id)
            PRIMARY KEY(transaction_id, step_index)
        )
    """)
    await db.commit()


# ------------------------------
# TRANSACTION CLASS
# ------------------------------


class Transaction:
    """
    A Transaction is a wrapper around a workflow instance.
    It uses the DB connection via the ASGI app's state to update and report its state.
    No in memory state is maintained, queries the DB for the current state on each access.
    """

    def __init__(self, transaction_id: str, app: FastAPI):
        self.app = app
        self.id = transaction_id
        self.step_count = 0
        self.pending_io_request = None

    async def _initialize(self, action_slug: str):
        """
        Start the transaction. If it already exists, no action is taken.
        """
        db: aiosqlite.Connection = self.app.state.db
        now = int(time.time())

        await db.execute(
            """
            INSERT INTO transactions (id, action_slug, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
            """,
            (
                self.id,
                action_slug,
                TransactionStatus.RUNNING.value,
                now,
                now,
            ),
        )
        await db.commit()
        await self.publish_event()

    async def update(self, status: str, result: Optional[Any] = None):
        """Update the transactions table for this transaction and publish an event."""
        db: aiosqlite.Connection = self.app.state.db

        await db.execute(
            "UPDATE transactions SET status = ?, result = ?, update_count = update_count + 1, updated_at = ? WHERE id = ?",
            (status, json.dumps(result) if result else None, int(time.time()), self.id),
        )
        await db.commit()
        await self.publish_event()

    async def publish_event(self):
        """Publish an event to the event queue."""

        state = await self.render()
        for queue in self.app.state.event_queues.get(self.id, []):
            await queue.put(state)
        print(f"Published event for transaction {self.id}")

    async def io(self, method: str, payload: Dict[str, Any]) -> Any:
        """
        This helper suspends the action awaiting user input.
        Includes timeout handling and ability to resume from previous IO state.
        """
        db: aiosqlite.Connection = self.app.state.db
        now = int(time.time())

        # Increment the step count
        self.step_count += 1

        # Check if this step has been processed before, even partially.
        step_seen_before = False
        async with db.execute(
            "SELECT * FROM steps WHERE transaction_id = ? AND step_index = ?",
            (self.id, self.step_count),
        ) as cursor:
            row = await cursor.fetchone()
            if row is not None:
                step_seen_before = True
                if row["response"] is not None:
                    return json.loads(row["response"])

        # Insert the IO request into the steps table.
        request_data = dict(method=method, payload=payload)

        if not step_seen_before:
            await db.execute(
                """
                INSERT INTO steps (transaction_id, step_index, step_type, data, response, created_at, updated_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.id,
                    self.step_count,
                    "io",
                    json.dumps(request_data),
                    None,
                    now,
                    now,
                ),
            )
            await db.commit()

        # Create a Future and store it for later resolution.
        request_id = f"{self.id}-{self.step_count}"
        future = asyncio.get_event_loop().create_future()
        self.pending_io_request = {"future": future, "request_id": request_id}

        # Update the transaction state. If the transaction timed out earlier, the transaction
        # would be ina suspended state already.
        if not step_seen_before:
            await self.update(status=TransactionStatus.SUSPENDED.value)

        # Action waits here until response is set. If it times out, the transaction will be
        # removed from the active transactions list, and restored later when the response arrives.
        async with asyncio.timeout(TIMEOUT_IO_REQUEST):
            result = await future

        # Once resolved, update the IO request record.
        await db.execute(
            "UPDATE steps SET response = ?, updated_at = ? WHERE transaction_id = ? AND step_index = ?",
            (json.dumps(result), now, self.id, self.step_count),
        )
        await db.commit()

        # Update the transaction state.
        await self.update(status=TransactionStatus.RUNNING.value)

        # Clear the pending io future.
        self.pending_io_request = None

        return result

    async def run(self, action_slug: str):
        try:
            self.app.state.transactions_active[self.id] = self
            await self._initialize(action_slug)
            result = await self.app.state.actions[action_slug](self)
            await self.update(status=TransactionStatus.SUCCESS.value, result=result)
            print(f"Transaction {self.id} finished with result: {result}")
        except TimeoutError:
            print(f"Transaction {self.id} timed out while suspended on IO request.")
        except Exception as e:
            await self.update(status=TransactionStatus.ERROR.value, result=str(e))
            print(f"Transaction {self.id} errored: {e}")
        finally:
            self.app.state.transactions_active.pop(self.id, None)

    async def render(self):
        """Render the current state of the transaction as a JSON object, that the frontend can use to render the UI.
        Must cover all casese
        - if it isn't running yet, we want a start button
        - if it is running, probably a loading spinner
        - if it is suspended on an IO request, we want to display the IO request form for the user to respond to.
        - if it has finished with success/error, we want to display the final state.
        """

        async with self.app.state.db.execute(
            "SELECT status, result, update_count FROM transactions WHERE id = ?",
            (self.id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return {
                    "status": TransactionStatus.NOT_INITIALIZED.value,
                    "updateCount": 0,
                }

        if row["status"] == TransactionStatus.RUNNING.value:
            return {
                "status": TransactionStatus.RUNNING.value,
                "updateCount": row["update_count"],
            }
        elif row["status"] == TransactionStatus.SUSPENDED.value:
            async with self.app.state.db.execute(
                """
                SELECT step_index, data 
                FROM steps 
                WHERE transaction_id = ? AND response IS NULL 
                ORDER BY step_index DESC 
                LIMIT 1
                """,
                (self.id,),
            ) as cursor:
                step = await cursor.fetchone()
                if step is None:
                    raise ValueError(
                        "Transaction suspended but no pending IO request found"
                    )

                request_data = json.loads(step["data"])
                return {
                    "status": TransactionStatus.SUSPENDED.value,
                    "updateCount": row["update_count"],
                    "ioRequest": {
                        "id": f"{self.id}-{step['step_index']}",
                        "method": request_data["method"],
                        "payload": request_data["payload"],
                    },
                }
        elif row["status"] in [
            TransactionStatus.SUCCESS.value,
            TransactionStatus.ERROR.value,
        ]:
            return {
                "status": row["status"],
                "result": json.loads(row["result"]) if row["result"] else None,
                "updateCount": row["update_count"],
            }
        else:
            raise ValueError(f"Unknown transaction status: {row['status']}")


# ------------------------------
# Pydantic Models for RPC
# ------------------------------


class InvokeTransactionRequest(BaseModel):
    actionSlug: str


# ------------------------------
# APP FACTORY
# ------------------------------


def create_interval_app(
    *,
    static_dir: str,
    db_path: str = "database.db",
    actions: Dict[str, ActionFunction],
) -> FastAPI:
    """
    Factory function to create an Interval-style ASGI app.

    - db_path: Path to the SQLite database file.
    - actions: Dictionary mapping action slugs to async functions.
    - static_dir: Optional directory containing UI files to serve.
    """

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        # Open the database and initialize schema.
        app.state.db = await aiosqlite.connect(db_path)
        await init_db(app.state.db)
        print(f"Connected to SQLite DB at {db_path}")

        try:
            yield
        finally:
            # Cancel any pending IO futures
            for tx in app.state.transactions_active.values():
                if (
                    tx.pending_io_request is not None
                    and not tx.pending_io_request["future"].done()
                ):
                    tx.pending_io_request["future"].set_exception(
                        TimeoutError("Application shutdown")
                    )

            # Cleanup all event queues
            for tx_id in app.state.event_queues:
                for queue in app.state.event_queues[tx_id]:
                    queue.put_nowait(None)
            app.state.event_queues.clear()

            # Close the database connection
            if hasattr(app.state, "db"):
                await app.state.db.close()
                print("DB connection closed.")

    app = FastAPI(lifespan=lifespan)

    # Allow CORS (adjust as needed)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Place to store runtime state.
    # These are used only for pending (in-flight) transactions.
    app.state.db_path = db_path
    app.state.actions = actions
    app.state.transactions_active = {}
    app.state.event_queues = {}

    async def event_stream(tx_id: str):
        """Event stream for a transaction."""

        queue = asyncio.Queue()
        if tx_id not in app.state.event_queues:
            app.state.event_queues[tx_id] = []
        app.state.event_queues[tx_id].append(queue)

        try:
            # Send the initial state, and updates afterwards
            tx = Transaction(tx_id, app)
            state = await tx.render()
            yield f"data: {json.dumps(state)}\n\n"

            while True:
                state = await queue.get()
                if state is None:
                    break
                yield f"data: {json.dumps(state)}\n\n"
        finally:
            if tx_id in app.state.event_queues:
                app.state.event_queues[tx_id].remove(queue)

    async def run_transaction(tx_id: str, action_slug: str):
        try:
            tx = Transaction(tx_id, app)
            app.state.transactions_active[tx_id] = tx
            await tx.run(action_slug)
        except TimeoutError:
            print(f"Transaction {tx_id} timed out while suspended on IO request.")
        finally:
            app.state.transactions_active.pop(tx_id, None)

    # ------------------------------
    # API ENDPOINTS
    # ------------------------------

    @app.post("/api/transaction/invoke")
    async def invoke_transaction(req: InvokeTransactionRequest):
        print(f"Received invoke_transaction request: {req}")

        action_slug = req.actionSlug
        if action_slug not in app.state.actions:
            raise HTTPException(status_code=404, detail="Action not found")

        tx_id = str(uuid.uuid4())
        tx = Transaction(tx_id, app)

        if app.state.transactions_active.get(tx_id, None) is not None:
            raise HTTPException(status_code=400, detail="Transaction already exists")
        asyncio.create_task(tx.run(action_slug))
        print(f"Created transaction {tx_id}")

        return {"status": "ok", "transactionId": tx_id}

    @app.get("/api/transaction/{transaction_id}/events")
    async def subscribe_events(transaction_id: str):
        return StreamingResponse(
            event_stream(transaction_id), media_type="text/event-stream"
        )

    @app.post("/api/transaction/state")
    async def get_transaction_state(req: fastapi.Request):
        body = await req.json()
        print(f"Received get_transaction_state request: {body}")

        tx = Transaction(body["transactionId"], app)
        return await tx.render()

    @app.post("/api/transaction/io")
    async def process_io_response(req: fastapi.Request):
        body = await req.json()
        print(f"Received IO response: {body}")

        tx_id = body["transactionId"]

        # Get or restore transaction
        if tx_id not in app.state.transactions_active:
            print(f"Transaction {tx_id} not found, restoring from DB")

            async with app.state.db.execute(
                "SELECT action_slug FROM transactions WHERE id = ?", (tx_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Transaction not found")

            # Restart the transaction and wait for it to reach IO state
            tx = Transaction(tx_id, app)
            task = asyncio.create_task(tx.run(row["action_slug"]))

            # Wait for transaction to reach suspended state or complete
            while True:
                await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
                if (
                    app.state.transactions_active.get(tx_id, None)
                    and app.state.transactions_active[tx_id].pending_io_request
                ):
                    break
                if task.done():
                    raise HTTPException(
                        status_code=400,
                        detail="Transaction completed without reaching expected IO state",
                    )

        # Verify request ID and set result
        tx = app.state.transactions_active[tx_id]
        if tx.pending_io_request["request_id"] != body["requestId"]:
            raise HTTPException(status_code=400, detail="Request ID mismatch")

        tx.pending_io_request["future"].set_result(body["body"])
        return {"status": "ok"}

    @app.post("/api/transaction/list")
    async def list_transactions():
        # Return all transactions from the DB.
        db: aiosqlite.Connection = app.state.db
        cursor = await db.execute("SELECT * FROM transactions")
        rows = await cursor.fetchall()

        fields = ["id", "action_slug", "status", "result", "created_at", "updated_at"]
        return [{f: row[f] for f in fields} for row in rows]

    @app.post("/api/action/list")
    async def list_available_actions():
        return [{"slug": slug} for slug in app.state.actions.keys()]

    # ------------------------------
    # UI SERVING
    # ------------------------------

    # Mount the static files app at "/".
    assert os.path.isdir(static_dir)
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
    print(f"Serving UI from {static_dir}")

    return app


# ------------------------------
# MAIN ENTRY POINT (for testing)
# ------------------------------

if __name__ == "__main__":
    import uvicorn

    # Example action: a simple hello-world workflow.
    async def hello_world(tx: Transaction):
        first = await tx.io("INPUT_TEXT", {"label": "What is your first name?"})
        last = await tx.io("INPUT_TEXT", {"label": "What is your last name?"})
        greeting = f"Hello {first} {last}"
        return greeting

    async def add_numbers(tx: Transaction):
        total_numbers = await tx.io(
            "INPUT_TEXT", {"label": "How many numbers do you want to sum?"}
        )
        numbers = []
        for i in range(int(total_numbers)):
            number = await tx.io("INPUT_TEXT", {"label": f"Enter number {i + 1}"})
            numbers.append(int(number))
        return sum(numbers)

    # Register actions and initialize the app.
    my_actions = {
        "hello_world": hello_world,
        "add_numbers": add_numbers,
    }
    app = create_interval_app(
        db_path="interval.db",
        actions=my_actions,
        static_dir="/home/ubuntu/repos/interval-mini/py-sdk/ui",
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        timeout_keep_alive=2,
        timeout_graceful_shutdown=2,
    )
