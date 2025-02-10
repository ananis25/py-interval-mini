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
            created_at REAL DEFAULT (unixepoch()),
            updated_at REAL DEFAULT (unixepoch())
        )
    """)

    # Create a table for all events in a transaction
    # data is a JSON string of event data. For IO requests, this is the request body. If it been responded to, the response is put under the "response" key.
    await db.execute("""
        CREATE TABLE IF NOT EXISTS events (
            transaction_id TEXT,
            event_index INTEGER,
            event_type TEXT,
            data TEXT,
            response TEXT,
            created_at REAL DEFAULT (unixepoch()),
            updated_at REAL DEFAULT (unixepoch()),
            FOREIGN KEY(transaction_id) REFERENCES transactions(id)
            PRIMARY KEY(transaction_id, event_index)
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
        self.id = transaction_id
        self.app = app

    async def start(self, action_slug: str):
        """
        Start the transaction.
        """
        db: aiosqlite.Connection = self.app.state.db
        await db.execute(
            "INSERT INTO transactions (id, action_slug, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (
                self.id,
                action_slug,
                TransactionStatus.RUNNING.value,
                int(time.time()),
                int(time.time()),
            ),
        )
        await db.commit()

    async def update(self, status: str, result: Optional[Any] = None):
        """Update the transactions table for this transaction and publish an event."""
        db: aiosqlite.Connection = self.app.state.db

        await db.execute(
            "UPDATE transactions SET status = ?, result = ?, updated_at = ? WHERE id = ?",
            (status, json.dumps(result) if result else None, int(time.time()), self.id),
        )
        await db.commit()

    async def io(self, method: str, payload: Dict[str, Any]) -> Any:
        """
        This helper suspends the action awaiting user input.
        Includes timeout handling and ability to resume from previous IO state.
        """
        db: aiosqlite.Connection = self.app.state.db
        now = int(time.time())

        # Get the last event index.
        async with db.execute(
            "SELECT event_index FROM events WHERE transaction_id = ? ORDER BY event_index DESC LIMIT 1",
            (self.id,),
        ) as cursor:
            row = await cursor.fetchone()
            last_index = row["event_index"] if row else 0
        event_index = last_index + 1

        # Insert the IO request into the events table.
        request_data = dict(method=method, payload=payload)
        async with db.execute(
            "INSERT INTO events (transaction_id, event_index, event_type, data, response) VALUES (?, ?, ?, ?, ?)",
            (self.id, event_index, "io", json.dumps(request_data), None),
        ) as cursor:
            await db.commit()

        request_id = f"{self.id}-{event_index}"

        # Create a Future and store it for later resolution.
        future = asyncio.get_event_loop().create_future()
        self.app.state.io_futures[self.id] = {
            "future": future,
            "request_id": request_id,
            "request_data": request_data,
        }

        # Update the transaction state.
        await self.update(status=TransactionStatus.SUSPENDED.value)
        await self.app.state.event_queues[self.id].put(await self.render())

        result = await future  # Action waits here until response is set.

        # Once resolved, update the IO request record.
        await db.execute(
            "UPDATE events SET response = ?, updated_at = ? WHERE transaction_id = ? AND event_index = ?",
            (json.dumps(result), now, self.id, event_index),
        )
        await db.commit()

        # Update the transaction state.
        await self.update(status=TransactionStatus.RUNNING.value)
        await self.app.state.event_queues[self.id].put(await self.render())

        # Clear the pending io future.
        if self.id in self.app.state.io_futures:
            del self.app.state.io_futures[self.id]

        return result

    async def render(self):
        """Render the current state of the transaction as a JSON object, that the frontend can use to render the UI.
        Must cover all casese
        - if it isn't running yet, we want a start button
        - if it is running, probably a loading spinner
        - if it is suspended on an IO request, we want to display the IO request form for the user to respond to.
        - if it has finished with success/error, we want to display the final state.
        """

        async with self.app.state.db.execute(
            "SELECT status, result FROM transactions WHERE id = ?",
            (self.id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return {"status": TransactionStatus.NOT_INITIALIZED.value}

        if row["status"] == TransactionStatus.RUNNING.value:
            return {"status": TransactionStatus.RUNNING.value}
        elif row["status"] == TransactionStatus.SUSPENDED.value:
            # Get the pending IO request from memory
            assert self.id in self.app.state.io_futures
            io_future = self.app.state.io_futures[self.id]

            return {
                "status": TransactionStatus.SUSPENDED.value,
                "ioRequest": {
                    "id": io_future["request_id"],
                    "method": io_future["request_data"]["method"],
                    "payload": io_future["request_data"]["payload"],
                },
            }
        elif row["status"] in [
            TransactionStatus.SUCCESS.value,
            TransactionStatus.ERROR.value,
        ]:
            return {
                "status": row["status"],
                "result": json.loads(row["result"]) if row["result"] else None,
            }
        else:
            raise ValueError(f"Unknown transaction status: {row['status']}")


# ------------------------------
# Pydantic Models for RPC
# ------------------------------


class InvokeTransactionRequest(BaseModel):
    actionSlug: str
    transactionId: Optional[str] = None


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
            # Cleanup queues and futures
            for queue in app.state.event_queues.values():
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

            # Cancel any pending IO futures
            for future_data in app.state.io_futures.values():
                if not future_data["future"].done():
                    future_data["future"].cancel()

            # Clear the state
            app.state.event_queues.clear()
            app.state.io_futures.clear()

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
    app.state.io_futures = {}
    app.state.event_queues = {}

    async def event_stream(tx_id: str):
        """Event stream for a transaction."""
        if tx_id not in app.state.event_queues:
            app.state.event_queues[tx_id] = asyncio.Queue()
        queue: asyncio.Queue = app.state.event_queues[tx_id]

        # Optionally, send an initial state.
        tx = Transaction(tx_id, app)
        state = await tx.render()
        yield f"data: {json.dumps(state)}\n\n"
        while True:
            # TODO: Make sure all updates go through the queue.
            state = await queue.get()
            yield f"data: {json.dumps(state)}\n\n"

    # ------------------------------
    # API ENDPOINTS
    # ------------------------------

    @app.post("/api/transaction/invoke")
    async def invoke_transaction(req: InvokeTransactionRequest):
        print(f"Received invoke_transaction request: {req}")

        action_slug = req.actionSlug
        if action_slug not in app.state.actions:
            raise HTTPException(status_code=404, detail="Action not found")

        if req.transactionId is not None:
            # Check if transaction already exists. If so, just return already.
            tx_id = req.transactionId
            tx = Transaction(tx_id, app)
            existing_state = await tx.render()
            if existing_state["status"] != TransactionStatus.NOT_INITIALIZED.value:
                return {"status": "ok"}
        else:
            tx_id = str(uuid.uuid4())

        # Transaction doesn't already exist, create a new one.
        tx = Transaction(tx_id, app)
        await tx.start(action_slug)
        await app.state.event_queues[tx_id].put(await tx.render())

        # Run the action function in the background
        async def run_action():
            try:
                result = await app.state.actions[action_slug](tx)
                await tx.update(status=TransactionStatus.SUCCESS.value, result=result)
                print(f"Transaction {tx_id} finished with result: {result}")
            except Exception as e:
                await tx.update(status=TransactionStatus.ERROR.value, result=str(e))
                print(f"Transaction {tx_id} errored: {e}")
            await app.state.event_queues[tx_id].put(await tx.render())

        asyncio.create_task(run_action())
        print(f"Created transaction {tx_id}")
        return {"status": "ok"}

    @app.get("/api/transaction/{transaction_id}/events")
    async def get_events(transaction_id: str):
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
    async def respond_to_io_request(req: fastapi.Request):
        body = await req.json()
        print(f"Received IO request: {body}")

        tx_id = body["transactionId"]
        if tx_id not in app.state.io_futures:
            raise HTTPException(status_code=400, detail="No pending IO request")

        future_data = app.state.io_futures[tx_id]
        if future_data["request_id"] != body["requestId"]:
            raise HTTPException(status_code=400, detail="Request ID mismatch")

        future_data["future"].set_result(body["body"])
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

    # Register actions.
    my_actions = {
        "hello_world": hello_world,
        "add_numbers": add_numbers,
    }

    my_static_dir = "/home/ubuntu/repos/interval-mini/py-sdk/ui"

    app = create_interval_app(
        db_path="interval.db", actions=my_actions, static_dir=my_static_dir
    )

    # Update the uvicorn run config to handle signals properly
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        timeout_keep_alive=2,
        timeout_graceful_shutdown=5,
    )
