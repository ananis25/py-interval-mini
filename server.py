import asyncio
import json
import uuid
from typing import Any, Dict

import fastapi
import httpx
import restate
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from pydantic import BaseModel
from restate.context import WorkflowContext, WorkflowSharedContext
from restate.workflow import Workflow

RESTATE_SERVER_URL = "http://localhost:8080"
EVENT_PUBLISH_QUEUE = asyncio.Queue(maxsize=50)

# ------------------------------
# Action definitions
# ------------------------------


class Action:
    def __init__(self, ctx: WorkflowContext):
        self.ctx = ctx
        self.event_counter = 0

    @classmethod
    def name(cls) -> str:
        return cls.__name__.lower()

    async def publish_event(self):
        """
        Publish an event to the message queue, exactly once.
        """
        payload = {
            "data": await self.ctx.get("render"),
            "workflow_id": self.ctx.key(),
            "workflow_name": self.name(),
        }

        async def _publish():
            logger.info(f"Publishing event: {payload}")
            await EVENT_PUBLISH_QUEUE.put(payload)

        await self.ctx.run("publish-event", _publish)

    async def io(self, method: str, payload: Dict[str, Any]) -> str:
        """Suspend the workflow for a user input."""
        self.event_counter += 1
        key = f"io-{self.event_counter}"
        promise = self.ctx.promise(key)
        self.ctx.set(
            "render",
            {
                "type": "io",
                "data": {"id": key, "method": method, "payload": payload},
            },
        )
        await self.publish_event()
        return await promise.value()

    @classmethod
    def workflow(cls) -> Workflow:
        wf = Workflow(name=cls.name())

        @wf.main(name="run")
        async def _run(ctx: WorkflowContext):
            action = cls(ctx)

            ctx.set(
                "render",
                {"type": "text", "data": "status: started"},
            )
            await action.publish_event()

            try:
                result = await action.run()
                ctx.set(
                    "render",
                    {"type": "text", "data": f"status: success\nresult: {result}"},
                )
            except Exception as e:
                ctx.set(
                    "render",
                    {"type": "text", "data": f"status: error\nerror: {e}"},
                )
                await action.publish_event()
                raise
            await action.publish_event()

            return result

        @wf.handler(name="render")
        async def _render(ctx: WorkflowSharedContext):
            return await ctx.get("render")

        @wf.handler(name="io")
        async def _io(ctx: WorkflowSharedContext, payload: Dict[str, Any]):
            # TODO: Add validation and reject the promise if the data is invalid
            return await ctx.promise(payload["key"]).resolve(payload["data"])

        return wf

    async def run(self):
        raise NotImplementedError()


class HelloWorld(Action):
    async def run(self):
        first = await self.io("INPUT_TEXT", {"label": "What is your first name?"})
        last = await self.io("INPUT_TEXT", {"label": "What is your last name?"})
        return f"Hello {first} {last}"


class AddNumbers(Action):
    async def run(self):
        total_numbers = await self.io(
            "INPUT_TEXT", {"label": "How many numbers do you want to sum?"}
        )
        numbers = []
        for i in range(int(total_numbers)):
            number = await self.io("INPUT_TEXT", {"label": f"Enter number {i + 1}"})
            numbers.append(int(number))
        return sum(numbers)


ALL_WORKFLOWS = [HelloWorld.workflow(), AddNumbers.workflow()]


# ------------------------------
# FastAPI App
# ------------------------------


def create_restate_app():
    return restate.endpoint.app(ALL_WORKFLOWS)


def create_interval_app(*, static_dir: str) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.event_queues = {}

    class ActionInvokeRequest(BaseModel):
        actionSlug: str

    # ------------------------------
    # API endpoints
    # ------------------------------

    @app.get("/test")
    async def test():
        return {"status": "ok"}

    @app.post("/transaction/invoke")
    async def invoke_transaction(req: ActionInvokeRequest):
        """
        Invoke a new transaction for the given action.
        """
        workflow_name = req.actionSlug
        if workflow_name not in [wf.name for wf in ALL_WORKFLOWS]:
            raise fastapi.HTTPException(status_code=404, detail="Action not found")

        workflow_id = str(uuid.uuid4())
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{RESTATE_SERVER_URL}/{workflow_name}/{workflow_id}/run/send",
                    json={},
                )
                logger.info(f"Response status: {response.status_code}")
                if response.is_success:
                    return {"status": "ok", "transactionId": workflow_id}
                else:
                    response.raise_for_status()

        except Exception as e:
            logger.error(f"Error: {e}")
            raise fastapi.HTTPException(
                status_code=500, detail="Workflow invocation failed"
            )

    @app.post("/transaction/io")
    async def process_io_response(req: fastapi.Request):
        """
        Process an IO response from the given transaction.
        """
        body = await req.json()

        workflow_name = body["actionSlug"]
        workflow_id = body["transactionId"]

        # Resolve the promise with the IO response
        async with httpx.AsyncClient() as client:
            _response = await client.post(
                f"{RESTATE_SERVER_URL}/{workflow_name}/{workflow_id}/io",
                json={"key": body["requestId"], "data": body["body"]},
            )
            return {"status": "ok"}

    @app.post("/transaction/state")
    async def get_transaction_state(req: fastapi.Request):
        """
        Get the current render state of the given transaction.
        """
        body = await req.json()
        logger.info(f"Received get_transaction_state request: {body}")

        workflow_name = body["actionSlug"]
        workflow_id = body["transactionId"]

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{RESTATE_SERVER_URL}/{workflow_name}/{workflow_id}/render"
            )
            return response.json()

    @app.get("/transaction/events/{actionSlug}/{transactionId}")
    async def subscribe_events(actionSlug: str, transactionId: str):
        """
        Set up an SSE stream to listen for events on the given transaction.
        """

        async def event_stream():
            workflow_name = actionSlug
            workflow_id = transactionId

            queue = asyncio.Queue()
            if workflow_id not in app.state.event_queues:
                app.state.event_queues[workflow_id] = []
            app.state.event_queues[workflow_id].append(queue)

            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        f"{RESTATE_SERVER_URL}/{workflow_name}/{workflow_id}/render"
                    )
                    data = response.json()
                    yield f"data: {json.dumps(data)}\n\n"

                while True:
                    event = await queue.get()
                    yield f"data: {json.dumps(event['data'])}\n\n"

            finally:
                if workflow_id in app.state.event_queues:
                    app.state.event_queues[workflow_id].remove(queue)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.post("/transaction/list")
    async def list_transactions():
        """
        List all transactions.
        """
        process = await asyncio.create_subprocess_shell(
            """
            restate sql "select * from sys_invocation where target_service_ty='workflow'" --json
            """,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        # Find the JSON lines in the output (skip header line)
        rows = []
        for line in stdout.decode().split("\n"):
            if line.startswith("["):
                rows.extend(json.loads(line.strip()))
        return [
            {
                "id": row["target_service_key"],
                "action_slug": row["target_service_name"],
                "status": row["status"],
                "result": row.get("completion_result", None),
                "created_at": row["created_at"],
                "updated_at": row["modified_at"],
            }
            for row in rows
        ]

    @app.post("/action/list")
    async def list_available_actions():
        """
        List all available actions.
        """
        return [{"slug": slug.name} for slug in ALL_WORKFLOWS]

    return app


if __name__ == "__main__":
    import hypercorn.asyncio
    from hypercorn.config import Config
    from hypercorn.middleware import DispatcherMiddleware

    # Create all the apps
    restate_app = create_restate_app()
    server_app = create_interval_app(static_dir="ui")
    static_app = StaticFiles(directory="ui", html=True)

    app = DispatcherMiddleware(
        {
            "/api": server_app,
            "/worker": restate_app,
            "/": static_app,
        }  # type: ignore
    )

    async def events_consumer(server_app):
        """
        FIXME: Hack to get around Hypercorn not supporting the lifespan method.
        """
        logger.info("Starting event consumer")
        while True:
            event = await EVENT_PUBLISH_QUEUE.get()
            logger.info(f"Consuming event: {event}")
            for subscriber in server_app.state.event_queues.get(
                event["workflow_id"], []
            ):
                await subscriber.put(event)

    # Run the combined app with Hypercorn
    async def run_app():
        config = Config()
        config.bind = ["localhost:8000"]

        consumer_task = asyncio.create_task(events_consumer(server_app))
        try:
            await hypercorn.asyncio.serve(app, config)
        finally:
            logger.info("Cancelling event consumer")
            consumer_task.cancel()

    asyncio.run(run_app())
