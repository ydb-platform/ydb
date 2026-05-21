import json
import unittest
from unittest import mock

from aiohttp import web

from ydb.tools.mnc.agent.api import errors as errors_api
from ydb.tools.mnc.agent.api import tasks as tasks_api
from ydb.tools.mnc.agent.schemas.task import TaskStatsSchema
from ydb.tools.mnc.agent.services.tasks import TaskBasic, TaskStatus


class FakeRequest:
    def __init__(self, match_info=None):
        self.match_info = match_info or {}


class DummyTask(TaskBasic):
    async def do(self):
        return None


class ApiTasksTest(unittest.IsolatedAsyncioTestCase):
    def response_json(self, response):
        return json.loads(response.text)

    async def test_get_tasks_returns_all_tasks_and_stats(self):
        task = DummyTask(task_id="t1")
        task.status = TaskStatus.COMPLETED
        stats = TaskStatsSchema(
            total=1,
            pending=0,
            running=0,
            completed=1,
            failed=0,
            cancelled=0,
            queue_size=0,
            max_inflight=1,
            current_inflight=0,
        )

        with mock.patch.object(tasks_api.task_service, "get_all_tasks", return_value={"t1": task}), \
                mock.patch.object(tasks_api.task_service, "get_task_stats", return_value=stats):
            response = await tasks_api.get_tasks(FakeRequest())

        body = self.response_json(response)
        self.assertEqual(body["stats"]["total"], 1)
        self.assertEqual(len(body["tasks"]), 1)
        self.assertEqual(body["tasks"][0]["id"], "t1")
        self.assertEqual(body["tasks"][0]["status"], "completed")

    async def test_get_task_returns_schema_for_existing_task(self):
        task = DummyTask(task_id="t1")
        task.status = TaskStatus.RUNNING

        with mock.patch.object(tasks_api.task_service, "get_task", return_value=task):
            response = await tasks_api.get_task(FakeRequest(match_info={"task_id": "t1"}))

        body = self.response_json(response)
        self.assertEqual(body["id"], "t1")
        self.assertEqual(body["status"], "running")

    async def test_get_task_raises_404_for_unknown(self):
        with mock.patch.object(tasks_api.task_service, "get_task", return_value=None):
            with self.assertRaises(web.HTTPNotFound):
                await tasks_api.get_task(FakeRequest(match_info={"task_id": "missing"}))

    async def test_get_task_stats_returns_stats(self):
        stats = TaskStatsSchema(
            total=3,
            pending=1,
            running=1,
            completed=1,
            failed=0,
            cancelled=0,
            queue_size=1,
            max_inflight=4,
            current_inflight=1,
        )

        with mock.patch.object(tasks_api.task_service, "get_task_stats", return_value=stats):
            response = await tasks_api.get_task_stats(FakeRequest())

        self.assertEqual(self.response_json(response)["total"], 3)
        self.assertEqual(self.response_json(response)["max_inflight"], 4)


class ApiErrorMiddlewareTest(unittest.IsolatedAsyncioTestCase):
    def response_json(self, response):
        return json.loads(response.text)

    async def test_not_found_handler_returns_404_json(self):
        response = await errors_api.not_found_handler(FakeRequest())
        self.assertEqual(response.status, 404)
        body = self.response_json(response)
        self.assertEqual(body["error"], "Not found")

    async def test_internal_error_handler_returns_500_json(self):
        response = await errors_api.internal_error_handler(FakeRequest(), RuntimeError("boom"))
        self.assertEqual(response.status, 500)
        body = self.response_json(response)
        self.assertEqual(body["error"], "Internal server error")

    async def test_error_middleware_converts_unexpected_exceptions(self):
        async def handler(request):
            raise RuntimeError("boom")

        response = await errors_api.error_middleware(FakeRequest(), handler)
        self.assertEqual(response.status, 500)

    async def test_error_middleware_converts_http_not_found(self):
        async def handler(request):
            raise web.HTTPNotFound(text="Task not found")

        response = await errors_api.error_middleware(FakeRequest(), handler)
        self.assertEqual(response.status, 404)
        body = self.response_json(response)
        self.assertEqual(body["error"], "Not found")

    async def test_error_middleware_passes_through_other_http_exceptions(self):
        async def handler(request):
            raise web.HTTPBadRequest(text="bad")

        with self.assertRaises(web.HTTPBadRequest):
            await errors_api.error_middleware(FakeRequest(), handler)

    async def test_error_middleware_passes_through_success(self):
        async def handler(request):
            return web.json_response({"ok": True})

        response = await errors_api.error_middleware(FakeRequest(), handler)
        self.assertEqual(self.response_json(response), {"ok": True})
