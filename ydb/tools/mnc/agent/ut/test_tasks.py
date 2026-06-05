import asyncio
import unittest

from ydb.tools.mnc.agent.schemas.task import TaskResult
from ydb.tools.mnc.agent.services.tasks import TaskBasic, TaskService, TaskStatus


class SuccessfulTask(TaskBasic):
    async def do(self):
        return TaskResult(success=True, message="ok", data={"value": 1})


class FailingTask(TaskBasic):
    async def do(self):
        raise RuntimeError("boom")


class BlockingTask(TaskBasic):
    def __init__(self, event: asyncio.Event):
        super().__init__()
        self.event = event

    async def do(self):
        await self.event.wait()
        return TaskResult(success=True, message="unblocked", data=None)


class TaskServiceTest(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        if hasattr(self, "service"):
            await self.service.stop()

    async def test_add_task_runs_to_completion(self):
        self.service = TaskService(max_inflight=1)
        await self.service.start()

        task = SuccessfulTask()
        future = await self.service.add_task(task)
        completed = await future

        self.assertIs(completed, task)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.result, TaskResult(success=True, message="ok", data={"value": 1}))
        self.assertIs(self.service.get_task(task.task_id), task)

    async def test_failing_task_sets_failed_status_and_error(self):
        self.service = TaskService(max_inflight=1)
        await self.service.start()

        task = FailingTask()
        future = await self.service.add_task(task)

        with self.assertRaises(RuntimeError):
            await future

        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.error, "boom")

    async def test_cancel_pending_task(self):
        self.service = TaskService(max_inflight=1)

        task = SuccessfulTask()
        future = await self.service.add_task(task)

        self.assertTrue(self.service.cancel_task(task.task_id))
        await self.service.start()
        completed = await future

        self.assertIs(completed, task)
        self.assertEqual(task.status, TaskStatus.CANCELLED)
        self.assertFalse(task.result.success)

    async def test_stats_include_running_task(self):
        self.service = TaskService(max_inflight=1)
        await self.service.start()
        event = asyncio.Event()

        task = BlockingTask(event)
        future = await self.service.add_task(task)

        for _ in range(20):
            if task.status == TaskStatus.RUNNING:
                break
            await asyncio.sleep(0.01)

        stats = self.service.get_task_stats()
        self.assertEqual(stats.total, 1)
        self.assertEqual(stats.running, 1)
        self.assertEqual(stats.current_inflight, 1)

        event.set()
        await future

    async def test_start_and_stop_are_idempotent(self):
        self.service = TaskService(max_inflight=2)

        await self.service.start()
        await self.service.start()
        self.assertEqual(len(self.service.worker_tasks), 2)

        await self.service.stop()
        await self.service.stop()
        self.assertEqual(self.service.worker_tasks, [])

    async def test_delayed_task_is_scheduled_after_delay(self):
        self.service = TaskService(max_inflight=1)
        await self.service.start()

        task = SuccessfulTask()
        future = await self.service.add_task(task, delay=0.05)

        # Right after add_task the task is not yet queued, but is tracked.
        self.assertIs(self.service.get_task(task.task_id), task)

        completed = await asyncio.wait_for(future, timeout=2.0)
        self.assertIs(completed, task)
        self.assertEqual(task.status, TaskStatus.COMPLETED)

    async def test_cancel_non_pending_task_returns_false(self):
        self.service = TaskService(max_inflight=1)
        await self.service.start()
        event = asyncio.Event()

        task = BlockingTask(event)
        future = await self.service.add_task(task)

        for _ in range(50):
            if task.status == TaskStatus.RUNNING:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(task.status, TaskStatus.RUNNING)

        self.assertFalse(self.service.cancel_task(task.task_id))
        self.assertEqual(task.status, TaskStatus.RUNNING)

        event.set()
        await future

    async def test_cancel_unknown_task_returns_false(self):
        self.service = TaskService(max_inflight=1)
        self.assertFalse(self.service.cancel_task("does-not-exist"))

    async def test_get_all_tasks_returns_a_copy(self):
        self.service = TaskService(max_inflight=1)

        task = SuccessfulTask()
        await self.service.add_task(task)

        snapshot = self.service.get_all_tasks()
        self.assertIn(task.task_id, snapshot)

        snapshot.pop(task.task_id)
        self.assertIn(task.task_id, self.service.tasks)

    async def test_pending_queue_stats_before_worker_runs(self):
        self.service = TaskService(max_inflight=1)
        # Do not start workers — task remains pending in queue.
        task_a = SuccessfulTask()
        task_b = SuccessfulTask()
        await self.service.add_task(task_a)
        await self.service.add_task(task_b)

        stats = self.service.get_task_stats()
        self.assertEqual(stats.total, 2)
        self.assertEqual(stats.pending, 2)
        self.assertEqual(stats.running, 0)
        self.assertEqual(stats.queue_size, 2)
        self.assertEqual(stats.max_inflight, 1)
        self.assertEqual(stats.current_inflight, 0)
