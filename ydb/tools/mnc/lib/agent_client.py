import logging
import asyncio
from typing import Optional

import aiohttp

from ydb.tools.mnc.lib import progress


logger = logging.getLogger(__name__)


async def _request_json(method: str, host: str, path: str, payload: dict = None, port: int = 8999) -> Optional[dict]:
    url = f"http://{host}:{port}{path}"
    async with aiohttp.ClientSession() as session:
        try:
            request = session.post(url, json=payload) if method == "POST" else session.get(url)
            async with request as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"agent request failed; method: {method} host: {host} path: {path} status: {response.status} body: {text}")
                    return None
                return await response.json()
        except Exception as e:
            logger.error(f"agent request failed; method: {method} host: {host} path: {path} error: {e}")
            return None


async def _get_json(host: str, path: str, port: int = 8999) -> Optional[dict]:
    return await _request_json("GET", host, path, port=port)


async def _post_json(host: str, path: str, payload: dict, port: int = 8999) -> Optional[dict]:
    return await _request_json("POST", host, path, payload, port)


class PostJsonStep(progress.SimpleStep):
    def __init__(self, host: str, path: str, payload: dict, port: int = 8999):
        super().__init__(
            title=f'[yellow]{host}[/] [bold cyan]POST[/] [green]{path}[/]',
        )
        self.host = host
        self.path = path
        self.payload = payload
        self.port = port
        self.response = None

    async def action(self):
        self.response = await _post_json(self.host, self.path, self.payload, self.port)
        return self.response is not None


async def post_json(host: str, path: str, payload: dict, port: int = 8999, parent_task: progress.TaskNode = None) -> Optional[dict]:
    if parent_task is None:
        return await _post_json(host, path, payload, port)
    step = PostJsonStep(host, path, payload, port)
    result = await step.run(parent_task)
    if not result:
        return None
    return step.response


async def get_json(host: str, path: str, port: int = 8999) -> Optional[dict]:
    return await _get_json(host, path, port)


async def wait_task(host: str, task_id: str, port: int = 8999, poll_interval: float = 1.0) -> Optional[dict]:
    while True:
        task = await _get_json(host, f"/tasks/{task_id}", port)
        if task is None:
            return None
        status = task.get("status")
        if status == "completed":
            result = task.get("result") or {}
            return result.get("data")
        if status in {"failed", "cancelled"}:
            logger.error(f"agent task failed; host: {host} task_id: {task_id} status: {status} error: {task.get('error')}")
            return None
        await asyncio.sleep(poll_interval)


async def post_json_and_wait(
    host: str,
    path: str,
    payload: dict,
    port: int = 8999,
    parent_task: progress.TaskNode = None,
) -> Optional[dict]:
    response = await post_json(host, path, payload, port=port, parent_task=parent_task)
    if response is None:
        return None
    task_id = response.get("task_id")
    if not task_id:
        logger.error(f"agent did not return task_id; host: {host} path: {path}")
        return None
    return await wait_task(host, task_id, port=port)


class CheckAgentHealthOnHost(progress.SimpleStep):
    def __init__(self, host: str):
        super().__init__(
            title=f'[yellow]{host} [bold cyan]check agent health',
        )
        self.host = host

    async def action(self):
        response = await _get_json(self.host, "/health")
        if response is None:
            return False
        features = response.get('enabled_features', [])
        missing_features = {'nodes', 'disks'} - set(features)
        if missing_features:
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                message=f'Agent on {self.host} does not support features: {", ".join(sorted(missing_features))}'
            )
        return True


class CheckAgentHealthOnHosts(progress.ParallelStepGroup):
    def __init__(self, hosts: list[str]):
        super().__init__(
            title="[bold blue]Check agents on hosts",
            steps=[CheckAgentHealthOnHost(host) for host in hosts],
        )


@progress.with_parent_task
async def check_agents_on_hosts(hosts, parent_task: progress.TaskNode = None):
    step = CheckAgentHealthOnHosts(hosts)
    return await step.run(parent_task)
