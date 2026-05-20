import logging
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
