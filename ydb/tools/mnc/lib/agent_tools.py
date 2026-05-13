import logging
import aiohttp
from ydb.tools.mnc.lib import progress


logger = logging.getLogger(__name__)


class CheckAgentHealthOnHost(progress.SimpleStep):
    def __init__(self, host: str):
        super().__init__(
            title=f'[yellow]{host} [bold cyan]check agent health',
        )
        self.host = host

    async def action(self):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f'http://{self.host}:8999/health') as response:
                    return response.status == 200
            except Exception as e:
                return progress.TaskResult(progress.TaskResultStatus.ERROR, f'Failed to check agent on {self.host}', exception=e)


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
