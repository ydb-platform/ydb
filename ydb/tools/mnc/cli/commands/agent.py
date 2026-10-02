import asyncio
import logging
import os.path
import shlex

import rich
import yaml

from ydb.tools.mnc.lib import agent_client, common, deploy_ctx, progress, service, term, tools
from ydb.tools.mnc.scheme import agent


logger = logging.getLogger(__name__)

expected_config = agent.scheme


def _agent_bin_path():
    return os.path.join(deploy_ctx.git_ydb_root, "ydb", "tools", "mnc", "agent", "mnc_agent")


def _remote_agent_bin_path():
    return os.path.join(deploy_ctx.deploy_path, "mnc_agent", "bin", "mnc_agent")


def _remote_agent_cfg_path():
    return os.path.join(deploy_ctx.deploy_path, "mnc_agent", "cfg", "mnc_agent.yaml")


def _local_agent_cfg_path():
    return os.path.join(deploy_ctx.work_directory, "mnc_agent.yaml")


def _agent_run_command(config: dict, host: str):
    args = [_remote_agent_bin_path()]
    args.extend(["--config", _remote_agent_cfg_path()])
    args.extend(["--host", host])
    if config.get("port") is not None:
        args.extend(["--port", str(config["port"])])
    if config.get("mnc_home") is not None:
        args.extend(["--mnc-home", config["mnc_home"]])
    return " ".join(shlex.quote(arg) for arg in args)


async def prepare_host(host: str):
    commands = [
        f"mkdir -p {shlex.quote(deploy_ctx.deploy_path)}",
        f"mkdir -p {shlex.quote(os.path.join(deploy_ctx.deploy_path, 'mnc_agent'))}",
        f"mkdir -p {shlex.quote(os.path.join(deploy_ctx.deploy_path, 'mnc_agent', 'bin'))}",
        f"mkdir -p {shlex.quote(os.path.join(deploy_ctx.deploy_path, 'mnc_agent', 'cfg'))}",
        f"mkdir -p {shlex.quote(os.path.join(deploy_ctx.deploy_path, 'run'))}",
    ]
    result = await term.ssh_run(host, " && ".join(commands))
    if not result:
        return progress.TaskResult(
            level=progress.TaskResultLevel.ERROR,
            message=f"Failed to prepare agent directories on {host}",
        )
    return True


def make_prepare_host_step(host: str):
    return progress.Step(
        title=f"[yellow]{host}[/] [bold cyan]prepare agent dirs[/]",
        command=lambda parent_task, kv_storage: prepare_host(host),
    )


def make_prepare_hosts_step(hosts: list[str]):
    return progress.ParallelStepGroup(
        title="[bold blue]Prepare agent directories[/]",
        steps=[make_prepare_host_step(host) for host in hosts],
    )


def make_build_agent_step():
    return tools.make_build_mnc_agent_step(["-r"])


def make_deploy_agent_bin_step(hosts: list[str]):
    return progress.ParallelStepGroup(
        title="[bold blue]Deploy agent binary[/]",
        steps=[
            tools.make_rsync_step(_agent_bin_path(), host, _remote_agent_bin_path())
            for host in hosts
        ],
        inflight=4,
    )


async def write_agent_cfg(config: dict):
    with open(_local_agent_cfg_path(), "w") as file:
        yaml.safe_dump(config, file)
    return True


def make_write_agent_cfg_step(config: dict):
    return progress.SimpleStep(
        title="[bold blue]Write agent config[/]",
        action=lambda: write_agent_cfg(config),
    )


def make_deploy_agent_cfg_step(hosts: list[str]):
    return progress.ParallelStepGroup(
        title="[bold blue]Deploy agent config[/]",
        steps=[
            tools.make_rsync_step(_local_agent_cfg_path(), host, _remote_agent_cfg_path())
            for host in hosts
        ],
        inflight=4,
    )


async def stop_agent(host: str):
    return await service.cmd_custom_stop(host, "mnc_agent", "mnc_agent", force=True)


async def remove_agent(host: str):
    result = await term.ssh_run(host, f"rm -rf {shlex.quote(os.path.join(deploy_ctx.deploy_path, 'mnc_agent'))}")
    if not result:
        return progress.TaskResult(
            level=progress.TaskResultLevel.ERROR,
            message=f"Failed to remove agent files on {host}",
        )
    return True


def make_stop_agent_step(host: str):
    return progress.Step(
        title=f"[yellow]{host}[/] [bold cyan]stop agent[/]",
        command=lambda parent_task, kv_storage: stop_agent(host),
    )


def make_stop_agents_step(hosts: list[str]):
    return progress.ParallelStepGroup(
        title="[bold blue]Stop agents[/]",
        steps=[make_stop_agent_step(host) for host in hosts],
    )


def make_remove_agent_step(host: str):
    return progress.Step(
        title=f"[yellow]{host}[/] [bold cyan]remove agent files[/]",
        command=lambda parent_task, kv_storage: remove_agent(host),
    )


def make_remove_agents_step(hosts: list[str]):
    return progress.ParallelStepGroup(
        title="[bold blue]Remove agent files[/]",
        steps=[make_remove_agent_step(host) for host in hosts],
    )


async def start_agent(host: str, config: dict):
    return await service.cmd_custom_start(host, "mnc_agent", run_command=_agent_run_command(config, host))


def make_start_agent_step(host: str, config: dict):
    return progress.Step(
        title=f"[yellow]{host}[/] [bold cyan]start agent[/]",
        command=lambda parent_task, kv_storage: start_agent(host, config),
    )


def make_start_agents_step(hosts: list[str], config: dict):
    return progress.ParallelStepGroup(
        title="[bold blue]Start agents[/]",
        steps=[make_start_agent_step(host, config) for host in hosts],
    )


def make_waiting_step(waiting: int):
    async def sleep(task: progress.TaskNode):
        for _ in range(waiting):
            await asyncio.sleep(1)
            await task.update(advance=1)
        return True

    return progress.Step(
        title=f"[bold blue]Waiting[/] [yellow]{waiting}s[/]",
        command=lambda parent_task, kv_storage: sleep(parent_task),
        task_args={"total": waiting},
    )


def make_install_steps(hosts: list[str], config: dict, do_not_build: bool, do_not_start: bool, waiting: int):
    steps = []
    if not do_not_build:
        steps.append(make_build_agent_step())
    steps.extend([
        make_stop_agents_step(hosts),
        make_prepare_hosts_step(hosts),
        make_deploy_agent_bin_step(hosts),
        make_write_agent_cfg_step(config),
        make_deploy_agent_cfg_step(hosts),
    ])
    if not do_not_start:
        steps.extend([
            make_start_agents_step(hosts, config),
            make_waiting_step(waiting),
            agent_client.CheckAgentHealthOnHosts(hosts),
        ])
    return progress.SequentialStepGroup(title="[bold blue]Install agents[/]", steps=steps)


def make_uninstall_steps(hosts: list[str]):
    return progress.SequentialStepGroup(
        title="[bold blue]Uninstall agents[/]",
        steps=[
            make_stop_agents_step(hosts),
            make_remove_agents_step(hosts),
        ],
    )


def make_start_steps(hosts: list[str], config: dict, waiting: int):
    return progress.SequentialStepGroup(
        title="[bold blue]Start agents[/]",
        steps=[
            make_start_agents_step(hosts, config),
            make_waiting_step(waiting),
            agent_client.CheckAgentHealthOnHosts(hosts),
        ],
    )


def make_stop_steps(hosts: list[str]):
    return progress.SequentialStepGroup(
        title="[bold blue]Stop agents[/]",
        steps=[make_stop_agents_step(hosts)],
    )


def make_restart_steps(hosts: list[str], config: dict, waiting: int):
    return progress.SequentialStepGroup(
        title="[bold blue]Restart agents[/]",
        steps=[
            make_stop_agents_step(hosts),
            make_start_agents_step(hosts, config),
            make_waiting_step(waiting),
            agent_client.CheckAgentHealthOnHosts(hosts),
        ],
    )


async def run_agent_steps(steps, title: str, console=None):
    with progress.MyProgress(console=console) as pbar:
        result = await progress.run_steps([steps], progress=pbar, title=title)
    console.print(result.to_rich_panel())
    return result


async def act(hosts, config, do_not_build=False, do_not_start=False, waiting=2, console=None):
    install_steps = make_install_steps(hosts, config, do_not_build, do_not_start, waiting)
    return await run_agent_steps(install_steps, "[bold]Install agents[/]", console=console)


async def act_uninstall(hosts, console=None):
    return await run_agent_steps(make_uninstall_steps(hosts), "[bold]Uninstall agents[/]", console=console)


async def act_start(hosts, config, waiting=2, console=None):
    return await run_agent_steps(make_start_steps(hosts, config, waiting), "[bold]Start agents[/]", console=console)


async def act_stop(hosts, console=None):
    return await run_agent_steps(make_stop_steps(hosts), "[bold]Stop agents[/]", console=console)


async def act_restart(hosts, config, waiting=2, console=None):
    return await run_agent_steps(make_restart_steps(hosts, config, waiting), "[bold]Restart agents[/]", console=console)


def add_arguments(parser):
    subparsers = parser.add_subparsers(help="Commands", dest="cmd", required=True)

    install_parser = subparsers.add_parser("install")
    common.add_common_options(install_parser)
    install_parser.add_argument("--do-not-build", action="store_const", const=True, default=False)
    install_parser.add_argument("--do-not-start", action="store_const", const=True, default=False)
    install_parser.add_argument("--waiting", dest="waiting", type=int, default=2)

    uninstall_parser = subparsers.add_parser("uninstall")
    common.add_common_options(uninstall_parser)

    start_parser = subparsers.add_parser("start")
    common.add_common_options(start_parser)
    start_parser.add_argument("--waiting", dest="waiting", type=int, default=2)

    stop_parser = subparsers.add_parser("stop")
    common.add_common_options(stop_parser)

    restart_parser = subparsers.add_parser("restart")
    common.add_common_options(restart_parser)
    restart_parser.add_argument("--waiting", dest="waiting", type=int, default=2)


async def do_install(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    return await act(
        hosts,
        args.config,
        do_not_build=args.do_not_build,
        do_not_start=args.do_not_start,
        waiting=args.waiting,
        console=console,
    )


async def do_uninstall(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    return await act_uninstall(hosts, console=console)


async def do_start(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    return await act_start(hosts, args.config, waiting=args.waiting, console=console)


async def do_stop(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    return await act_stop(hosts, console=console)


async def do_restart(args):
    console = rich.console.Console()
    hosts = await common.get_machines(args.config)
    return await act_restart(hosts, args.config, waiting=args.waiting, console=console)


async def do(args):
    actions = {
        "install": do_install,
        "uninstall": do_uninstall,
        "start": do_start,
        "stop": do_stop,
        "restart": do_restart,
    }
    return await actions[args.cmd](args)
