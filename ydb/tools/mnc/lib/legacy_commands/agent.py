from . import deploy, service

from ydb.tools.mnc.lib import common, tools, deploy_ctx, term
import ydb.tools.mnc.scheme.agent as agent

import yaml
import os.path
import os
import asyncio


expected_config = agent.scheme


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    install_parser = subparsers.add_parser('install')
    common.add_common_options(install_parser)
    install_parser.add_argument('--ensure-running', action='store_true', help='Ensure agent is running')

    service_parser = subparsers.add_parser('service')
    common.add_common_options(service_parser)
    service_parser.add_argument('operation', choices=tuple(service.allowed_commands))


async def prepare_dir(config):
    for host in config['hosts']:
        await term.ssh_run(host, ' && '.join([
            f'mkdir -p {deploy_ctx.deploy_path}',
            f'mkdir -p {deploy_ctx.deploy_path}/mnc_agent',
            f'mkdir -p {deploy_ctx.deploy_path}/mnc_agent/bin',
            f'mkdir -p {deploy_ctx.deploy_path}/mnc_agent/cfg',
        ]))
    return True


async def deploy_bin(config):
    bin_path = os.path.join(deploy_ctx.arcadia_root, 'ydb', 'tools', 'mnc', 'agent', 'mnc_agent')
    result_path = os.path.join(deploy_ctx.deploy_path, 'mnc_agent', 'bin', 'mnc_agent')
    return await deploy.deploy_file(config['hosts'], bin_path, result_path)


async def deploy_cfg(config):
    config_yaml = yaml.safe_dump(config)
    temp_config_path = os.path.join(deploy_ctx.work_directory, 'mnc_agent.yaml')
    with open(temp_config_path, 'w') as file:
        print(config_yaml, file=file)
    result_path = os.path.join(deploy_ctx.deploy_path, 'mnc_agent', 'cfg', 'mnc_agent.yaml')
    return await deploy.deploy_file(config['hosts'], temp_config_path, result_path)


async def cmd_agent_start(host):
    return await service.cmd_custom_start(
        host,
        'mnc_agent',
        run_command=f'{deploy_ctx.deploy_path}/mnc_agent/bin/mnc_agent'
    )


async def cmd_agent_stop(host):
    return await service.cmd_custom_stop(host, 'mnc_agent', 'mnc_agent', force=True)


async def cmd_agent_restart(host):
    return await tools.chain_async(
        cmd_agent_stop(host),
        cmd_agent_start(host),
    )


custom_cmd_func_dict = {
    'start': cmd_agent_start,
    'stop': cmd_agent_stop,
    'restart': cmd_agent_restart,
}


async def act_install(config, ensure_running=False):
    has_agent = {host: await service.check_agent(host) for host in config['hosts']}
    for host in config['hosts']:
        if has_agent[host]:
            await cmd_agent_stop(host)
    result = tools.build_mnc_agent() and await tools.chain_async(
        prepare_dir(config),
        deploy_bin(config),
        deploy_cfg(config),
    )
    for host in config['hosts']:
        if has_agent[host] or ensure_running:
            await cmd_agent_start(host)
    if ensure_running:
        await asyncio.sleep(1)
        for host in config['hosts']:
            is_healthy = False
            for _ in range(10):
                is_healthy = await service.check_agent(host)
                if is_healthy:
                    break
                await asyncio.sleep(0.5)
            print(f'{host}: {"RUNNING" if is_healthy else "FAILED"}')
            if not is_healthy and not has_agent[host]:
                result = False
    return result


async def act_service(operation, config):
    for host in config['hosts']:
        if not await custom_cmd_func_dict[operation](host):
            return False
    return True


async def do_install(args):
    if await act_install(args.config, args.ensure_running):
        print('success')
    else:
        print('failed')


async def do_service(args):
    if await act_service(args.operation, args.config):
        print('success')
    else:
        print('failed')


async def do(args):
    if args.cmd == 'install':
        await do_install(args)
    if args.cmd == 'service':
        await do_service(args)
