import sys
import logging
import rich

from ydb.tools.mnc.lib import common, deploy_ctx, term, tools, progress
from ydb.tools.mnc.lib.draft import tools as draft_tools
from . import configs, deploy, disks, service, init
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)


expected_config = multinode.scheme


def title(text):
    print('\033[1;35m', text, '\033[0m\n', sep='')


async def build_dependencies(config: dict):
    steps = []
    if config['ydb_config_type'] == 'v1':
        build_kikimr_configure_step = draft_tools.make_build_cfg_step(config['build_args'])
        steps.append(build_kikimr_configure_step)

    if not deploy_ctx.is_manual_path_to_bin:
        build_kikimr_step = draft_tools.make_build_kikimr_step(config['build_args'])
        steps.append(build_kikimr_step)
        if deploy_ctx.do_strip:
            strip_kikimr_step = draft_tools.make_strip_kikimr_step()
            steps.append(strip_kikimr_step)
    result = await progress.run_steps(steps, title='Build dependencies')
    if not result:
        rich.print(result)
    return bool(result)


async def generate_cfg(hosts, config: dict):
    return await tools.chain_async(
        term.shell(
            f'mkdir -p {deploy_ctx.work_directory}/static'
            f' && mkdir -p {deploy_ctx.work_directory}/dynamic'
            f' && mkdir -p {deploy_ctx.work_directory}/nbs'
            f' && mkdir -p {deploy_ctx.work_directory}/nbs/cfg'
        ),
        term.shell(f'rm -rf {deploy_ctx.work_directory}/static/* && rm -rf {deploy_ctx.work_directory}/dynamic/* && rm -rf {deploy_ctx.work_directory}/nbs/cfg/*'),
        configs.act_generate(hosts, config),
    )


async def promote(
    hosts,
    config,
    without_test_install=False,
    waiting=60,
    bin_path=None,
    do_not_init=False,
    ignore_failed_stop=False,
):
    title('Full cycle multinode cluser preparation start')

    if bin_path is not None:
        deploy_ctx.update_path_to_bin(bin_path)

    if deploy_ctx.do_rebuild:
        title('Build dependency')
        if not await build_dependencies(config):
            logger.error('fail to build dependencies')
            return False
    title('Stop previous processes')
    await service.act_hosts_original('stop', hosts)
    ok = await service.act_hosts('stop', hosts)
    if not ok and not ignore_failed_stop:
        logger.error('Failed to stop processes')
        return False
    title('Uninstall previous multinode')
    if not await deploy.act_uninstall(hosts):
        print('Error during unintalling', file=sys.stderr)
        return

    if deploy_ctx.do_redeploy_bin:
        title('Deploy binary')
        if not await deploy.act_update_bin(hosts, config):
            print('Failed to update bin', file=sys.stderr)
            return False

    if config['sector_map']['use'] != 'always':
        title('Split disks')
        if not await disks.act_split(hosts, config, part_size=common.Memory('{0}GB'.format(config['disk_size']))):
            print('Failed to split', file=sys.stderr)
            return False
        title('Format disks')
        if not await disks.act_obliterate(hosts, config):
            print('Failed to format', file=sys.stderr)
            return False
    title('Generate configs')
    if not await generate_cfg(hosts, config):
        print('Failed to generate configs', file=sys.stderr)
        return False
    title('Install multinode')
    if not await deploy.act_install(hosts, config):
        print('Failed to install', file=sys.stderr)
        return False
    title('Start processes')
    await service.act_hosts('start', hosts, 'static')
    # await affinity.act(hosts)
    title(f'Wait {waiting} seconds')
    await term.shell(f'sleep {waiting}')
    if do_not_init:
        return True
    title('Init kikimr')

    if config['ydb_config_type'] == 'v1':
        ok = await tools.chain_async(
            term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_storage.bash; {deploy_ctx.work_directory}/dynamic/init_storage.bash'),
            term.shell(f'rm {deploy_ctx.work_directory}/dynamic/Configure-{config["domain"]["name"]}.txt', silent_error=True),
            term.shell(
                f'cp {deploy_ctx.work_directory}/special_dynamic/Configure-domain.txt {deploy_ctx.work_directory}/dynamic/Configure-{config["domain"]["name"]}.txt'
            ),
            term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_cms.bash && {deploy_ctx.work_directory}/dynamic/init_cms.bash'),
            term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_compute.bash && {deploy_ctx.work_directory}/dynamic/init_compute.bash'),
        )
    else:
        ok = await init.act_static(config)

    if not ok:
        logger.error('Failed to init storage')
        return False

    if config['domain'] is not None:
        title("Wait 5 seconds")
        await term.shell('sleep 5')
        title('Init dynamic')
        if config['ydb_config_type'] == 'v1':
            ok = await tools.chain_async(
                term.shell(f'chmod +x {deploy_ctx.work_directory}/dynamic/init_databases.bash && {deploy_ctx.work_directory}/dynamic/init_databases.bash'),
                service.act_hosts('start', hosts, 'dynamic'),
            )
        else:
            ok = await tools.chain_async(
                init.act_dynamic(config),
                service.act_hosts('start', hosts, 'dynamic'),
            )
        if not ok:
            logger.error('Failed to init storage')
            return False

    title('\033[0;32mDone')
    return True


async def demote(hosts, config, ignore_failed_stop=False):
    title('Stop previous processes')
    ok = await service.act_hosts('stop', hosts)
    if not ok and not ignore_failed_stop:
        logger.error('Failed to stop processes')
        return False
    title('Uninstall multinode')
    ok = await deploy.act_uninstall(hosts)
    if not ok:
        logger.error('Failed to uninstall multinode')
        return False
    if config['sector_map']['use'] != 'always':
        title('Return disks')
        ok = await disks.act_unite(hosts, config)
        if not ok:
            logger.error('Failed to return disks')
            return False
    title('\033[0;32mDone')
    return True


async def act(
    hosts,
    command,
    config,
    without_test_install=False,
    waiting: int = 60,
    bin_path: str = None,
    do_not_init: bool = False,
    ignore_failed_stop: bool = False,
):
    if command == 'promote':
        return await promote(
            hosts,
            config,
            without_test_install=without_test_install,
            waiting=waiting,
            bin_path=bin_path,
            do_not_init=do_not_init,
            ignore_failed_stop=ignore_failed_stop
        )
    else:
        return await demote(hosts, config, ignore_failed_stop=ignore_failed_stop)


def add_arguments(parser):
    common.add_common_options(parser)
    parser.add_argument('command', choices=('demote', 'promote'))
    parser.add_argument(
        '--without-test-install', '--without_test_install', dest='without_test_install', action='store_const', const=True, default=False
    )
    parser.add_argument('--waiting', dest='waiting', type=int, default=15)
    parser.add_argument('--bin-path', '--bin_path', default=None, type=str, help='path to binary file')
    parser.add_argument('--do-not-init', '--do_not_init', action='store_const', const=True, default=False, help='do not init bsc and etc')
    parser.add_argument('--ignore-failed-stop', action='store_const', const=True, default=False, help='ignore failed stop')


async def do(args):
    hosts = await common.get_machines(args.config)
    result = await act(
        hosts,
        args.command,
        args.config,
        without_test_install=args.without_test_install,
        waiting=args.waiting,
        bin_path=args.bin_path,
        do_not_init=args.do_not_init,
        ignore_failed_stop=args.ignore_failed_stop
    )
    if not result:
        sys.exit(1)
