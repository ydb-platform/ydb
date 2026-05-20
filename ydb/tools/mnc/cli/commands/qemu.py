import asyncio
import logging
import os
import shlex

from ydb.tools.mnc.lib import common, deploy_ctx, output, progress, term
from ydb.tools.mnc.lib.exceptions import CliError
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)


expected_config = multinode.scheme


def shell_quote(value):
    return shlex.quote(str(value))


def remote_dir_from_args(args):
    return args.remote_dir or f'{deploy_ctx.deploy_path}/qemu'


def default_socket_path(disk_id: str):
    return f'/tmp/{disk_id}.sock'


def remote_blockstore_client_path(remote_dir: str):
    return f'{remote_dir}/blockstore-client'


def remote_run_script_path(remote_dir: str):
    return f'{remote_dir}/run_qemu.sh'


def qemu_pid_path(disk_id: str):
    return f'{deploy_ctx.deploy_path}/run/qemu-{disk_id}.pid'


def qemu_log_path(remote_dir: str, disk_id: str):
    return f'{remote_dir}/qemu-{disk_id}.log'


def validate_host(host: str, hosts: list[str]):
    if host not in hosts:
        raise CliError(f"host '{host}' is not listed in config hosts")


def generate_run_qemu_script():
    return '''#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

disk_id=""
socket=""
ssh_port="8679"
qmp_port="8678"
memory="16G"
smp="4,sockets=1,cores=4,threads=1"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --disk-id)
            disk_id="$2"
            shift 2
            ;;
        --socket)
            socket="$2"
            shift 2
            ;;
        --ssh-port)
            ssh_port="$2"
            shift 2
            ;;
        --qmp-port)
            qmp_port="$2"
            shift 2
            ;;
        --memory)
            memory="$2"
            shift 2
            ;;
        --smp)
            smp="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$disk_id" ]]; then
    echo "--disk-id is required" >&2
    exit 2
fi

if [[ -z "$socket" ]]; then
    socket="/tmp/${disk_id}.sock"
fi

QEMU_TAR="$SCRIPT_DIR/qemu-bin.tar.gz"
QEMU="$SCRIPT_DIR/usr/bin/qemu-system-x86_64"
QEMU_FIRMWARE="$SCRIPT_DIR/usr/share/qemu"
ROOTFS="$SCRIPT_DIR/rootfs.img"

if [[ ! -x "$QEMU" ]]; then
    echo "Expanding QEMU tar from $QEMU_TAR"
    tar -xzf "$QEMU_TAR" -C "$SCRIPT_DIR"
fi

exec "$QEMU" \\
    -L "$QEMU_FIRMWARE" \\
    -snapshot \\
    -nodefaults \\
    -cpu host \\
    -smp "$smp" \\
    -enable-kvm \\
    -m "$memory" \\
    -name debug-threads=on \\
    -qmp "tcp:127.0.0.1:${qmp_port},server,nowait" \\
    -object "memory-backend-memfd,id=mem,size=${memory},share=on" \\
    -numa node,memdev=mem \\
    -netdev "user,id=netdev0,hostfwd=tcp::${ssh_port}-:22" \\
    -device virtio-net-pci,netdev=netdev0,id=net0 \\
    -object iothread,id=iot0 \\
    -drive "format=qcow2,file=${ROOTFS},id=lbs0,if=none,aio=native,cache=none,discard=unmap" \\
    -device virtio-blk-pci,scsi=off,drive=lbs0,id=virtio-disk0,iothread=iot0,bootindex=1 \\
    -chardev "socket,id=vhost0,path=${socket}" \\
    -device vhost-user-blk-pci,chardev=vhost0,id=vhost-user-blk0,num-queues=1 \\
    -nographic \\
    -serial stdio
'''


async def result_or_error(result: term.Result, step_title: str, message: str):
    if result:
        return True
    details = f'{message}\nReturn code: {result.returncode}'
    if result.stderr:
        details += f'\n\nstderr:\n{result.stderr}'
    if result.stdout:
        details += f'\n\nstdout:\n{result.stdout}'
    if result.log_path:
        details += f'\n\nFull log: {result.log_path}'
    return progress.TaskResult(level=progress.TaskResultLevel.ERROR, step_title=step_title, message=details)


async def run_local_shell(cmd: str, step_title: str, message: str):
    return await result_or_error(await term.shell(cmd, step_title=step_title), step_title, message)


async def run_remote_shell(host: str, cmd: str, step_title: str, message: str):
    return await result_or_error(await term.ssh_run(host, cmd, step_title=step_title), step_title, message)


async def resolve_blockstore_client_bin(args):
    if not os.path.exists(args.blockstore_client_bin):
        return progress.TaskResult(
            level=progress.TaskResultLevel.ERROR,
            step_title='Resolve blockstore-client',
            message=f'blockstore-client binary does not exist: {args.blockstore_client_bin}',
        )
    return args.blockstore_client_bin


def write_local_run_script(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as script_file:
        script_file.write(generate_run_qemu_script())
    os.chmod(path, 0o755)


async def rsync_file(source: str, host: str, destination: str, step_title: str):
    cmd = f'rsync -L --progress {shell_quote(source)} {shell_quote(f"{host}:{destination}")}'
    return await run_local_shell(cmd, step_title, f'Failed to copy {source} to {host}:{destination}')


async def act_prepare(args):
    hosts = await common.get_machines(args.config)
    validate_host(args.host, hosts)

    for path, name in ((args.rootfs_img, 'rootfs image'), (args.qemu_bin_tar, 'qemu binary tar')):
        if not os.path.exists(path):
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                step_title='Prepare QEMU',
                message=f'{name} does not exist: {path}',
            )

    blockstore_client = await resolve_blockstore_client_bin(args)
    if isinstance(blockstore_client, progress.TaskResult):
        return blockstore_client

    remote_dir = remote_dir_from_args(args)
    local_script = os.path.join(deploy_ctx.work_directory, 'run_qemu.sh')
    write_local_run_script(local_script)

    steps = [
        lambda: run_remote_shell(args.host, f'mkdir -p {shell_quote(remote_dir)}', 'Prepare remote QEMU dir', 'Failed to create remote QEMU directory'),
        lambda: rsync_file(args.rootfs_img, args.host, f'{remote_dir}/rootfs.img', 'Copy rootfs.img'),
        lambda: rsync_file(args.qemu_bin_tar, args.host, f'{remote_dir}/qemu-bin.tar.gz', 'Copy qemu-bin.tar.gz'),
        lambda: rsync_file(blockstore_client, args.host, remote_blockstore_client_path(remote_dir), 'Copy blockstore-client'),
        lambda: rsync_file(local_script, args.host, remote_run_script_path(remote_dir), 'Copy run_qemu.sh'),
        lambda: run_remote_shell(
            args.host,
            f'chmod +x {shell_quote(remote_blockstore_client_path(remote_dir))} {shell_quote(remote_run_script_path(remote_dir))}',
            'Chmod QEMU assets',
            'Failed to chmod QEMU assets',
        ),
    ]
    for step in steps:
        result = await step()
        if not result:
            return result
    return progress.TaskResult(
        level=progress.TaskResultLevel.OK,
        step_title='Prepare QEMU',
        message=f'QEMU assets prepared on {args.host}:{remote_dir}',
    )


async def validate_prepare_inputs(args):
    hosts = await common.get_machines(args.config)
    validate_host(args.host, hosts)

    for path, name in ((args.rootfs_img, 'rootfs image'), (args.qemu_bin_tar, 'qemu binary tar')):
        if not os.path.exists(path):
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                step_title='Validate QEMU assets',
                message=f'{name} does not exist: {path}',
            )

    blockstore_client = await resolve_blockstore_client_bin(args)
    if isinstance(blockstore_client, progress.TaskResult):
        return blockstore_client
    return True


async def write_local_run_script_for_args(args):
    write_local_run_script(os.path.join(deploy_ctx.work_directory, 'run_qemu.sh'))
    return True


def build_start_endpoint_command(remote_dir: str, disk_id: str, socket: str, client_id: str, instance_id: str, encryption_args: list[str] = None):
    blockstore_client = remote_blockstore_client_path(remote_dir)
    args = [
        shell_quote(blockstore_client),
        'startendpoint',
        '--ipc-type',
        'vhost',
        '--socket',
        shell_quote(socket),
        '--client-id',
        shell_quote(client_id),
        '--instance-id',
        shell_quote(instance_id),
        '--disk-id',
        shell_quote(disk_id),
        '--persistent',
    ]
    if encryption_args:
        args.extend(encryption_args)
    return ' '.join(args)


def build_stop_endpoint_command(remote_dir: str, socket: str):
    return f'{shell_quote(remote_blockstore_client_path(remote_dir))} stopendpoint --socket {shell_quote(socket)}'


async def act_start_endpoint(args):
    hosts = await common.get_machines(args.config)
    validate_host(args.host, hosts)
    remote_dir = remote_dir_from_args(args)
    socket = args.socket or default_socket_path(args.disk_id)
    encryption_args = []
    if args.encryption_key_path:
        encryption_args = ['--encryption-mode', 'aes-xts', '--encryption-key-path', shell_quote(args.encryption_key_path)]
    command = (
        f'{build_stop_endpoint_command(remote_dir, socket)} || true; '
        f'{build_start_endpoint_command(remote_dir, args.disk_id, socket, args.client_id, args.instance_id, encryption_args)}; '
        f'test -S {shell_quote(socket)}'
    )
    return await run_remote_shell(args.host, command, 'Start QEMU NBS endpoint', 'Failed to start QEMU NBS endpoint')


async def act_stop_endpoint(args):
    hosts = await common.get_machines(args.config)
    validate_host(args.host, hosts)
    remote_dir = remote_dir_from_args(args)
    socket = args.socket or default_socket_path(args.disk_id)
    return await run_remote_shell(
        args.host,
        build_stop_endpoint_command(remote_dir, socket),
        'Stop QEMU NBS endpoint',
        'Failed to stop QEMU NBS endpoint',
    )


def build_qemu_run_command(remote_dir: str, disk_id: str, socket: str, ssh_port: int, qmp_port: int, memory: str, smp: str):
    log_path = qemu_log_path(remote_dir, disk_id)
    pid_path = qemu_pid_path(disk_id)
    script = remote_run_script_path(remote_dir)
    return (
        f'mkdir -p {shell_quote(os.path.dirname(pid_path))}; '
        f'cd {shell_quote(remote_dir)}; '
        f'nohup {shell_quote(script)} '
        f'--disk-id {shell_quote(disk_id)} '
        f'--socket {shell_quote(socket)} '
        f'--ssh-port {shell_quote(ssh_port)} '
        f'--qmp-port {shell_quote(qmp_port)} '
        f'--memory {shell_quote(memory)} '
        f'--smp {shell_quote(smp)} '
        f'> {shell_quote(log_path)} 2>&1 & '
        f'echo $! > {shell_quote(pid_path)}; '
        f'sleep 1; '
        f'pid=$(cat {shell_quote(pid_path)}); '
        f'ps -p "$pid" >/dev/null'
    )


def build_qemu_ssh_command(host: str, vm_user: str, ssh_port: int, identity_file: str = None):
    remote_ssh_args = [
        'ssh',
        '-t',
        '-o',
        'StrictHostKeyChecking=no',
        '-o',
        'UserKnownHostsFile=/dev/null',
        '-p',
        shell_quote(ssh_port),
    ]
    if identity_file:
        remote_ssh_args.extend(['-i', shell_quote(identity_file)])
    remote_ssh_args.append(shell_quote(f'{vm_user}@127.0.0.1'))
    remote_ssh_cmd = ' '.join(remote_ssh_args)
    return f'ssh -A -t {shell_quote(host)} {shell_quote(remote_ssh_cmd)}'


async def run_interactive_shell(cmd: str):
    proc = await asyncio.create_subprocess_shell(cmd)
    return await proc.wait()


async def act_run(args):
    hosts = await common.get_machines(args.config)
    validate_host(args.host, hosts)
    remote_dir = remote_dir_from_args(args)
    socket = args.socket or default_socket_path(args.disk_id)

    if not args.no_start_endpoint:
        endpoint_result = await act_start_endpoint(args)
        if not endpoint_result:
            return endpoint_result

    result = await run_remote_shell(
        args.host,
        build_qemu_run_command(remote_dir, args.disk_id, socket, args.ssh_port, args.qmp_port, args.memory, args.smp),
        'Run QEMU',
        'Failed to run QEMU',
    )
    if not result:
        return result
    return progress.TaskResult(
        level=progress.TaskResultLevel.OK,
        step_title='Run QEMU',
        message=f'QEMU started on {args.host}; pid file: {qemu_pid_path(args.disk_id)}; log: {qemu_log_path(remote_dir, args.disk_id)}',
    )


async def act_run_qemu_process(args):
    remote_dir = remote_dir_from_args(args)
    socket = args.socket or default_socket_path(args.disk_id)
    result = await run_remote_shell(
        args.host,
        build_qemu_run_command(remote_dir, args.disk_id, socket, args.ssh_port, args.qmp_port, args.memory, args.smp),
        'Run QEMU',
        'Failed to run QEMU',
    )
    if not result:
        return result
    return progress.TaskResult(
        level=progress.TaskResultLevel.OK,
        step_title='Run QEMU',
        message=f'QEMU started on {args.host}; pid file: {qemu_pid_path(args.disk_id)}; log: {qemu_log_path(remote_dir, args.disk_id)}',
    )


async def act_ssh(args):
    hosts = await common.get_machines(args.config)
    validate_host(args.host, hosts)
    returncode = await run_interactive_shell(
        build_qemu_ssh_command(args.host, args.vm_user, args.ssh_port, args.identity_file)
    )
    if returncode == 0:
        return progress.TaskResult(
            level=progress.TaskResultLevel.OK,
            step_title='SSH to QEMU VM',
            message=f'SSH session to {args.vm_user}@127.0.0.1:{args.ssh_port} finished',
        )
    return progress.TaskResult(
        level=progress.TaskResultLevel.ERROR,
        step_title='SSH to QEMU VM',
        message=f'SSH session failed with return code {returncode}',
    )


def make_command_step(args):
    if args.cmd == 'prepare':
        remote_dir = remote_dir_from_args(args)
        local_script = os.path.join(deploy_ctx.work_directory, 'run_qemu.sh')
        return progress.SequentialStepGroup(
            title='[bold blue]Prepare QEMU[/]',
            steps=[
                progress.SimpleStep('[bold blue]Validate QEMU assets[/]', action=lambda: validate_prepare_inputs(args)),
                progress.SimpleStep('[bold blue]Write run_qemu.sh[/]', action=lambda: write_local_run_script_for_args(args)),
                progress.SimpleStep(
                    '[bold blue]Create remote QEMU dir[/]',
                    action=lambda: run_remote_shell(args.host, f'mkdir -p {shell_quote(remote_dir)}', 'Prepare remote QEMU dir', 'Failed to create remote QEMU directory'),
                ),
                progress.SimpleStep('[bold blue]Copy rootfs.img[/]', action=lambda: rsync_file(args.rootfs_img, args.host, f'{remote_dir}/rootfs.img', 'Copy rootfs.img')),
                progress.SimpleStep('[bold blue]Copy qemu-bin.tar.gz[/]', action=lambda: rsync_file(args.qemu_bin_tar, args.host, f'{remote_dir}/qemu-bin.tar.gz', 'Copy qemu-bin.tar.gz')),
                progress.SimpleStep(
                    '[bold blue]Copy blockstore-client[/]',
                    action=lambda: rsync_file(
                        args.blockstore_client_bin,
                        args.host,
                        remote_blockstore_client_path(remote_dir),
                        'Copy blockstore-client',
                    ),
                ),
                progress.SimpleStep(
                    '[bold blue]Copy run_qemu.sh[/]',
                    action=lambda: rsync_file(
                        local_script,
                        args.host,
                        remote_run_script_path(remote_dir),
                        'Copy run_qemu.sh',
                    ),
                ),
                progress.SimpleStep(
                    '[bold blue]Chmod QEMU assets[/]',
                    action=lambda: run_remote_shell(
                        args.host,
                        f'chmod +x {shell_quote(remote_blockstore_client_path(remote_dir))} {shell_quote(remote_run_script_path(remote_dir))}',
                        'Chmod QEMU assets',
                        'Failed to chmod QEMU assets',
                    ),
                ),
            ],
        )
    if args.cmd == 'run':
        steps = []
        if not args.no_start_endpoint:
            steps.append(progress.SimpleStep(
                f'[bold blue]Start QEMU NBS endpoint[/] [green]{args.disk_id}[/]',
                action=lambda: act_start_endpoint(args),
            ))
        steps.append(progress.SimpleStep(
            f'[bold blue]Run QEMU[/] [green]{args.disk_id}[/]',
            action=lambda: act_run_qemu_process(args),
        ))
        return progress.SequentialStepGroup(title=f'[bold blue]Run QEMU[/] [green]{args.disk_id}[/]', steps=steps)
    if args.cmd == 'start-endpoint':
        return progress.SimpleStep(
            f'[bold blue]Start QEMU NBS endpoint[/] [green]{args.disk_id}[/]',
            action=lambda: act_start_endpoint(args),
        )
    if args.cmd == 'stop-endpoint':
        return progress.SimpleStep(
            f'[bold blue]Stop QEMU NBS endpoint[/] [green]{args.disk_id}[/]',
            action=lambda: act_stop_endpoint(args),
        )
    raise CliError(f'Unknown QEMU command: {args.cmd}')


def add_common_qemu_options(parser):
    common.add_common_options(parser)
    parser.add_argument('--host', required=True)
    parser.add_argument('--remote-dir', default=None)


def add_endpoint_options(parser):
    parser.add_argument('--disk-id', required=True)
    parser.add_argument('--socket', default=None)
    parser.add_argument('--client-id', default='client-1')
    parser.add_argument('--instance-id', default='localhost')


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    prepare_parser = subparsers.add_parser('prepare')
    add_common_qemu_options(prepare_parser)
    prepare_parser.add_argument('--rootfs-img', required=True)
    prepare_parser.add_argument('--qemu-bin-tar', required=True)
    prepare_parser.add_argument('--blockstore-client-bin', required=True)

    start_endpoint_parser = subparsers.add_parser('start-endpoint')
    add_common_qemu_options(start_endpoint_parser)
    add_endpoint_options(start_endpoint_parser)
    start_endpoint_parser.add_argument('--encryption-key-path', default=None)

    stop_endpoint_parser = subparsers.add_parser('stop-endpoint')
    add_common_qemu_options(stop_endpoint_parser)
    stop_endpoint_parser.add_argument('--disk-id', required=True)
    stop_endpoint_parser.add_argument('--socket', default=None)

    run_parser = subparsers.add_parser('run')
    add_common_qemu_options(run_parser)
    add_endpoint_options(run_parser)
    run_parser.add_argument('--encryption-key-path', default=None)
    run_parser.add_argument('--no-start-endpoint', action='store_true', default=False)
    run_parser.add_argument('--ssh-port', type=int, default=8679)
    run_parser.add_argument('--qmp-port', type=int, default=8678)
    run_parser.add_argument('--memory', default='16G')
    run_parser.add_argument('--smp', default='4,sockets=1,cores=4,threads=1')

    ssh_parser = subparsers.add_parser('ssh')
    add_common_qemu_options(ssh_parser)
    ssh_parser.add_argument('--ssh-port', type=int, default=8679)
    ssh_parser.add_argument('--vm-user', default='root')
    ssh_parser.add_argument('--identity-file', default=None)


async def do(args):
    if args.cmd == 'ssh':
        result = await act_ssh(args)
    else:
        result = await progress.run_steps([make_command_step(args)], title='[bold]QEMU[/]')
    output.get_console().print(result.to_rich_panel(verbose=getattr(args, 'verbose', False)))
    return result
