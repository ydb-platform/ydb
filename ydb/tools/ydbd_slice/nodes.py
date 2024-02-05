import os
import sys
import logging
import subprocess
import asyncio


logger = logging.getLogger(__name__)


async def run(cmd, check_retcode=None, report_error=None):
    #DEBUG: logger.error(cmd)
    assert isinstance(cmd, list)

    report_error = True if report_error is None else False

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()

    if proc.returncode != 0 and report_error:
        status_line = f'''{cmd!r} exited with {proc.returncode}'''
        logger.critical(f'''{status_line}
            stdout:
            {stdout.decode()}
            stderr:
            {stderr.decode()}
        ''')
        if check_retcode:
            sys.exit(status_line)


async def parallel(cmds, check_retcode=None, report_error=None):
    runs = [run(i, check_retcode=check_retcode, report_error=report_error) for i in cmds]
    await asyncio.gather(*runs)


def run_parallel(cmds, check_retcode=None, report_error=None):
    asyncio.run(parallel(cmds, check_retcode=check_retcode, report_error=report_error))


SSH_CONTROL_BASENAME = '~/.ssh/ydbd_slice-master-conn-'
SSH_CONTROL_PATH = SSH_CONTROL_BASENAME + '%r@%h:%p'

class Nodes(object):
    def __init__(self, nodes, hosts, dry_run=False):
        assert isinstance(nodes, list)
        assert len(nodes) > 0
        assert isinstance(nodes[0], str)
        self._nodes = nodes
        self._hosts = hosts
        self._dry_run = bool(dry_run)
        self._logger = logger.getChild(self.__class__.__name__)

        # cleanup ssh master connections left from previous runs
        self._exit_master_connections()

    @property
    def nodes_list(self):
        return self._nodes

    @staticmethod
    def _wrap_ssh_cmd(cmd, host):
        # merge multiple lines into the single line, stripping excess whitespaces
        cmd = ' '.join((i.strip() for i in cmd.splitlines()))
        return ['ssh',
            '-o', 'ControlMaster=auto', '-o', 'ControlPersist=yes', '-o', f'ControlPath={SSH_CONTROL_PATH}',
            '-o', 'LogLevel=ERROR', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null',
            '-A',
            host,
            cmd
        ]

    def _exit_master_connections(self):
        run_parallel(
            [['ssh', '-q', '-O', 'exit', '-o', f'ControlPath={SSH_CONTROL_PATH}', i] for i in self._hosts],
            check_retcode=False,
            report_error=False,
        )
        subprocess.run(f'rm -f {SSH_CONTROL_BASENAME}*', shell=True, check=False)

    def _prepare_ssh_commands(self, cmd, nodes=None, log_commands=True):
        if nodes is None:
            nodes = self._nodes
        else:
            assert isinstance(nodes, list)
            assert len(nodes) > 0
            assert isinstance(nodes[0], str)

        if log_commands:
            for host in nodes:
                self._logger.info(f"{host}: execute '{cmd}'")

        if self._dry_run:
            return []

        return [self._wrap_ssh_cmd(cmd, host) for host in nodes]

    def execute_ssh_commands(self, prepared_ssh_commands, check_retcode=True):
        assert isinstance(prepared_ssh_commands, list)
        if not self._dry_run:
            run_parallel(prepared_ssh_commands, check_retcode=check_retcode)

    def execute_async(self, cmd, check_retcode=True, nodes=None, log_commands=True):
        prepared = self._prepare_ssh_commands(cmd, nodes=nodes, log_commands=log_commands)
        self.execute_ssh_commands(prepared, check_retcode=check_retcode)

    def _copy_on_node(self, local_path, host, remote_path):
        self._logger.info(
            "copy from localhost path '{local_path}' to host '{host}' in '{remote_path}'".format(
                local_path=local_path,
                host=host,
                remote_path=remote_path
            )
        )
        if self._dry_run:
            return
        destination = "{host}:{path}".format(host=host, path=remote_path)
        subprocess.check_call(["rsync", "-avqLW", "--del", "--no-o", "--no-g", "--rsync-path=sudo rsync", "--progress",
                              local_path, destination])

    def _copy_between_nodes(self, hub, hub_path, hosts, remote_path):
        if isinstance(hosts, str):
            hosts = [hosts]
        assert isinstance(hosts, list)

        src = "{hub}:{hub_path}".format(hub=hub, hub_path=hub_path)
        for dst in hosts:
            self._logger.info(
                "copy from '{src_host}:{src_path}' to host '{dst_host}:{dst_path}'".format(
                    src_host=hub,
                    src_path=hub_path,
                    dst_host=dst,
                    dst_path=remote_path,
                )
            )

        if self._dry_run:
            return

        cmd = f"""sudo rsync -avqW --del --no-o --no-g
            --rsh=\'ssh -o LogLevel=ERROR -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -l {os.getenv("USER")}\'
            {src} {remote_path}
        """

        self.execute_async(cmd, nodes=hosts, log_commands=False)

    # copy local_path to remote_path for every node in nodes
    def copy(self, local_path, remote_path, directory=False, compressed_path=None):
        if directory:
            local_path += '/'
            remote_path += '/'
        if compressed_path is not None:
            self._logger.info('compressing %s to %s' % (local_path, compressed_path))
            if not os.path.isfile(compressed_path) or os.stat(local_path).st_mtime != os.stat(compressed_path).st_mtime:
                subprocess.check_call(['zstd', '-5f', local_path, '-o', compressed_path, '-T0'])
            local_path = compressed_path
            original_remote_path = remote_path
            remote_path += '.zstd'
        hub = self._nodes[0]

        self._logger.info(f"copy '{local_path}' to nodes:'{remote_path}', via hub {hub}")

        self._copy_on_node(local_path, hub, remote_path)
        self._copy_between_nodes(hub, remote_path, self._nodes[1:], remote_path)
        if compressed_path is not None:
            self.execute_async('if [ "{from_}" -nt "{to}" -o "{to}" -nt "{from_}" ]; then sudo zstd -df "{from_}" -o "{to}" -T0; fi'.format(
                from_=remote_path,
                to=original_remote_path,
            ))
