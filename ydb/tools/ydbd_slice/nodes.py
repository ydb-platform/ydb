import os
import sys
import logging
import subprocess


logger = logging.getLogger(__name__)


class Nodes(object):
    def __init__(self, nodes, dry_run=False, ssh_user=None):
        assert isinstance(nodes, list)
        assert len(nodes) > 0
        assert isinstance(nodes[0], str)
        self._nodes = nodes
        self._dry_run = bool(dry_run)
        self._ssh_user = ssh_user
        self._logger = logger.getChild(self.__class__.__name__)

    @property
    def nodes_list(self):
        return self._nodes

    def _get_ssh_command_prefix(self):
        command = []
        command.extend(['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-A'])
        if (self._ssh_user):
            command.extend(['-l', self._ssh_user])

        return command

    def _check_async_execution(self, running_jobs, check_retcode=True, results=None):
        if self._dry_run:
            return

        assert results is None or isinstance(results, dict)

        for cmd, process, host in running_jobs:
            out, err = process.communicate()

            if out is None:
                out = "<None>"
            else:
                out = out.decode("utf-8", errors='replace')

            if err is None:
                err = "<None>"
            else:
                err = err.decode("utf-8", errors='replace')

            retcode = process.poll()
            if retcode != 0:
                status_line = "execution '{cmd}' finished with '{retcode}' retcode".format(
                    cmd=cmd,
                    retcode=retcode,
                )
                self._logger.critical(
                    "{status_line}"
                    "stdout is:\n"
                    "{out}\n"
                    "stderr is:\n"
                    "{err}".format(
                        status_line=status_line,
                        out=out,
                        err=err
                    )
                )
                if check_retcode:
                    sys.exit(status_line)
            if results is not None:
                results[host] = {
                    'retcode': retcode,
                    'stdout': out,
                    'stderr': err
                }

    def execute_async_ret(self, cmd, check_retcode=True, nodes=None, results=None):
        running_jobs = []
        for host in (nodes if nodes is not None else self._nodes):
            self._logger.info("execute '{cmd}' at '{host}'".format(cmd=cmd, host=host))
            if self._dry_run:
                continue

            actual_cmd = self._get_ssh_command_prefix() + [host, cmd]
            process = subprocess.Popen(actual_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            running_jobs.append((actual_cmd, process, host))
        return running_jobs

    def execute_async(self, cmd, check_retcode=True, nodes=None, results=None):
        running_jobs = self.execute_async_ret(cmd, check_retcode, nodes, results)
        self._check_async_execution(running_jobs, check_retcode, results)

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
        rsh = " ".join(self._get_ssh_command_prefix())
        subprocess.check_call(["rsync", "-avqLW", "--del", "--no-o", "--no-g",
                               "--rsh={}".format(rsh),
                               "--rsync-path=sudo rsync", "--progress", local_path, destination])

    def _copy_between_nodes(self, hub, hub_path, hosts, remote_path):
        if isinstance(hosts, str):
            hosts = [hosts]
        assert isinstance(hosts, list)

        src = "{hub}:{hub_path}".format(hub=hub, hub_path=hub_path)
        running_jobs = []
        for dst in hosts:
            self._logger.info(
                "copy from '{src_host}:{src_path}' to host '{dst_host}:{dst_path}'".format(
                    src_host=hub,
                    src_path=hub_path,
                    dst_host=dst,
                    dst_path=remote_path
                )
            )
            if self._dry_run:
                continue
            cmd = self._get_ssh_command_prefix() + [dst]
            rsh = " ".join(self._get_ssh_command_prefix())
            cmd.extend([
                "sudo", "SSH_AUTH_SOCK=$SSH_AUTH_SOCK", "rsync", "-avqW", "--del", "--no-o", "--no-g",
                "--rsh='{}'".format(rsh),
                src, remote_path
            ])
            process = subprocess.Popen(cmd)
            running_jobs.append((cmd, process, dst))

        self._check_async_execution(running_jobs)

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

        self.execute_async("sudo mkdir -p {}".format(os.path.dirname(remote_path)))

        hub = self._nodes[0]
        self._copy_on_node(local_path, hub, remote_path)
        self._copy_between_nodes(hub, remote_path, self._nodes[1:], remote_path)
        if compressed_path is not None:
            self.execute_async('if [ "{from_}" -nt "{to}" -o "{to}" -nt "{from_}" ]; then sudo zstd -df "{from_}" -o "{to}" -T0; fi'.format(from_=remote_path, to=original_remote_path))
