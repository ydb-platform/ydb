# -*- coding: utf-8 -*-
import argparse
import logging.config
import subprocess as sp
import os
import tempfile

import logging

from ydb.tests.tools.nemesis.library import monitor
from ydb.tests.tools.nemesis.library import catalog
from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster


def setup_logging_config(filename=None):
    handler = {'class': 'logging.StreamHandler', 'level': 'DEBUG', 'formatter': 'base'}
    if filename:
        handler = {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': filename, 'when': 'midnight', 'level': 'DEBUG', 'formatter': 'base'
        }
    return {
        'version': 1,
        'formatters': {
            'base': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            },
        },
        'handlers': {
            'handler': handler,
        },
        'root': {
            'level': 'DEBUG',
            'handlers': (
                'handler',
            )
        },
        'ydb.tests.library.harness.kikimr_runner': {
            'level': 'DEBUG',
            'handlers': (
                'handler',
            )
        }
    }


logger = logging.getLogger(__name__)


class SshAgent(object):
    def __init__(self):
        self._env = {}
        self._env_backup = {}
        self._keys = {}
        self.start()

    @property
    def pid(self):
        return int(self._env["SSH_AGENT_PID"])

    def start(self):
        self._env_backup["SSH_AUTH_SOCK"] = os.environ.get("SSH_AUTH_SOCK")
        self._env_backup["SSH_OPTIONS"] = os.environ.get("SSH_OPTIONS")

        for line in self._run(["ssh-agent"]).splitlines():
            name, _, value = line.decode('utf-8').partition("=")
            if _ == "=":
                value = value.split(";", 1)[0]
                self._env[name] = value
                os.environ[name] = value

        os.environ["SSH_OPTIONS"] = "{}UserKnownHostsFile=/dev/null,StrictHostKeyChecking=no".format(
            "," + os.environ["SSH_OPTIONS"] if os.environ.get("SSH_OPTIONS") else ""
        )

    def stop(self):
        self._run(['kill', '-9', str(self.pid)])

    def add(self, key):
        key_pub = self._key_pub(key)
        self._run(["ssh-add", "-"], stdin=key)
        return key_pub

    def remove(self, key_pub):
        with tempfile.NamedTemporaryFile() as f:
            f.write(key_pub)
            f.flush()
            self._run(["ssh-add", "-d", f.name])

    def _key_pub(self, key):
        with tempfile.NamedTemporaryFile() as f:
            f.write(key)
            f.flush()
            return self._run(["ssh-keygen", "-y", "-f", f.name])

    @staticmethod
    def _run(cmd, stdin=None):
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, stdin=sp.PIPE if stdin else None)
        stdout, stderr = p.communicate(stdin)

        # Listing keys from empty ssh-agent results in exit code 1
        if stdout.strip() == "The agent has no identities.":
            return ""

        if p.returncode:
            message = stderr.strip() + "\n" + stdout.strip()
            raise RuntimeError(message.strip())

        return stdout


class Key(object):
    def __init__(self, key_file):
        self.key_file = key_file
        with open(key_file) as fd:
            self.key = fd.read()
        self._key_pub = None
        self._ssh_agent = SshAgent()

    def __enter__(self):
        self._key_pub = self._ssh_agent.add(self.key.encode('utf-8'))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ssh_agent.remove(self._key_pub)
        self._ssh_agent.stop()


def nemesis_logic(arguments):
    logging.config.dictConfig(setup_logging_config(arguments.log_file))
    nemesis = catalog.nemesis_factory(
        ExternalKiKiMRCluster(
            arguments.ydb_cluster_template,
            binary_path=arguments.ydb_binary_path,
            output_path=tempfile.gettempdir(),
        ),
        enable_nemesis_list_filter_by_hostname=arguments.enable_nemesis_list_filter_by_hostname,
    )
    nemesis.start()
    monitor.setup_page(arguments.mon_host, arguments.mon_port)
    nemesis.stop()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ydb-cluster-template', required=True, help='Path to the Yandex DB cluster template')
    parser.add_argument('--ydb-binary-path', required=True, help='Path to the Yandex DB binary')
    parser.add_argument('--private-key-file', default='')
    parser.add_argument('--log-file', default=None)
    parser.add_argument('--mon-port', default=8666, type=lambda x: int(x))
    parser.add_argument('--mon-host', default='::', type=lambda x: str(x))
    parser.add_argument('--enable-nemesis-list-filter-by-hostname', action='store_true')
    arguments = parser.parse_args()

    if arguments.private_key_file:
        with Key(arguments.private_key_file):
            nemesis_logic(arguments)
    else:
        nemesis_logic(arguments)


if __name__ == '__main__':
    main()
