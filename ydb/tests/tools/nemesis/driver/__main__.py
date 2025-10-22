# -*- coding: utf-8 -*-
import argparse
import logging.config
import subprocess as sp
import os
import tempfile
import signal
import sys
import time
import logging


if not logging.getLogger().handlers:
    sys.stdout = sys.stderr
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stderr)
        ]
    )

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
            name, _, value = line.partition("=")
            if _ == "=":
                value = value.split(";", 1)[0]
                self._env[name] = value
                os.environ[name] = value

        os.environ["SSH_OPTIONS"] = "{}UserKnownHostsFile=/dev/null,StrictHostKeyChecking=no".format(
            "," + os.environ["SSH_OPTIONS"] if os.environ.get("SSH_OPTIONS") else ""
        )

    def stop(self):
        try:
            self._run(['kill', '-9', str(self.pid)])
        except Exception as e:
            logging.warning("Failed to stop ssh-agent: {}".format(e))
            pass

    def add(self, key):
        key_pub = self._key_pub(key)
        self._run(["ssh-add", "-"], stdin=key)
        return key_pub

    def remove(self, key_pub):
        try:
            with tempfile.NamedTemporaryFile() as f:
                f.write(key_pub)
                f.flush()
                self._run(["ssh-add", "-d", f.name])
        except Exception as e:
            logging.warning("Failed to remove key: {}".format(e))
            pass

    def _key_pub(self, key):
        with tempfile.NamedTemporaryFile() as f:
            f.write(key)
            f.flush()
            return self._run(["ssh-keygen", "-y", "-f", f.name])

    @staticmethod
    def _run(cmd, stdin=None):
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, stdin=sp.PIPE if stdin else None)
        stdout, stderr = p.communicate(stdin)

        if stdout.decode('utf-8', errors='ignore').strip() == "The agent has no identities.":
            return ""

        if p.returncode:
            stderr_str = stderr.decode('utf-8', errors='ignore').strip()
            stdout_str = stdout.decode('utf-8', errors='ignore').strip()
            message = stderr_str + "\n" + stdout_str
            raise RuntimeError(message.strip())

        return stdout.decode('utf-8', errors='ignore')


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
        try:
            if hasattr(self, '_key_pub') and self._key_pub:
                self._ssh_agent.remove(self._key_pub)
        except Exception:
            pass
        try:
            self._ssh_agent.stop()
        except Exception:
            pass


def nemesis_logic(arguments):
    logger = logging.getLogger(__name__)

    if arguments.log_file:
        existing_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        if not existing_handlers:
            file_handler = logging.FileHandler(arguments.log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(file_handler)
            logger.info("Added file handler for: %s", arguments.log_file)
        else:
            logger.info("File handler already exists for: %s", arguments.log_file)

    logger.info("Starting nemesis logic")
    logger.info("Arguments: %s", arguments)

    ssh_username = os.getenv('NEMESIS_USER', 'robot-nemesis')
    logger.info("SSH username: %s", ssh_username)

    yaml_config = arguments.yaml_config
    logger.info("YAML config: %s", yaml_config)

    try:
        if yaml_config is not None:
            logger.info("Creating cluster with YAML config")
            cluster = ExternalKiKiMRCluster(
                cluster_template=arguments.ydb_cluster_template,
                kikimr_configure_binary_path=None,
                kikimr_path=arguments.ydb_binary_path,
                ssh_username=ssh_username,
                yaml_config=yaml_config,
            )
        else:
            logger.info("Creating cluster without YAML config")
            cluster = ExternalKiKiMRCluster(
                cluster_template=arguments.ydb_cluster_template,
                kikimr_configure_binary_path=None,
                kikimr_path=arguments.ydb_binary_path,
                ssh_username=ssh_username,
            )

        logger.info("Cluster created successfully: %s", cluster)
        logger.info("Cluster hostnames: %s", cluster.hostnames if hasattr(cluster, 'hostnames') else 'N/A')

        nemesis = catalog.nemesis_factory(
            cluster,
            ssh_username=ssh_username,
            enable_nemesis_list_filter_by_hostname=arguments.enable_nemesis_list_filter_by_hostname,
        )
        logger.info("Nemesis factory created successfully")

    except Exception as e:
        logger.error("Failed to create nemesis: %s", e)
        raise

    def signal_handler(signum, frame):
        logger.info("Catched %d (SIGTERM/SIGINT), starting graceful shutdown", signum)
        try:
            nemesis.stop()
        except Exception as e:
            logger.error("Shutdown failed: %s", e)
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        nemesis.start()
        monitor.setup_page(arguments.mon_host, arguments.mon_port)

        while nemesis.is_alive():
            time.sleep(1)

    except Exception as e:
        logger.error("Ошибка в nemesis_logic: %s", e)
        raise
    finally:
        try:
            nemesis.stop()
        except Exception as e:
            logger.error("Ошибка при остановке nemesis в finally: %s", e)


def main():
    logger = logging.getLogger(__name__)
    logger.info("Starting nemesis driver")

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ydb-cluster-template', required=True, help='Path to the YDB cluster template')
    parser.add_argument('--ydb-binary-path', required=True, help='Path to the YDB binary')
    parser.add_argument('--yaml-config', required=False, default=None, help='Path to the YDB configuration v2')
    parser.add_argument('--private-key-file', default='')
    parser.add_argument('--log-file', default=None)
    parser.add_argument('--mon-port', default=8666, type=lambda x: int(x))
    parser.add_argument('--mon-host', default='::', type=lambda x: str(x))
    parser.add_argument('--enable-nemesis-list-filter-by-hostname', action='store_true')
    arguments = parser.parse_args()

    logger.info("Parsed arguments: %s", arguments)

    try:
        if arguments.private_key_file:
            logger.info("Using private key file: %s", arguments.private_key_file)
            with Key(arguments.private_key_file):
                nemesis_logic(arguments)
        else:
            logger.info("No private key file specified")
            nemesis_logic(arguments)
    except Exception as e:
        logger.error("Failed to run nemesis: %s", e)
        raise


if __name__ == '__main__':
    main()
