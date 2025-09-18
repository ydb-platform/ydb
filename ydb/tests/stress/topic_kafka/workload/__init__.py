import logging
import subprocess
import os
import stat
from library.python import resource
from dataclasses import dataclass

from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbTopicWorkload")


@dataclass
class WriteProfile:
    message_rate: float
    message_size: int
    keys_count: int
    key_prefix: str
    producers: int


def parse_write_profile(profile) -> WriteProfile:
    message_rate, message_size, keys_count, key_prefix, producers = profile
    return WriteProfile(
        message_rate=float(message_rate),
        message_size=int(message_size),
        keys_count=int(keys_count),
        key_prefix=key_prefix,
        producers=int(producers),
    )


class YdbTopicWorkload(WorkloadBase):
    def __init__(
        self,
        endpoint,
        database,
        duration,
        consumers,
        consumer_threads,
        partitions,
        write_profiles: list[WriteProfile],
        tables_prefix,
        restart_interval,
        cleanup_policy_compact,
    ):
        super().__init__(None, tables_prefix, 'topic', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.consumers = str(consumers)
        self.consumer_threads = consumer_threads
        self.write_profiles = write_profiles
        self.partitions = partitions
        self.restart_interval = restart_interval
        self.cleanup_policy_compact = cleanup_policy_compact
        self._unpack_resource('ydb_cli')

    def _unpack_resource(self, name):
        self.working_dir = os.path.join(os.getcwd(), "topic_kafka_ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def get_command_prefix(self, subcmds: list[str]) -> list[str]:
        return (
            [
                self.cli_path,
                '--verbose',
                '--endpoint',
                self.endpoint,
                '--database={}'.format(self.database),
                'workload',
                'topic',
            ]
            + subcmds
            + ['--topic', f'{self.table_prefix}']
        )

    def cmd_run(self, cmd):
        cmd = [str(arg) for arg in cmd]
        logger.info(f"Running cmd {' '.join(cmd)}")
        subprocess.run(cmd, check=True, text=True)

    def __read_loop(self):
        def run():
            subcmds = [
                'run',
                'read',
                '--seconds',
                self.duration,
                '--consumers',
                self.consumers,
                '--threads',
                self.consumer_threads,
                '--no-commit',
            ]
            if self.restart_interval is not None:
                subcmds += [
                    '--restart-interval',
                    self.restart_interval,
                ]
            self.cmd_run(self.get_command_prefix(subcmds=subcmds))

        return run

    def __write_loop(self, profile: WriteProfile):
        def run():
            subcmds = [
                'run',
                'write',
                '--seconds',
                self.duration,
                '--message-rate',
                profile.message_rate,
                '--message-size',
                profile.message_size,
                '--key-count',
                profile.keys_count,
                '--key-prefix',
                profile.key_prefix,
                '--threads',
                profile.producers,
                '--warmup',
                '0',
            ]
            self.cmd_run(self.get_command_prefix(subcmds=subcmds))

        return run

    def tear_up(self):
        subcmds = ['init', '--consumers', self.consumers, '--partitions', self.partitions]
        if self.cleanup_policy_compact:
            subcmds += ['--cleanup-policy-compact']
        self.cmd_run(
            self.get_command_prefix(subcmds=subcmds)
        )

    def tear_down(self):
        self.cmd_run(self.get_command_prefix(subcmds=['clean']))

    def get_workload_thread_funcs(self):
        writers = [self.__write_loop(profile) for profile in self.write_profiles]
        readers = [self.__read_loop()]
        return writers + readers
