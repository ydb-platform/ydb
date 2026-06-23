import logging
import os
import stat
import subprocess
import tempfile
import time

from library.python import resource
from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("YdbFulltextWorkload")

MARKOV_MODEL_URL = "https://storage.yandexcloud.net/ydb-public/markov_dict.tsv.gz"
MARKOV_MODEL_FILENAME = "markov_dict.tsv.gz"


class YdbFulltextWorkload(WorkloadBase):
    def __init__(self, endpoint, database, duration, rows=100000, targets=1000, threads=10):
        super().__init__(None, '', 'fulltext_workload', None)
        self.endpoint = endpoint
        self.database = database
        self.duration = str(duration)
        self.rows = str(rows)
        self.targets = str(targets)
        self.threads = str(threads)
        self.tempdir = None
        self._unpack_resource('ydb_cli')
        self._download_model()

    def __del__(self):
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "fulltext_ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def _download_model(self):
        self.model_path = os.path.join(self.working_dir, MARKOV_MODEL_FILENAME)
        logger.info(f"Downloading markov model from {MARKOV_MODEL_URL}")
        print(f"Downloading markov model from {MARKOV_MODEL_URL}")
        subprocess.run(
            ["wget", "-q", "-O", self.model_path, MARKOV_MODEL_URL],
            check=True,
            text=True,
        )
        print(f"Downloaded markov model to {self.model_path}")

    def get_command_prefix(self, subcmds):
        return [
            self.cli_path,
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'workload', 'fulltext'
        ] + subcmds

    def cmd_run(self, cmd):
        logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")

    def __loop(self):
        # init
        self.cmd_run(
            self.get_command_prefix(subcmds=['init', '--clear'])
        )
        # import generator
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'import', 'generator',
                '--model', self.model_path,
                '--rows', self.rows,
            ])
        )
        # run select
        self.cmd_run(
            self.get_command_prefix(subcmds=[
                'run', 'select',
                '--model', self.model_path,
                '--seconds', self.duration,
                '--threads', self.threads,
                '--top-size', self.targets,
            ])
        )
        # clean
        self.cmd_run(
            self.get_command_prefix(subcmds=['clean'])
        )

    def get_workload_thread_funcs(self):
        return [self.__loop]
