import logging
import queue
import os
import pexpect
import subprocess
import sys
import threading
import time
import ydb
import yaml

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from pexpect import fdpexpect
from typing import Set, Tuple, Any

from .common import CmsClient
from .benchmark_abstract import BenchmarkSample, DatabaseAccessor
from .benchmark_factory import create_benchmark


class SubprocessPtySpawn(fdpexpect.fdspawn):
    """pexpect-compatible spawn that uses subprocess.Popen with a manually allocated PTY.

    pexpect.spawn calls pty.fork() which deadlocks when invoked from a process that already
    has background threads (e.g. gRPC IO threads). subprocess.Popen uses vfork()/posix_spawn(),
    which is safe under those conditions while still letting us hand a real PTY slave to the
    child so it enters interactive mode.
    """

    def __init__(self, argv, env, encoding, timeout):
        master_fd, slave_fd = os.openpty()
        try:
            self._proc = subprocess.Popen(
                argv,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                env=env,
                close_fds=True,
                start_new_session=True,
            )
        except BaseException:
            os.close(master_fd)
            os.close(slave_fd)
            raise
        os.close(slave_fd)
        super().__init__(master_fd, encoding=encoding, timeout=timeout)

    def close(self, force: bool = False):
        try:
            super().close()
        except pexpect.exceptions.ExceptionPexpect:
            pass
        if self._proc.poll() is None:
            try:
                if force:
                    self._proc.kill()
                else:
                    self._proc.terminate()
            except OSError:
                pass
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                self._proc.kill()
            except OSError:
                pass
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass


class BenchmarkRunner:
    SETUP_TIMEOUT = 5

    class DatabaseInfo(DatabaseAccessor):
        def __init__(
            self,
            database: str,
            endpoint: str,
            user: str,
            model_token: str,
            ydb_auth_token: str,
            models_profiles: dict[str, Path],
            cli_profile_path: Path,
            ydb_cli_binary: str,
        ):
            self.cli_profile_path = cli_profile_path
            self.models_profiles = models_profiles
            self.ydb_cli_binary = ydb_cli_binary
            self.database = database
            self.endpoint = endpoint
            self.user = f"{user}@builtin"
            self.model_token = model_token
            self.driver = ydb.Driver(
                ydb.DriverConfig(
                    endpoint=self.endpoint,
                    database=self.database,
                    auth_token=ydb_auth_token,
                )
            )
            self.driver.wait(timeout=BenchmarkRunner.SETUP_TIMEOUT)
            self.scheme_client = ydb.SchemeClient(self.driver)
            self.pool = ydb.QuerySessionPool(self.driver)

        def cleanup(self, directory=None):
            if directory is None:
                directory = self.database
            for child in self.scheme_client.list_directory(directory).children:
                if child.is_row_table():
                    self.execute_query(f"DROP TABLE `{directory}/{child.name}`")
                elif child.is_directory():
                    if child.name.startswith("."):
                        continue
                    self.cleanup(f"{directory}/{child.name}")
                    self.scheme_client.remove_directory(f"{directory}/{child.name}")
                else:
                    raise ValueError(
                        f"Cleanup failed, unknown scheme entry type: {child.type}, path: {directory}/{child.name}"
                    )

        def execute_query(self, query: str, parameters=None):
            return self.pool.execute_with_retries(query, parameters)

        def run_interactive(self, model: str, timeout: int) -> SubprocessPtySpawn:
            profile = self.models_profiles.get(model)
            if not profile:
                raise ValueError(
                    f"Profile for model {model} not found, please provide `\"models\": {{...{model} configuration...}}`"
                )

            env = os.environ.copy()
            env["TERM"] = "xterm-256color"
            env["YDB_TOKEN"] = self.user
            env["YDB_CLI_AI_PROFILE_FILE"] = str(profile)
            env["YDB_CLI_AI_AUDIT_LOG"] = "1"
            if self.model_token is not None:
                env["YDB_CLI_AI_TOKEN"] = self.model_token
            child = SubprocessPtySpawn(
                [
                    self.ydb_cli_binary,
                    "-vv",
                    "--endpoint",
                    self.endpoint,
                    "--database",
                    self.database,
                    "--profile-file",
                    str(self.cli_profile_path),
                ],
                env=env,
                encoding="utf-8",
                timeout=timeout,
            )

            if logging.getLogger().level == logging.DEBUG:
                child.logfile_read = sys.stdout

            return child

    class DatabasesPool:
        def __init__(
            self,
            databases: Set[str],
            endpoint: str,
            model_token: str,
            ydb_auth_token: str,
            models_profiles: dict[str, Path],
            cli_profile_path: Path,
            ydb_cli_binary: str,
        ):
            self._queue = queue.Queue()
            for i, database in enumerate(databases):
                self._queue.put(
                    BenchmarkRunner.DatabaseInfo(
                        database,
                        endpoint,
                        f"benchmark_user_{i}",
                        model_token,
                        ydb_auth_token,
                        models_profiles,
                        cli_profile_path,
                        ydb_cli_binary,
                    )
                )

        @contextmanager
        def get(self):
            value = self._queue.get()
            try:
                yield value
            finally:
                self._queue.put(value)

        def clear(self):
            while not self._queue.empty():
                self._queue.get().driver.stop()

    def __init__(
        self,
        config: dict,
        database: str,
        endpoint: str,
        concurrency: int,
        ydb_cli_binary: str,
        dry_run: bool = False,
    ):
        self.logger = logging.getLogger("benchmark_runner")
        self.config = config
        self.database = database
        self.endpoint = endpoint
        self.ydb_cli_binary = ydb_cli_binary
        self.dry_run = dry_run

        self.concurrency = concurrency
        if self.concurrency <= 0:
            raise ValueError(f"Concurrency must be greater than 0, got: {self.concurrency}")

        if not self.database or self.database[0] != "/":
            raise ValueError(f"Database path must start with '/', got: {self.database}")
        if self.database.endswith("/"):
            raise ValueError(f"Database path must not end with '/', got: {self.database}")
        if len(self.database) < 2 or self.database[1] == "/":
            raise ValueError(f"Database domain name not found, got database path: {self.database}")
        self.serverless_database_prefix = self.database[: self.database.rfind("/")]
        self.domain_database = self.database[1:].split("/")[0]

        self.driver = None
        self.cms_client = None
        self.scheme_client = None
        self.ydb_auth_token = os.environ.get("YDB_TOKEN")
        if not self.ydb_auth_token:
            self.logger.warning("YDB_TOKEN environment variable is not set, using anonymous access")

        self.model_token = os.environ.get("MODEL_TOKEN")
        if not self.model_token:
            self.logger.warning("MODEL_TOKEN environment variable is not set, using anonymous access")

        self.statistics_path = Path("statistics")
        self.statistics_path.mkdir(parents=True, exist_ok=True)
        self.databases = set()
        self.models_profiles = {}

    def __init_configs(self):
        configs_path = Path("configs")
        configs_path.mkdir(parents=True, exist_ok=True)

        self.cli_profile_path = configs_path / "profile.yaml"
        self.cli_profile_path.touch()

        models = self.config.get("models", {})
        if not models:
            raise ValueError('Models are not configured, please provide `"models": {...models configuration...}`')
        if not isinstance(models, dict):
            raise ValueError(
                'Invalid `"models"` configuration, please provide a name-to-config mapping like `"models": {"model_name": {...}}`'
            )

        for model_name, model_config in models.items():
            if not isinstance(model_config, dict):
                raise ValueError(
                    f'Invalid configuration for model {model_name}, please provide a mapping like `"models": {{"{model_name}": {{...}}}}`'
                )

            endpoint = model_config.get("endpoint")
            if not endpoint:
                raise ValueError(
                    f"Endpoint is not configured for model {model_name}, please provide `\"endpoint\": \"localhost:12345\"`"
                )

            api_type = model_config.get("api")
            if not api_type:
                raise ValueError(
                    f"API type is not configured for model {model_name}, please provide `\"api\": \"openai\"` or `\"api\": \"anthropic\"`"
                )
            if api_type not in ["openai", "anthropic"]:
                raise ValueError(
                    f"Invalid API type {api_type} for model {model_name}, please provide `\"api\": \"openai\"` or `\"api\": \"anthropic\"`"
                )

            if self.dry_run:
                tool_auto_action = {
                    "list_directory": 3,  # Hide
                    "exec_query": 3,  # Hide
                    "explain_query": 3,  # Hide
                    "describe": 3,  # Hide
                    "ydb_help": 3,  # Hide
                    "exec_shell": 3,  # Hide
                }
            else:
                tool_auto_action = {
                    "exec_query": 1,  # Execute
                    "exec_shell": 2,  # Reject
                    "ydb_help": 2,  # Reject
                }

            model_config_path = configs_path / f"profile_{model_name}.yaml"
            with open(model_config_path, "w") as f:
                yaml.dump(
                    {
                        "current_profile": "benchmark",
                        "interactive_mode": 1,
                        "ai_profiles": {
                            "benchmark": {
                                "name": model_name,
                                "endpoint": endpoint,
                                "api_type": 0 if api_type == "openai" else 1,
                                "model": model_config.get("model", ""),
                            }
                        },
                        "tool_auto_action": tool_auto_action,
                        "system_prompt_enabled": not self.dry_run,
                    },
                    f,
                )

            self.models_profiles[model_name] = model_config_path.resolve()

    def __init_clients(self):
        self.driver = ydb.Driver(
            ydb.DriverConfig(
                endpoint=self.endpoint,
                database=f"/{self.domain_database}",
                auth_token=self.ydb_auth_token,
            )
        )
        self.driver.wait(timeout=self.SETUP_TIMEOUT)
        self.logger.info(f"Successfully connected to YDB on {self.endpoint}")

        self.scheme_client = ydb.SchemeClient(self.driver)
        self.cms_client = CmsClient(self.driver, self.logger)

    def __create_serverless_databases(self):
        self.databases = set(
            f"{self.serverless_database_prefix}/ydb_cli_bench_serverless_db_{i}" for i in range(self.concurrency)
        )

        shared_database_resources_kind = self.cms_client.describe_database(self.database).WhichOneof("resources_kind")
        if not shared_database_resources_kind.endswith("shared_resources"):
            raise ValueError(
                f"Database {self.database} is not a shared database, resources kind: {shared_database_resources_kind}"
            )

        filtered_sls_database_names = set()
        for child in self.scheme_client.list_directory(self.serverless_database_prefix).children:
            path = f"{self.serverless_database_prefix.rstrip('/')}/{child.name}"
            if path not in self.databases:
                continue

            if not ydb.SchemeEntryType.is_database(child.type):
                raise ValueError(f"Path {path} already exists and is not a database, type: {child.type}")

            database_description = self.cms_client.describe_database(path)
            database_resources_kind = database_description.WhichOneof("resources_kind")
            if database_resources_kind != "serverless_resources":
                raise ValueError(
                    f"Path {path} already exists and is not a serverless database, resources kind: {database_resources_kind}"
                )

            if database_description.serverless_resources.shared_database_path != self.database:
                raise ValueError(
                    f"Path {path} already exists, but points to a different shared database, shared database path: {database_description.serverless_resources.shared_database_path}"
                )

            filtered_sls_database_names.add(path)

        to_create = len(self.databases) - len(filtered_sls_database_names)
        if to_create > 0:
            self.logger.info(f"Creating {len(self.databases) - len(filtered_sls_database_names)} serverless databases")
        else:
            self.logger.info("All serverless databases already exist")

        for i, database in enumerate(self.databases):
            if database not in filtered_sls_database_names:
                self.cms_client.create_database(database, self.database)

            # Wait for the database to be created
            timeout = time.monotonic() + 10
            while True:
                try:
                    self.scheme_client.describe_path(database)
                    break
                except (ydb.issues.SchemeError, ydb.issues.Unavailable) as e:
                    self.logger.debug(f"Failed to describe path {database}, error: {e}")
                    if time.monotonic() > timeout:
                        raise ValueError(f"Timeout waiting for database {database} to be created")
                    time.sleep(0.01)

            self.scheme_client.modify_permissions(
                database,
                ydb.ModifyPermissionsSettings().set_permissions(
                    f"benchmark_user_{i}@builtin", ["ydb.granular.select_row", "ydb.granular.describe_schema"]
                ),
            )

    def init(self):
        self.__init_configs()

        self.logger.info(f"Initializing database {self.database} with endpoint {self.endpoint}")
        self.__init_clients()
        self.__create_serverless_databases()

    @staticmethod
    def __format_seconds(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.1f}s"
        m, s = divmod(int(seconds), 60)
        if m < 60:
            return f"{m}m{s:02d}s"
        h, m = divmod(m, 60)
        return f"{h}h{m:02d}m{s:02d}s"

    def __run_sample(self, item: Tuple[int, BenchmarkSample], databases: DatabasesPool, amount: int) -> Any:
        index, sample = item

        with self._stats_lock:
            completed = self._completed_samples
        eta_suffix = ""
        if completed > 0:
            elapsed = time.monotonic() - self._dataset_started_at
            avg = elapsed / completed
            remaining = max(0, amount - completed)
            eta_suffix = f" (avg {self.__format_seconds(avg)}/sample, ETA {self.__format_seconds(avg * remaining)})"
        self.logger.info(f"Running sample {index + 1} / {amount}...{eta_suffix}")

        with databases.get() as database:
            database.cleanup()
            result = sample.run(database)

        if isinstance(result, dict):
            result["run_seq_no"] = index + 1

        with self._stats_lock:
            self._completed_samples += 1

        return result

    def run(self):
        datasets = self.config.get("datasets", {})
        if not datasets:
            raise ValueError('Datasets are not configured, please provide `"datasets": {...datasets configuration...}`')

        databases = self.DatabasesPool(
            self.databases,
            self.endpoint,
            self.model_token,
            self.ydb_auth_token,
            self.models_profiles,
            self.cli_profile_path,
            self.ydb_cli_binary,
        )

        for dataset_name, dataset_config in datasets.items():
            unknown = [m for m in dataset_config.get("models", []) if m not in self.models_profiles]
            if unknown:
                raise ValueError(
                    f"Dataset `{dataset_name}` references unconfigured models {unknown}; "
                    f"add them under `\"models\"` in the top-level config"
                )

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            for dataset_name, dataset_config in datasets.items():
                self.logger.info(f"Creating benchmark {dataset_name}...")
                benchmark = create_benchmark(dataset_name, {**dataset_config, "dry_run": self.dry_run})
                samples_count = len(benchmark)
                self.logger.info(f"Running benchmark {dataset_name} with {samples_count} samples...")
                self._stats_lock = threading.Lock()
                self._completed_samples = 0
                self._dataset_started_at = time.monotonic()
                results = list(
                    executor.map(lambda x: self.__run_sample(x, databases, samples_count), enumerate(benchmark))
                )
                benchmark.collect_statistics(results, self.statistics_path)
        databases.clear()

    def __remove_serverless_databases(self):
        for name in self.databases:
            self.cms_client.remove_database(name)

    def cleanup(self, remove_databases: bool):
        if remove_databases:
            self.__remove_serverless_databases()
        if self.driver is not None:
            self.driver.stop()
            self.driver = None
