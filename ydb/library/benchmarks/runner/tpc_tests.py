import yatest.common
import pathlib
import sys
import os
import shutil
import json
import time
import ydb
import subprocess


DATABASE_ENDPOINT = "grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"
DATABASE_PATH = "/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"


class Runner:
    DEPS = {
        "run_tests" : "ydb/library/benchmarks/runner/run_tests",
        "dqrun" : "ydb/library/yql/tools/dqrun",
        "gen-queries" : "ydb/library/benchmarks/gen_queries",
        "result-compare" : "ydb/library/benchmarks/runner/result_compare",
        "runner" : "ydb/library/benchmarks/runner/runner"
    }

    DATA = {
        "fs-cfg" : "ydb/library/yql/tools/dqrun/examples/fs.conf",
        "gateways-cfg" : "ydb/library/benchmarks/runner/runner/test-gateways.conf",
        "flame-graph" : "contrib/tools/flame-graph",
        "donwloaders-dir" : "ydb/library/benchmarks/runner",
    }

    UDFS = [
        "ydb/library/yql/udfs/common/set",
        "ydb/library/yql/udfs/common/url_base",
        "ydb/library/yql/udfs/common/datetime2",
        "ydb/library/yql/udfs/common/re2"
    ]

    def __init__(self):
        self.deps = {name : pathlib.Path(yatest.common.binary_path(path)) for name, path in self.DEPS.items()}
        self.udfs = [pathlib.Path(yatest.common.binary_path(path)) for path in self.UDFS]
        self.data = {name : pathlib.Path(yatest.common.source_path(path)) for name, path in self.DATA.items() if name}
        self.output = pathlib.Path(yatest.common.output_path()).resolve()
        self.results_path = self.output / "results"
        self.results_path.mkdir()

        self.cmd = [str(self.deps["run_tests"]) + "/run_tests", "--is-test"]
        self.cmd += ["--dqrun", str(self.deps["dqrun"]) + "/dqrun"]
        self.cmd += ["--gen-queries", str(self.deps["gen-queries"]) + "/gen_queries"]
        self.cmd += ["--result-compare", str(self.deps["result-compare"]) + "/result_compare"]
        self.cmd += ["--downloaders-dir", str(self.data["donwloaders-dir"])]
        self.cmd += ["--runner", str(self.deps["runner"]) + "/runner"]
        self.cmd += ["--flame-graph", str(self.data["flame-graph"])]
        self.cmd += ["--udfs-dir", ";".join(map(str, self.udfs))]
        self.cmd += ["--fs-cfg", str(self.data["fs-cfg"])]
        self.cmd += ["--gateways-cfg", str(self.data["gateways-cfg"])]
        self.cmd += ["-o", str(self.results_path)]

    def wrapped_run(self, variant, datasize, tasks, query_filter):
        cmd = self.cmd
        cmd += ["--variant", f"{variant}"]
        cmd += ["--datasize", f"{datasize}"]
        cmd += ["--tasks", f"{tasks}"]
        if query_filter:
            cmd += ["--query-filter", f"{query_filter}"]
        yatest.common.execute(cmd)


class RunParams:
    def __init__(self, is_spilling, variant, datasize, tasks, query):
        self.is_spilling = is_spilling
        self.variant = variant
        self.datasize = datasize
        self.tasks = tasks
        self.query = query

    def __repr__(self):
        result = []
        for key, value in self.__dict__.items():
            result.append(f"{key}: {value}")
        return "RunParams(" + ", ".join(result) + "})"


class RunResults:
    def __init__(self):
        self.exitcode = None
        self.read_bytes = None
        self.write_bytes = None
        self.user_time = None
        self.system_time = None
        self.rss = None
        self.output_hash = None
        self.perf_file_path = None

    def from_json(self, json):
        self.exitcode = json["exitcode"]
        io_info = json["io"]
        self.read_bytes = io_info["read_bytes"]
        self.write_bytes = io_info["write_bytes"]
        resourse_usage = json["rusage"]
        self.user_time = resourse_usage["utime"]
        self.system_time = resourse_usage["stime"]
        self.rss = resourse_usage["maxrss"]

    def __repr__(self):
        result = []
        for key, value in self.__dict__.items():
            result.append(f"{key}: {value}")
        return "RunQueryData(" + ", ".join(result) + "})"


def upload_results(result_path, s3_folder, test_start):
    results_map = {}
    for entry in result_path.glob("*/*"):
        if not entry.is_dir():
            continue
        this_result = {}
        suffix = entry.relative_to(result_path)
        # {no|with}-spilling/<variant>-<datasize>-<tasks>
        is_spilling = suffix.parts[0].split("-")[0] == "with"
        variant, datasize, tasks = suffix.parts[1].split("-")

        print(list(entry.iterdir()), file=sys.stderr)

        for file in entry.iterdir():
            if not file.is_file():
                continue
            name = file.name
            print(file, name, file=sys.stderr)
            if len(file.suffixes) > 0:
                name = name.rsplit(file.suffixes[0])[0]
            if name[0] == "q":
                query_num = int(name[1:].split("-")[0])
                if query_num not in this_result:
                    this_result[query_num] = RunResults()

                if file.suffix == ".svg":
                    dst = file.relative_to(result_path)
                    this_result[query_num].perf_file_path = dst
                    # copying files to folder that will be synced with s3
                    dst = (s3_folder / dst).resolve()
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    res = shutil.copy2(str(file.resolve()), str(dst))
                    print(res, file=sys.stderr)
                # q<num>-stdout.txt
                if file.stem == f"q{query_num}-stdout":
                    with open(file, "r") as stdout:
                        this_result[query_num].output_hash = hash(stdout.read().strip())

        summary_file = entry / "summary.json"

        with open(summary_file, "r") as res_file:
            for line in res_file.readlines()[1:]:
                info = json.loads(line)
                query_num = int(info["q"][1:])
                this_result[query_num].from_json(info)

        for key, value in this_result.items():
            params = RunParams(is_spilling, variant, datasize, tasks, key)
            results_map[params] = value

    print(results_map, file=sys.stderr)

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=5)

        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        for params, results in results_map.items():
            with session.transaction() as tx:
                mapping = {
                    "BenchmarkType" : params.variant,
                    "Scale" : params.datasize,
                    "QueryNum" : params.query,
                    "WithSpilling" : params.is_spilling,
                    "Timestamp" : test_start,
                    "WasSpillingInAggregation" : None,
                    "WasSpillingInJoin" : None,
                    "WasSpillingInChannels" : None,
                    "MaxTasksPerStage" : params.tasks,
                    "PerfFileLink" : results.perf_file_path,
                    "ExitCode" : results.exitcode,
                    "ResultHash" : results.output_hash,
                    "SpilledBytes" : results.read_bytes,
                    "UserTime" : results.user_time,
                    "SystemTime" : results.syste_time
                }
                sql = 'UPSERT INTO dq_spilling_nightly_runs ({columns}) VALUES ({values})'.format(
                    columns=", ".join(mapping.keys()),
                    values=", ".join(mapping.values()))
                print(sql, file=sys.stderr)
                tx.execute(sql, commit_tx=True)


def prepare_login_to_ydb():
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        raise AttributeError("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping uploading")
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]

    subprocess.check_call([sys.executable, "-m", "pip", "install", "ydb", "ydb[yc]"])


def test_tpc():
    test_start = time.time()

    is_ci = os.environ.get("PUBLIC_DIR") is not None
    print("is ci: ", is_ci, file=sys.stderr)

    runner = Runner()
    runner.wrapped_run("h", 1, 1, r"q1\.sql")
    result_path = runner.results_path.resolve()
    print("results path:", result_path, file=sys.stderr)

    if is_ci:
        prepare_login_to_ydb()

        s3_folder = pathlib.Path(os.environ["PUBLIC_DIR"]).resolve()
        print(f"s3 folder: {s3_folder}", file=sys.stderr)

        upload_results(result_path, s3_folder, test_start)
    exit(1)
