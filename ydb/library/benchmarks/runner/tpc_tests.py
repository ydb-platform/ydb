import yatest.common
import pathlib
import sys
import os


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
        "downloaders-dir" : "ydb/library/benchmarks/runner",
    }

    UDFS = [
        "yql/essentials/udfs/common/set",
        "yql/essentials/udfs/common/url_base",
        "yql/essentials/udfs/common/datetime2",
        "yql/essentials/udfs/common/re2"
    ]

    def __init__(self):
        self.deps = {name : pathlib.Path(yatest.common.binary_path(path)) for name, path in self.DEPS.items()}
        self.udfs = [pathlib.Path(yatest.common.binary_path(path)) for path in self.UDFS]
        self.data = {name : pathlib.Path(yatest.common.source_path(path)) for name, path in self.DATA.items() if name}
        self.output = pathlib.Path(yatest.common.output_path())
        self.results_path = self.output / "results"
        self.results_path.mkdir()

        self.cmd = [str(self.deps["run_tests"]) + "/run_tests", "--is-test"]
        self.cmd += ["--dqrun", str(self.deps["dqrun"]) + "/dqrun"]
        self.cmd += ["--gen-queries", str(self.deps["gen-queries"]) + "/gen_queries"]
        self.cmd += ["--result-compare", str(self.deps["result-compare"]) + "/result_compare"]
        self.cmd += ["--downloaders-dir", str(self.data["downloaders-dir"])]
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
        cmd += ["--clean-old"]
        if query_filter:
            cmd += ["--query-filter", f"{query_filter}"]
        yatest.common.execute(cmd, stdout=sys.stdout, stderr=sys.stderr)


def upload(result_path, s3_folder, try_num):
    uploader = pathlib.Path(yatest.common.source_path("ydb/library/benchmarks/runner/upload_results.py")).resolve()
    cmd = ["python3", str(uploader)]
    cmd += ["--result-path", str(result_path)]
    cmd += ["--s3-folder", str(s3_folder)]
    cmd += ["--try-num", str(try_num)] if try_num else []
    yatest.common.execute(cmd, stdout=sys.stdout, stderr=sys.stderr)


def test_tpc():
    is_ci = os.environ.get("CURRENT_PUBLIC_DIR") is not None

    runner = Runner()
    runner.wrapped_run("h", 1, 1, None)
    result_path = runner.results_path.resolve()
    print("Results path: ", result_path, file=sys.stderr)

    if is_ci:
        s3_folder = pathlib.Path(os.environ["CURRENT_PUBLIC_DIR"]).resolve()
        try:
            try_num = int(s3_folder.name.split("try_")[-1])
        except Exception:
            try_num = None

        upload(result_path, s3_folder, try_num)
