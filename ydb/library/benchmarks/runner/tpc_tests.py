import run_tests.run_tests as run_tests
import yatest.common
import pathlib
import sys


class TestRunner:
    DEPS = {
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

        print(self.data["donwloaders-dir"], file=sys.stderr)

        self.cmd = []
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
        cmd += ["--query-filter", f"{query_filter}"]
        print(" ".join(cmd), file=sys.stderr)
        run_tests.run(cmd)


def test_tpc():
    runner = TestRunner()
    runner.wrapped_run("h", 1, 1, r"q1\.sql")
    print("results path:", runner.results_path.resolve(), file=sys.stderr)
