import argparse
import subprocess
import pathlib
import os
from sys import stderr


def variant(string):
    if string not in ["h", "ds"]:
        raise ValueError("variant must be h or ds")
    return string


def paths(string):
    return list(map(pathlib.Path, string.split(";")))


def parse_args():
    subparser = argparse.ArgumentParser()

    subparser.add_argument('--is-test', action="store_true", default=False)

    subparser.add_argument('--datasize', type=int, default=1)
    subparser.add_argument('--variant', type=variant, default='h')
    subparser.add_argument('--tasks', type=int, default=1)
    subparser.add_argument('--perf', action="store_true", default=False)

    subparser.add_argument('-o', '--output', default="./results")
    subparser.add_argument('--clean-old', action="store_true", default=False)
    subparser.add_argument('--query-filter', action="append", default=[])

    args, argv = subparser.parse_known_args()

    if args.is_test:
        parser = argparse.ArgumentParser()

        parser.add_argument('--dqrun', type=pathlib.Path)
        parser.add_argument('--gen-queries', type=pathlib.Path)
        parser.add_argument('--downloaders-dir', type=pathlib.Path)
        parser.add_argument('--udfs-dir', type=paths)
        parser.add_argument('--fs-cfg', type=pathlib.Path)
        parser.add_argument('--flame-graph', type=pathlib.Path)
        parser.add_argument('--result-compare', type=pathlib.Path)
        parser.add_argument('--gateways-cfg', type=pathlib.Path)
        parser.add_argument('--runner-path', type=pathlib.Path)

        return parser.parse_args(argv, namespace=args)
    else:
        parser = argparse.ArgumentParser()

        parser.add_argument('--ydb-root', type=lambda path: pathlib.Path(path).resolve(), default="../../../../")

        args, argv = parser.parse_known_args(argv, namespace=args)

        args.gen_queries = args.ydb_root / "ydb" / "library" / "benchmarks" / "gen_queries" / "gen_queries"
        args.downloaders_dir = args.ydb_root / "ydb" / "library" / "benchmarks" / "runner"
        args.flame_graph = args.ydb_root / "contrib" / "tools" / "flame-graph"
        args.result_compare = args.ydb_root / "ydb" / "library" / "benchmarks" / "runner" / "result_compare" / "result_compare"
        args.runner_path = args.ydb_root / "ydb" / "library" / "benchmarks" / "runner" / "runner" / "runner"

        def_dqrun = args.ydb_root / "ydb" / "library" / "yql" / "tools" / "dqrun" / "dqrun"
        def_fs_cfg = args.ydb_root / "ydb" / "library" / "yql" / "tools" / "dqrun" / "examples" / "fs.conf"
        def_gateways_cfg = args.ydb_root / "ydb" / "library" / "benchmarks" / "runner" / "runner" / "test-gateways.conf"

        override_parser = argparse.ArgumentParser()
        override_parser.add_argument('--dqrun', type=pathlib.Path, default=def_dqrun)
        override_parser.add_argument('--fs-cfg', type=pathlib.Path, default=def_fs_cfg)
        override_parser.add_argument('--gateways-cfg', type=pathlib.Path, default=def_gateways_cfg)

        args = override_parser.parse_args(argv, namespace=args)

        udfs_prefix = args.ydb_root / "yql" / "essentials" / "udfs" / "common"
        args.udfs_dir = [udfs_prefix / name for name in ["set", "url_base", "datetime2", "re2", "math", "unicode_base"]]

        return args


class Runner:
    def prepare_queries_dir(self, custom_pragmas):
        print("Preparing queries...", file=stderr)
        self.queries_dir.mkdir(parents=True, exist_ok=True)
        cmd = [str(self.args.gen_queries)]
        cmd += ["--output", f"{self.queries_dir}"]
        cmd += ["--variant", f"{self.args.variant}"]
        cmd += ["--syntax", "yql"]
        cmd += ["--dataset-size", f"{self.args.datasize}"]
        for it in custom_pragmas:
            cmd += ["--pragma", it]
        res = subprocess.run(cmd)
        if res.returncode != 0:
            raise OSError("Failed to prepare queries")

    def prepare_tpc_dir(self):
        print("Preparing tpc...", file=stderr)
        cmd = [f"./download_files_{self.args.variant}_{self.args.datasize}.sh"]
        res = subprocess.run(cmd, cwd=self.args.downloaders_dir)
        if res.returncode != 0:
            raise OSError("Failed to prepare tpc")

    def __init__(self, args, enable_spilling):
        self.args = args
        self.enable_spilling = enable_spilling

        self.queries_dir = pathlib.Path(f"queries{"+" if self.enable_spilling else "-"}spilling-{args.datasize}-{args.tasks}")
        if self.args.clean_old or not self.queries_dir.exists():
            self.prepare_queries_dir([
                f"dq.MaxTasksPerStage={self.args.tasks}",
                "dq.OptLLVM=ON",
                "dq.UseFinalizeByKey=true",
            ] + ([
                "dq.EnableSpillingInChannels=true",
                "dq.EnableSpillingNodes=All",
            ] if self.enable_spilling else []))
        self.tpc_dir = pathlib.Path(f"{self.args.downloaders_dir}/tpc/{self.args.variant}/{self.args.datasize}")
        if not self.tpc_dir.exists():
            self.prepare_tpc_dir()
        if not pathlib.Path("./tpc").exists():
            os.symlink(f"{self.args.downloaders_dir}/tpc", f"{pathlib.Path("./tpc")}", target_is_directory=True)

        self.result_dir = pathlib.Path(f"{self.args.output}/{"with" if self.enable_spilling else "no"}-spilling/{args.variant}-{args.datasize}-{args.tasks}").resolve()
        self.result_dir.mkdir(parents=True, exist_ok=True)

    def run(self):
        cmd = ["/usr/bin/time", f"{str(self.args.runner_path)}"]
        cmd += ["--perf"] if self.args.perf else []
        for it in self.args.query_filter:
            cmd += ["--include-q", it]
        cmd += ["--query-dir", f"{str(self.queries_dir)}/{self.args.variant}"]
        cmd += ["--bindings", f"{str(self.queries_dir)}/{self.args.variant}/bindings.json"]
        cmd += ["--result-dir", str(self.result_dir)]
        cmd += ["--flame-graph", str(self.args.flame_graph)]
        cmd += [f"{self.args.dqrun}", "-s"]
        cmd += ["--enable-spilling"] if self.enable_spilling else []
        cmd += ["--udfs-dir", ";".join(map(str, self.args.udfs_dir))]
        cmd += ["--fs-cfg", f"{str(self.args.fs_cfg)}"]
        cmd += ["--gateways-cfg", f"{str(self.args.gateways_cfg)}"]
        print("Running runner...", file=stderr)
        subprocess.run(cmd)

        print("Run results at: ", self.result_dir)
        return self.result_dir


def result_compare(args, to_compare):
    print("Comparing...")
    cmd = [f"{args.result_compare}"]
    cmd += ["-v"]
    cmd += to_compare
    with open(f"{args.output}/result-{args.variant}-{args.datasize}-{args.tasks}.htm", "w") as result_table:
        res = subprocess.run(cmd, stdout=result_table)
    if res.returncode != 0:
        raise OSError("Failed to compare result")


def main():
    args = parse_args()

    results = []
    print("With spilling...", file=stderr)
    results.append(Runner(args, True).run())
    print("No spilling...", file=stderr)
    results.append(Runner(args, False).run())

    if not args.is_test:
        result_compare(args, results)


if __name__ == "__main__":
    main()
