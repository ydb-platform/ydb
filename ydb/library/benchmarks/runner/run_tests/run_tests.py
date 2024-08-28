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


def parse_args(passed=None):

    parser = argparse.ArgumentParser()

    parser.add_argument('--datasize', type=int, default=1)
    parser.add_argument('--variant', type=variant, default='h')
    parser.add_argument('--tasks', type=int, default=1)

    parser.add_argument('--dqrun', type=pathlib.Path)
    parser.add_argument('--gen-queries', type=pathlib.Path)
    parser.add_argument('--downloaders-dir', type=pathlib.Path)
    parser.add_argument('--udfs-dir', type=paths)
    parser.add_argument('--fs-cfg', type=pathlib.Path)
    parser.add_argument('--flame-graph', type=pathlib.Path)
    parser.add_argument('--result-compare', type=pathlib.Path)
    parser.add_argument('--gateways-cfg', type=pathlib.Path)
    parser.add_argument('--runner-path', type=pathlib.Path)

    parser.add_argument('-o', '--output', default="./results")
    parser.add_argument('--clean-old', action="store_true", default=False)
    parser.add_argument('--query-filter', action="append", default=[])

    return parser.parse_args(passed)


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
        subprocess.run(cmd)

    def prepare_tpc_dir(self):
        print("Preparing tpc...", file=stderr)
        cmd = [f"{self.args.downloaders_dir}/download_files_{self.args.variant}_{self.args.datasize}.sh"]
        subprocess.run(cmd)

    def __init__(self, args, enable_spilling):
        self.args = args
        self.enable_spilling = enable_spilling

        self.queries_dir = pathlib.Path(f"queries{"+" if self.enable_spilling else "-"}spilling-{args.datasize}-{args.tasks}").resolve()
        if self.args.clean_old or not self.queries_dir.exists():
            self.prepare_queries_dir([
                f"dq.MaxTasksPerStage={self.args.tasks}",
                "dq.OptLLVM=ON"
            ] + [
                "dq.UseFinalizeByKey=true",
                "dq.EnableSpillingNodes=All",
            ] if self.enable_spilling else [])

        self.tpc_dir = pathlib.Path(f"{self.args.downloaders_dir}/tpc/{self.args.variant}/{self.args.datasize}").resolve()
        if self.args.clean_old or not self.tpc_dir.exists():
            self.prepare_tpc_dir()
        if not pathlib.Path("./tpc").exists():
            os.symlink(f"{self.args.downloaders_dir}/tpc", f"{pathlib.Path("./tpc")}", target_is_directory=True)

        self.result_dir = pathlib.Path(f"{self.args.output}/{"with" if self.enable_spilling else "no"}-spilling/{args.variant}-{args.datasize}-{args.tasks}").resolve()
        self.result_dir.mkdir(parents=True, exist_ok=True)

    def run(self):
        cmd = ["/usr/bin/time", f"{str(self.args.runner_path)}"]
        # cmd += ["--perf"]
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

        return self.result_dir


def result_compare(args, to_compare):
    cmd = [f"{args.result_compare}"]
    cmd += ["-v"]
    cmd += to_compare
    with open(f"{args.output}/result-{args.variant}-{args.datasize}-{args.tasks}.htm", "w") as result_table:
        subprocess.run(cmd, stdout=result_table)


def run(passed=None):
    args = parse_args(passed)

    results = []
    print("With spilling...", file=stderr)
    results.append(Runner(args, True).run())
    print("No spilling...", file=stderr)
    results.append(Runner(args, False).run())

    result_compare(args, results)


def main():
    run()


if __name__ == "__main__":
    main()
