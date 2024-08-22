import argparse
import subprocess
import pathlib
from sys import stderr

def parse_args(passed=None):
    YDB_ROOT = "../../../"
    
    def variant(string):
        if string not in ["h", "ds"]:
            raise ValueError("variant must be h or ds")
        return string
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--datasize', type=int, default=1)
    parser.add_argument('--variant', type=variant, default='h')
    parser.add_argument('--tasks', type=int, default=1)
    parser.add_argument('--ydb-root', type=lambda path: pathlib.Path(path).resolve(), default=YDB_ROOT)
    parser.add_argument('-o', '--output', default="./results")
    parser.add_argument('--clean-old', action="store_true", default=False)
    
    return parser.parse_known_intermixed_args(passed)

class Runner:
    def prepare_queries_dir(self, custom_pragmas):
        self.queries_dir.mkdir(parents=True, exist_ok=True)
        cmd = [self.args.gen_queries]
        cmd += ["--output", f"{self.queries_dir}"]
        cmd += ["--variant", f"{self.args.variant}"]
        cmd += ["--syntax", "yql"]
        cmd += ["--dataset-size", f"{self.args.datasize}"]
        for it in custom_pragmas:
            cmd += ["--pragma", it]
        subprocess.run(cmd)
    
    def prepare_tpc_dir(self):
        cmd = [f"{self.args.downloaders_dir}/download_files_{self.args.variant}_{self.args.datasize}.sh"]
        subprocess.run(cmd)
    
    def __init__(self, args, enable_spilling):
        self.args = args
        self.enable_spilling = enable_spilling
        
        self.queries_dir = pathlib.Path(f"queries{"+" if self.enable_spilling else "-"}spilling-{args.datasize}-{args.tasks}")
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
        
        self.result_dir = pathlib.Path(f"{self.args.output}/{"with" if self.enable_spilling else "no"}-spilling/{args.variant}-{args.datasize}-{args.tasks}").resolve()
        self.result_dir.mkdir(parents=True, exist_ok=True)

    def run(self):
        cmd = ["/usr/bin/time", f"{self.args.runner_path}"]
        cmd += ["--perf"]
        cmd += ["--query-dir", f"{self.queries_dir}/{self.args.variant}"]
        cmd += ["--bindings", f"{self.queries_dir}/{self.args.variant}/bindings.json"]
        cmd += ["--result-dir", f"{self.result_dir}"]
        cmd += [f"{self.args.dqrun}", "-s"]
        cmd += ["--enable-spilling"] if self.enable_spilling else []
        cmd += ["--udfs-dir", f"{self.args.udfs_dir}"]
        cmd += ["--fs-cfg", f"{self.args.fs_cfg}"]
        cmd += ["--gateways-cfg", f"{self.args.gateways_cfg}"]
        subprocess.run(cmd)
        
        return self.result_dir

def result_compare(args, to_compare):
    cmd = [f"{args.result_compare}"]
    cmd += ["-v"]
    cmd += to_compare
    print(cmd, file=stderr)
    with open(f"{args.output}/result-{args.variant}-{args.datasize}-{args.tasks}.htm", "w") as result_table:
        subprocess.run(cmd, stdout=result_table)

def main(passed=None):
    args, _ = parse_args(passed)
    args.dqrun = args.ydb_root / "library" / "yql" / "tools" / "dqrun" / "dqrun"
    args.gen_queries = args.ydb_root / "library" / "benchmarks" / "gen_queries" / "gen_queries"
    args.downloaders_dir = args.ydb_root / "library" / "benchmarks" / "runner"
    args.udfs_dir = args.ydb_root / "library" / "yql" / "udfs" / "common"
    args.fs_cfg = args.ydb_root / "library" / "yql" / "tools" / "dqrun" / "examples" / "fs.conf"
    args.result_compare = args.ydb_root / "library" / "benchmarks" / "runner" / "result_compare" / "result_compare"
    args.gateways_cfg = args.ydb_root / "library" / "benchmarks" / "runner" / "runner" / "test-gateways.conf"
    args.runner_path = args.ydb_root / "library" / "benchmarks" / "runner" / "runner" / "runner"
    
    print(args)
    
    results = []
    print("With spilling...", file=stderr)
    results.append(Runner(args, True).run())
    print("No spilling...", file=stderr)
    results.append(Runner(args, False).run())
    
    print(results, file=stderr)
    
    result_compare(args, results)

if __name__ == "__main__":
    main()
    