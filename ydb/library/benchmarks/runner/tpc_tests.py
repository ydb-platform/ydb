import run_tests.run_tests as run_tests


def wrapped_run(variant, datasize, tasks):
    cmd = []
    cmd += ["--variant", f"{variant}"]
    cmd += ["--datasize", f"{datasize}"]
    cmd += ["--tasks", f"{tasks}"]
    cmd += ["--ydb-root", "/home/vladluk/ydbwork/ydb/ydb"]
    cmd += ["-o", "/home/vladluk/ydbwork/ydb/ydb/library/benchmarks/runner/results"]
    run_tests.main(cmd)


def test_tpc_h_10_1():
    wrapped_run("h", 10, 1)
