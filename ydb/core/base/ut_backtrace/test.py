import yatest.common


def test_crash_reports_build_info():
    binary = yatest.common.binary_path("ydb/core/base/ut_backtrace/helper/helper")

    res = yatest.common.execute([binary], check_exit_code=False)
    stderr = res.std_err.decode("utf-8", errors="replace")

    assert "build: commit " in stderr, stderr
    assert ("(dirty)" in stderr) or ("(clean)" in stderr), stderr
