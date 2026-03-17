import sys
from subprocess import run


def test_runapp(tmp_path):
    test_py = tmp_path / "test.py"
    with test_py.open("w") as f:
        f.write("print('hello')")

    p = run(
        [sys.executable, "-m", "jupyter_client.runapp", str(test_py)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert p.stdout.strip() == "hello"


def test_no_such_kernel(tmp_path):
    test_py = tmp_path / "test.py"
    with test_py.open("w") as f:
        f.write("print('hello')")
    kernel_name = "nosuchkernel"
    p = run(
        [sys.executable, "-m", "jupyter_client.runapp", "--kernel", kernel_name, str(test_py)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert p.returncode
    assert "Could not find kernel" in p.stderr
    assert kernel_name in p.stderr
    # shouldn't show a traceback
    assert "Traceback" not in p.stderr
