from pathlib import Path
import subprocess
import os
import sys

import wasabi

TEST_DATA = Path(__file__).absolute().parent / "test-data"
WASABI_DIR = Path(wasabi.__file__).absolute().parent.parent


def test_jupyter():
    # This runs some code in a jupyter notebook environment, but without actually
    # starting up the notebook UI. Historically we once had a bug that caused crashes
    # when importing wasabi in a jupyter notebook, because they replace
    # sys.stdout/stderr with custom objects that aren't "real" files/ttys. So this makes
    # sure that we can import and use wasabi inside a notebook without crashing.
    env = dict(os.environ)
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{WASABI_DIR}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = str(WASABI_DIR)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "nbconvert",
            str(TEST_DATA / "wasabi-test-notebook.ipynb"),
            "--execute",
            "--stdout",
            "--to",
            "notebook",
        ],
        env=env,
        check=True,
    )
