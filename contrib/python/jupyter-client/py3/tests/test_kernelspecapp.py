"""Tests for the kernelspecapp"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os
import warnings
from pathlib import Path

import pytest

from jupyter_client.kernelspecapp import (
    InstallKernelSpec,
    KernelSpecApp,
    ListKernelSpecs,
    ListProvisioners,
    RemoveKernelSpec,
)


@pytest.mark.xfail
def test_kernelspec_sub_apps(jp_kernel_dir):
    app = InstallKernelSpec()
    prefix = os.path.dirname(os.environ["JUPYTER_DATA_DIR"])
    kernel_dir = os.path.join(prefix, "share/jupyter/kernels")
    app.kernel_spec_manager.kernel_dirs.append(kernel_dir)
    app.prefix = prefix
    app.initialize([str(jp_kernel_dir)])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        app.start()

    app1 = ListKernelSpecs()
    app1.kernel_spec_manager.kernel_dirs.append(kernel_dir)
    specs = app1.start()
    assert specs and "echo" in specs

    app2 = RemoveKernelSpec(spec_names=["echo"], force=True)
    app2.kernel_spec_manager.kernel_dirs.append(kernel_dir)
    app2.start()

    app3 = ListKernelSpecs()
    app3.kernel_spec_manager.kernel_dirs.append(kernel_dir)
    specs = app3.start()
    assert specs and "echo" not in specs

    app4 = ListKernelSpecs(missing_kernels=True)
    app4.kernel_spec_manager.kernel_dirs.append(kernel_dir)
    specs = app4.start()
    assert specs is None


def test_kernelspec_app():
    app = KernelSpecApp()
    app.initialize(["list"])
    app.start()


def test_list_provisioners_app():
    app = ListProvisioners()
    app.initialize([])
    app.start()


@pytest.fixture
def dummy_kernelspecs():
    import sys

    p = Path.cwd().resolve()
    # some missing kernelspecs
    out = {
        name: {
            "resource_dir": str(p / name),
            "spec": {
                "argv": [
                    str(p / name / "bin" / "python"),
                    "-Xfrozen_modules=off",
                    "-m",
                    "ipykernel_launcher",
                    "-f",
                    "{connection_file}",
                ],
                "env": {},
                "display_name": "Python [venv: dummy0]",
                "language": "python",
                "interrupt_mode": "signal",
                "metadata": {"debugger": True},
            },
        }
        for name in ("dummy0", "dummy1")
    }

    out["good"] = {
        "resource_dir": str(p / "good"),
        "spec": {
            "argv": [
                sys.executable,
                "-Xfrozen_modules=off",
                "-m",
                "ipykernel_launcher",
                "-f",
                "{connection_file}",
            ],
            "env": {},
            "display_name": "Python [venv: dummy0]",
            "language": "python",
            "interrupt_mode": "signal",
            "metadata": {"debugger": True},
        },
    }
    return out


def test__limit_to_missing(dummy_kernelspecs) -> None:
    from jupyter_client.kernelspecapp import _limit_to_missing

    paths = {k: v["resource_dir"] for k, v in dummy_kernelspecs.items()}

    paths, specs = _limit_to_missing(paths, dummy_kernelspecs)

    assert specs == {k: v for k, v in dummy_kernelspecs.items() if k != "good"}
    assert paths == {k: v["resource_dir"] for k, v in dummy_kernelspecs.items() if k != "good"}
