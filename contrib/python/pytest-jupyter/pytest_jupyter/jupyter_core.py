"""Fixtures for use with jupyter core and downstream."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import json
import os
import sys
import warnings
from inspect import iscoroutinefunction
from pathlib import Path

import jupyter_core
import pytest
from jupyter_core.utils import ensure_event_loop

from .utils import mkdir

try:
    import resource
except ImportError:
    # Windows
    resource = None  # type:ignore[assignment]


# Handle resource limit
# Ensure a minimal soft limit of DEFAULT_SOFT if the current hard limit is at least that much.
if resource is not None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

    DEFAULT_SOFT = 4096
    if hard >= DEFAULT_SOFT:
        soft = DEFAULT_SOFT

    if hard < soft:
        hard = soft

    resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


@pytest.fixture(autouse=True)
def jp_asyncio_loop():
    """Get an asyncio loop."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*WindowsSelectorEventLoopPolicy.*",
            category=DeprecationWarning,
        )
        loop = ensure_event_loop(prefer_selector_loop=True)
    yield loop
    loop.close()


@pytest.hookimpl(tryfirst=True)
def pytest_pycollect_makeitem(collector, name, obj):
    """Custom pytest collection hook."""
    if collector.funcnamefilter(name) and iscoroutinefunction(obj):
        return list(collector._genfunctions(name, obj))
    return None


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    """Custom pytest function call hook."""
    funcargs = pyfuncitem.funcargs
    testargs = {arg: funcargs[arg] for arg in pyfuncitem._fixtureinfo.argnames}

    if not iscoroutinefunction(pyfuncitem.obj):
        pyfuncitem.obj(**testargs)
        return True

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*WindowsSelectorEventLoopPolicy.*",
            category=DeprecationWarning,
        )
        loop = ensure_event_loop(prefer_selector_loop=True)
    loop.run_until_complete(pyfuncitem.obj(**testargs))
    return True


@pytest.fixture
def jp_home_dir(tmp_path):
    """Provides a temporary HOME directory value."""
    return mkdir(tmp_path, "home")


@pytest.fixture
def jp_data_dir(tmp_path):
    """Provides a temporary Jupyter data dir directory value."""
    return mkdir(tmp_path, "data")


@pytest.fixture
def jp_config_dir(tmp_path):
    """Provides a temporary Jupyter config dir directory value."""
    return mkdir(tmp_path, "config")


@pytest.fixture
def jp_runtime_dir(tmp_path):
    """Provides a temporary Jupyter runtime dir directory value."""
    return mkdir(tmp_path, "runtime")


@pytest.fixture
def jp_system_jupyter_path(tmp_path):
    """Provides a temporary Jupyter system path value."""
    return mkdir(tmp_path, "share", "jupyter")


@pytest.fixture
def jp_env_jupyter_path(tmp_path):
    """Provides a temporary Jupyter env system path value."""
    return mkdir(tmp_path, "env", "share", "jupyter")


@pytest.fixture
def jp_system_config_path(tmp_path):
    """Provides a temporary Jupyter config path value."""
    return mkdir(tmp_path, "etc", "jupyter")


@pytest.fixture
def jp_env_config_path(tmp_path):
    """Provides a temporary Jupyter env config path value."""
    return mkdir(tmp_path, "env", "etc", "jupyter")


@pytest.fixture
def jp_kernel_dir(jp_data_dir):
    """Get the directory for kernel specs."""
    return mkdir(jp_data_dir, "kernels")


@pytest.fixture
def echo_kernel_spec(jp_kernel_dir):
    """Install a kernel spec for the echo kernel."""
    test_dir = Path(jp_kernel_dir) / "echo"
    test_dir.mkdir(parents=True, exist_ok=True)
    argv = [
        sys.executable,
        "-m",
        "pytest_jupyter.echo_kernel",
        "-f",
        "{connection_file}",
    ]
    kernel_data = {"argv": argv, "display_name": "echo", "language": "echo"}
    spec_file_path = Path(test_dir / "kernel.json")
    spec_file_path.write_text(json.dumps(kernel_data), "utf8")
    return str(test_dir)


@pytest.fixture
def jp_environ(
    monkeypatch,
    tmp_path,
    jp_home_dir,
    jp_data_dir,
    jp_config_dir,
    jp_runtime_dir,
    echo_kernel_spec,
    jp_system_jupyter_path,
    jp_system_config_path,
    jp_env_jupyter_path,
    jp_env_config_path,
):
    """Configures a temporary environment based on Jupyter-specific environment variables."""
    monkeypatch.setenv("HOME", str(jp_home_dir))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))
    # monkeypatch.setenv("JUPYTER_NO_CONFIG", "1")
    monkeypatch.setenv("JUPYTER_CONFIG_DIR", str(jp_config_dir))
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(jp_data_dir))
    monkeypatch.setenv("JUPYTER_RUNTIME_DIR", str(jp_runtime_dir))
    monkeypatch.setattr(jupyter_core.paths, "SYSTEM_JUPYTER_PATH", [str(jp_system_jupyter_path)])
    monkeypatch.setattr(jupyter_core.paths, "ENV_JUPYTER_PATH", [str(jp_env_jupyter_path)])
    monkeypatch.setattr(jupyter_core.paths, "SYSTEM_CONFIG_PATH", [str(jp_system_config_path)])
    monkeypatch.setattr(jupyter_core.paths, "ENV_CONFIG_PATH", [str(jp_env_config_path)])
