from __future__ import annotations

import asyncio
import os
import shutil
import sys
from tempfile import mkdtemp
from unittest.mock import patch

import pytest
from traitlets import Integer

from jupyter_core.application import JupyterApp, JupyterAsyncApp, NoStart
from jupyter_core.utils import ensure_event_loop

pjoin = os.path.join


@pytest.fixture(autouse=True)
def patch_data_dir(monkeypatch):
    import yatest.common
    monkeypatch.setenv('JUPYTER_DATA_DIR', yatest.common.work_path())


def test_basic():
    JupyterApp()


def test_default_traits():
    app = JupyterApp()
    for trait_name in app.traits():
        getattr(app, trait_name)


class DummyApp(JupyterApp):
    name = "dummy-app"
    m = Integer(0, config=True)
    n = Integer(0, config=True)


_dummy_config = """
c.DummyApp.n = 10
"""


def test_custom_config():
    app = DummyApp()
    td = mkdtemp()
    fname = pjoin(td, "config.py")
    with open(fname, "w", encoding="utf-8") as f:
        f.write(_dummy_config)
    app.initialize(["--config", fname])
    shutil.rmtree(td)
    assert app.config_file == fname
    assert app.n == 10


def test_cli_override():
    app = DummyApp()
    td = mkdtemp()
    fname = pjoin(td, "config.py")
    with open(fname, "w", encoding="utf-8") as f:
        f.write(_dummy_config)
    app.initialize(["--config", fname, "--DummyApp.n=20"])
    shutil.rmtree(td)
    assert app.n == 20


def test_generate_config():
    td = mkdtemp()
    app = DummyApp(config_dir=td)
    app.initialize(["--generate-config"])
    assert app.generate_config

    with pytest.raises(NoStart):
        app.start()

    assert os.path.exists(os.path.join(td, "dummy_app_config.py"))


def test_load_config():
    config_dir = mkdtemp()
    os.environ["JUPYTER_CONFIG_PATH"] = str(config_dir)
    with open(pjoin(config_dir, "dummy_app_config.py"), "w", encoding="utf-8") as f:
        f.write("c.DummyApp.m = 1\n")
        f.write("c.DummyApp.n = 1")

    app = DummyApp(config_dir=config_dir)
    app.initialize([])

    assert app.n == 1, "Loaded config from config dir"
    assert app.m == 1, "Loaded config from config dir"

    shutil.rmtree(config_dir)
    del os.environ["JUPYTER_CONFIG_PATH"]


def test_load_config_no_cwd():
    config_dir = mkdtemp()
    wd = mkdtemp()
    with open(pjoin(wd, "dummy_app_config.py"), "w", encoding="utf-8") as f:
        f.write("c.DummyApp.m = 1\n")
        f.write("c.DummyApp.n = 1")
    with patch.object(os, "getcwd", lambda: wd):
        app = DummyApp(config_dir=config_dir)
        app.initialize([])

    assert app.n == 0
    assert app.m == 0

    shutil.rmtree(config_dir)
    shutil.rmtree(wd)


def test_load_bad_config():
    config_dir = mkdtemp()
    os.environ["JUPYTER_CONFIG_PATH"] = str(config_dir)
    with open(pjoin(config_dir, "dummy_app_config.py"), "w", encoding="utf-8") as f:
        f.write('c.DummyApp.m = "a\n')  # Syntax error

    with pytest.raises(SyntaxError):  # noqa: PT012
        app = DummyApp(config_dir=config_dir)
        app.raise_config_file_errors = True
        app.initialize([])

    shutil.rmtree(config_dir)
    del os.environ["JUPYTER_CONFIG_PATH"]


def test_runtime_dir_changed():
    app = DummyApp()
    td = mkdtemp()
    shutil.rmtree(td)
    app.runtime_dir = td
    assert os.path.isdir(td)
    shutil.rmtree(td)


class AsyncioRunApp(JupyterApp):
    async def _inner(self):
        pass

    def start(self):
        asyncio.run(self._inner())


def test_asyncio_run():
    AsyncioRunApp.launch_instance([])
    AsyncioRunApp.clear_instance()


class SyncTornadoApp(JupyterApp):
    async def _inner(self):
        self.running_loop = asyncio.get_running_loop()

    def start(self):
        self.starting_loop = ensure_event_loop()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._inner())
        loop.close()


def test_sync_tornado_run():
    SyncTornadoApp.launch_instance([])
    app = SyncTornadoApp.instance()
    assert app.running_loop == app.starting_loop
    SyncTornadoApp.clear_instance()


class AsyncApp(JupyterAsyncApp):
    async def initialize_async(self, argv):
        self.value = 10

    async def start_async(self):
        assert self.value == 10


def test_async_app():
    AsyncApp.launch_instance([])
    app = AsyncApp.instance()
    assert app.value == 10
    AsyncApp.clear_instance()


class AsyncTornadoApp(AsyncApp):
    _prefer_selector_loop = True


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_async_tornado_app():
    AsyncTornadoApp.launch_instance([])
    app = AsyncApp.instance()
    assert app._prefer_selector_loop is True
    AsyncTornadoApp.clear_instance()
