import os

import pytest
from _pytest._io import TerminalWriter

from xprocess import XProcess


def get_log_files(root_dir):
    proc_dirs = [f.path for f in os.scandir(root_dir) if f.is_dir()]
    return [
        os.path.join(proc_dir, f)
        for proc_dir in proc_dirs
        for f in os.listdir(proc_dir)
        if f.endswith("log")
    ]


def getrootdir(config):
    return config.cache.makedir(".xprocess")


def pytest_addoption(parser):
    group = parser.getgroup(
        "xprocess", "managing external processes across test-runs [xprocess]"
    )
    group.addoption("--xkill", action="store_true", help="kill all external processes")
    group.addoption(
        "--xshow", action="store_true", help="show status of external process"
    )


def pytest_cmdline_main(config):
    xkill = config.option.xkill
    xshow = config.option.xshow
    if xkill or xshow:
        config._do_configure()
        tw = TerminalWriter()
        rootdir = getrootdir(config)
        xprocess = XProcess(config, rootdir)
    if xkill:
        return xprocess._xkill(tw)
    if xshow:
        return xprocess._xshow(tw)


@pytest.fixture(scope="session")
def xprocess(request):
    """yield session-scoped XProcess helper to manage long-running
    processes required for testing."""

    rootdir = getrootdir(request.config)
    with XProcess(request.config, rootdir) as xproc:
        # pass in xprocess object into pytest_unconfigure
        # through config for proper cleanup during teardown
        request.config._xprocess = xproc
        # start every run with clean log files
        for log_file in get_log_files(xproc.rootdir):
            open(log_file, errors="surrogateescape").close()
        yield xproc


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    logfiles = getattr(item.config, "_extlogfiles", None)
    report = yield
    if logfiles is None:
        return
    for name in sorted(logfiles):
        content = logfiles[name].read()
        if content:
            longrepr = getattr(report, "longrepr", None)
            if hasattr(longrepr, "addsection"):  # pragma: no cover
                longrepr.addsection("%s log" % name, content)


def pytest_unconfigure(config):
    verbosity_level = config.getoption("verbose")
    if verbosity_level >= 1:
        print(
            "pytest-xprocess reminder::Be sure to terminate the started process "
            "by running 'pytest --xkill' if you have not explicitly done so in your "
            "fixture with 'xprocess.getinfo(<process_name>).terminate()'."
        )


def pytest_configure(config):
    config.pluginmanager.register(InterruptionHandler())


class InterruptionHandler:
    """The purpose of this class is exposing the
    config object containing references necessary
    to properly clean-up in the event of an exception
    during test runs"""

    def pytest_configure(self, config):
        self.config = config

    def info_objects(self):
        return [xrsc.info for xrsc in self.config._xprocess.resources]

    def interruption_clean_up(self):
        try:
            xprocess = self.config._xprocess
        except AttributeError:
            pass
        else:
            for info, terminate_on_interrupt in self.info_objects():
                if terminate_on_interrupt:
                    info.terminate()
            xprocess._force_clean_up()

    def pytest_keyboard_interrupt(self, excinfo):
        self.interruption_clean_up()

    def pytest_internalerror(self, excrepr, excinfo):
        self.interruption_clean_up()
