import pytest
from _typeshed import Incomplete
from collections.abc import Generator
from logfire import Logfire as Logfire, LogfireSpan as LogfireSpan
from typing import Any

class LogfirePluginConfig:
    """Configuration for the Logfire pytest plugin."""
    logfire_instance: Incomplete
    service_name: Incomplete
    trace_phases: Incomplete
    def __init__(self, logfire_instance: Logfire, service_name: str, trace_phases: bool) -> None: ...

def pytest_addoption(parser: pytest.Parser) -> None:
    """Add Logfire options to pytest."""
def pytest_configure(config: pytest.Config) -> None:
    """Configure Logfire when the plugin is enabled."""
def pytest_xdist_setupnodes(config: Any, specs: Any) -> None:
    """Inject TRACEPARENT into env before xdist spawns workers.

    Called in the controller before any ``makegateway()`` call, so all workers
    inherit the session-level trace context via ``os.environ``.

    NOTE: This relies on ``pytest_sessionstart`` (which creates the session span)
    running at default priority, *before* xdist's ``DSession.pytest_sessionstart``
    which uses ``trylast=True`` and calls ``setup_nodes()`` →
    ``pytest_xdist_setupnodes``.  Do not add ``trylast=True`` to our
    ``pytest_sessionstart`` or this ordering guarantee breaks.
    """
def pytest_sessionstart(session: pytest.Session) -> None:
    """Create a session span when the test session starts.

    IMPORTANT: This hook must run at default priority (no ``trylast=True``).
    ``pytest_xdist_setupnodes`` depends on the session span being active when
    it injects TRACEPARENT into ``os.environ`` for worker processes.  xdist's
    ``DSession.pytest_sessionstart`` uses ``trylast=True``, so our default-priority
    hook is guaranteed to run first.
    """
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None) -> Generator[None]:
    """Create a span for each test."""
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[Any]) -> Generator[None, pytest.TestReport, None]:
    """Record test outcomes and exceptions."""
def pytest_runtest_setup(item: pytest.Item) -> Generator[None]:
    """Trace test setup phase if --logfire-trace-phases is enabled."""
def pytest_runtest_call(item: pytest.Item) -> Generator[None]:
    """Trace test call phase if --logfire-trace-phases is enabled."""
def pytest_runtest_teardown(item: pytest.Item) -> Generator[None]:
    """Trace test teardown phase if --logfire-trace-phases is enabled."""
def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """End the session span and flush traces."""
@pytest.fixture
def logfire_pytest(request: pytest.FixtureRequest) -> Logfire:
    """Provide a Logfire instance configured for the pytest plugin.

    This fixture provides a Logfire instance that sends spans to Logfire when the
    pytest plugin is enabled (via `--logfire` flag). Use this instead of the global
    `logfire` module when you want spans created in tests to be sent to Logfire
    as part of your test traces.

    When the plugin is not enabled, this fixture returns a local-only instance
    that doesn't send data anywhere.
    """
def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> Generator[None]:
    """Re-attach the per-test span context for async test functions.

    The ``pytest_runtest_protocol`` hook creates a span per test and attaches it
    to the OTel context via ``context_api.attach()`` in the **synchronous** hook
    thread.  However, when tests are async (e.g. with anyio/pytest-asyncio), they
    may run inside an event-loop task whose ``contextvars`` snapshot was taken
    before the per-test span was attached (e.g. when ``asyncio.Runner`` reuses a
    saved context on Python 3.11+).  As a result, ``logfire.get_context()`` inside
    an async test can return a stale traceparent from a previous test (or no
    context at all).

    This hook wraps async test functions so that ``context_api.attach()`` is called
    *inside* the coroutine body, making the span visible to the test and any
    callbacks (e.g. httpx event hooks) that call ``logfire.get_context()``.
    """
