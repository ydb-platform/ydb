"""Pytest plugin for Logfire distributed tracing.

This module implements a pytest plugin that enables distributed tracing for tests.
The plugin is enabled via `--logfire` command-line option or auto-enabled in CI
when `LOGFIRE_TOKEN` is present.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import TYPE_CHECKING, Any

from opentelemetry.trace import StatusCode

if TYPE_CHECKING:
    import pytest

    from logfire import Logfire, LogfireSpan

# Stash keys for storing plugin data
try:
    import pytest

    _CONFIG_KEY: pytest.StashKey[LogfirePluginConfig] = pytest.StashKey()
    _SESSION_SPAN_KEY: pytest.StashKey[LogfireSpan] = pytest.StashKey()
    _SPAN_KEY: pytest.StashKey[Any] = pytest.StashKey()  # Stores OTel span
    _CONTEXT_TOKEN_KEY: pytest.StashKey[Any] = pytest.StashKey()  # Stores context token from TRACEPARENT
except ImportError:  # pragma: no cover
    pass


class LogfirePluginConfig:
    """Configuration for the Logfire pytest plugin."""

    def __init__(self, logfire_instance: Logfire, service_name: str, trace_phases: bool) -> None:
        self.logfire_instance = logfire_instance
        self.service_name = service_name
        self.trace_phases = trace_phases


def _is_enabled(config: pytest.Config) -> bool:
    """Check if the Logfire plugin is enabled."""
    # Explicit --no-logfire always disables
    if config.getoption('--no-logfire', default=False):
        return False

    # Explicit --logfire flag
    if config.getoption('--logfire', default=False):
        return True

    # INI option
    if config.getini('logfire'):
        return True

    # Auto-enable in CI
    if _should_auto_enable():
        return True

    return False


def _should_auto_enable() -> bool:
    """Auto-enable in CI environments when token is present."""
    if os.environ.get('CI', '').lower() not in ('true', '1'):
        return False
    if not os.environ.get('LOGFIRE_TOKEN'):
        return False
    return True


def _get_service_name(config: pytest.Config) -> str:
    """Get the service name from configuration."""
    # CLI option takes precedence
    cli_value = config.getoption('--logfire-service-name', default=None)
    if cli_value:
        return cli_value

    # Environment variable
    env_value = os.environ.get('LOGFIRE_SERVICE_NAME')
    if env_value:
        return env_value

    # INI option
    ini_value = config.getini('logfire_service_name')
    if ini_value:
        return str(ini_value)

    # Default
    return 'pytest'


def _get_trace_phases(config: pytest.Config) -> bool:
    """Check if phase tracing is enabled."""
    # CLI option takes precedence
    if config.getoption('--logfire-trace-phases', default=False):
        return True

    # INI option
    return bool(config.getini('logfire_trace_phases'))


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add Logfire options to pytest."""
    group = parser.getgroup('logfire', 'Logfire distributed tracing')
    group.addoption(
        '--logfire',
        action='store_true',
        default=False,
        help='Enable Logfire tracing for tests',
    )
    group.addoption(
        '--no-logfire',
        action='store_true',
        default=False,
        help='Disable Logfire tracing (overrides auto-enable)',
    )
    group.addoption(
        '--logfire-service-name',
        action='store',
        default=None,
        help='Service name for Logfire traces (default: pytest)',
    )
    group.addoption(
        '--logfire-trace-phases',
        action='store_true',
        default=False,
        help='Trace test setup/call/teardown phases',
    )

    parser.addini('logfire', 'Enable Logfire tracing', type='bool', default=False)
    parser.addini('logfire_service_name', 'Service name for traces', default='pytest')
    parser.addini('logfire_trace_phases', 'Trace test phases', type='bool', default=False)


def pytest_configure(config: pytest.Config) -> None:
    """Configure Logfire when the plugin is enabled."""
    if not _is_enabled(config):
        return

    import logfire

    service_name = _get_service_name(config)

    # Create a scoped logfire instance for pytest spans
    logfire_instance = logfire.configure(
        local=True,
        service_name=service_name,
        send_to_logfire='if-token-present',
        inspect_arguments=False,  # Avoid introspection warnings in pytest
        scrubbing=False,  # Disable scrubbing for test spans
    ).with_settings(custom_scope_suffix='pytest')

    # Store config in stash for later use
    config.stash[_CONFIG_KEY] = LogfirePluginConfig(
        logfire_instance=logfire_instance,
        service_name=service_name,
        trace_phases=_get_trace_phases(config),
    )


def _add_ci_metadata(span: LogfireSpan) -> None:
    """Add CI-specific metadata to span."""
    # GitHub Actions
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        attrs = {
            'ci.system': 'github-actions',
            'ci.workflow': os.environ.get('GITHUB_WORKFLOW', ''),
            'ci.ref': os.environ.get('GITHUB_REF', ''),
            'ci.sha': os.environ.get('GITHUB_SHA', ''),
        }
        job_id = os.environ.get('GITHUB_RUN_ID', '')
        if job_id:
            attrs['ci.job.id'] = job_id
        job_url = _build_github_job_url()
        if job_url:
            attrs['ci.job.url'] = job_url
        span.set_attributes(attrs)
        return

    # GitLab CI
    if os.environ.get('GITLAB_CI') == 'true':
        attrs = {
            'ci.system': 'gitlab-ci',
            'ci.ref': os.environ.get('CI_COMMIT_REF_NAME', ''),
            'ci.sha': os.environ.get('CI_COMMIT_SHA', ''),
        }
        job_id = os.environ.get('CI_JOB_ID', '')
        if job_id:
            attrs['ci.job.id'] = job_id
        job_url = os.environ.get('CI_JOB_URL', '')
        if job_url:
            attrs['ci.job.url'] = job_url
        pipeline_id = os.environ.get('CI_PIPELINE_ID', '')
        if pipeline_id:
            attrs['ci.pipeline.id'] = pipeline_id
        span.set_attributes(attrs)
        return

    # CircleCI
    if os.environ.get('CIRCLECI') == 'true':
        attrs = {
            'ci.system': 'circleci',
            'ci.ref': os.environ.get('CIRCLE_BRANCH', ''),
            'ci.sha': os.environ.get('CIRCLE_SHA1', ''),
        }
        job_id = os.environ.get('CIRCLE_BUILD_NUM', '')
        if job_id:
            attrs['ci.job.id'] = job_id
        job_url = os.environ.get('CIRCLE_BUILD_URL', '')
        if job_url:
            attrs['ci.job.url'] = job_url
        span.set_attributes(attrs)
        return

    # Jenkins
    if os.environ.get('JENKINS_URL'):
        attrs = {
            'ci.system': 'jenkins',
            'ci.ref': os.environ.get('GIT_BRANCH', ''),
            'ci.sha': os.environ.get('GIT_COMMIT', ''),
        }
        job_id = os.environ.get('BUILD_NUMBER', '')
        if job_id:
            attrs['ci.job.id'] = job_id
        job_url = os.environ.get('BUILD_URL', '')
        if job_url:
            attrs['ci.job.url'] = job_url
        span.set_attributes(attrs)
        return

    # Generic CI detection
    if os.environ.get('CI'):
        span.set_attribute('ci.system', 'unknown')


def _build_github_job_url() -> str:
    """Build GitHub Actions job URL from environment."""
    server = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')
    repo = os.environ.get('GITHUB_REPOSITORY', '')
    run_id = os.environ.get('GITHUB_RUN_ID', '')
    if repo and run_id:
        return f'{server}/{repo}/actions/runs/{run_id}'
    return ''


def _inject_traceparent_env() -> None:
    """Inject the current OTel trace context into TRACEPARENT/TRACESTATE env vars.

    This allows child processes (xdist workers, subprocess.Popen, etc.) to inherit
    the trace context.
    """
    from opentelemetry import propagate

    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    if 'traceparent' in carrier:  # pragma: no branch
        os.environ['TRACEPARENT'] = carrier['traceparent']
    if 'tracestate' in carrier:  # pragma: no branch
        os.environ['TRACESTATE'] = carrier['tracestate']


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_setupnodes(config: Any, specs: Any) -> None:  # pragma: no cover
    """Inject TRACEPARENT into env before xdist spawns workers.

    Called in the controller before any ``makegateway()`` call, so all workers
    inherit the session-level trace context via ``os.environ``.

    NOTE: This relies on ``pytest_sessionstart`` (which creates the session span)
    running at default priority, *before* xdist's ``DSession.pytest_sessionstart``
    which uses ``trylast=True`` and calls ``setup_nodes()`` →
    ``pytest_xdist_setupnodes``.  Do not add ``trylast=True`` to our
    ``pytest_sessionstart`` or this ordering guarantee breaks.
    """
    del specs  # unused
    if not _is_enabled(config):
        return
    _inject_traceparent_env()


def _get_xdist_worker_id() -> str | None:
    """Get the xdist worker ID if running in a worker process."""
    return os.environ.get('PYTEST_XDIST_WORKER')


def pytest_sessionstart(session: pytest.Session) -> None:
    """Create a session span when the test session starts.

    IMPORTANT: This hook must run at default priority (no ``trylast=True``).
    ``pytest_xdist_setupnodes`` depends on the session span being active when
    it injects TRACEPARENT into ``os.environ`` for worker processes.  xdist's
    ``DSession.pytest_sessionstart`` uses ``trylast=True``, so our default-priority
    hook is guaranteed to run first.
    """
    if not _is_enabled(session.config):
        return

    plugin_config = session.config.stash.get(_CONFIG_KEY, None)
    if not plugin_config:  # pragma: no cover
        # Defensive check: plugin should be configured if _is_enabled returned True
        return

    logfire_instance = plugin_config.logfire_instance

    # Attach parent trace context if TRACEPARENT is set
    traceparent = os.environ.get('TRACEPARENT')
    if traceparent:
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        carrier = {'traceparent': traceparent}
        tracestate = os.environ.get('TRACESTATE')
        if tracestate:
            carrier['tracestate'] = tracestate

        context = TraceContextTextMapPropagator().extract(carrier=carrier)
        from opentelemetry import context as context_api

        token = context_api.attach(context)
        # Store token to detach later if needed
        session.stash[_CONTEXT_TOKEN_KEY] = token

    # Create session span
    rootpath_name = session.config.rootpath.name
    args = ' '.join(session.config.invocation_params.args)
    worker_id = _get_xdist_worker_id()

    if worker_id:
        span = logfire_instance.span(
            'pytest: {project} ({worker_id})',
            project=rootpath_name,
            worker_id=worker_id,
        )
    else:
        span = logfire_instance.span(
            'pytest: {project}',
            project=rootpath_name,
        )

    attrs: dict[str, Any] = {
        'pytest.args': args,
        'pytest.rootpath': str(session.config.rootpath),
        'pytest.startpath': str(session.startpath),
    }
    if worker_id:
        attrs['pytest.xdist.worker_id'] = worker_id
    worker_count = os.environ.get('PYTEST_XDIST_WORKER_COUNT')
    if worker_count:
        try:
            attrs['pytest.xdist.worker_count'] = int(worker_count)
        except ValueError:  # pragma: no cover
            pass
    span.set_attributes(attrs)

    # Add CI metadata if in CI
    _add_ci_metadata(span)

    span.__enter__()
    session.stash[_SESSION_SPAN_KEY] = span


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(
    item: pytest.Item,
    nextitem: pytest.Item | None,
) -> Generator[None]:
    """Create a span for each test."""
    if not _is_enabled(item.config):
        yield
        return

    plugin_config = item.config.stash.get(_CONFIG_KEY, None)
    if not plugin_config:  # pragma: no cover
        # Defensive check: plugin should be configured if test is running
        yield
        return

    from opentelemetry import trace

    logfire_instance = plugin_config.logfire_instance

    with logfire_instance.span(item.nodeid) as span:
        location = item.location
        span.set_attributes(
            {
                'test.name': item.name,
                'test.nodeid': item.nodeid,
                'code.filepath': location[0],
                'code.lineno': location[1],
                'code.function': location[2],
            }
        )

        # Add class/module info if available
        if hasattr(item, 'cls') and item.cls:  # type: ignore[attr-defined]
            span.set_attribute('test.class', item.cls.__name__)  # type: ignore[attr-defined]
        if hasattr(item, 'module') and item.module:  # type: ignore[attr-defined] pragma: no cover
            span.set_attribute('test.module', item.module.__name__)  # type: ignore[attr-defined]

        # Handle parameterized tests
        if hasattr(item, 'callspec'):
            import json

            try:
                params_json = json.dumps(item.callspec.params)  # type: ignore[attr-defined]
                span.set_attribute('test.parameters', params_json)
            except (TypeError, ValueError):
                # If parameters can't be serialized, just store the keys
                span.set_attribute('test.parameters', str(list(item.callspec.params.keys())))  # type: ignore[attr-defined]

        item.stash[_SPAN_KEY] = trace.get_current_span()

        # Propagate trace context via env vars so child processes
        # (e.g. subprocess.Popen, subprocess.run) inherit the test's trace.
        old_traceparent = os.environ.get('TRACEPARENT')
        old_tracestate = os.environ.get('TRACESTATE')
        _inject_traceparent_env()
        try:
            yield
        finally:
            if old_traceparent is not None:
                os.environ['TRACEPARENT'] = old_traceparent
            else:
                os.environ.pop('TRACEPARENT', None)
            if old_tracestate is not None:
                os.environ['TRACESTATE'] = old_tracestate
            else:
                os.environ.pop('TRACESTATE', None)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item,
    call: pytest.CallInfo[Any],
) -> Generator[None, pytest.TestReport, None]:
    """Record test outcomes and exceptions."""
    outcome = yield
    report: pytest.TestReport = outcome.get_result()

    if report.when != 'call':
        return

    span = item.stash.get(_SPAN_KEY, None)
    if not span:
        return

    span.set_attribute('test.outcome', report.outcome)
    span.set_attribute('test.duration_ms', report.duration * 1000)

    if report.failed:
        span.set_status(
            StatusCode.ERROR,
            f'{call.excinfo.typename}: {call.excinfo.value}' if call.excinfo else 'Test failed',  # pragma: no branch
        )
        # Record exception details
        if call.excinfo and call.excinfo.value:  # pragma: no branch
            # Branch coverage: excinfo.value is always present for failed tests in normal pytest execution
            span.record_exception(call.excinfo.value)
    elif report.skipped:  # pragma: no cover
        # TODO: this needs improvement in processing skip reasons
        skip_reason = ''
        if hasattr(report, 'wasxfail'):
            skip_reason = report.wasxfail or 'xfailed'
        elif report.longrepr:
            # For skips, longrepr is typically a tuple (file, line, reason)
            if isinstance(report.longrepr, tuple) and len(report.longrepr) > 2:
                skip_reason = str(report.longrepr[2])
            else:  # pragma: no cover
                # Edge case: longrepr is not a tuple or has unexpected structure
                skip_reason = str(report.longrepr)
        span.set_attribute('test.skip_reason', skip_reason)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_setup(item: pytest.Item) -> Generator[None]:
    """Trace test setup phase if --logfire-trace-phases is enabled."""
    if not _should_trace_phases(item.config):
        yield
        return

    plugin_config = item.config.stash.get(_CONFIG_KEY, None)
    if not plugin_config:  # pragma: no cover
        # Defensive check: plugin should be configured if test is running
        yield
        return

    logfire_instance = plugin_config.logfire_instance

    with logfire_instance.span('setup: {nodeid}', nodeid=item.nodeid):
        yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Generator[None]:
    """Trace test call phase if --logfire-trace-phases is enabled."""
    if not _should_trace_phases(item.config):
        yield
        return

    plugin_config = item.config.stash.get(_CONFIG_KEY, None)
    if not plugin_config:  # pragma: no cover
        # Defensive check: plugin should be configured if test is running
        yield
        return

    logfire_instance = plugin_config.logfire_instance

    with logfire_instance.span('call: {nodeid}', nodeid=item.nodeid):
        yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item: pytest.Item) -> Generator[None]:
    """Trace test teardown phase if --logfire-trace-phases is enabled."""
    if not _should_trace_phases(item.config):
        yield
        return

    plugin_config = item.config.stash.get(_CONFIG_KEY, None)
    if not plugin_config:  # pragma: no cover
        # Defensive check: plugin should be configured if test is running
        yield
        return

    logfire_instance = plugin_config.logfire_instance

    with logfire_instance.span('teardown: {nodeid}', nodeid=item.nodeid):
        yield


def _should_trace_phases(config: pytest.Config) -> bool:
    """Check if phase tracing is enabled."""
    if not _is_enabled(config):
        return False

    plugin_config = config.stash.get(_CONFIG_KEY, None)
    if not plugin_config:
        return False

    return plugin_config.trace_phases


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """End the session span and flush traces."""
    if not _is_enabled(session.config):
        return

    plugin_config = session.config.stash.get(_CONFIG_KEY, None)
    if not plugin_config:  # pragma: no cover
        # Defensive check: plugin should be configured if _is_enabled returned True
        return

    logfire_instance = plugin_config.logfire_instance

    # End session span
    span = session.stash.get(_SESSION_SPAN_KEY, None)
    if span:  # pragma: no branch
        # Branch coverage: span should always exist if sessionstart ran successfully
        span.set_attribute('pytest.exitstatus', exitstatus)
        span.set_attribute('pytest.testscollected', session.testscollected)
        span.set_attribute('pytest.testsfailed', session.testsfailed)
        span.__exit__(None, None, None)

    # Detach the trace context if it was attached from TRACEPARENT
    context_token = session.stash.get(_CONTEXT_TOKEN_KEY, None)
    if context_token:
        from opentelemetry import context as context_api

        context_api.detach(context_token)

    # Flush traces
    logfire_instance.force_flush(timeout_millis=5000)


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
    config = request.config
    plugin_config = config.stash.get(_CONFIG_KEY, None)

    if plugin_config:
        # Plugin is enabled, return the configured instance
        return plugin_config.logfire_instance

    # Plugin is not enabled, create a local-only instance
    import logfire as logfire_module

    return logfire_module.configure(
        local=True,
        send_to_logfire=False,
        console=False,
    )


@pytest.hookimpl(hookwrapper=True)
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
    import inspect

    if not _is_enabled(pyfuncitem.config):
        yield
        return

    otel_span = pyfuncitem.stash.get(_SPAN_KEY, None)
    if otel_span is None or not inspect.iscoroutinefunction(pyfuncitem.obj):
        yield
        return

    import functools

    from opentelemetry import context as context_api, trace

    original = pyfuncitem.obj

    @functools.wraps(original)
    async def _wrapped(*args: Any, **kwargs: Any) -> Any:
        ctx = trace.set_span_in_context(otel_span)
        token = context_api.attach(ctx)
        try:
            return await original(*args, **kwargs)
        finally:
            context_api.detach(token)

    pyfuncitem.obj = _wrapped
    yield
