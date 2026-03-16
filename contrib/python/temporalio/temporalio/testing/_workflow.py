"""Workflow test environment."""

from __future__ import annotations

try:
    import yatest.common
    import yatest.common.runtime
except ImportError:
    test_server_path = None
    dev_server_path = None
else:
    try:
        test_server_path = yatest.common.binary_path('infra/temporal/tools/temporal-test-server/temporal-test-server.sh')
        dev_server_path = yatest.common.binary_path('infra/temporal/tools/temporal/temporal')
    except yatest.common.runtime.NoRuntimeFormed:
        test_server_path = None
        dev_server_path = None
    except Exception as e:
        raise RuntimeError('Have you forgot to INCLUDE(${ARCADIA_ROOT}/infra/temporal/lib/py/tests.inc)?') from e
    else:
        import os

        if not os.environ.get("JAVA_HOME"):
            global_resources = yatest.common.runtime.global_resources()
            if global_resources and 'JDK_DEFAULT_RESOURCE_GLOBAL' in global_resources:
                os.environ["JAVA_HOME"] = global_resources['JDK_DEFAULT_RESOURCE_GLOBAL']

import asyncio
import logging
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from typing import (
    Any,
    AsyncIterator,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

import google.protobuf.empty_pb2

import temporalio.api.testservice.v1
import temporalio.bridge.testing
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.runtime
import temporalio.service
import temporalio.types
import temporalio.worker

logger = logging.getLogger(__name__)


class WorkflowEnvironment:
    """Workflow environment for testing workflows.

    Most developers will want to use the static :py:meth:`start_time_skipping`
    to start a test server process that automatically skips time as needed.
    Alternatively, :py:meth:`start_local` may be used for a full, local Temporal
    server with more features. To use an existing server, use
    :py:meth:`from_client`.

    This environment is an async context manager, so it can be used with
    ``async with`` to make sure it shuts down properly. Otherwise,
    :py:meth:`shutdown` can be manually called.

    To use the environment, simply use the :py:attr:`client` on it.

    Workflows invoked on the workflow environment are automatically configured
    to have ``assert`` failures fail the workflow with the assertion error.
    """

    @staticmethod
    def from_client(client: temporalio.client.Client) -> WorkflowEnvironment:
        """Create a workflow environment from the given client.

        :py:attr:`supports_time_skipping` will always return ``False`` for this
        environment. :py:meth:`sleep` will sleep the actual amount of time and
        :py:meth:`get_current_time` will return the current time.

        Args:
            client: The client to use for the environment.

        Returns:
            The workflow environment that runs against the given client.
        """
        # Add the assertion interceptor
        return WorkflowEnvironment(
            _client_with_interceptors(client, _AssertionErrorInterceptor())
        )

    @staticmethod
    async def start_local(
        *,
        namespace: str = "default",
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        interceptors: Sequence[temporalio.client.Interceptor] = [],
        plugins: Sequence[temporalio.client.Plugin] = [],
        default_workflow_query_reject_condition: Optional[
            temporalio.common.QueryRejectCondition
        ] = None,
        retry_config: Optional[temporalio.client.RetryConfig] = None,
        rpc_metadata: Mapping[str, str] = {},
        identity: Optional[str] = None,
        tls: bool | temporalio.client.TLSConfig = False,
        ip: str = "127.0.0.1",
        port: Optional[int] = None,
        download_dest_dir: Optional[str] = None,
        ui: bool = False,
        runtime: Optional[temporalio.runtime.Runtime] = None,
        search_attributes: Sequence[temporalio.common.SearchAttributeKey] = (),
        dev_server_existing_path: Optional[str] = dev_server_path,
        dev_server_database_filename: Optional[str] = None,
        dev_server_log_format: str = "pretty",
        dev_server_log_level: Optional[str] = "warn",
        dev_server_download_version: str = "default",
        dev_server_extra_args: Sequence[str] = [],
        dev_server_download_ttl: Optional[timedelta] = None,
    ) -> WorkflowEnvironment:
        """Start a full Temporal server locally, downloading if necessary.

        This environment is good for testing full server capabilities, but does
        not support time skipping like :py:meth:`start_time_skipping` does.
        :py:attr:`supports_time_skipping` will always return ``False`` for this
        environment. :py:meth:`sleep` will sleep the actual amount of time and
        :py:meth:`get_current_time` will return the current time.

        Internally, this uses the Temporal CLI dev server from
        https://github.com/temporalio/cli. This is a self-contained binary for
        Temporal using Sqlite persistence. This call will download the CLI to a
        temporary directory by default if it has not already been downloaded
        before and ``dev_server_existing_path`` is not set.

        In the future, the dev server implementation may be changed to another
        implementation. Therefore, all ``dev_server_`` prefixed parameters are
        dev-server specific and may not apply to newer versions.

        Args:
            namespace: Namespace name to use for this environment.
            data_converter: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            interceptors: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            default_workflow_query_reject_condition: See parameter of the same
                name on :py:meth:`temporalio.client.Client.connect`.
            retry_config: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            rpc_metadata: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            identity: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            tls: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            ip: IP address to bind to, or 127.0.0.1 by default.
            port: Port number to bind to, or an OS-provided port by default.
            download_dest_dir: Directory to download binary to if a download is
                needed. If unset, this is the system's temporary directory.
            ui: If ``True``, will start a UI in the dev server.
            runtime: Specific runtime to use or default if unset.
            search_attributes: Search attributes to register with the dev
                server.
            dev_server_existing_path: Existing path to the CLI binary.
                If present, no download will be attempted to fetch the binary.
            dev_server_database_filename: Path to the Sqlite database to use
                for the dev server. Unset default means only in-memory Sqlite
                will be used.
            dev_server_log_format: Log format for the dev server.
            dev_server_log_level: Log level to use for the dev server. Default
                is ``warn``, but if set to ``None`` this will translate the
                Python logger's level to a dev server log level.
            dev_server_download_version: Specific CLI version to download.
                Defaults to ``default`` which downloads the version known to
                work best with this SDK.
            dev_server_extra_args: Extra arguments for the CLI binary.
            dev_server_download_ttl: TTL for the downloaded CLI binary. If unset, it will be
                cached indefinitely.

        Returns:
            The started CLI dev server workflow environment.
        """
        # Use the logger's configured level if none given
        if not dev_server_log_level:
            if logger.isEnabledFor(logging.DEBUG):
                dev_server_log_level = "debug"
            elif logger.isEnabledFor(logging.INFO):
                dev_server_log_level = "info"
            elif logger.isEnabledFor(logging.WARNING):
                dev_server_log_level = "warn"
            elif logger.isEnabledFor(logging.ERROR):
                dev_server_log_level = "error"
            else:
                dev_server_log_level = "fatal"
        # Add search attributes
        if search_attributes:
            new_args = []
            for attr in search_attributes:
                new_args.append("--search-attribute")
                new_args.append(f"{attr.name}={attr._metadata_type}")
            new_args += dev_server_extra_args
            dev_server_extra_args = new_args
        # Start CLI dev server
        runtime = runtime or temporalio.runtime.Runtime.default()
        download_ttl_ms = None
        if dev_server_download_ttl is not None:
            download_ttl_ms = int(dev_server_download_ttl.total_seconds() * 1000)
        server = await temporalio.bridge.testing.EphemeralServer.start_dev_server(
            runtime._core_runtime,
            temporalio.bridge.testing.DevServerConfig(
                existing_path=dev_server_existing_path,
                sdk_name="sdk-python",
                sdk_version=temporalio.service.__version__,
                download_version=dev_server_download_version,
                download_dest_dir=download_dest_dir,
                namespace=namespace,
                ip=ip,
                port=port,
                database_filename=dev_server_database_filename,
                ui=ui,
                log_format=dev_server_log_format,
                log_level=dev_server_log_level,
                extra_args=dev_server_extra_args,
                download_ttl_ms=download_ttl_ms,
            ),
        )
        # If we can't connect to the server, we should shut it down
        try:
            return _EphemeralServerWorkflowEnvironment(
                await temporalio.client.Client.connect(
                    server.target,
                    namespace=namespace,
                    data_converter=data_converter,
                    interceptors=interceptors,
                    plugins=plugins,
                    default_workflow_query_reject_condition=default_workflow_query_reject_condition,
                    tls=tls,
                    retry_config=retry_config,
                    rpc_metadata=rpc_metadata,
                    identity=identity,
                    runtime=runtime,
                ),
                server,
            )
        except:
            try:
                await server.shutdown()
            except:
                logger.warn(
                    "Failed stopping local server on client connection failure",
                    exc_info=True,
                )
            raise

    @staticmethod
    async def start_time_skipping(
        *,
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        interceptors: Sequence[temporalio.client.Interceptor] = [],
        plugins: Sequence[temporalio.client.Plugin] = [],
        default_workflow_query_reject_condition: Optional[
            temporalio.common.QueryRejectCondition
        ] = None,
        retry_config: Optional[temporalio.client.RetryConfig] = None,
        rpc_metadata: Mapping[str, str] = {},
        identity: Optional[str] = None,
        port: Optional[int] = None,
        download_dest_dir: Optional[str] = None,
        runtime: Optional[temporalio.runtime.Runtime] = None,
        test_server_existing_path: Optional[str] = test_server_path,
        test_server_download_version: str = "default",
        test_server_extra_args: Sequence[str] = [],
        test_server_download_ttl: Optional[timedelta] = None,
    ) -> WorkflowEnvironment:
        """Start a time skipping workflow environment.

        By default, this environment will automatically skip to the next events
        in time when a workflow's
        :py:meth:`temporalio.client.WorkflowHandle.result` is awaited on (which
        includes :py:meth:`temporalio.client.Client.execute_workflow`). Before
        the result is awaited on, time can be manually skipped forward using
        :py:meth:`sleep`. The currently known time can be obtained via
        :py:meth:`get_current_time`.

        Internally, this environment lazily downloads a test-server binary for
        the current OS/arch into the temp directory if it is not already there.
        Then the executable is started and will be killed when
        :py:meth:`shutdown` is called (which is implicitly done if this is
        started via
        ``async with await WorkflowEnvironment.start_time_skipping()``).

        Users can reuse this environment for testing multiple independent
        workflows, but not concurrently. Time skipping, which is automatically
        done when awaiting a workflow result and manually done on
        :py:meth:`sleep`, is global to the environment, not to the workflow
        under test.

        In the future, the test server implementation may be changed to another
        implementation. Therefore, all ``test_server_`` prefixed parameters are
        test server specific and may not apply to newer versions.

        Args:
            data_converter: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            interceptors: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            default_workflow_query_reject_condition: See parameter of the same
                name on :py:meth:`temporalio.client.Client.connect`.
            retry_config: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            rpc_metadata: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            identity: See parameter of the same name on
                :py:meth:`temporalio.client.Client.connect`.
            port: Port number to bind to, or an OS-provided port by default.
            download_dest_dir: Directory to download binary to if a download is
                needed. If unset, this is the system's temporary directory.
            runtime: Specific runtime to use or default if unset.
            test_server_existing_path: Existing path to the test server binary.
                If present, no download will be attempted to fetch the binary.
            test_server_download_version: Specific test server version to
                download. Defaults to ``default`` which downloads the version
                known to work best with this SDK.
            test_server_extra_args: Extra arguments for the test server binary.
            test_server_download_ttl: TTL for the downloaded test server binary. If unset, it
                will be cached indefinitely.

        Returns:
            The started workflow environment with time skipping.
        """
        # Start test server
        runtime = runtime or temporalio.runtime.Runtime.default()
        download_ttl_ms = None
        if test_server_download_ttl:
            download_ttl_ms = int(test_server_download_ttl.total_seconds() * 1000)
        server = await temporalio.bridge.testing.EphemeralServer.start_test_server(
            runtime._core_runtime,
            temporalio.bridge.testing.TestServerConfig(
                existing_path=test_server_existing_path,
                sdk_name="sdk-python",
                sdk_version=temporalio.service.__version__,
                download_version=test_server_download_version,
                download_dest_dir=download_dest_dir,
                download_ttl_ms=download_ttl_ms,
                port=port,
                extra_args=test_server_extra_args,
            ),
        )
        # If we can't connect to the server, we should shut it down
        try:
            return _EphemeralServerWorkflowEnvironment(
                await temporalio.client.Client.connect(
                    server.target,
                    data_converter=data_converter,
                    interceptors=interceptors,
                    plugins=plugins,
                    default_workflow_query_reject_condition=default_workflow_query_reject_condition,
                    retry_config=retry_config,
                    rpc_metadata=rpc_metadata,
                    identity=identity,
                    runtime=runtime,
                ),
                server,
            )
        except:
            try:
                await server.shutdown()
            except:
                logger.warn(
                    "Failed stopping test server on client connection failure",
                    exc_info=True,
                )
            raise

    def __init__(self, client: temporalio.client.Client) -> None:
        """Create a workflow environment from a client.

        Most users would use a static method instead.
        """
        self._client = client

    async def __aenter__(self) -> WorkflowEnvironment:
        """Noop for ``async with`` support."""
        return self

    async def __aexit__(self, *args) -> None:
        """For ``async with`` support to just call :py:meth:`shutdown`."""
        await self.shutdown()

    @property
    def client(self) -> temporalio.client.Client:
        """Client to this environment."""
        return self._client

    async def shutdown(self) -> None:
        """Shut down this environment."""
        pass

    async def sleep(self, duration: Union[timedelta, float]) -> None:
        """Sleep in this environment.

        This awaits a regular :py:func:`asyncio.sleep` in regular environments,
        or manually skips time in time-skipping environments.

        Args:
            duration: Amount of time to sleep.
        """
        await asyncio.sleep(
            duration.total_seconds() if isinstance(duration, timedelta) else duration
        )

    async def get_current_time(self) -> datetime:
        """Get the current time known to this environment.

        For non-time-skipping environments this is simply the system time. For
        time-skipping environments this is whatever time has been skipped to.
        """
        return datetime.now(timezone.utc)

    @property
    def supports_time_skipping(self) -> bool:
        """Whether this environment supports time skipping."""
        return False

    @contextmanager
    def auto_time_skipping_disabled(self) -> Iterator[None]:
        """Disable any automatic time skipping if this is a time-skipping
        environment.

        This is a context manager for use via ``with``. Usually in time-skipping
        environments, waiting on a workflow result causes time to automatically
        skip until the next event. This can disable that. However, this only
        applies to results awaited inside this context. This will not disable
        automatic time skipping on previous results.

        This has no effect on non-time-skipping environments.
        """
        # It's always disabled for this base class
        yield None


class _EphemeralServerWorkflowEnvironment(WorkflowEnvironment):
    def __init__(
        self,
        client: temporalio.client.Client,
        server: temporalio.bridge.testing.EphemeralServer,
    ) -> None:
        # Add assertion interceptor to client and if time skipping is supported,
        # add time skipping interceptor
        self._supports_time_skipping = server.has_test_service
        interceptors: List[temporalio.client.Interceptor] = [
            _AssertionErrorInterceptor()
        ]
        if self._supports_time_skipping:
            interceptors.append(_TimeSkippingClientInterceptor(self))
        super().__init__(_client_with_interceptors(client, *interceptors))
        self._server = server
        self._auto_time_skipping = True

    async def shutdown(self) -> None:
        await self._server.shutdown()

    async def sleep(self, duration: Union[timedelta, float]) -> None:
        # Use regular sleep if no time skipping
        if not self._supports_time_skipping:
            return await super().sleep(duration)
        req = temporalio.api.testservice.v1.SleepRequest()
        req.duration.FromTimedelta(
            duration if isinstance(duration, timedelta) else timedelta(seconds=duration)
        )
        await self._client.test_service.unlock_time_skipping_with_sleep(req)

    async def get_current_time(self) -> datetime:
        # Use regular time if no time skipping
        if not self._supports_time_skipping:
            return await super().get_current_time()
        resp = await self._client.test_service.get_current_time(
            google.protobuf.empty_pb2.Empty()
        )
        return resp.time.ToDatetime().replace(tzinfo=timezone.utc)

    @property
    def supports_time_skipping(self) -> bool:
        return self._supports_time_skipping

    @contextmanager
    def auto_time_skipping_disabled(self) -> Iterator[None]:
        already_disabled = not self._auto_time_skipping
        self._auto_time_skipping = False
        try:
            yield None
        finally:
            if not already_disabled:
                self._auto_time_skipping = True

    @asynccontextmanager
    async def time_skipping_unlocked(self) -> AsyncIterator[None]:
        # If it's disabled or not supported, no locking/unlocking, just yield
        # and return
        if not self._supports_time_skipping or not self._auto_time_skipping:
            yield None
            return
        # Unlock to start time skipping, lock again to stop it
        await self.client.test_service.unlock_time_skipping(
            temporalio.api.testservice.v1.UnlockTimeSkippingRequest()
        )
        try:
            yield None
            # Lock it back, throwing on error
            await self.client.test_service.lock_time_skipping(
                temporalio.api.testservice.v1.LockTimeSkippingRequest()
            )
        except:
            # Lock it back, swallowing error
            try:
                await self.client.test_service.lock_time_skipping(
                    temporalio.api.testservice.v1.LockTimeSkippingRequest()
                )
            except:
                logger.exception("Failed locking time skipping after error")
            raise


class _AssertionErrorInterceptor(
    temporalio.client.Interceptor, temporalio.worker.Interceptor
):
    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> Optional[Type[temporalio.worker.WorkflowInboundInterceptor]]:
        return _AssertionErrorWorkflowInboundInterceptor


class _AssertionErrorWorkflowInboundInterceptor(
    temporalio.worker.WorkflowInboundInterceptor
):
    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        with self.assert_error_as_app_error():
            return await super().execute_workflow(input)

    async def handle_signal(self, input: temporalio.worker.HandleSignalInput) -> None:
        with self.assert_error_as_app_error():
            return await super().handle_signal(input)

    @contextmanager
    def assert_error_as_app_error(self) -> Iterator[None]:
        try:
            yield None
        except AssertionError as err:
            app_err = temporalio.exceptions.ApplicationError(
                str(err), type="AssertionError", non_retryable=True
            )
            app_err.__traceback__ = err.__traceback__
            raise app_err from None


class _TimeSkippingClientInterceptor(temporalio.client.Interceptor):
    def __init__(self, env: _EphemeralServerWorkflowEnvironment) -> None:  # type: ignore[reportMissingSuperCall]
        self.env = env

    def intercept_client(
        self, next: temporalio.client.OutboundInterceptor
    ) -> temporalio.client.OutboundInterceptor:
        return _TimeSkippingClientOutboundInterceptor(next, self.env)


class _TimeSkippingClientOutboundInterceptor(temporalio.client.OutboundInterceptor):
    def __init__(
        self,
        next: temporalio.client.OutboundInterceptor,
        env: _EphemeralServerWorkflowEnvironment,
    ) -> None:
        super().__init__(next)
        self.env = env

    async def start_workflow(
        self, input: temporalio.client.StartWorkflowInput
    ) -> temporalio.client.WorkflowHandle[Any, Any]:
        # We need to change the class of the handle so we can override result
        handle = cast(_TimeSkippingWorkflowHandle, await super().start_workflow(input))
        handle.__class__ = _TimeSkippingWorkflowHandle
        handle.env = self.env
        return handle


class _TimeSkippingWorkflowHandle(temporalio.client.WorkflowHandle):
    env: _EphemeralServerWorkflowEnvironment  # type: ignore[reportUninitializedInstanceAttribute]

    async def result(
        self,
        *,
        follow_runs: bool = True,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        async with self.env.time_skipping_unlocked():
            return await super().result(
                follow_runs=follow_runs,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )


def _client_with_interceptors(
    client: temporalio.client.Client, *interceptors: temporalio.client.Interceptor
) -> temporalio.client.Client:
    # Shallow clone client and add interceptors
    config = client.config()
    config_interceptors = list(config["interceptors"])
    config_interceptors.extend(interceptors)
    config["interceptors"] = config_interceptors
    return temporalio.client.Client(**config)
