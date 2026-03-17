import atexit
import dataclasses
import requests
from ..propagate import NoExtractTraceContextPropagator as NoExtractTraceContextPropagator, WarnOnExtractTraceContextPropagator as WarnOnExtractTraceContextPropagator
from ..types import ExceptionCallback as ExceptionCallback
from .client import InvalidProjectName as InvalidProjectName, LogfireClient as LogfireClient, ProjectAlreadyExists as ProjectAlreadyExists
from .config_params import ParamManager as ParamManager, PydanticPluginRecordValues as PydanticPluginRecordValues, normalize_token as normalize_token
from .constants import LEVEL_NUMBERS as LEVEL_NUMBERS, LevelName as LevelName, RESOURCE_ATTRIBUTES_CODE_ROOT_PATH as RESOURCE_ATTRIBUTES_CODE_ROOT_PATH, RESOURCE_ATTRIBUTES_CODE_WORK_DIR as RESOURCE_ATTRIBUTES_CODE_WORK_DIR, RESOURCE_ATTRIBUTES_DEPLOYMENT_ENVIRONMENT_NAME as RESOURCE_ATTRIBUTES_DEPLOYMENT_ENVIRONMENT_NAME, RESOURCE_ATTRIBUTES_VCS_REPOSITORY_REF_REVISION as RESOURCE_ATTRIBUTES_VCS_REPOSITORY_REF_REVISION, RESOURCE_ATTRIBUTES_VCS_REPOSITORY_URL as RESOURCE_ATTRIBUTES_VCS_REPOSITORY_URL
from .exporters.console import ConsoleColorsValues as ConsoleColorsValues, ConsoleLogExporter as ConsoleLogExporter, IndentedConsoleSpanExporter as IndentedConsoleSpanExporter, ShowParentsConsoleSpanExporter as ShowParentsConsoleSpanExporter, SimpleConsoleSpanExporter as SimpleConsoleSpanExporter
from .exporters.dynamic_batch import DynamicBatchSpanProcessor as DynamicBatchSpanProcessor
from .exporters.logs import CheckSuppressInstrumentationLogProcessorWrapper as CheckSuppressInstrumentationLogProcessorWrapper, MainLogProcessorWrapper as MainLogProcessorWrapper
from .exporters.otlp import BodySizeCheckingOTLPSpanExporter as BodySizeCheckingOTLPSpanExporter, OTLPExporterHttpSession as OTLPExporterHttpSession, QuietLogExporter as QuietLogExporter, QuietSpanExporter as QuietSpanExporter, RetryFewerSpansSpanExporter as RetryFewerSpansSpanExporter
from .exporters.processor_wrapper import CheckSuppressInstrumentationProcessorWrapper as CheckSuppressInstrumentationProcessorWrapper, MainSpanProcessorWrapper as MainSpanProcessorWrapper
from .exporters.quiet_metrics import QuietMetricExporter as QuietMetricExporter
from .exporters.remove_pending import RemovePendingSpansExporter as RemovePendingSpansExporter
from .exporters.test import TestExporter as TestExporter
from .integrations.executors import instrument_executors as instrument_executors
from .logs import ProxyLoggerProvider as ProxyLoggerProvider
from .main import Logfire as Logfire
from .metrics import ProxyMeterProvider as ProxyMeterProvider
from .scrubbing import BaseScrubber as BaseScrubber, NOOP_SCRUBBER as NOOP_SCRUBBER, Scrubber as Scrubber, ScrubbingOptions as ScrubbingOptions
from .stack_info import warn_at_user_stacklevel as warn_at_user_stacklevel
from .tracer import OPEN_SPANS as OPEN_SPANS, PendingSpanProcessor as PendingSpanProcessor, ProxyTracerProvider as ProxyTracerProvider
from .utils import SeededRandomIdGenerator as SeededRandomIdGenerator, ensure_data_dir_exists as ensure_data_dir_exists, handle_internal_errors as handle_internal_errors, platform_is_emscripten as platform_is_emscripten, suppress_instrumentation as suppress_instrumentation
from _typeshed import Incomplete
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from logfire._internal.auth import PYDANTIC_LOGFIRE_TOKEN_PATTERN as PYDANTIC_LOGFIRE_TOKEN_PATTERN, REGIONS as REGIONS
from logfire._internal.baggage import DirectBaggageAttributesSpanProcessor as DirectBaggageAttributesSpanProcessor
from logfire.exceptions import LogfireConfigError as LogfireConfigError
from logfire.sampling import SamplingOptions as SamplingOptions
from logfire.sampling._tail_sampling import TailSamplingProcessor as TailSamplingProcessor
from logfire.variables import VariablesConfig as VariablesConfig
from logfire.variables.abstract import NoOpVariableProvider as NoOpVariableProvider, VariableProvider as VariableProvider
from logfire.version import VERSION as VERSION
from opentelemetry.sdk._logs import LogRecordProcessor as LogRecordProcessor
from opentelemetry.sdk.metrics.export import MetricReader as MetricReader
from opentelemetry.sdk.metrics.view import View
from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.sdk.trace.id_generator import IdGenerator
from pathlib import Path
from typing import Any, Callable, ClassVar, Literal, TextIO, TypedDict
from typing_extensions import Self, Unpack

CREDENTIALS_FILENAME: str
COMMON_REQUEST_HEADERS: Incomplete
PROJECT_NAME_PATTERN: str
METRICS_PREFERRED_TEMPORALITY: Incomplete

@dataclass
class ConsoleOptions:
    """Options for controlling console output."""
    colors: ConsoleColorsValues = ...
    span_style: Literal['simple', 'indented', 'show-parents'] = ...
    include_timestamps: bool = ...
    include_tags: bool = ...
    verbose: bool = ...
    min_log_level: LevelName = ...
    show_project_link: bool = ...
    output: TextIO | None = ...

@dataclass
class AdvancedOptions:
    """Options primarily used for testing by Logfire developers."""
    base_url: str | None = ...
    id_generator: IdGenerator = dataclasses.field(default_factory=Incomplete)
    ns_timestamp_generator: Callable[[], int] = ...
    log_record_processors: Sequence[LogRecordProcessor] = ...
    exception_callback: ExceptionCallback | None = ...
    def generate_base_url(self, token: str) -> str: ...

@dataclass
class PydanticPlugin:
    """Options for the Pydantic plugin.

    This class is deprecated for external use. Use `logfire.instrument_pydantic()` instead.
    """
    record: PydanticPluginRecordValues = ...
    include: set[str] = field(default_factory=set)
    exclude: set[str] = field(default_factory=set)

@dataclass
class MetricsOptions:
    """Configuration of metrics."""
    DEFAULT_VIEWS: ClassVar[Sequence[View]] = ...
    additional_readers: Sequence[MetricReader] = ...
    collect_in_spans: bool = ...
    views: Sequence[View] = field(default_factory=Incomplete)

@dataclass
class CodeSource:
    """Settings for the source code of the project."""
    repository: str
    revision: str
    root_path: str = ...

@dataclass
class VariablesOptions:
    """Configuration for managed variables using the Logfire remote API.

    This is the recommended configuration for production use. Variables are managed
    through the Logfire UI and fetched via the Logfire API.
    """
    block_before_first_resolve: bool = ...
    polling_interval: timedelta | float = ...
    timeout: tuple[float, float] = ...
    include_resource_attributes_in_context: bool = ...
    include_baggage_in_context: bool = ...
    instrument: bool = ...
    def __post_init__(self) -> None: ...

@dataclass
class LocalVariablesOptions:
    """Configuration for managed variables using a local in-memory configuration.

    Use this for development, testing, or self-hosted setups where you don't
    want to connect to the Logfire API.
    """
    config: VariablesConfig
    include_resource_attributes_in_context: bool = ...
    include_baggage_in_context: bool = ...
    instrument: bool = ...

class DeprecatedKwargs(TypedDict): ...

def configure(*, local: bool = False, send_to_logfire: bool | Literal['if-token-present'] | None = None, token: str | list[str] | None = None, api_key: str | None = None, service_name: str | None = None, service_version: str | None = None, environment: str | None = None, console: ConsoleOptions | Literal[False] | None = None, config_dir: Path | str | None = None, data_dir: Path | str | None = None, additional_span_processors: Sequence[SpanProcessor] | None = None, metrics: MetricsOptions | Literal[False] | None = None, scrubbing: ScrubbingOptions | Literal[False] | None = None, inspect_arguments: bool | None = None, sampling: SamplingOptions | None = None, min_level: int | LevelName | None = None, add_baggage_to_attributes: bool = True, code_source: CodeSource | None = None, variables: VariablesOptions | LocalVariablesOptions | None = None, distributed_tracing: bool | None = None, advanced: AdvancedOptions | None = None, **deprecated_kwargs: Unpack[DeprecatedKwargs]) -> Logfire:
    """Configure the logfire SDK.

    Args:
        local: If `True`, configures and returns a `Logfire` instance that is not the default global instance.
            Use this to create multiple separate configurations, e.g. to send to different projects.
        send_to_logfire: Whether to send logs to logfire.dev.

            Defaults to the `LOGFIRE_SEND_TO_LOGFIRE` environment variable if set, otherwise defaults to `True`.
            If `if-token-present` is provided, logs will only be sent if a token is present.

        token: The project write token(s). Can be a single token string or a list of tokens to send data
            to multiple projects simultaneously (useful for project migration).

            Defaults to the `LOGFIRE_TOKEN` environment variable (supports comma-separated tokens).

        api_key: API key for the Logfire API.

            If not provided, will be loaded from the `LOGFIRE_API_KEY` environment variable.

        service_name: Name of this service.

            Defaults to the `LOGFIRE_SERVICE_NAME` environment variable.

        service_version: Version of this service.

            Defaults to the `LOGFIRE_SERVICE_VERSION` environment variable, or the current git commit hash if available.

        environment: The environment this service is running in, e.g. `'staging'` or `'prod'`. Sets the
            [`deployment.environment.name`](https://opentelemetry.io/docs/specs/semconv/resource/deployment-environment/)
            resource attribute. Useful for filtering within projects in the Logfire UI.

            Defaults to the `LOGFIRE_ENVIRONMENT` environment variable.

        console: Whether to control terminal output. If `None` uses the `LOGFIRE_CONSOLE_*` environment variables,
            otherwise defaults to `ConsoleOption(colors='auto', indent_spans=True, include_timestamps=True, include_tags=True, verbose=False)`.
            If `False` disables console output. It can also be disabled by setting `LOGFIRE_CONSOLE` environment variable to `false`.

        config_dir: Directory that contains the `pyproject.toml` file for this project. If `None` uses the
            `LOGFIRE_CONFIG_DIR` environment variable, otherwise defaults to the current working directory.

        data_dir: Directory to store credentials, and logs. If `None` uses the `LOGFIRE_CREDENTIALS_DIR` environment variable, otherwise defaults to `'.logfire'`.
        additional_span_processors: Span processors to use in addition to the default processor which exports spans to Logfire's API.
        metrics: Set to `False` to disable sending all metrics,
            or provide a `MetricsOptions` object to configure metrics, e.g. additional metric readers.
        scrubbing: Options for scrubbing sensitive data. Set to `False` to disable.
        inspect_arguments: Whether to enable
            [f-string magic](https://logfire.pydantic.dev/docs/guides/onboarding-checklist/add-manual-tracing/#f-strings).
            If `None` uses the `LOGFIRE_INSPECT_ARGUMENTS` environment variable.

            Defaults to `True` if and only if the Python version is at least 3.11.

            Also enables magic argument inspection in [`logfire.instrument_print()`][logfire.Logfire.instrument_print].

        min_level:
            Minimum log level for logs and spans to be created. By default, all logs and spans are created.
            For example, set to 'info' to only create logs with level 'info' or higher, thus filtering out debug logs.
            For spans, this only applies when `_level` is explicitly specified in `logfire.span`.
            Changing the level of a span _after_ it is created will be ignored by this.
            If a span is not created, this has no effect on the current active span, or on logs/spans created inside the
            filtered `logfire.span` context manager.
            If set to `None`, uses the `LOGFIRE_MIN_LEVEL` environment variable; if that is not set, there is no minimum level.
        sampling: Sampling options. See the [sampling guide](https://logfire.pydantic.dev/docs/guides/advanced/sampling/).
        add_baggage_to_attributes: Set to `False` to prevent OpenTelemetry Baggage from being added to spans as attributes.
            See the [Baggage documentation](https://logfire.pydantic.dev/docs/reference/advanced/baggage/) for more details.
        code_source: Settings for the source code of the project.
        variables: Options related to managed variables.
        distributed_tracing: By default, incoming trace context is extracted, but generates a warning.
            Set to `True` to disable the warning.
            Set to `False` to suppress extraction of incoming trace context.
            See [Unintentional Distributed Tracing](https://logfire.pydantic.dev/docs/how-to-guides/distributed-tracing/#unintentional-distributed-tracing)
            for more information.
            This setting always applies globally, and the last value set is used, including the default value.
        advanced: Advanced options primarily used for testing by Logfire developers.
    """

@dataclasses.dataclass
class _LogfireConfigData:
    """Data-only parent class for LogfireConfig.

    This class can be pickled / copied and gives a nice repr,
    while allowing us to keep the ugly stuff only in LogfireConfig.

    In particular, using this dataclass as a base class of LogfireConfig allows us to use
    `dataclasses.asdict` in `integrations/executors.py` to get a dict with just the attributes from
    `_LogfireConfigData`, and none of the attributes added in `LogfireConfig`.
    """
    send_to_logfire: bool | Literal['if-token-present']
    token: str | list[str] | None
    api_key: str | None
    service_name: str
    service_version: str | None
    environment: str | None
    console: ConsoleOptions | Literal[False] | None
    data_dir: Path
    additional_span_processors: Sequence[SpanProcessor] | None
    scrubbing: ScrubbingOptions | Literal[False]
    inspect_arguments: bool
    sampling: SamplingOptions
    min_level: int
    add_baggage_to_attributes: bool
    code_source: CodeSource | None
    variables: VariablesOptions | LocalVariablesOptions | None
    distributed_tracing: bool | None
    advanced: AdvancedOptions

class LogfireConfig(_LogfireConfigData):
    def __init__(self, send_to_logfire: bool | Literal['if-token-present'] | None = None, token: str | list[str] | None = None, api_key: str | None = None, service_name: str | None = None, service_version: str | None = None, environment: str | None = None, console: ConsoleOptions | Literal[False] | None = None, config_dir: Path | None = None, data_dir: Path | None = None, additional_span_processors: Sequence[SpanProcessor] | None = None, metrics: MetricsOptions | Literal[False] | None = None, scrubbing: ScrubbingOptions | Literal[False] | None = None, inspect_arguments: bool | None = None, sampling: SamplingOptions | None = None, min_level: int | LevelName | None = None, add_baggage_to_attributes: bool = True, variables: VariablesOptions | None = None, code_source: CodeSource | None = None, distributed_tracing: bool | None = None, advanced: AdvancedOptions | None = None) -> None:
        """Create a new LogfireConfig.

        Users should never need to call this directly, instead use `logfire.configure`.

        See `_LogfireConfigData` for parameter documentation.
        """
    def configure(self, send_to_logfire: bool | Literal['if-token-present'] | None, token: str | list[str] | None, api_key: str | None, service_name: str | None, service_version: str | None, environment: str | None, console: ConsoleOptions | Literal[False] | None, config_dir: Path | None, data_dir: Path | None, additional_span_processors: Sequence[SpanProcessor] | None, metrics: MetricsOptions | Literal[False] | None, scrubbing: ScrubbingOptions | Literal[False] | None, inspect_arguments: bool | None, sampling: SamplingOptions | None, min_level: int | LevelName | None, add_baggage_to_attributes: bool, code_source: CodeSource | None, variables: VariablesOptions | LocalVariablesOptions | None, distributed_tracing: bool | None, advanced: AdvancedOptions | None) -> None: ...
    def initialize(self) -> None:
        """Configure internals to start exporting traces and metrics."""
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Force flush all spans and metrics.

        Args:
            timeout_millis: The timeout in milliseconds.

        Returns:
            Whether the flush of spans was successful.
        """
    def get_tracer_provider(self) -> ProxyTracerProvider:
        """Get a tracer provider from this `LogfireConfig`.

        This is used internally and should not be called by users of the SDK.

        Returns:
            The tracer provider.
        """
    def get_meter_provider(self) -> ProxyMeterProvider:
        """Get a meter provider from this `LogfireConfig`.

        This is used internally and should not be called by users of the SDK.

        Returns:
            The meter provider.
        """
    def get_logger_provider(self) -> ProxyLoggerProvider:
        """Get a logger provider from this `LogfireConfig`.

        This is used internally and should not be called by users of the SDK.

        Returns:
            The logger provider.
        """
    def get_variable_provider(self) -> VariableProvider:
        """Get a variable provider from this `LogfireConfig`.

        This is used internally and should not be called by users of the SDK.

        If no provider has been explicitly configured (i.e. `variables=` was not passed to
        `configure()`), but a `LOGFIRE_API_KEY` is available, a `LogfireRemoteVariableProvider`
        will be lazily created on the first call.

        Returns:
            The variable provider.
        """
    def warn_if_not_initialized(self, message: str): ...
    def suppress_scopes(self, *scopes: str) -> None: ...

@atexit.register
def exit_open_spans() -> None: ...

original_os_exit: Incomplete

def patched_os_exit(code: int): ...

GLOBAL_CONFIG: Incomplete

@dataclasses.dataclass
class LogfireCredentials:
    """Credentials for logfire.dev."""
    token: str
    project_name: str
    project_url: str
    logfire_api_url: str
    @classmethod
    def load_creds_file(cls, creds_dir: Path) -> Self | None:
        """Check if a credentials file exists and if so load it.

        Args:
            creds_dir: Path to the credentials directory.

        Returns:
            The loaded credentials or `None` if the file does not exist.

        Raises:
            LogfireConfigError: If the credentials file exists but is invalid.
        """
    @classmethod
    def from_token(cls, token: str, session: requests.Session, base_url: str) -> Self | None:
        """Check that the token is valid.

        Issue a warning if the Logfire API is unreachable, or we get a response other than 200 or 401.

        We continue unless we get a 401. If something is wrong, we'll later store data locally for back-fill.

        Raises:
            LogfireConfigError: If the token is invalid.
        """
    @classmethod
    def use_existing_project(cls, *, client: LogfireClient, projects: list[dict[str, Any]], organization: str | None = None, project_name: str | None = None) -> dict[str, Any] | None:
        """Configure one of the user projects to be used by Logfire.

        It configures the project if organization/project_name is a valid project that
        the user has access to it. Otherwise, it asks the user to select a project interactively.

        Args:
            client: The Logfire client to use when making requests.
            projects: List of user projects.
            organization: Project organization.
            project_name: Name of project that has to be used.

        Returns:
            The configured project information.

        Raises:
            LogfireConfigError: If there was an error configuring the project.
        """
    @classmethod
    def create_new_project(cls, *, client: LogfireClient, organization: str | None = None, default_organization: bool = False, project_name: str | None = None) -> dict[str, Any]:
        """Create a new project and configure it to be used by Logfire.

        It creates the project under the organization if both project and organization are valid.
        Otherwise, it asks the user to select organization and enter a valid project name interactively.

        Args:
            client: The Logfire client to use when making requests.
            organization: The organization name of the new project.
            default_organization: Whether to create the project under the user default organization.
            project_name: The default name of the project.

        Returns:
            The created project information.

        Raises:
            LogfireConfigError: If there was an error creating projects.
        """
    @classmethod
    def initialize_project(cls, *, client: LogfireClient) -> Self:
        """Create a new project or use an existing project on logfire.dev requesting the given project name.

        Args:
            client: The Logfire client to use when making requests.

        Returns:
            The new credentials.

        Raises:
            LogfireConfigError: If there was an error on creating/configuring the project.
        """
    def write_creds_file(self, creds_dir: Path) -> None:
        """Write a credentials file to the given path."""
    def print_token_summary(self) -> None:
        """Print a summary of the existing project."""

def get_base_url_from_token(token: str) -> str:
    """Get the base API URL from the token's region."""
def get_git_revision_hash() -> str:
    """Get the current git commit hash."""
def sanitize_project_name(name: str) -> str:
    """Convert `name` to a string suitable for the `requested_project_name` API parameter."""
def default_project_name(): ...
def get_runtime_version() -> str: ...

class LogfireNotConfiguredWarning(UserWarning): ...
