# Copyright (c) 2016-2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import os
import threading
from typing import Any, Optional, Dict

import opentracing
from opentracing.propagation import Format
from opentracing.scope_manager import ScopeManager
from . import Tracer
from .local_agent_net import LocalAgentSender
from .throttler import RemoteThrottler, Throttler
from .reporter import (
    BaseReporter,
    Reporter,
    CompositeReporter,
    LoggingReporter,
)
from .sampler import (
    ConstSampler,
    ProbabilisticSampler,
    RateLimitingSampler,
    RemoteControlledSampler,
    Sampler)
from .constants import (
    DEFAULT_SAMPLING_INTERVAL,
    DEFAULT_FLUSH_INTERVAL,
    SAMPLER_TYPE_CONST,
    SAMPLER_TYPE_PROBABILISTIC,
    SAMPLER_TYPE_RATE_LIMITING,
    TRACE_ID_HEADER,
    BAGGAGE_HEADER_PREFIX,
    DEBUG_ID_HEADER_KEY,
    MAX_TAG_VALUE_LENGTH,
    MAX_TRACEBACK_LENGTH,
    DEFAULT_THROTTLER_REFRESH_INTERVAL,
)
from .metrics import LegacyMetricsFactory, MetricsFactory, Metrics
from .utils import get_boolean, ErrorReporter
from .codecs import B3Codec

DEFAULT_REPORTING_HOST = 'localhost'
DEFAULT_REPORTING_PORT = 6831
DEFAULT_SAMPLING_PORT = 5778
DEFAULT_THROTTLER_PORT = DEFAULT_SAMPLING_PORT
LOCAL_AGENT_DEFAULT_ENABLED = True

logger = logging.getLogger('jaeger_tracing')


class Config(object):
    """
    Wraps a YAML configuration section for configuring Jaeger Tracer.

    service_name is required, but can be passed either as constructor
    parameter, or as config property.

    Example:

    .. code-block:: yaml

        enabled: true
        reporter_batch_size: 10
        logging: true
        metrics: true
        sampler:
            type: const
            param: true

    """

    _initialized = False
    _initialized_lock = threading.Lock()

    def __init__(
        self,
        config: dict,
        metrics: Optional[Metrics] = None,
        service_name: Optional[str] = None,
        metrics_factory: Optional[MetricsFactory] = None,
        validate: bool = False,
        scope_manager: Optional[ScopeManager] = None
    ) -> None:
        """
        :param metrics: an instance of Metrics class, or None. This parameter
            has been deprecated, please use metrics_factory instead.
        :param service_name: default service name.
            Can be overwritten by config['service_name'].
        :param metrics_factory: an instance of MetricsFactory class, or None.
        :param scope_manager: an instance of a scope manager, or None for
            default (ThreadLocalScopeManager).
        """
        if validate:
            self._validate_config(config)
        self.config = config
        self.scope_manager = scope_manager
        if get_boolean(self.config.get('metrics', True), True):
            self._metrics_factory = metrics_factory or LegacyMetricsFactory(metrics or Metrics())
        else:
            # if metrics are explicitly disabled, use a dummy
            self._metrics_factory = MetricsFactory()
        self._service_name = config.get('service_name', service_name)
        if not self._service_name:
            raise ValueError('service_name required in the config or param')

        self._error_reporter = ErrorReporter(
            metrics=Metrics(),
            logger=logger if self.logging else None,
        )

    def _validate_config(self, config):
        allowed_keys = ['logging',
                        'local_agent',
                        'sampler',
                        'tags',
                        'enabled',
                        'reporter_batch_size',
                        'reporter_queue_size',
                        'propagation',
                        'max_tag_value_length',
                        'max_traceback_length',
                        'reporter_flush_interval',
                        'sampling_refresh_interval',
                        'trace_id_header',
                        'generate_128bit_trace_id',
                        'baggage_header_prefix',
                        'service_name',
                        'throttler']
        config_keys = config.keys()
        unexpected_config_keys = [k for k in config_keys if k not in allowed_keys]
        if unexpected_config_keys:
            raise ValueError('Unexpected keys found in config:{}'.
                             format(','.join(unexpected_config_keys)))

    @property
    def service_name(self) -> str:
        return self._service_name

    @property
    def metrics(self) -> None:
        """Deprecated."""
        return None

    @property
    def error_reporter(self) -> ErrorReporter:
        return self._error_reporter

    @property
    def enabled(self) -> bool:
        return get_boolean(self.config.get('enabled', True), True)

    @property
    def reporter_batch_size(self) -> int:
        return int(self.config.get('reporter_batch_size', 10))

    @property
    def reporter_queue_size(self) -> int:
        return int(self.config.get('reporter_queue_size', 100))

    @property
    def logging(self) -> bool:
        return get_boolean(self.config.get('logging', False), False)

    @property
    def trace_id_header(self) -> str:
        """
        :return: Returns the name of the HTTP header used to encode trace ID
        """
        return self.config.get('trace_id_header', TRACE_ID_HEADER)

    @property
    def generate_128bit_trace_id(self) -> bool:
        """
        :return: Returns boolean value to indicate if 128bit trace_id
        generation is enabled
        """
        if 'generate_128bit_trace_id' in self.config:
            return get_boolean(self.config['generate_128bit_trace_id'], False)
        return os.getenv('JAEGER_TRACEID_128BIT') == 'true'

    @property
    def baggage_header_prefix(self) -> str:
        """
        :return: Returns the prefix for HTTP headers used to record baggage
        items
        """
        return self.config.get('baggage_header_prefix', BAGGAGE_HEADER_PREFIX)

    @property
    def debug_id_header(self) -> str:
        """
        :return: Returns the name of HTTP header or a TextMap carrier key
        which, if found in the carrier, forces the trace to be sampled as
        "debug" trace. The value of the header is recorded as the tag on the
        root span, so that the trace can be found in the UI using this value
        as a correlation ID.
        """
        return self.config.get('debug_id_header', DEBUG_ID_HEADER_KEY)

    @property
    def max_tag_value_length(self) -> int:
        """
        :return: Returns max allowed tag value length. Longer values will
        be truncated.
        """
        return self.config.get('max_tag_value_length', MAX_TAG_VALUE_LENGTH)

    @property
    def max_traceback_length(self) -> int:
        """
        :return: Returns max allowed traceback length when logging an error.
        Longer values will be truncated.
        """
        return self.config.get('max_traceback_length', MAX_TRACEBACK_LENGTH)

    @property
    def sampler(self) -> Optional[Sampler]:
        sampler_config = self.config.get('sampler', {})
        if isinstance(sampler_config, Sampler):
            return sampler_config
        sampler_type = sampler_config.get('type', None)
        sampler_param = sampler_config.get('param', None)
        if not sampler_type:
            return None
        elif sampler_type == SAMPLER_TYPE_CONST:
            return ConstSampler(decision=get_boolean(sampler_param, False))
        elif sampler_type == SAMPLER_TYPE_PROBABILISTIC:
            return ProbabilisticSampler(rate=float(sampler_param))
        elif sampler_type in [SAMPLER_TYPE_RATE_LIMITING, 'rate_limiting']:
            return RateLimitingSampler(
                max_traces_per_second=float(sampler_param))

        raise ValueError('Unknown sampler type %s' % sampler_type)

    @property
    def sampling_refresh_interval(self) -> int:
        return self.config.get('sampling_refresh_interval',
                               DEFAULT_SAMPLING_INTERVAL)

    @property
    def reporter_flush_interval(self) -> int:
        return self.config.get('reporter_flush_interval',
                               DEFAULT_FLUSH_INTERVAL)

    def local_agent_group(self) -> Optional[dict]:
        return self.config.get('local_agent', None)

    @property
    def local_agent_enabled(self) -> bool:
        # noinspection PyBroadException
        try:
            return get_boolean(self.local_agent_group().get('enabled',  # type:ignore
                               LOCAL_AGENT_DEFAULT_ENABLED),
                               LOCAL_AGENT_DEFAULT_ENABLED)
        except:  # noqa: E722
            return LOCAL_AGENT_DEFAULT_ENABLED

    @property
    def local_agent_sampling_port(self) -> int:
        # noinspection PyBroadException
        try:
            return int(self.local_agent_group()['sampling_port'])  # type:ignore
        except:  # noqa: E722
            return DEFAULT_SAMPLING_PORT

    @property
    def local_agent_reporting_port(self) -> int:
        # noinspection PyBroadException
        try:
            return int(self.local_agent_group()['reporting_port'])  # type:ignore
        except:  # noqa: E722
            pass

        try:
            return int(os.getenv('JAEGER_AGENT_PORT'))  # type:ignore
        except:  # noqa: E722
            return DEFAULT_REPORTING_PORT

    @property
    def local_agent_reporting_host(self) -> str:
        # noinspection PyBroadException
        try:
            return self.local_agent_group()['reporting_host']  # type:ignore
        except:  # noqa: E722
            pass

        if 'JAEGER_AGENT_HOST' in os.environ:
            return os.environ['JAEGER_AGENT_HOST']
        else:
            return DEFAULT_REPORTING_HOST

    @property
    def max_operations(self) -> Optional[Any]:
        return self.config.get('max_operations', None)

    @property
    def tags(self) -> Dict[str, Any]:
        """
        :return: Returns tags from config and `JAEGER_TAGS` environment variable
        to use as process-wide tracer tags
        """
        tags = self.config.get('tags', {})
        env_tags = os.environ.get('JAEGER_TAGS', '')
        if env_tags:
            for kv in env_tags.split(','):
                key, value = kv.split('=')
                tags[key.strip()] = value.strip()
        return tags

    @property
    def propagation(self) -> Dict[str, Any]:
        propagation = self.config.get('propagation')
        if propagation == 'b3':
            # replace the codec with a B3 enabled instance
            return {Format.HTTP_HEADERS: B3Codec(
                generate_128bit_trace_id=self.generate_128bit_trace_id
            )}
        return {}

    def throttler_group(self) -> Optional[Any]:
        return self.config.get('throttler', None)

    @property
    def throttler_port(self) -> Optional[int]:
        throttler_config = self.throttler_group()
        if throttler_config is None:
            return None
        # noinspection PyBroadException
        try:
            return int(throttler_config['port'])
        except:  # noqa: E722
            return DEFAULT_THROTTLER_PORT

    @property
    def throttler_refresh_interval(self) -> Optional[int]:
        throttler_config = self.throttler_group()
        if throttler_config is None:
            return None
        # noinspection PyBroadException
        try:
            return int(throttler_config['refresh_interval'])
        except:  # noqa: E722
            return DEFAULT_THROTTLER_REFRESH_INTERVAL

    @staticmethod
    def initialized() -> bool:
        with Config._initialized_lock:
            return Config._initialized

    def initialize_tracer(self, io_loop: Optional[Any] = None) -> Optional[Tracer]:
        """
        Initialize Jaeger Tracer based on the passed `jaeger_client.Config`.
        Save it to `opentracing.tracer` global variable.
        Only the first call to this method has any effect.
        """

        with Config._initialized_lock:
            if Config._initialized:
                logger.warning('Jaeger tracer already initialized, skipping')
                return None
            Config._initialized = True

        tracer = self.new_tracer(io_loop)

        self._initialize_global_tracer(tracer=tracer)
        return tracer

    def new_tracer(self, io_loop: Optional[Any] = None) -> Tracer:
        """
        Create a new Jaeger Tracer based on the passed `jaeger_client.Config`.
        Does not set `opentracing.tracer` global variable.
        """
        channel = self._create_local_agent_channel(io_loop=io_loop)
        sampler = self.sampler
        if not sampler:
            sampler = RemoteControlledSampler(
                channel=channel,
                service_name=self.service_name,
                logger=logger,
                metrics_factory=self._metrics_factory,
                error_reporter=self.error_reporter,
                sampling_refresh_interval=self.sampling_refresh_interval,
                max_operations=self.max_operations)
        logger.info('Using sampler %s', sampler)

        reporter: BaseReporter = Reporter(
            channel=channel,
            queue_capacity=self.reporter_queue_size,
            batch_size=self.reporter_batch_size,
            flush_interval=self.reporter_flush_interval,
            logger=logger,
            metrics_factory=self._metrics_factory,
            error_reporter=self.error_reporter)

        if self.logging:
            reporter = CompositeReporter(reporter, LoggingReporter(logger))

        throttler: Optional[Throttler]
        if not self.throttler_group() is None:
            throttler = RemoteThrottler(
                channel,
                self.service_name,
                refresh_interval=self.throttler_refresh_interval,
                logger=logger,
                metrics_factory=self._metrics_factory,
                error_reporter=self.error_reporter)
        else:
            throttler = None

        return self.create_tracer(
            reporter=reporter,
            sampler=sampler,
            throttler=throttler,
        )

    def create_tracer(
        self, reporter: BaseReporter, sampler: Sampler, throttler: Optional[Throttler] = None
    ) -> Tracer:
        return Tracer(
            service_name=self.service_name,
            reporter=reporter,
            sampler=sampler,
            metrics_factory=self._metrics_factory,
            trace_id_header=self.trace_id_header,
            generate_128bit_trace_id=self.generate_128bit_trace_id,
            baggage_header_prefix=self.baggage_header_prefix,
            debug_id_header=self.debug_id_header,
            tags=self.tags,
            max_tag_value_length=self.max_tag_value_length,
            max_traceback_length=self.max_traceback_length,
            extra_codecs=self.propagation,
            throttler=throttler,
            scope_manager=self.scope_manager,
        )

    def _initialize_global_tracer(self, tracer):
        opentracing.set_global_tracer(tracer)
        logger.info('opentracing.tracer initialized to %s[app_name=%s]',
                    tracer, self.service_name)

    def _create_local_agent_channel(self, io_loop):
        """
        Create an out-of-process channel communicating to local jaeger-agent.
        Spans are submitted as SOCK_DGRAM Thrift, sampling strategy is polled
        via JSON HTTP.

        :param self: instance of Config
        """
        logger.info('Initializing Jaeger Tracer with UDP reporter')
        return LocalAgentSender(
            host=self.local_agent_reporting_host,
            sampling_port=self.local_agent_sampling_port,
            reporting_port=self.local_agent_reporting_port,
            throttling_port=self.throttler_port,
            io_loop=io_loop
        )
