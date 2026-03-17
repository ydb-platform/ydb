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


import json
import logging
import random

from threading import Lock
from tornado.ioloop import PeriodicCallback
from .constants import (
    _max_id_bits,
    DEFAULT_SAMPLING_INTERVAL,
    SAMPLER_TYPE_CONST,
    SAMPLER_TYPE_PROBABILISTIC,
    SAMPLER_TYPE_RATE_LIMITING,
    SAMPLER_TYPE_LOWER_BOUND,
)
from .metrics import Metrics, LegacyMetricsFactory, MetricsFactory
from .utils import ErrorReporter
from .rate_limiter import RateLimiter
from typing import Any, Dict, Optional, Tuple

default_logger = logging.getLogger('jaeger_tracing')

SAMPLER_TYPE_TAG_KEY = 'sampler.type'
SAMPLER_PARAM_TAG_KEY = 'sampler.param'
DEFAULT_SAMPLING_PROBABILITY = 0.001
DEFAULT_LOWER_BOUND = 1.0 / (10.0 * 60.0)  # sample once every 10 minutes
DEFAULT_MAX_OPERATIONS = 2000

STRATEGIES_STR = 'perOperationStrategies'
OPERATION_STR = 'operation'
DEFAULT_LOWER_BOUND_STR = 'defaultLowerBoundTracesPerSecond'
PROBABILISTIC_SAMPLING_STR = 'probabilisticSampling'
SAMPLING_RATE_STR = 'samplingRate'
DEFAULT_SAMPLING_PROBABILITY_STR = 'defaultSamplingProbability'
OPERATION_SAMPLING_STR = 'operationSampling'
MAX_TRACES_PER_SECOND_STR = 'maxTracesPerSecond'
RATE_LIMITING_SAMPLING_STR = 'rateLimitingSampling'
STRATEGY_TYPE_STR = 'strategyType'
PROBABILISTIC_SAMPLING_STRATEGY = 'PROBABILISTIC'
RATE_LIMITING_SAMPLING_STRATEGY = 'RATE_LIMITING'

_TagsType = Dict[str, Any]
_IsSampledType = Tuple[bool, _TagsType]


class Sampler(object):
    """
    Sampler is responsible for deciding if a particular span should be
    "sampled", i.e. recorded in permanent storage.
    """

    def __init__(self, tags: Optional[_TagsType] = None) -> None:
        self._tags = tags or {}

    def is_sampled(self, trace_id: int, operation: str = '') -> _IsSampledType:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, self.__class__) and self.__dict__ == other.__dict__
        )

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)


class ConstSampler(Sampler):
    """ConstSampler always returns the same decision."""

    def __init__(self, decision: bool) -> None:
        super(ConstSampler, self).__init__(
            tags={
                SAMPLER_TYPE_TAG_KEY: SAMPLER_TYPE_CONST,
                SAMPLER_PARAM_TAG_KEY: decision,
            }
        )
        self.decision = decision

    def is_sampled(self, trace_id: int, operation: str = '') -> _IsSampledType:
        return self.decision, self._tags

    def close(self):
        pass

    def __str__(self) -> str:
        return 'ConstSampler(%s)' % self.decision


class ProbabilisticSampler(Sampler):
    """
    A sampler that randomly samples a certain percentage of traces specified
    by the samplingRate, in the range between 0.0 and 1.0.

    It relies on the fact that new trace IDs are 64bit random numbers
    themselves, thus making the sampling decision without generating a new
    random number, but simply calculating if traceID < (samplingRate * 2^64).
    Note that we actually ignore (zero out) the most significant bit.
    """

    def __init__(self, rate: float) -> None:
        super(ProbabilisticSampler, self).__init__(
            tags={
                SAMPLER_TYPE_TAG_KEY: SAMPLER_TYPE_PROBABILISTIC,
                SAMPLER_PARAM_TAG_KEY: rate,
            }
        )
        assert 0.0 <= rate <= 1.0, 'Sampling rate must be between 0.0 and 1.0'
        self.rate = rate
        self.max_number = 1 << _max_id_bits
        self.boundary = rate * self.max_number

    def is_sampled(self, trace_id: int, operation: str = '') -> _IsSampledType:
        trace_id = trace_id & (self.max_number - 1)
        return trace_id < self.boundary, self._tags

    def close(self) -> None:
        pass

    def __str__(self) -> str:
        return 'ProbabilisticSampler(%s)' % self.rate


class RateLimitingSampler(Sampler):
    """
    Samples at most max_traces_per_second. The distribution of sampled
    traces follows burstiness of the service, i.e. a service with uniformly
    distributed requests will have those requests sampled uniformly as well,
    but if requests are bursty, especially sub-second, then a number of
    sequential requests can be sampled each second.
    """

    def __init__(self, max_traces_per_second: float = 10) -> None:
        super(RateLimitingSampler, self).__init__()
        self.rate_limiter: RateLimiter = None  # type:ignore  # value is set below
        self._init(max_traces_per_second)

    def _init(self, max_traces_per_second):
        assert max_traces_per_second >= 0, \
            'max_traces_per_second must not be negative'
        self._tags = {
            SAMPLER_TYPE_TAG_KEY: SAMPLER_TYPE_RATE_LIMITING,
            SAMPLER_PARAM_TAG_KEY: max_traces_per_second,
        }
        self.traces_per_second = max_traces_per_second
        max_balance = max(self.traces_per_second, 1.0)
        if not self.rate_limiter:
            self.rate_limiter = RateLimiter(
                credits_per_second=self.traces_per_second,
                max_balance=max_balance
            )
        else:
            self.rate_limiter.update(max_traces_per_second, max_balance)

    def is_sampled(self, trace_id: int, operation: str = '') -> _IsSampledType:
        return self.rate_limiter.check_credit(1.0), self._tags

    def close(self) -> None:
        pass

    def __eq__(self, other: Any) -> bool:
        """The last_tick and balance fields can be different"""
        if not isinstance(other, self.__class__):
            return False
        d1 = dict(self.rate_limiter.__dict__)
        d2 = dict(other.rate_limiter.__dict__)
        d1['balance'] = d2['balance']
        d1['last_tick'] = d2['last_tick']
        return d1 == d2

    def update(self, max_traces_per_second: float) -> bool:
        if self.traces_per_second == max_traces_per_second:
            return False
        self._init(max_traces_per_second)
        return True

    def __str__(self) -> str:
        return 'RateLimitingSampler(%s)' % self.traces_per_second


class GuaranteedThroughputProbabilisticSampler(Sampler):
    """
    A sampler that leverages both ProbabilisticSampler and RateLimitingSampler.
    The RateLimitingSampler is used as a guaranteed lower bound sampler such
    that every operation is sampled at least once in a time interval defined by
    the lower_bound. ie a lower_bound of 1.0 / (60 * 10) will sample an
    operation at least once every 10 minutes.

    The ProbabilisticSampler is given higher priority when tags are emitted,
    ie. if is_sampled() for both samplers return true, the tags for
    ProbabilisticSampler will be used.
    """
    def __init__(self, operation: str, lower_bound: float, rate: float) -> None:
        super(GuaranteedThroughputProbabilisticSampler, self).__init__(
            tags={
                SAMPLER_TYPE_TAG_KEY: SAMPLER_TYPE_LOWER_BOUND,
                SAMPLER_PARAM_TAG_KEY: rate,
            }
        )
        self.probabilistic_sampler = ProbabilisticSampler(rate)
        self.lower_bound_sampler = RateLimitingSampler(lower_bound)
        self.operation = operation
        self.rate = rate
        self.lower_bound = lower_bound

    def is_sampled(self, trace_id: int, operation: str = '') -> _IsSampledType:
        sampled, tags = \
            self.probabilistic_sampler.is_sampled(trace_id, operation)
        if sampled:
            self.lower_bound_sampler.is_sampled(trace_id, operation)
            return True, tags
        sampled, _ = self.lower_bound_sampler.is_sampled(trace_id, operation)
        return sampled, self._tags

    def close(self) -> None:
        self.probabilistic_sampler.close()
        self.lower_bound_sampler.close()

    def update(self, lower_bound: int, rate: float) -> None:
        # (NB) This function should only be called while holding a Write lock.
        if self.rate != rate:
            self.probabilistic_sampler = ProbabilisticSampler(rate)
            self.rate = rate
            self._tags = {
                SAMPLER_TYPE_TAG_KEY: SAMPLER_TYPE_LOWER_BOUND,
                SAMPLER_PARAM_TAG_KEY: rate,
            }
        if self.lower_bound != lower_bound:
            self.lower_bound_sampler.update(lower_bound)
            self.lower_bound = lower_bound

    def __str__(self) -> str:
        return 'GuaranteedThroughputProbabilisticSampler(%s, %f, %f)' \
               % (self.operation, self.rate, self.lower_bound)


class AdaptiveSampler(Sampler):
    """
    A sampler that leverages both ProbabilisticSampler and RateLimitingSampler
    via the GuaranteedThroughputProbabilisticSampler. This sampler keeps track
    of all operations and delegates calls the the respective
    GuaranteedThroughputProbabilisticSampler.
    """
    def __init__(self, strategies: Dict[str, Any], max_operations: int) -> None:
        super(AdaptiveSampler, self).__init__()

        samplers = {}
        for strategy in strategies.get(STRATEGIES_STR, []):
            operation = strategy.get(OPERATION_STR)
            sampler = GuaranteedThroughputProbabilisticSampler(
                operation,
                strategies.get(DEFAULT_LOWER_BOUND_STR, DEFAULT_LOWER_BOUND),
                get_sampling_probability(strategy)
            )
            samplers[operation] = sampler

        self.samplers = samplers
        self.default_sampler = \
            ProbabilisticSampler(strategies.get(DEFAULT_SAMPLING_PROBABILITY_STR,
                                                DEFAULT_SAMPLING_PROBABILITY))
        self.default_sampling_probability = \
            strategies.get(DEFAULT_SAMPLING_PROBABILITY_STR, DEFAULT_SAMPLING_PROBABILITY)
        self.lower_bound = strategies.get(DEFAULT_LOWER_BOUND_STR, DEFAULT_LOWER_BOUND)
        self.max_operations = max_operations

    def is_sampled(self, trace_id: int, operation: str = '') -> _IsSampledType:
        sampler = self.samplers.get(operation)
        if not sampler:
            if len(self.samplers) >= self.max_operations:
                return self.default_sampler.is_sampled(trace_id, operation)
            sampler = GuaranteedThroughputProbabilisticSampler(
                operation,
                self.lower_bound,
                self.default_sampling_probability
            )
            self.samplers[operation] = sampler
            return sampler.is_sampled(trace_id, operation)
        return sampler.is_sampled(trace_id, operation)

    def update(self, strategies: Dict[str, Any]) -> None:
        # (NB) This function should only be called while holding a Write lock.
        for strategy in strategies.get(STRATEGIES_STR, []):
            operation = strategy.get(OPERATION_STR)
            lower_bound = strategies.get(DEFAULT_LOWER_BOUND_STR, DEFAULT_LOWER_BOUND)
            sampling_rate = get_sampling_probability(strategy)
            sampler = self.samplers.get(operation)
            if not sampler:
                sampler = GuaranteedThroughputProbabilisticSampler(
                    operation,
                    lower_bound,
                    sampling_rate
                )
                self.samplers[operation] = sampler
            else:
                sampler.update(lower_bound, sampling_rate)
        self.lower_bound = strategies.get(DEFAULT_LOWER_BOUND_STR, DEFAULT_LOWER_BOUND)
        if self.default_sampling_probability != strategies.get(DEFAULT_SAMPLING_PROBABILITY_STR,
                                                               DEFAULT_SAMPLING_PROBABILITY):
            self.default_sampling_probability = \
                strategies.get(DEFAULT_SAMPLING_PROBABILITY_STR, DEFAULT_SAMPLING_PROBABILITY)
            self.default_sampler = \
                ProbabilisticSampler(self.default_sampling_probability)

    def close(self) -> None:
        for _, sampler in self.samplers.items():
            sampler.close()

    def __str__(self) -> str:
        return 'AdaptiveSampler(%f, %f, %d)' \
               % (self.default_sampling_probability, self.lower_bound,
                  self.max_operations)


class RemoteControlledSampler(Sampler):
    """Periodically loads the sampling strategy from a remote server."""
    def __init__(self, channel: Any, service_name: str, **kwargs: Any) -> None:
        """
        :param channel: channel for communicating with jaeger-agent
        :param service_name: name of this application
        :param kwargs: optional parameters
            - init_sampler: initial value of the sampler,
                else ProbabilisticSampler(0.001)
            - sampling_refresh_interval: interval in seconds for polling
              for new strategy
            - logger: Logger instance
            - metrics: metrics facade, used to emit metrics on errors.
                This parameter has been deprecated, please use
                metrics_factory instead.
            - metrics_factory: used to generate metrics for errors
            - error_reporter: ErrorReporter instance
            - max_operations: maximum number of unique operations the
              AdaptiveSampler will keep track of
        :param init:
        :return:
        """
        super(RemoteControlledSampler, self).__init__()
        self._channel = channel
        self.service_name = service_name
        self.logger = kwargs.get('logger', default_logger)
        self.sampler = kwargs.get('init_sampler')
        self.sampling_refresh_interval = \
            kwargs.get('sampling_refresh_interval') or DEFAULT_SAMPLING_INTERVAL
        self.metrics_factory = kwargs.get('metrics_factory') \
            or LegacyMetricsFactory(kwargs.get('metrics') or Metrics())
        self.metrics = SamplerMetrics(self.metrics_factory)
        self.error_reporter = kwargs.get('error_reporter') or \
            ErrorReporter(Metrics())
        self.max_operations = kwargs.get('max_operations') or \
            DEFAULT_MAX_OPERATIONS

        if not self.sampler:
            self.sampler = ProbabilisticSampler(DEFAULT_SAMPLING_PROBABILITY)
        else:
            self.sampler.is_sampled(0)  # assert we got valid sampler API

        self.lock = Lock()
        self.running = True
        self.periodic = None

        self.io_loop = channel.io_loop
        if not self.io_loop:
            self.logger.error(
                'Cannot acquire IOLoop, sampler will not be updated')
        else:
            # according to IOLoop docs, it's not safe to use timeout methods
            # unless already running in the loop, so we use `add_callback`
            self.io_loop.add_callback(self._init_polling)

    def is_sampled(self, trace_id: int, operation: str = '') -> _IsSampledType:
        with self.lock:
            assert self.sampler  # needed for mypy
            return self.sampler.is_sampled(trace_id, operation)

    def _init_polling(self):
        """
        Bootstrap polling for sampling strategy.

        To avoid spiky traffic from the samplers, we use a random delay
        before the first poll.
        """
        with self.lock:
            if not self.running:
                return
            r = random.Random()
            delay = r.random() * self.sampling_refresh_interval
            self.io_loop.call_later(delay=delay,
                                    callback=self._delayed_polling)
            self.logger.info(
                'Delaying sampling strategy polling by %d sec', delay)

    def _delayed_polling(self):
        periodic = self._create_periodic_callback()
        self._poll_sampling_manager()  # Initialize sampler now
        with self.lock:
            if not self.running:
                return
            self.periodic = periodic
            periodic.start()  # start the periodic cycle
            self.logger.info(
                'Tracing sampler started with sampling refresh '
                'interval %d sec', self.sampling_refresh_interval)

    def _create_periodic_callback(self):
        return PeriodicCallback(
            callback=self._poll_sampling_manager,
            # convert interval to milliseconds
            callback_time=self.sampling_refresh_interval * 1000)

    def _sampling_request_callback(self, future):
        exception = future.exception()
        if exception:
            self.metrics.sampler_query_failure(1)
            self.error_reporter.error(
                'Fail to get sampling strategy from jaeger-agent: %s',
                exception)
            return

        response = future.result()

        # In Python 3.5 response.body is of type bytes and json.loads() does only support str
        # See: https://github.com/jaegertracing/jaeger-client-python/issues/180
        if hasattr(response.body, 'decode') and callable(response.body.decode):
            response_body = response.body.decode('utf-8')
        else:
            response_body = response.body

        try:
            sampling_strategies_response = json.loads(response_body)
            self.metrics.sampler_retrieved(1)
        except Exception as e:
            self.metrics.sampler_query_failure(1)
            self.error_reporter.error(
                'Fail to parse sampling strategy '
                'from jaeger-agent: %s [%s]', e, response_body)
            return

        self._update_sampler(sampling_strategies_response)
        self.logger.debug('Tracing sampler set to %s', self.sampler)

    def _update_sampler(self, response):
        with self.lock:
            try:
                if response.get(OPERATION_SAMPLING_STR):
                    self._update_adaptive_sampler(response.get(OPERATION_SAMPLING_STR))
                else:
                    self._update_rate_limiting_or_probabilistic_sampler(response)
            except Exception as e:
                self.metrics.sampler_update_failure(1)
                self.error_reporter.error(
                    'Fail to update sampler'
                    'from jaeger-agent: %s [%s]', e, response)

    def _update_adaptive_sampler(self, per_operation_strategies):
        if isinstance(self.sampler, AdaptiveSampler):
            self.sampler.update(per_operation_strategies)
        else:
            self.sampler = AdaptiveSampler(per_operation_strategies, self.max_operations)
        self.metrics.sampler_updated(1)

    def _update_rate_limiting_or_probabilistic_sampler(self, response):
        s_type = response.get(STRATEGY_TYPE_STR)
        new_sampler = self.sampler
        if s_type == PROBABILISTIC_SAMPLING_STRATEGY:
            sampling_rate = get_sampling_probability(response)
            new_sampler = ProbabilisticSampler(rate=sampling_rate)
        elif s_type == RATE_LIMITING_SAMPLING_STRATEGY:
            mtps = get_rate_limit(response)
            if mtps < 0 or mtps >= 500:
                raise ValueError(
                    'Rate limiting parameter not in [0, 500) range: %s' % mtps)
            if isinstance(self.sampler, RateLimitingSampler):
                if self.sampler.update(max_traces_per_second=mtps):
                    self.metrics.sampler_updated(1)
            else:
                new_sampler = RateLimitingSampler(max_traces_per_second=mtps)
        else:
            raise ValueError('Unsupported sampling strategy type: %s' % s_type)

        if self.sampler != new_sampler:
            self.sampler = new_sampler
            self.metrics.sampler_updated(1)

    def _poll_sampling_manager(self):
        self.logger.debug('Requesting tracing sampler refresh')
        fut = self._channel.request_sampling_strategy(self.service_name)
        fut.add_done_callback(self._sampling_request_callback)

    def close(self) -> None:
        with self.lock:
            self.running = False
            if self.periodic:
                self.periodic.stop()


def get_sampling_probability(strategy: Optional[Dict[str, Any]] = None) -> float:
    if not strategy:
        return DEFAULT_SAMPLING_PROBABILITY
    probability_strategy = strategy.get(PROBABILISTIC_SAMPLING_STR)
    if not probability_strategy:
        return DEFAULT_SAMPLING_PROBABILITY
    return probability_strategy.get(SAMPLING_RATE_STR, DEFAULT_SAMPLING_PROBABILITY)


def get_rate_limit(strategy: Optional[Dict[str, Any]] = None) -> float:
    if not strategy:
        return DEFAULT_LOWER_BOUND
    rate_limit_strategy = strategy.get(RATE_LIMITING_SAMPLING_STR)
    if not rate_limit_strategy:
        return DEFAULT_LOWER_BOUND
    return rate_limit_strategy.get(MAX_TRACES_PER_SECOND_STR, DEFAULT_LOWER_BOUND)


class SamplerMetrics(object):
    """Sampler specific metrics."""

    def __init__(self, metrics_factory: MetricsFactory) -> None:
        self.sampler_retrieved = \
            metrics_factory.create_counter(name='jaeger:sampler_queries', tags={'result': 'ok'})
        self.sampler_query_failure = \
            metrics_factory.create_counter(name='jaeger:sampler_queries', tags={'result': 'err'})
        self.sampler_updated = \
            metrics_factory.create_counter(name='jaeger:sampler_updates', tags={'result': 'ok'})
        self.sampler_update_failure = \
            metrics_factory.create_counter(name='jaeger:sampler_updates', tags={'result': 'err'})
