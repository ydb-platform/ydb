# Copyright (c) 2018 Uber Technologies, Inc.
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
from typing import Any, Optional

from tornado.ioloop import PeriodicCallback

from .constants import DEFAULT_THROTTLER_REFRESH_INTERVAL
from .metrics import Metrics, MetricsFactory
from .utils import ErrorReporter

MINIMUM_CREDITS = 1.0
default_logger = logging.getLogger('jaeger_tracing')


class Throttler(object):
    def set_client_id(self, client_id: int) -> None:
        """
        Called by tracer to set client ID of throttler.
        """
        pass

    def is_allowed(self, operation: str) -> bool:
        raise NotImplementedError()

    def close(self) -> None:
        pass


class RemoteThrottler(Throttler):
    """
    RemoteThrottler controls the flow of spans emitted from client to prevent
    flooding. RemoteThrottler requests credits from the throttling service
    periodically. These credits determine the amount of debug spans a client
    may emit for a particular operation without receiving more credits.
    :param channel: channel for communicating with jaeger-agent
    :param service_name: name of this application
    :param kwargs: optional parameters
        - refresh_interval: interval in seconds for requesting more credits
        - logger: Logger instance
        - metrics_factory: factory to create throttler-specific metrics
        - error_reporter: ErrorReporter instance
    """

    def __init__(self, channel: Any, service_name: str, **kwargs: Any) -> None:
        self.channel = channel
        self.service_name = service_name
        self.client_id: Optional[int] = None
        self.refresh_interval = \
            kwargs.get('refresh_interval', DEFAULT_THROTTLER_REFRESH_INTERVAL)
        self.logger = kwargs.get('logger', default_logger)
        metrics_factory = kwargs.get('metrics_factory', MetricsFactory())
        self.metrics = ThrottlerMetrics(metrics_factory)
        self.error_reporter = kwargs.get('error_reporter', ErrorReporter(Metrics()))
        self.credits: dict = {}
        self.lock = Lock()
        self.running = True
        self.periodic = None

        if not self.channel.io_loop:
            self.logger.error(
                'Cannot acquire IOLoop, throttler will not be updated')
        else:
            self.channel.io_loop.add_callback(self._init_polling)

    def is_allowed(self, operation: str) -> bool:
        with self.lock:
            if operation not in self.credits:
                self.credits[operation] = 0.0
                self.metrics.throttled_debug_spans(1)
                return False
            value = self.credits[operation]
            if value < MINIMUM_CREDITS:
                self.metrics.throttled_debug_spans(1)
                return False

            self.credits[operation] = value - MINIMUM_CREDITS
            return True

    def set_client_id(self, client_id: int) -> None:
        with self.lock:
            if self.client_id is None:
                self.client_id = client_id

    def _init_polling(self):
        """
        Bootstrap polling for throttler.

        To avoid spiky traffic from throttler clients, we use a random delay
        before the first poll.
        """
        with self.lock:
            if not self.running:
                return

            r = random.Random()
            delay = r.random() * self.refresh_interval
            self.channel.io_loop.call_later(
                delay=delay, callback=self._delayed_polling)
            self.logger.info(
                'Delaying throttling credit polling by %d sec', delay)

    def _operations(self):
        with self.lock:
            return self.credits.keys()

    def _delayed_polling(self):
        def callback():
            self._fetch_credits(self._operations())

        periodic = PeriodicCallback(
            callback=callback,
            # convert interval to milliseconds
            callback_time=self.refresh_interval * 1000)
        self._fetch_credits(self._operations())
        with self.lock:
            if not self.running:
                return
            self.periodic = periodic
            self.periodic.start()
            self.logger.info(
                'Throttling client started with refresh interval %d sec',
                self.refresh_interval)

    def _fetch_credits(self, operations):
        if not operations:
            return
        self.logger.debug('Requesting throttling credits')
        fut = self.channel.request_throttling_credits(
            self.service_name, self.client_id, operations)
        fut.add_done_callback(self._request_callback)

    def _request_callback(self, future):
        exception = future.exception()
        if exception:
            self.metrics.throttler_update_failure(1)
            self.error_reporter.error(
                'Failed to get throttling credits from jaeger-agent: %s',
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
            throttling_response = json.loads(response_body)
            self.logger.debug('Received throttling response: %s',
                              throttling_response)
            self._update_credits(throttling_response)
            self.metrics.throttler_update_success(1)
        except Exception as e:
            self.metrics.throttler_update_failure(1)
            self.error_reporter.error(
                'Failed to parse throttling credits response '
                'from jaeger-agent: %s [%s]', e, response_body)
            return

    def _update_credits(self, response):
        with self.lock:
            for op_balance in response['balances']:
                op = op_balance['operation']
                balance = op_balance['balance']
                if op not in self.credits:
                    self.credits[op] = 0
                self.credits[op] += balance
            self.logger.debug('credits = %s', self.credits)

    def close(self) -> None:
        with self.lock:
            self.running = False
            if self.periodic:
                self.periodic.stop()


class ThrottlerMetrics(object):
    """
    Metrics specific to throttler.
    """

    def __init__(self, metrics_factory: MetricsFactory) -> None:
        self.throttled_debug_spans = \
            metrics_factory.create_counter(name='jaeger:throttled_debug_spans')
        self.throttler_update_success = \
            metrics_factory.create_counter(name='jaeger:throttler_update',
                                           tags={'result': 'ok'})
        self.throttler_update_failure = \
            metrics_factory.create_counter(name='jaeger:throttler_update',
                                           tags={'result': 'err'})
