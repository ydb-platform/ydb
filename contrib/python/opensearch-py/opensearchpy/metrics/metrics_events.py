# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import time
from typing import Optional

from events import Events

from opensearchpy.metrics.metrics import Metrics


class MetricsEvents(Metrics):
    """
    The MetricsEvents class implements the Metrics abstract base class
    and tracks metrics such as start time, end time, and service time
    during request processing.
    """

    @property
    def start_time(self) -> Optional[float]:
        return self._start_time

    @property
    def end_time(self) -> Optional[float]:
        return self._end_time

    @property
    def service_time(self) -> Optional[float]:
        return self._service_time

    def __init__(self) -> None:
        self.events = Events()
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._service_time: Optional[float] = None

        # Subscribe to the request_start and request_end events
        self.events.request_start += self._on_request_start
        self.events.request_end += self._on_request_end

    def request_start(self) -> None:
        self.events.request_start()

    def _on_request_start(self) -> None:
        self._start_time = time.perf_counter()
        self._end_time = None
        self._service_time = None

    def request_end(self) -> None:
        self.events.request_end()

    def _on_request_end(self) -> None:
        self._end_time = time.perf_counter()
        if self._start_time is not None:
            self._service_time = self._end_time - self._start_time
