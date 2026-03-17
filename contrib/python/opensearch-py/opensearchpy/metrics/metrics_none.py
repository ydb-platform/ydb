# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from typing import Optional

from opensearchpy.metrics.metrics import Metrics


class MetricsNone(Metrics):
    """
    Default metrics class. It sets the start time, end time, and service time to None.
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
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._service_time: Optional[float] = None

    # request_start and request_end are placeholders,
    # not implementing actual metrics collection in this subclass.

    def request_start(self) -> None:
        self._start_time = None
        self._end_time = None
        self._service_time = None

    def request_end(self) -> None:
        self._end_time = None
        self._service_time = None
