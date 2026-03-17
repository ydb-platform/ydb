# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from abc import ABC, abstractmethod
from typing import Optional


class Metrics(ABC):
    """
    The Metrics class defines methods and properties for managing
    request metrics, including start time, end time, and service time,
    serving as a blueprint for concrete implementations.
    """

    @abstractmethod
    def request_start(self) -> None:
        pass

    @abstractmethod
    def request_end(self) -> None:
        pass

    @property
    @abstractmethod
    def start_time(self) -> Optional[float]:
        pass

    @property
    @abstractmethod
    def end_time(self) -> Optional[float]:
        pass

    @property
    @abstractmethod
    def service_time(self) -> Optional[float]:
        pass
