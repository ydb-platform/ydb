# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

SERVICE_CRITICALITY: Final = "service.criticality"
"""
The operational criticality of the service.
Note: Application developers are encouraged to set `service.criticality` to express the operational importance of their services. Telemetry consumers MAY use this attribute to optimize telemetry collection or improve user experience.
"""

SERVICE_INSTANCE_ID: Final = "service.instance.id"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.service_attributes.SERVICE_INSTANCE_ID`.
"""

SERVICE_NAME: Final = "service.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.service_attributes.SERVICE_NAME`.
"""

SERVICE_NAMESPACE: Final = "service.namespace"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.service_attributes.SERVICE_NAMESPACE`.
"""

SERVICE_PEER_NAME: Final = "service.peer.name"
"""
Logical name of the service on the other side of the connection. SHOULD be equal to the actual [`service.name`](/docs/resource/README.md#service) resource attribute of the remote service if any.
"""

SERVICE_PEER_NAMESPACE: Final = "service.peer.namespace"
"""
Logical namespace of the service on the other side of the connection. SHOULD be equal to the actual [`service.namespace`](/docs/resource/README.md#service) resource attribute of the remote service if any.
"""

SERVICE_VERSION: Final = "service.version"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.service_attributes.SERVICE_VERSION`.
"""


class ServiceCriticalityValues(Enum):
    CRITICAL = "critical"
    """Service is business-critical; downtime directly impacts revenue, user experience, or core functionality."""
    HIGH = "high"
    """Service is important but has degradation tolerance or fallback mechanisms."""
    MEDIUM = "medium"
    """Service provides supplementary functionality; degradation has limited user impact."""
    LOW = "low"
    """Service is non-essential to core operations; used for background tasks or internal tools."""
