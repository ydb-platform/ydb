# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

DEPLOYMENT_ENVIRONMENT: Final = "deployment.environment"
"""
Deprecated: Replaced by `deployment.environment.name`.
"""

DEPLOYMENT_ENVIRONMENT_NAME: Final = "deployment.environment.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.deployment_attributes.DEPLOYMENT_ENVIRONMENT_NAME`.
"""

DEPLOYMENT_ID: Final = "deployment.id"
"""
The id of the deployment.
"""

DEPLOYMENT_NAME: Final = "deployment.name"
"""
The name of the deployment.
"""

DEPLOYMENT_STATUS: Final = "deployment.status"
"""
The status of the deployment.
"""


@deprecated(
    "Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.deployment_attributes.DeploymentEnvironmentNameValues`."
)
class DeploymentEnvironmentNameValues(Enum):
    PRODUCTION = "production"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.deployment_attributes.DeploymentEnvironmentNameValues.PRODUCTION`."""
    STAGING = "staging"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.deployment_attributes.DeploymentEnvironmentNameValues.STAGING`."""
    TEST = "test"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.deployment_attributes.DeploymentEnvironmentNameValues.TEST`."""
    DEVELOPMENT = "development"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.deployment_attributes.DeploymentEnvironmentNameValues.DEVELOPMENT`."""


class DeploymentStatusValues(Enum):
    FAILED = "failed"
    """failed."""
    SUCCEEDED = "succeeded"
    """succeeded."""
