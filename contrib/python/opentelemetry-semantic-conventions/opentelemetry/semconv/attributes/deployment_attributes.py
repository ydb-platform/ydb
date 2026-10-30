# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

DEPLOYMENT_ENVIRONMENT_NAME: Final = "deployment.environment.name"
"""
Name of the [deployment environment](https://wikipedia.org/wiki/Deployment_environment) (aka deployment tier).
Note: `deployment.environment.name` does not affect the uniqueness constraints defined through
the `service.namespace`, `service.name` and `service.instance.id` resource attributes.
This implies that resources carrying the following attribute combinations MUST be
considered to be identifying the same service:

- `service.name=frontend`, `deployment.environment.name=production`
- `service.name=frontend`, `deployment.environment.name=staging`.
"""


class DeploymentEnvironmentNameValues(Enum):
    PRODUCTION = "production"
    """Production environment."""
    STAGING = "staging"
    """Staging environment."""
    TEST = "test"
    """Testing environment."""
    DEVELOPMENT = "development"
    """Development environment."""
