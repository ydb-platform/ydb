#!/usr/bin/env python
import logging
from abc import ABCMeta

from hvac.api.vault_api_base import VaultApiBase

logger = logging.getLogger(__name__)


class SystemBackendMixin(VaultApiBase, metaclass=ABCMeta):
    """Base class for System Backend API endpoints."""
