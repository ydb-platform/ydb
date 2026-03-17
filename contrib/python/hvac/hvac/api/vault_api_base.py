"""Base class used by all hvac "api" classes."""
import logging
from abc import ABCMeta

logger = logging.getLogger(__name__)


class VaultApiBase(metaclass=ABCMeta):
    """Base class for API endpoints."""

    def __init__(self, adapter):
        """Default api class constructor.

        :param adapter: Instance of :py:class:`hvac.adapters.Adapter`; used for performing HTTP requests.
        :type adapter: hvac.adapters.Adapter
        """
        self._adapter = adapter
