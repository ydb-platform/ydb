import logging
from pyroute2.config.asyncio import asyncio_config

log = logging.getLogger(__name__)
log.warning("Please use pyroute2.config.asyncio.asyncio_config")
log.warning("The eventlet module will be dropped soon ")

eventlet_config = asyncio_config
