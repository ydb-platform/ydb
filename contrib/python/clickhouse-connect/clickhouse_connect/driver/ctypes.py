import logging
import os

import clickhouse_connect.driver.dataconv as pydc
import clickhouse_connect.driver.npconv as pync
from clickhouse_connect.driver.buffer import ResponseBuffer
from clickhouse_connect.driver.common import coerce_bool

logger = logging.getLogger(__name__)

RespBuffCls = ResponseBuffer
data_conv = pydc
# numpy_conv is resolved lazily via __getattr__ to avoid eagerly importing numpy


def connect_c_modules():
    if not coerce_bool(os.environ.get("CLICKHOUSE_CONNECT_USE_C", True)):
        logger.info("ClickHouse Connect C optimizations disabled")
        return

    global RespBuffCls, data_conv
    try:
        import clickhouse_connect.driverc.dataconv as cdc
        from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer

        data_conv = cdc
        RespBuffCls = CResponseBuffer
        logger.debug("Successfully imported ClickHouse Connect C data optimizations")
    except ImportError as ex:
        logger.warning("Unable to connect optimized C data functions [%s], falling back to pure Python", str(ex))


def _resolve_numpy_conv():
    if "numpy_conv" in globals():
        return
    if coerce_bool(os.environ.get("CLICKHOUSE_CONNECT_USE_C", True)):
        try:
            import clickhouse_connect.driverc.npconv as cnc

            globals()["numpy_conv"] = cnc
            logger.debug("Successfully import ClickHouse Connect C/Numpy optimizations")
            return
        except ImportError as ex:
            logger.debug("Unable to connect ClickHouse Connect C to Numpy API [%s], falling back to pure Python", str(ex))
    globals()["numpy_conv"] = pync


def __getattr__(name):
    if name == "numpy_conv":
        _resolve_numpy_conv()
        return globals()["numpy_conv"]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


connect_c_modules()
