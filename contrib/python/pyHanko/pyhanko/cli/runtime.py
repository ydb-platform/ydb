import logging
import sys
from contextlib import contextmanager

import click

from pyhanko.cli.utils import logger
from pyhanko.config.logging import LogConfig, StdLogOutput
from pyhanko.pdf_utils import misc
from pyhanko.pdf_utils.layout import LayoutError
from pyhanko.sign.general import SigningError


class NoStackTraceFormatter(logging.Formatter):
    def formatException(self, ei) -> str:
        return ""  # pragma: nocover


LOG_FORMAT_STRING = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def logging_setup(log_configs, verbose: bool):
    log_config: LogConfig
    for module, log_config in log_configs.items():
        cur_logger = logging.getLogger(module)
        cur_logger.setLevel(log_config.level)
        handler: logging.StreamHandler
        if isinstance(log_config.output, StdLogOutput):
            if StdLogOutput == StdLogOutput.STDOUT:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = logging.StreamHandler()
            # when logging to the console, don't output stack traces
            # unless in verbose mode
            if verbose:
                formatter = logging.Formatter(LOG_FORMAT_STRING)
            else:
                formatter = NoStackTraceFormatter(LOG_FORMAT_STRING)
        else:
            handler = logging.FileHandler(log_config.output)
            formatter = logging.Formatter(LOG_FORMAT_STRING)
        handler.setFormatter(formatter)
        cur_logger.addHandler(handler)


@contextmanager
def pyhanko_exception_manager():
    msg = exception = None
    try:
        yield
    except click.ClickException:
        raise
    except misc.PdfStrictReadError as e:
        exception = e
        msg = (
            "Failed to read PDF file in strict mode; rerun with "
            "--no-strict-syntax to try again.\n"
            f"Error message: {e.msg}"
        )
    except misc.PdfReadError as e:
        exception = e
        msg = f"Failed to read PDF file: {e.msg}"
    except misc.PdfWriteError as e:
        exception = e
        msg = f"Failed to write PDF file: {e.msg}"
    except SigningError as e:
        exception = e
        msg = f"Error raised while producing signed file: {e.msg}"
    except LayoutError as e:
        exception = e
        msg = f"Error raised while producing signature layout: {e.msg}"
    except Exception as e:
        exception = e
        msg = "Generic processing error."

    if exception is not None:
        logger.error(msg, exc_info=exception)
        raise click.ClickException(msg)


DEFAULT_CONFIG_FILE = 'pyhanko.yml'
