import logging

LOGGING_SECRET_LVL = 5
LOGGING_SECRET_NAME = "SECDEBUG"


def ensure_debug_secrets():
    """Idempotent: add debug_secrets to Python's logging.Logger class."""
    if "debug_secrets" not in logging.Logger.__dict__:

        def _log_secrets(self, *args, **kwargs):
            self.log(LOGGING_SECRET_LVL, *args, **kwargs)

        logging.Logger.debug_secrets = _log_secrets


# noinspection PyClassHasNoInit
class LoggerContext:
    """Superclass for all classes that require namespaced logger."""

    _logger = None

    @classmethod
    def logger(cls, method=None):
        if cls._logger is None:
            logger_name = cls.__module__ + "." + cls.__name__
            cls._logger = logging.getLogger(logger_name)

        if method is not None:
            return cls._logger.getChild(method)
        return cls._logger
