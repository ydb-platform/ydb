"""
A utility logger that allows multiple loggers to be daisy-chained.
"""
from typing import Dict, Any, Optional, IO
import sys

from spacy import Language
from .util import LoggerT


def chain_logger_v1(
    logger1: Optional[LoggerT] = None,
    logger2: Optional[LoggerT] = None,
    logger3: Optional[LoggerT] = None,
    logger4: Optional[LoggerT] = None,
    logger5: Optional[LoggerT] = None,
    logger6: Optional[LoggerT] = None,
    logger7: Optional[LoggerT] = None,
    logger8: Optional[LoggerT] = None,
    logger9: Optional[LoggerT] = None,
    logger10: Optional[LoggerT] = None,
) -> LoggerT:
    def setup_logger(nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr):
        loggers = [
            logger1,
            logger2,
            logger3,
            logger4,
            logger5,
            logger6,
            logger7,
            logger8,
            logger9,
            logger10,
        ]
        if not any(loggers):
            raise ValueError("No loggers passed to chain logger")
        callbacks = [
            setup(nlp, stdout, stderr) for setup in loggers if setup is not None
        ]

        def log_step(info: Optional[Dict[str, Any]]):
            nonlocal callbacks
            for log_stepper, _ in callbacks:
                log_stepper(info)

        def finalize():
            nonlocal callbacks
            for _, finalizer in callbacks:
                finalizer()

        return log_step, finalize

    return setup_logger
