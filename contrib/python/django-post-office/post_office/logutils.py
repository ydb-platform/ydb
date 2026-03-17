import logging
from logging.config import dictConfig


# Taken from https://github.com/nvie/rq/blob/master/rq/logutils.py
def setup_loghandlers(level=None):
    # Setup logging for post_office if not already configured
    logger = logging.getLogger('post_office')
    if not logger.handlers:
        dictConfig(
            {
                'version': 1,
                'disable_existing_loggers': False,
                'formatters': {
                    'post_office': {
                        'format': '[%(levelname)s]%(asctime)s PID %(process)d: %(message)s',
                        'datefmt': '%Y-%m-%d %H:%M:%S',
                    },
                },
                'handlers': {
                    'post_office': {'level': 'DEBUG', 'class': 'logging.StreamHandler', 'formatter': 'post_office'},
                },
                'loggers': {'post_office': {'handlers': ['post_office'], 'level': level or 'DEBUG'}},
            }
        )
    return logger
