import logging

def get_default_logger(level=None):
    logger = logging.getLogger("falcon_cors")
    logger.setLevel(logging.INFO)
    logger.propogate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        logger.addHandler(handler)
    return logger


