from .common import update_from_env

LOG_LEVEL = None
LOG_PATTERN = "%(asctime)-15s\t%(levelname)s\t%(message)s"
LOG_PATH = None

update_from_env(globals())
