from . import version  # noqa
from .config_obj import Config, deserialize, serialize  # noqa

__version__ = version.get_versions()["version"]
