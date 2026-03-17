import json
import os.path
import sys
from importlib.metadata import distribution

VERSION = (1, 14, 0)
__version__ = ".".join(map(str, VERSION))


PKG_ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.join(PKG_ROOT, "data-files")


def is_installed():
    if not is_dev():
        return True
    if _is_venv():
        return True
    return _is_devel_install()


def is_dev():
    parent = os.path.dirname(PKG_ROOT)
    return os.path.exists(os.path.join(parent, "pyproject.toml"))


def _is_venv():
    if sys.base_prefix == sys.prefix:
        return False
    return True


def _is_devel_install():
    # pip install -e <path-to-git-checkout> will do a "devel" install.
    # This means it creates a link back to the checkout instead
    # of copying the files.

    direct_url = distribution("pyperformance").read_text("direct_url.json")
    if direct_url:
        return json.loads(direct_url).get("dir_info", {}).get("editable", False)
    return False
