from .. import uwsgi
from ..utils import decode


variable_set = uwsgi.set_logvar


def variable_get(name: str) -> str:
    """Return user-defined log variable contents.

    * http://uwsgi.readthedocs.io/en/latest/LogFormat.html#user-defined-logvars

    :param name:

    """
    return decode(uwsgi.get_logvar(name))


log_message = uwsgi.log

get_current_log_size = uwsgi.logsize
