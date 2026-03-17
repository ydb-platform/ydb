from typing import Callable, Sequence, Tuple, List, Optional

from .. import uwsgi
from ..utils import encode, decode, get_logger, decode_deep

_LOG = get_logger(__name__)


def register_rpc(name: str = None) -> Callable:
    """Decorator. Allows registering a function for RPC.

    * http://uwsgi.readthedocs.io/en/latest/RPC.html

    .. code-block:: python

        @register_rpc()
        def expose_me(arg1, arg2=15):
            print(f'RPC called {arg1}')
            return b'some'

        make_rpc_call('expose_me', ['value1'])

    .. warning:: Function expected to accept bytes args.
        Also expected to return bytes or ``None``.

    :param name: RPC function name to associate
        with decorated function.

    """
    def wrapper(func: Callable):
        func_name = func.__name__
        rpc_name = name or func_name

        uwsgi.register_rpc(rpc_name, func)

        _LOG.debug(f"Registering '{func_name}' for RPC under '{rpc_name}' alias ...")

        return func

    return wrapper


def make_rpc_call(func_name: str, *, args: Sequence[str] = None, remote: str = None) -> Optional[str]:
    """Performs an RPC function call (local or remote) with the given arguments.

    :param func_name: RPC function name to call.

    :param Iterable args: Function arguments.

        .. warning:: Strings are expected.

    :param remote:

    :raises ValueError: If unable to call RPC function.

    """
    args = args or []
    args = [encode(str(arg)) for arg in args]

    func_name = encode(func_name)

    if remote:
        result = uwsgi.rpc(encode(remote), func_name, *args)

    else:
        result = uwsgi.call(func_name, *args)

    return decode(result)


def get_rpc_list() -> List[str]:
    """Returns registered RPC functions names."""
    return decode_deep(uwsgi.rpc_list())
