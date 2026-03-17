import functools
import warnings


def deprecated(reason):
    """Decorator which can be used to mark function or method as deprecated.
    It will result a warning being emitted when the function is called."""

    def decorator(func):
        @functools.wraps(func)
        def deprecated_call(*args, **kwargs):
            warnings.simplefilter("always", DeprecationWarning)
            warnings.warn(reason, DeprecationWarning, stacklevel=2)
            warnings.simplefilter("default", DeprecationWarning)
            return func(*args, **kwargs)

        return deprecated_call

    return decorator


def _rewrite_server_name(server_name, new_port):
    """Rewrite server port in ``server_name`` with ``new_port`` value."""
    sep = ":"
    if sep in server_name:
        server_name, _ = server_name.split(sep, 1)
    return sep.join((server_name, new_port))


def _determine_scope(*, fixture_name, config):
    return config.getini("live_server_scope")


def _make_accept_header(mimetype):
    return [("Accept", mimetype)]
