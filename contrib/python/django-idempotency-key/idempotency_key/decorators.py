from functools import wraps

from idempotency_key import utils

# NOTE:
# The following decorators must be specified BEFORE the @api_view decorator or the
# function will not be marked correctly.
#
# i.e:
#
# @idempotency_key
# @api_view(['POST'])
# def my_view_func()
#   ...


def idempotency_key(*args, optional=False, cache_name=None):
    """
    Allows an optional cache name to be specified so that different cache settings can
    be used on a per-view function basis.
    :param args: optional arguments. This can contain the view function object if
                 cache_name is not specified
    :param optional: Mark idempotency key header as optional
    :param cache_name: The name of the cache to use from the settings file under
                       CACHES={...}
    :return: wrapped function
    """

    def _idempotency_key(view_func):
        """
        Mark a view function as requiring idempotency key protection but the view
        should control the response.
        """

        @wraps(view_func)
        def wrapped_view(*args, **kwargs):
            return view_func(*args, **kwargs)

        wrapped_view.idempotency_key = True
        wrapped_view.idempotency_key_optional = optional

        if cache_name:
            wrapped_view.idempotency_key_cache_name = cache_name
            utils.get_storage_class().validate_storage(cache_name)

        return wrapped_view

    # if there is an argument passed and it is a callable then this will be the view
    # function object so pass it to the wrapper
    if len(args) > 0 and callable(args[0]):
        return _idempotency_key(args[0])

    # otherwise just return the wrapper object
    return _idempotency_key


def idempotency_key_exempt(view_func):
    """
    Mark a view function as being exempt from the idempotency key protection.
    """

    def wrapped_view(*args, **kwargs):
        return view_func(*args, **kwargs)

    wrapped_view.idempotency_key_exempt = True
    return wraps(view_func)(wrapped_view)


def idempotency_key_manual(view_func):
    """
    Mark a view function as requiring idempotency key protection but the view should
    control the response.
    """

    def wrapped_view(*args, **kwargs):
        return view_func(*args, **kwargs)

    wrapped_view.idempotency_key_manual = True
    return wraps(view_func)(wrapped_view)
