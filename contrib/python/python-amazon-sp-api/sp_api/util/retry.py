import time


def retry(exception_classes=None, tries=10, delay=5, rate=1.3):
    """
    retry(exception_classes=None, tries=10, delay=5, rate=1.3)

    Retry a call against an endpoint <tries> time


    Args:
        exception_classes: tuple | The Exceptions to be caught
        tries: int | How often the call should be retried
        delay: float | The delay after an error was caught
        rate: float | The rate to increment delay by

    Returns:

    """
    if exception_classes is None:
        exception_classes = (Exception,)

    tries_counter = {
        'count': 1,
        'last_delay': delay
    }

    def decorator(function):
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except exception_classes as e:
                if tries_counter.get('count') + 1 > tries:
                    raise e

                delay_now = delay if tries_counter.get('count') == 1 else tries_counter.get('last_delay') * rate
                tries_counter.update({
                    'count': tries_counter.get('count') + 1,
                    'last_delay': delay_now
                })
                time.sleep(delay_now)
                return wrapper(*args, **kwargs)
            finally:
                tries_counter.update({
                    'count': 1,
                    'last_delay': delay
                })

        wrapper.__doc__ = function.__doc__
        return wrapper

    return decorator


def sp_retry(exception_classes=(), tries=10, delay=5, rate=1.3):
    """
    This is a shorthand for retry that catches all exceptions thrown by this library

    Retry a call against an endpoint <tries> time
    Args:
        exception_classes:
        tries:
        delay:
        rate:

    Returns:

    """
    from sp_api.base import SellingApiException
    return retry((SellingApiException,) + exception_classes, tries, delay, rate)


def throttle_retry(exception_classes=(), tries=10, delay=5, rate=1.3):
    """
    This is a shorthand for retry that catches SellingApiRequestThrottledException

    Retry a call against an endpoint <tries> time
    Args:
        exception_classes:
        tries:
        delay:
        rate:

    Returns:

    """
    from sp_api.base import SellingApiRequestThrottledException
    return retry((SellingApiRequestThrottledException,) + exception_classes, tries, delay, rate)
