import time


def make_sleep_time(rate_limit, use_rate_limit_header, throttle_by_seconds):
    if use_rate_limit_header and rate_limit:
        return 1 / float(rate_limit)
    return throttle_by_seconds



def load_all_pages(throttle_by_seconds: float = 2, next_token_param='NextToken', use_rate_limit_header: bool = False,
                   extras: dict = None):
    """
    Load all pages if a next token is returned

    Args:
        throttle_by_seconds: float
        next_token_param: str | The param amazon expects to hold the next token
        use_rate_limit_header: if the function should try to use amazon's rate limit header
        extras: additional data to be sent with NextToken, e.g `dict(QueryType='NEXT_TOKEN')` for `FulfillmentInbound`
    Returns:
        Transforms the function in a generator, returning all pages
    """
    if not extras:
        extras = {}

    def decorator(function):
        def wrapper(*args, **kwargs):
            done = False
            while not done:
                res = function(*args, **kwargs)
                yield res
                if res.next_token:
                    sleep_time = make_sleep_time(res.rate_limit, use_rate_limit_header, throttle_by_seconds)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    kwargs.update({next_token_param: res.next_token, **extras})
                else:
                    done = True

        wrapper.__doc__ = function.__doc__
        return wrapper

    return decorator
