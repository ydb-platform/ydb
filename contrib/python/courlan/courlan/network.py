"""
Functions devoted to requests over the WWW.
"""

import logging

import urllib3


LOGGER = logging.getLogger(__name__)
urllib3.disable_warnings()


RETRY_STRATEGY = urllib3.util.Retry(
    total=2,
    redirect=2,
    raise_on_redirect=False,
    status_forcelist=[
        429,
        499,
        500,
        502,
        503,
        504,
        509,
        520,
        521,
        522,
        523,
        524,
        525,
        526,
        527,
        530,
        598,
    ],  # unofficial: https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#Unofficial_codes
    backoff_factor=1,
)
HTTP_POOL = urllib3.PoolManager(
    cert_reqs="CERT_NONE", num_pools=100, retries=RETRY_STRATEGY, timeout=10
)

ACCEPTABLE_CODES = {200, 300, 301, 302, 303, 304, 305, 306, 307, 308}


# Test redirects
def redirection_test(url: str) -> str:
    """Test final URL to handle redirects
    Args:
        url: url to check

    Returns:
        The final URL seen.

    Raises:
        Nothing.
    """
    # headers.update({
    #    "User-Agent" : str(sample(settings.USER_AGENTS, 1)), # select a random user agent
    # })
    try:
        rhead = HTTP_POOL.request("HEAD", url)  # type:ignore[no-untyped-call]
    except Exception as err:
        LOGGER.exception("unknown error: %s %s", url, err)
    else:
        # response
        if rhead.status in ACCEPTABLE_CODES:
            LOGGER.debug("result found: %s %s", rhead.geturl(), rhead.status)
            return rhead.geturl()  # type: ignore
    # else:
    raise ValueError(f"cannot reach URL: ${url}")
