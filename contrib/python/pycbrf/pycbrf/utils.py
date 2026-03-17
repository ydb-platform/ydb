import requests


class WithRequests:
    """Mixin to perform HTTP requests."""

    req_timeout: int = 10
    
    req_user_agent: str = (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/74.0.3729.169 YaBrowser/19.6.2.594 (beta) Yowser/2.5 Safari/537.36'
    )

    @classmethod
    def _get_response(cls, url: str, **kwargs) -> requests.Response:

        kwargs_ = {
            'timeout': cls.req_timeout,
            'headers': {
                'User-Agent': cls.req_user_agent,
            },
        }
        kwargs_.update(kwargs)

        return requests.get(url, **kwargs_)
