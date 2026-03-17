from .sessions import Session
from .__version__ import __title__


def request(method, url, **kwargs):
    with Session() as session:
        return session.request(method=method, url=url, **kwargs)


def raw(url, data, **kwargs):
    kwargs.setdefault('allow_redirects', False)
    return request(__title__, url, data=data, **kwargs)
