import requests
from .adapters import RawAdapter
from .__version__ import __title__


class Session(requests.Session):
    def __init__(self):
        super(Session, self).__init__()
        self.mount("http://", RawAdapter())
        self.mount("https://", RawAdapter())

    def raw(self, url, data, **kwargs):
        # Fix https://github.com/realgam3/requests-raw/issues/4
        kwargs.setdefault('allow_redirects', False)
        return self.request(__title__, url, data=data, **kwargs)


def session():
    return Session()
