import requests


class BaseHttpEnv:
    def __init__(self, service):
        self.service = service

    @property
    def endpoint(self):
        return self.service.endpoint

    def get(self, path, **kwargs):
        kwargs.setdefault("timeout", 5)
        return requests.get(f"{self.endpoint}{path}", **kwargs)

    def post(self, path, **kwargs):
        kwargs.setdefault("timeout", 10)
        return requests.post(f"{self.endpoint}{path}", **kwargs)
