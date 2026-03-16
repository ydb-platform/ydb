class ClientExceptionProxy(object):
    def __init__(self, getter):
        self._getter = getter

    @classmethod
    def wrap_proxy_if_necessary(cls, exc):
        return exc if isinstance(exc, cls) else (lambda exceptions: exc)

    def __call__(self, exceptions):
        return self._getter(exceptions)
