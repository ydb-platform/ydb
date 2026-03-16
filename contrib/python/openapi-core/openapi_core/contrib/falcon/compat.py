"""OpenAPI core contrib falcon compat module"""
try:
    from falcon import App  # noqa: F401
    HAS_FALCON3 = True
except ImportError:
    HAS_FALCON3 = False


def get_request_media(req, default=None):
    # in falcon 3 media is deprecated
    return req.get_media(default_when_empty=default) if HAS_FALCON3 else \
        (req.media if req.media else default)


def get_response_text(resp):
    # in falcon 3 body is deprecated
    return getattr(resp, 'text') if HAS_FALCON3 else \
        getattr(resp, 'body')


def set_response_text(resp, text):
    # in falcon 3 body is deprecated
    setattr(resp, 'text', text) if HAS_FALCON3 else \
        setattr(resp, 'body', text)
