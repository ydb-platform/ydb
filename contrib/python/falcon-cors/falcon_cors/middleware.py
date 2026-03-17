class CORSMiddleware:
    """This is the middleware that applies a CORS object to requests.

    Args:
        cors (CORS, required): An instance of :py:class:`~falcon.cors.CORS`.
        default_enabled (bool, optional): Whether CORS processing should
            take place for every resource.  Default ``True``.
    """
    def __init__(self, cors, default_enabled=True):
        self.cors = cors
        self.default_enabled = default_enabled

    def process_resource(self, req, resp, resource, *args):
        if not getattr(resource, 'cors_enabled', self.default_enabled):
            return
        cors = getattr(resource, 'cors', self.cors)
        cors.process(req, resp, resource)
