from reversion.views import _request_creates_revision, create_revision


class RevisionMiddleware:

    """Wraps the entire request in a revision."""

    manage_manually = False

    using = None

    atomic = True

    def __init__(self, get_response):
        self.get_response = create_revision(
            manage_manually=self.manage_manually,
            using=self.using,
            atomic=self.atomic,
            request_creates_revision=self.request_creates_revision
        )(get_response)

    def request_creates_revision(self, request):
        return _request_creates_revision(request)

    def __call__(self, request):
        return self.get_response(request)
