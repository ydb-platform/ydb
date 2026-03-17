import attr

from openapi_core.exceptions import OpenAPIError


class MediaTypeFinderError(OpenAPIError):
    """Media type finder error"""


@attr.s(hash=True)
class MediaTypeNotFound(MediaTypeFinderError):
    mimetype = attr.ib()
    availableMimetypes = attr.ib()

    def __str__(self):
        return (
            "Content for the following mimetype not found: {0}. "
            "Valid mimetypes: {1}"
        ).format(self.mimetype, self.availableMimetypes)
