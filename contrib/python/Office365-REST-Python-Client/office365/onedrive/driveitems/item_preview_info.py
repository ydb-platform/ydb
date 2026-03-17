from office365.runtime.client_value import ClientValue


class ItemPreviewInfo(ClientValue):
    """Contains information about how to embed a preview of a driveItem.

    Either getUrl, postUrl, or both may be returned depending on the current state of support for the specified options.

    postParameters is a string formatted as application/x-www-form-urlencoded,
    and if performing a POST to the postUrl the content-type should be set accordingly. For example:

    POST https://www.onedrive.com/embed_by_post
    Content-Type: application/x-www-form-urlencoded

    param1=value&param2=another%20value
    """

    def __init__(self, get_url=None, post_parameters=None, post_url=None):
        """
        :param str get_url: URL suitable for embedding using HTTP GET (iframes, etc.)
        :param str post_parameters: POST parameters to include if using postUrl
        :param str post_url: URL suitable for embedding using HTTP POST (form post, JS, etc.)
        """
        self.getUrl = get_url
        self.postParameters = post_parameters
        self.postUrl = post_url
