from office365.runtime.client_value import ClientValue


class TitleArea(ClientValue):
    """Represents the title area of a given SharePoint page."""

    def __init__(
        self,
        alternative_text=None,
        enable_gradient_effect=None,
        image_web_url=None,
        show_author=None,
    ):
        """
        :param str alternative_text: Alternative text on the title area.
        :param bool enable_gradient_effect: Indicates whether the title area has a gradient effect enabled.
        :param str image_web_url:
        :param bool show_author:
        """
        self.alternativeText = alternative_text
        self.enableGradientEffect = enable_gradient_effect
        self.imageWebUrl = image_web_url
        self.showAuthor = show_author
