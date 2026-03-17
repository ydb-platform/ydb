from office365.runtime.client_value import ClientValue


class TeamFunSettings(ClientValue):
    """Settings to configure use of Giphy, memes, and stickers in the team."""

    def __init__(
        self,
        allow_custom_memes=None,
        allow_giphy=None,
        allow_stickers_and_memes=None,
        giphy_content_rating=None,
    ):
        """
        :param bool allow_custom_memes: f set to true, enables users to include custom memes.
        :param bool allow_giphy: If set to true, enables Giphy use.
        :param bool allow_stickers_and_memes: 	If set to true, enables users to include stickers and memes.
        :param str giphy_content_rating: Giphy content rating. Possible values are: moderate, strict.
        """
        super(TeamFunSettings, self).__init__()
        self.allowCustomMemes = allow_custom_memes
        self.allowGiphy = allow_giphy
        self.allowStickersAndMemes = allow_stickers_and_memes
        self.giphyContentRating = giphy_content_rating
