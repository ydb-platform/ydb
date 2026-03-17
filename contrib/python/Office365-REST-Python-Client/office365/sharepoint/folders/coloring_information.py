from office365.runtime.client_value import ClientValue


class FolderColors(object):
    Yellow = "#FFCE3C"

    Grey = "#B0B7BA"

    DarkRed = "#E73E29"

    LightRed = "#FFBCB2"

    DarkOrange = "#EE7110"

    LightOrange = "#FFBF84"

    DarkGreen = "#3F9F4A"

    LightGreen = "#8ED290"

    DarkTeal = "#27938E"

    LightTeal = "#7AD1CD"

    DarkBlue = "#1E84D0"

    LightBlue = "#86C8F7"

    DarkPurple = "#9A61C7"

    LightPurple = "#D4AFF6"

    DarkPink = "#CC53B4"

    LightPink = "#F7AAE7"


class FolderColoringInformation(ClientValue):
    """"""

    def __init__(self, color_hex=None, color_tag=None, emoji=None):
        """
        :param str color_hex:
        :param str color_tag:
        :param str emoji:
        """
        self.ColorHex = color_hex
        self.ColorTag = color_tag
        self.Emoji = emoji

    @property
    def entity_type_name(self):
        return "SP.FolderColoringInformation"
