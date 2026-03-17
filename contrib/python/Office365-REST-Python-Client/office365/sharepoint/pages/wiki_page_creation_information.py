from office365.runtime.client_value import ClientValue


class WikiPageCreationInformation(ClientValue):
    def __init__(self, server_relative_url, content):
        """
        Specifies wiki page creation information

        :param str server_relative_url: The server-relative URL of the wiki page to be created.
        :param str content: The HTML content of the wiki page.
        """
        super(WikiPageCreationInformation, self).__init__()
        self.ServerRelativeUrl = server_relative_url
        self.WikiHtmlContent = content

    @property
    def entity_type_name(self):
        return "SP.Utilities.WikiPageCreationInformation"
