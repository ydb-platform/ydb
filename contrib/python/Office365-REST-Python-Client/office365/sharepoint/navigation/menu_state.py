from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import GuidCollection
from office365.sharepoint.navigation.menu_node import MenuNode


class MenuState(ClientValue):
    """A menu tree which can be shown in the Quick Launch of a site."""

    def __init__(
        self,
        audience_ids=None,
        friendly_url_prefix=None,
        nodes=None,
        simple_url=None,
        site_prefix=None,
    ):
        """
        :param list[uuid] audience_ids:
        :param str friendly_url_prefix: Specifies the site collection relative URL for the root node of the menu tree.
        :param list[MenuNode] nodes: The child nodes of the root node of the menu tree.
        :param str simple_url: f the NodeType property (section 3.2.5.244.1.1.7) of the menu tree root node is set
            to "SimpleLink", this property represents the URL of the root node. The URL can be relative or absolute.
            If the value is a relative URL, it can begin with URL tokens "~site" and "~sitecollection".
            These tokens indicate that the URL is either relative to the site (2) or to the site collection
            respectively. If the NodeType property (section 3.2.5.244.1.1.7) of the menu tree root node is not set
            to "SimpleLink", this value MUST be NULL.
        :param str site_prefix: Defines the text that SHOULD be substituted for "~sitecollection/" in relative links
            (such as "~sitecollection/Pages/MyPage.aspx ").
        """
        self.AudienceIds = GuidCollection(audience_ids)
        self.FriendlyUrlPrefix = friendly_url_prefix
        self.Nodes = ClientValueCollection(MenuNode, nodes)
        self.SimpleUrl = simple_url
        self.SPSitePrefix = site_prefix
