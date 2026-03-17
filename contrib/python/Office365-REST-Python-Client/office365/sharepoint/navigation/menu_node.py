from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.translation.resource_entry import SPResourceEntry


class MenuNode(ClientValue):
    """Represents a navigation node in the navigation hierarchy. A navigation hierarchy is a tree structure of
    navigation nodes."""

    def __init__(
        self,
        audience_ids=None,
        current_lcid=None,
        title=None,
        is_deleted=None,
        is_hidden=None,
        key=None,
        nodes=None,
        node_type=None,
        open_in_new_window=None,
        simple_url=None,
        translations=None,
    ):
        """
        :param list[str] audience_ids:
        :param int current_lcid:
        :param str title: Specifies the title of the navigation node. The value is in the preferred language of the
            user, if available, or is in the default language of the site (2) as a fallback.
        :param int node_type: Specifies the type of the navigation node.
        :param bool is_deleted: A client MUST use this property set to TRUE to indicate that this node MUST be
             deleted by the server. When set to TRUE the server MUST delete the node including all the child nodes
             when present. When set to FALSE the server SHOULD NOT delete this node.
        :param bool is_hidden: Specifies whether the node will be hidden in the navigation menu. During editing,
             all nodes temporarily become visible so that they can be edited.
        :param str key: Specifies the identifier for the navigation node in the menu tree. If the navigation node does
             not exist on the protocol server, this value MUST be an empty string.
        :param list[MenuNode] nodes:
        :param bool open_in_new_window:
        :param str simple_url: If the NodeType (section 3.2.5.244.1.1.7) property is set to "SimpleLink",
            this property represents the URL of the navigation node. The URL can be relative or absolute.
            If the value is a relative URL, it can begin with URL tokens "~site" and "~sitecollection".
            These tokens indicate that the URL is either relative to the site (2) or to the site collection
            respectively. If the NodeType (section 3.2.5.244.1.1.7) property is not set to "SimpleLink",
            the value MUST be an empty string.
         :param list[SPResourceEntry] translations:
        """
        self.AudienceIds = audience_ids
        self.CurrentLCID = current_lcid
        self.IsDeleted = is_deleted
        self.IsHidden = is_hidden
        self.Key = key
        self.Nodes = ClientValueCollection(MenuNode, nodes)
        self.NodeType = node_type
        self.OpenInNewWindow = open_in_new_window
        self.SimpleUrl = simple_url
        self.Title = title
        self.Translations = ClientValueCollection(SPResourceEntry, translations)
