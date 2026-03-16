from office365.runtime.client_value import ClientValue


class NavigationNodeCreationInformation(ClientValue):
    def __init__(
        self,
        title=None,
        url=None,
        is_external=False,
        as_last_node=False,
        previous_node=None,
    ):
        """
        Describes a new navigation node to be created.

        :param NavigationNodeCreationInformation previous_node: Gets or sets a value that specifies the navigation node
            after which the new navigation node will appear in the navigation node collection.
        :param bool as_last_node: Gets or sets a value that specifies whether the navigation node will be created
            as the last node in the collection.
        :param str url: Gets or sets a value that specifies the URL to be stored with the navigation node.
        :param str title: Gets or sets a value that specifies the anchor text for the node navigation link.
        :param bool is_external: Gets or sets a value that specifies whether the navigation node URL potentially
            corresponds to pages outside of the site collection.


        """
        super(NavigationNodeCreationInformation, self).__init__()
        self.Title = title
        self.Url = url
        self.IsExternal = is_external
        self.AsLastNode = as_last_node
        self.PreviousNode = previous_node

    @property
    def entity_type_name(self):
        return "SP.NavigationNode"
