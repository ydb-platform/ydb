from office365.sharepoint.entity import Entity


class ReorderingRuleCollection(Entity):
    """Contains information about how to reorder the search results."""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.ReorderingRuleCollection"
