from office365.sharepoint.listhome.item import ListHomeItem


class FavoriteListHomeItem(ListHomeItem):
    """ """

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.ListHome.FavoriteListHomeItem"
