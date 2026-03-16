from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.analytics.item_activity_stat import ItemActivityStat
from office365.runtime.paths.resource_path import ResourcePath


class ItemAnalytics(Entity):
    """The itemAnalytics resource provides analytics about activities that took place on an item.
    This resource is currently only available on SharePoint and OneDrive for Business.
    """

    @property
    def all_time(self):
        """Analytics over the item's lifespan."""
        return self.properties.get(
            "allTime",
            ItemActivityStat(self.context, ResourcePath("allTime", self.resource_path)),
        )

    @property
    def item_activity_stats(self):
        # type: () -> EntityCollection[ItemActivityStat]
        return self.properties.get(
            "itemActivityStats",
            EntityCollection(
                self.context,
                ItemActivityStat,
                ResourcePath("itemActivityStats", self.resource_path),
            ),
        )

    @property
    def last_seven_days(self):
        """Analytics for the last seven days."""
        return self.properties.get(
            "lastSevenDays",
            ItemActivityStat(
                self.context, ResourcePath("lastSevenDays", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "allTime": self.all_time,
                "itemActivityStats": self.item_activity_stats,
                "lastSevenDays": self.last_seven_days,
            }
            default_value = property_mapping.get(name, None)
        return super(ItemAnalytics, self).get_property(name, default_value)
