from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.webs.time_zone_information import TimeZoneInformation


class TimeZone(Entity):
    """Represents the time zone setting that is implemented on a SharePoint Web site."""

    def local_time_to_utc(self, date):
        """
        Converts the specified date from local time to Coordinated Universal Time (UTC).

        :param datetime.datetime date: The local date and time value to convert.
        :return:
        """
        result = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "LocalTimeToUTC", [date], None, None, result)
        self.context.add_query(qry)
        return result

    def set_id(self, _id):
        """
        :type _id: int
        """
        qry = ServiceOperationQuery(self, "SetId", [_id], None, None, None)
        self.context.add_query(qry)
        return self

    @property
    def id(self):
        """Gets the identifier of the time zone."""
        return self.properties.get("Id", None)

    @property
    def description(self):
        """Gets the description of the time zone."""
        return self.properties.get("Description", None)

    @property
    def information(self):
        """Gets information about the time zone."""
        return self.properties.get("Information", TimeZoneInformation())


class TimeZoneCollection(EntityCollection[TimeZone]):
    """TimeZone collection"""

    def __init__(self, context, resource_path=None):
        super(TimeZoneCollection, self).__init__(context, TimeZone, resource_path)
