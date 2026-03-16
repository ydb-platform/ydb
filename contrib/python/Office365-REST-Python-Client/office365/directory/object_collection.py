from typing_extensions import Self

from office365.delta_collection import DeltaCollection
from office365.directory.object import DirectoryObject
from office365.entity_collection import EntityCollection
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.service_operation import ServiceOperationQuery


class DirectoryObjectCollection(DeltaCollection[DirectoryObject]):
    """DirectoryObject's collection"""

    def __init__(self, context, resource_path=None):
        super(DirectoryObjectCollection, self).__init__(
            context, DirectoryObject, resource_path
        )

    def get_by_ids(self, ids, types=None):
        """
        Returns the directory objects specified in a list of IDs.
        :param list[str] ids: A collection of IDs for which to return objects. The IDs are GUIDs, represented as
            strings. You can specify up to 1000 IDs.
        :param list[str] types: A collection of resource types that specifies the set of resource collections to search.
            If not specified, the default is directoryObject, which contains all of the resource types defined in
            the directory. Any object that derives from directoryObject may be specified in the collection;
            for example: user, group, and device objects.
        """
        return_type = DirectoryObjectCollection(self.context)
        params = {"ids": ids, "types": types}
        qry = ServiceOperationQuery(self, "getByIds", params, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def add(self, directory_object):
        # type: (DirectoryObject) -> Self
        """Adds directory objects to the collection."""

        def _add():
            payload = {"@odata.id": directory_object.resource_url}
            qry = ServiceOperationQuery(self, "$ref", None, payload)
            self.context.add_query(qry)

        directory_object.ensure_property("id", _add)
        return self

    def get_available_extension_properties(self, is_synced_from_on_premises=None):
        """
        Return all or a filtered list of the directory extension properties that have been registered in a directory.
        The following entities support extension properties: user, group, organization, device, application,
        and servicePrincipal.
        """
        from office365.directory.extensions.extension_property import ExtensionProperty

        return_type = EntityCollection(
            self.context,
            ExtensionProperty,
            self.context.directory_objects.resource_path,
        )
        payload = {"isSyncedFromOnPremises": is_synced_from_on_premises}
        qry = ServiceOperationQuery(
            self, "getAvailableExtensionProperties", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def remove(self, directory_object):
        # type: (DirectoryObject|str) -> Self
        """Removes directory object from the collection.
        :param str directory_object: Directory object or identifier
        """

        def _remove(id_):
            qry = ServiceOperationQuery(self, "{0}/$ref".format(id_))

            def _construct_request(request):
                # type: (RequestOptions) -> None
                request.method = HttpMethod.Delete

            self.context.add_query(qry).before_query_execute(_construct_request)

        if isinstance(directory_object, DirectoryObject):

            def _loaded():
                _remove(directory_object.id)

            directory_object.ensure_property("id", _loaded)
        else:
            _remove(directory_object)

        return self

    def validate_properties(
        self,
        entity_type=None,
        display_name=None,
        mail_nickname=None,
        on_behalf_of_userid=None,
    ):
        """
        Validate that a Microsoft 365 group's display name or mail nickname complies with naming policies.
        Clients can use this API to determine whether a display name or mail nickname is valid before trying to
        create a Microsoft 365 group. To validate the properties of an existing group, use the group:
        validateProperties function.

        :param str entity_type: Group is the only supported entity type.
        :param str display_name: The display name of the group to validate. The property is not individually required.
             However, at least one property (displayName or mailNickname) is required.
        :param str mail_nickname: The mail nickname of the group to validate.
             The property is not individually required. However, at least one property (displayName or mailNickname)
             is required.
        :param str on_behalf_of_userid: The ID of the user to impersonate when calling the API. The validation results
            are for the onBehalfOfUserId's attributes and roles.
        """
        payload = {
            "entityType": entity_type,
            "displayName": display_name,
            "mailNickname": mail_nickname,
            "onBehalfOfUserId": on_behalf_of_userid,
        }
        qry = ServiceOperationQuery(self, "validateProperties", None, payload)
        self.context.add_query(qry)
        return self
