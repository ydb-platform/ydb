from typing_extensions import Self

from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.contenttypes.fieldlinks.field_link import FieldLink
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.fields.field import Field


class FieldLinkCollection(EntityCollection[FieldLink]):
    """Specifies a Collection for field links."""

    def __init__(self, context, resource_path=None):
        super(FieldLinkCollection, self).__init__(context, FieldLink, resource_path)

    def add(self, field):
        """
        Add a field link with the specified link information to the collection.
        A reference to the SP.Field that was added is returned.

        :param str or office365.sharepoint.fields.field.Field field: Specifies the internal name of the field or type
        """

        def _add(field_internal_name):
            # type: (str) -> None
            return_type.set_property("FieldInternalName", field_internal_name)
            qry = CreateEntityQuery(self, return_type, return_type)
            self.context.add_query(qry)

        return_type = FieldLink(self.context)
        self.add_child(return_type)
        if isinstance(field, Field):

            def _field_loaded():
                _add(field.internal_name)

            field.ensure_property("InternalName", _field_loaded)
        else:
            _add(field)
        return return_type

    def get_by_id(self, _id):
        """
        Gets the field link with the given id from this collection.<20> If the id is not found in the collection,
        returns null.

        :param str _id: The GUID that specifies the Microsoft.SharePoint.Client.FieldLink (section 3.2.5.46)
            that is returned.
        """
        return FieldLink(
            self.context, ServiceOperationPath("GetById", [_id], self.resource_path)
        )

    def reorder(self, internal_names):
        # type: (list[str]) -> Self
        """
        Rearranges the collection of field links in the order in which field internal names are specified.

        :param list[str] internal_names: Specifies field internal names that are arranged in the order in which the
            collection of field links is reordered.
        """
        payload = {"internalNames": StringCollection(internal_names)}
        qry = ServiceOperationQuery(self, "Reorder", None, payload)
        self.context.add_query(qry)
        return self
