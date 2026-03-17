from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.webparts.definition import WebPartDefinition
from office365.sharepoint.webparts.definition_collection import (
    WebPartDefinitionCollection,
)


class LimitedWebPartManager(Entity):
    """Provides operations to access and modify the existing Web Parts on a Web Part Page, and add new ones
    to the Web Part Page."""

    def export_web_part(self, web_part):
        # type: (str or WebPartDefinition) -> ClientResult[str]
        """Exports the specified Web Part, given its ID.
        :param str or WebPartDefinition web_part: The WebPartDefinition or  Id of the Web Part to export.
        """
        return_type = ClientResult(self.context, str())
        self.web_parts.add_child(return_type)

        def _export_web_part(web_part_id):
            # type: (str) -> None
            params = {"webPartId": web_part_id}
            qry = ServiceOperationQuery(
                self, "ExportWebPart", params, None, None, return_type
            )
            self.context.add_query(qry)

        if isinstance(web_part, WebPartDefinition):

            def _web_part_loaded():
                _export_web_part(web_part.id)

            web_part.ensure_property("Id", _web_part_loaded)
        else:
            _export_web_part(web_part)

        return return_type

    def import_web_part(self, web_part_xml):
        """
        Imports a Web Part from a string in the .dwp format as specified in [MS-WPPS] section 2.2.4.2,
        or the .webpart format as specified in [MS-WPPS] section 2.2.3.1.
        After importing, the Web Part is not added to a Web Part Page. To add a Web Part to a Web Part Page,
        use AddWebPart, supplying the object (1) returned by this method.
        A reference to the added SP.WebParts.WebPartDefinition is returned.

        When Scope is User, the current user MUST have permissions to add and delete personalized Web Parts.
        When Scope is Shared, the current user MUST have permissions to customize pages.

        :param str web_part_xml: The Web Part markup to import.
        """
        return_type = WebPartDefinition(self.context)
        self.web_parts.add_child(return_type)
        payload = {"webPartXml": web_part_xml}
        qry = ServiceOperationQuery(
            self, "ImportWebPart", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def web_parts(self):
        """A collection of the Web Parts on the Web Part Page available to the current user based
        on the current user’s permissions."""
        return self.properties.get(
            "WebParts",
            WebPartDefinitionCollection(
                self.context, ResourcePath("WebParts", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        return "SP.WebParts.LimitedWebPartManager"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"WebParts": self.web_parts}
            default_value = property_mapping.get(name, None)
        return super(LimitedWebPartManager, self).get_property(name, default_value)
