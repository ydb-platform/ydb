from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class DocumentId(Entity):
    """
    Contains methods that enable or disable the capability to assign Document IDs to query Document ID feature
    and assignment status, and to query and set Document ID providers.

    Provides methods for assigning Document Ids to documents, and provides methods to use lookup and search
    to get documents by their Document ID.
    """

    def __init__(self, context):
        super(DocumentId, self).__init__(
            context, ResourcePath("SP.DocumentManagement.DocumentId")
        )

    def reset_docid_by_server_relative_path(self, decoded_url):
        """In case the document identifier assigned by the document id feature is not unique, MUST re-assign
        the identifier and URL to ensure they are globally unique in the farm.

        :param str decoded_url: server relative path to the specified document for which the document identifier
             MUST be reset if it is not unique.
        """
        payload = {"DecodedUrl": decoded_url}
        qry = ServiceOperationQuery(
            self, "ResetDocIdByServerRelativePath", None, payload, None, None
        )
        self.context.add_query(qry)
        return self

    def reset_doc_ids_in_library(self, decoded_url, content_type_id=None):
        """
        Performs the same function as ResetDocIdByServerRelativePath (section 3.1.5.10.2.1.1), but for every
        document in the specified document library.

        :param str decoded_url: Server relative path to the document library, for which all document identifiers
            MUST be reset to guarantee global uniqueness in the farm.
        :param str or None content_type_id: The content type identifier.
        """
        payload = {"decodedUrl": decoded_url, "contentTypeId": content_type_id}
        qry = ServiceOperationQuery(
            self, "ResetDocIdsInLibrary", None, payload, None, None
        )
        self.context.add_query(qry)
        return self

    def set_doc_id_site_prefix(
        self, prefix, schedule_assignment, overwrite_existing_ids
    ):
        """
        Allows to set or change the prefix used for Document IDs

        :param str prefix:
        :param bool schedule_assignment:
        :param bool overwrite_existing_ids:
        """
        payload = {
            "prefix": prefix,
            "scheduleAssignment": schedule_assignment,
            "overwriteExistingIds": overwrite_existing_ids,
        }
        qry = ServiceOperationQuery(
            self, "SetDocIdSitePrefix", None, payload, None, None
        )
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "SP.DocumentManagement.DocumentId"

    @property
    def entity_type_id(self):
        return "fb7276a2-cd36-448b-8a60-a589205f5a8f"
