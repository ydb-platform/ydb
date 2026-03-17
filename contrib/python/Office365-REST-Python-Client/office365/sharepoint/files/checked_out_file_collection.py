from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.files.checked_out_file import CheckedOutFile


class CheckedOutFileCollection(EntityCollection[CheckedOutFile]):
    def __init__(self, context, resource_path=None):
        super(CheckedOutFileCollection, self).__init__(
            context, CheckedOutFile, resource_path
        )

    def get_by_path(self, decoded_url):
        """
        Get a collection of checked-out files at the specified path.

        :param str decoded_url: Specifies the path for the checked-out file.
        """
        return_type = CheckedOutFile(self.context)
        qry = ServiceOperationQuery(
            self, "GetByPath", [decoded_url], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
