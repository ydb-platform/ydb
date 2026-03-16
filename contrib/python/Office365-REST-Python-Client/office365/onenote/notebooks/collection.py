from office365.entity_collection import EntityCollection
from office365.onenote.notebooks.copy_notebook_model import CopyNotebookModel
from office365.onenote.notebooks.notebook import Notebook
from office365.onenote.notebooks.recent import RecentNotebook
from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery


class NotebookCollection(EntityCollection[Notebook]):
    def __init__(self, context, resource_path=None):
        super(NotebookCollection, self).__init__(context, Notebook, resource_path)

    def add(self, display_name):
        """
        Create a new OneNote notebook.

        :param str display_name: Name for the notebook. Notebook names must be unique. The name cannot contain more
            than 128 characters or contain the following characters: ?*/:<>|'"
        """
        return super(NotebookCollection, self).add(displayName=display_name)

    def get_notebook_from_web_url(self, web_url):
        """
        Retrieve the properties and relationships of a notebook object by using its URL path.
        The location can be user notebooks on Microsoft 365, group notebooks,
        or SharePoint site-hosted team notebooks on Microsoft 365.

        :param str web_url: The URL path of the notebook to retrieve. It can also contain a "onenote:" prefix.
        """
        return_type = ClientResult(self.context, CopyNotebookModel())
        params = {"webUrl": web_url}
        qry = ServiceOperationQuery(
            self, "getNotebookFromWebUrl", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_recent_notebooks(self, include_personal_notebooks=True):
        """Get a list of recentNotebook instances that have been accessed by the signed-in user.

        :param bool include_personal_notebooks: Include notebooks owned by the user. Set to true to include notebooks
            owned by the user; otherwise, set to false. If you don't include the includePersonalNotebooks parameter,
            your request will return a 400 error response.
        """

        return_type = ClientResult(self.context, ClientValueCollection(RecentNotebook))
        params = {"includePersonalNotebooks": include_personal_notebooks}
        qry = FunctionQuery(self, "getRecentNotebooks", params, return_type)
        self.context.add_query(qry)
        return return_type
