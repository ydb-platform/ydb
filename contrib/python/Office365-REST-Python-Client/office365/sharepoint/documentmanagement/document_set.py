from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.client_query import ClientQuery
from office365.sharepoint.folders.folder import Folder
from office365.sharepoint.lists.list import List


class DocumentSet(Folder):
    @staticmethod
    def create(context, parent_folder, name, ct_id="0x0120D520"):
        """
        Creates a DocumentSet (section 3.1.5.3) object on the server.

        :type context: office365.sharepoint.client_context.ClientContext
        :param office365.sharepoint.folders.folder.Folder parent_folder: The folder inside which to create the new
            DocumentSet.
        :param str name: The name to give to the new DocumentSet
        :param office365.sharepoint.contenttypes.content_type_id.ContentTypeId ct_id: The identifier of the content
            type to give to the new document set.
        """

        return_type = DocumentSet(context)

        def _create(target_list):
            # type: (List) -> None
            qry = ClientQuery(context, return_type=return_type)
            folder_url = parent_folder.serverRelativeUrl + "/" + name
            return_type.set_property("ServerRelativeUrl", folder_url)

            def _construct_request(request):
                # type: (RequestOptions) -> None
                list_name = target_list.title.replace(" ", "")
                request.url = r"{0}/_vti_bin/listdata.svc/{1}".format(
                    context.base_url, list_name
                )
                request.set_header("Slug", "{0}|{1}".format(folder_url, ct_id))
                request.method = HttpMethod.Post

            context.add_query(qry).before_query_execute(_construct_request)

        def _parent_folder_loaded():
            custom_props = parent_folder.get_property("Properties")
            list_id = custom_props.get("vti_x005f_listname")
            target_list = context.web.lists.get_by_id(list_id)
            target_list.ensure_property("Title", _create, target_list=target_list)

        parent_folder.ensure_properties(
            ["UniqueId", "Properties", "ServerRelativeUrl"], _parent_folder_loaded
        )
        return return_type

    @staticmethod
    def get_document_set(context, folder):
        """Retrieves the document set object from a specified folder object.

        :type context: office365.sharepoint.client_context.ClientContext
        :param office365.sharepoint.folders.folder.Folder folder: the Folder object from which
            to get the document set
        """
        return_type = DocumentSet(context)
        return return_type
