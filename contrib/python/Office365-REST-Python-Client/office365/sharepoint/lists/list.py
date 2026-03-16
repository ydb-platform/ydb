import os
from datetime import datetime
from typing import IO, TYPE_CHECKING, AnyStr, Callable, Dict, Optional

from typing_extensions import Self

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.changes.collection import ChangeCollection
from office365.sharepoint.changes.log_item_query import ChangeLogItemQuery
from office365.sharepoint.changes.query import ChangeQuery
from office365.sharepoint.changes.token import ChangeToken
from office365.sharepoint.compliance.tag import ComplianceTag
from office365.sharepoint.contenttypes.collection import ContentTypeCollection
from office365.sharepoint.customactions.element_collection import (
    CustomActionElementCollection,
)
from office365.sharepoint.eventreceivers.definition_collection import (
    EventReceiverDefinitionCollection,
)
from office365.sharepoint.fields.collection import FieldCollection
from office365.sharepoint.fields.field import Field
from office365.sharepoint.fields.related_field_collection import RelatedFieldCollection
from office365.sharepoint.files.checked_out_file_collection import (
    CheckedOutFileCollection,
)
from office365.sharepoint.files.file import File
from office365.sharepoint.flows.synchronization_result import FlowSynchronizationResult
from office365.sharepoint.folders.folder import Folder
from office365.sharepoint.forms.collection import FormCollection
from office365.sharepoint.listitems.caml.query import CamlQuery
from office365.sharepoint.listitems.collection import ListItemCollection
from office365.sharepoint.listitems.creation_information import (
    ListItemCreationInformation,
)
from office365.sharepoint.listitems.creation_information_using_path import (
    ListItemCreationInformationUsingPath,
)
from office365.sharepoint.listitems.form_update_value import ListItemFormUpdateValue
from office365.sharepoint.listitems.listitem import ListItem
from office365.sharepoint.lists.bloom_filter import ListBloomFilter
from office365.sharepoint.lists.creatables_info import CreatablesInfo
from office365.sharepoint.lists.data_source import ListDataSource
from office365.sharepoint.lists.render_data_parameters import RenderListDataParameters
from office365.sharepoint.lists.rule import SPListRule
from office365.sharepoint.lists.version_policy_manager import VersionPolicyManager
from office365.sharepoint.navigation.configured_metadata_items import (
    ConfiguredMetadataNavigationItemCollection,
)
from office365.sharepoint.pages.wiki_page_creation_information import (
    WikiPageCreationInformation,
)
from office365.sharepoint.permissions.securable_object import SecurableObject
from office365.sharepoint.principal.users.user import User
from office365.sharepoint.sharing.object_sharing_settings import ObjectSharingSettings
from office365.sharepoint.sitescripts.utility import SiteScriptUtility
from office365.sharepoint.translation.user_resource import UserResource
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath
from office365.sharepoint.usercustomactions.collection import UserCustomActionCollection
from office365.sharepoint.utilities.utility import Utility
from office365.sharepoint.views.collection import ViewCollection
from office365.sharepoint.views.view import View
from office365.sharepoint.webhooks.subscription_collection import SubscriptionCollection

if TYPE_CHECKING:
    from office365.sharepoint.lists.collection import ListCollection


class List(SecurableObject):
    """
    Represents a list on a SharePoint Web site.

    A container within a SharePoint site that stores list items. A list has a customizable schema that is
    composed of one or more fields.
    """

    def __init__(self, context, resource_path=None):
        super(List, self).__init__(context, resource_path)

    def __repr__(self):
        return self.id or self.title or self.entity_type_name

    def export(self, local_file, include_content=False, item_exported=None):
        # type: (IO, bool, Callable[[ListItem|File|Folder], None]) -> Self
        """Exports SharePoint List"""
        from office365.sharepoint.lists.exporter import ListExporter

        return ListExporter.export(self, local_file, include_content, item_exported)

    def can_customize_forms(self):
        """"""
        from office365.sharepoint.flows.connector_result import ConnectorResult
        from office365.sharepoint.forms.customization import FormsCustomization

        return_type = ConnectorResult(self.context)

        def _can_customize_forms():
            FormsCustomization.can_customize_forms(
                self.context, self.title, return_type
            )

        self.ensure_property("Title", _can_customize_forms)
        return return_type

    def clear(self):
        """Clears the list."""

        def _clear(items):
            [item.delete_object() for item in items]

        self.items.get().after_execute(_clear, execute_first=True)
        return self

    def create_document_and_get_edit_link(
        self,
        file_name=None,
        folder_path=None,
        document_template_type=1,
        template_url=None,
    ):
        """
        Creates a document at the path and of the type specified within the current list.
        Returns an edit link to the file.

        :param str file_name: Specifies the name of the document.
        :param str folder_path: Specifies the path within the current list to create the document in.
        :param str document_template_type: A number representing the type of document to create.
        :param str template_url: Specifies the URL of the document template (2) to base the new document on.
        """
        return_type = ClientResult(self.context, str())  # type: ClientResult[str]
        payload = {
            "fileName": file_name,
            "folderPath": folder_path,
            "documentTemplateType": document_template_type,
            "templateUrl": template_url,
        }
        qry = ServiceOperationQuery(
            self, "CreateDocumentAndGetEditLink", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def delete_rule(self, rule_id):
        """
        :param str rule_id:
        """
        payload = {"ruleId": rule_id}
        qry = ServiceOperationQuery(self, "DeleteRule", None, payload)
        self.context.add_query(qry)
        return self

    def get_async_action_config(self):
        """ """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetAsyncActionConfig", return_type=return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_bloom_filter(self, start_item_id=None):
        """
        Generates a Bloom filter (probabilistic structure for checking the existence of list items) for the current list

        :param int start_item_id: he ID of the list item to start the search at.
        """
        return_type = ListBloomFilter(self.context)
        payload = {"startItemId": start_item_id}
        qry = ServiceOperationQuery(
            self, "GetBloomFilter", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_compliance_tag(self):
        """ """
        from office365.sharepoint.compliance.store_proxy import SPPolicyStoreProxy

        return_type = ClientResult(self.context, ComplianceTag())
        list_folder = self.root_folder

        def _get_compliance_tag():
            SPPolicyStoreProxy.get_list_compliance_tag(
                self.context,
                list_folder.serverRelativeUrl,
                return_type=return_type,
            )

        list_folder.ensure_property("ServerRelativeUrl", _get_compliance_tag)
        return return_type

    def get_metadata_navigation_settings(self):
        """Retrieves the configured metadata navigation settings for the list."""
        from office365.sharepoint.navigation.metadata_settings import (
            MetadataNavigationSettings,
        )

        return_type = ClientResult(
            self.context, ConfiguredMetadataNavigationItemCollection()
        )

        def _loaded():
            MetadataNavigationSettings.get_configured_settings(
                self.context, self.root_folder.serverRelativeUrl, return_type
            )

        self.root_folder.ensure_property("ServerRelativeUrl", _loaded)
        return return_type

    def get_flow_permission_level(self):
        """"""
        from office365.sharepoint.flows.connector_result import ConnectorResult
        from office365.sharepoint.flows.permissions import FlowPermissions

        return_type = ConnectorResult(self.context)

        def _loaded():
            FlowPermissions.get_flow_permission_level_on_list(
                self.context, self.title, return_type
            )

        self.ensure_property("Title", _loaded)
        return return_type

    def get_sharing_settings(self):
        """Retrieves a sharing settings for a List"""
        return_type = ObjectSharingSettings(self.context)

        def _get_sharing_settings():
            from office365.sharepoint.webs.web import Web

            list_abs_path = SPResPath.create_absolute(
                self.context.base_url, self.root_folder.serverRelativeUrl
            )
            Web.get_object_sharing_settings(
                self.context, str(list_abs_path), return_type=return_type
            )

        self.ensure_property("RootFolder", _get_sharing_settings)
        return return_type

    def get_site_script(self, options=None):
        # type: (Dict) -> ClientResult[str]
        """Creates site script syntax

        :param dict or None options:
        """
        return_type = ClientResult(self.context)

        def _list_loaded():
            list_abs_path = SPResPath.create_absolute(
                self.context.base_url, self.root_folder.serverRelativeUrl
            )
            SiteScriptUtility.get_site_script_from_list(
                self.context, str(list_abs_path), options, return_type=return_type
            )

        self.ensure_property("RootFolder", _list_loaded)
        return return_type

    def ensure_signoff_status_field(self):
        """"""
        return_type = Field(self.context)
        self.fields.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "EnsureSignoffStatusField", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_all_rules(self):
        """Retrieves rules of a List"""
        return_type = ClientResult(self.context, ClientValueCollection(SPListRule))
        qry = ServiceOperationQuery(self, "GetAllRules", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_business_app_operation_status(self):
        """ """
        return_type = FlowSynchronizationResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetBusinessAppOperationStatus", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def search_lookup_field_choices(
        self, target_field_name, begins_with_search_string, paging_info
    ):
        """
        :param str target_field_name:
        :param str begins_with_search_string:
        :param str paging_info:
        """
        return_type = FlowSynchronizationResult(self.context)
        payload = {
            "targetFieldName": target_field_name,
            "beginsWithSearchString": begins_with_search_string,
            "pagingInfo": paging_info,
        }
        qry = ServiceOperationQuery(
            self, "SearchLookupFieldChoices", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def sync_flow_callback_url(self, flow_id):
        """
        :param str flow_id:
        """
        return_type = FlowSynchronizationResult(self.context)
        qry = ServiceOperationQuery(
            self, "SyncFlowCallbackUrl", None, {"flowId": flow_id}, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def sync_flow_instance(self, flow_id):
        """
        :param str flow_id:
        """
        return_type = FlowSynchronizationResult(self.context)
        qry = ServiceOperationQuery(
            self, "SyncFlowInstance", None, {"flowId": flow_id}, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def sync_flow_instances(self, retrieve_group_flows):
        """
        :param bool retrieve_group_flows:
        """
        return_type = FlowSynchronizationResult(self.context)
        payload = {"retrieveGroupFlows": retrieve_group_flows}
        qry = ServiceOperationQuery(
            self, "SyncFlowInstances", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def sync_flow_templates(self, category):
        """
        :param str category:
        """
        return_type = FlowSynchronizationResult(self.context)
        payload = {"category": category}
        qry = ServiceOperationQuery(
            self, "SyncFlowTemplates", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def create_document_with_default_name(self, folder_path, extension):
        """
        Creates a empty document with default filename with the given extension at the path given by folderPath.
        Returns the name of the newly created document.

        :param str folder_path: The path within the current list at which to create the document.
        :param str extension: The file extension without dot prefix.
        """
        return_type = ClientResult(self.context)  # type: ClientResult[str]
        payload = {"folderPath": folder_path, "extension": extension}
        qry = ServiceOperationQuery(
            self, "CreateDocumentWithDefaultName", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def recycle(self):
        """Moves the list to the Recycle Bin and returns the identifier of the new Recycle Bin item."""
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "Recycle", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def start_recycle(self):
        """Moves the list to the Recycle Bin and returns the identifier of the new Recycle Bin item."""
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(self, "StartRecycle", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def render_list_data(self, view_xml):
        """
        Returns the data for the specified query view. The result is implementation-specific, used for
        providing data to a user interface.

        :param str view_xml:  Specifies the query as XML that conforms to the ViewDefinition type as specified in
            [MS-WSSCAML] section 2.3.2.17.
        """
        return_type = ClientResult(self.context)
        payload = {"viewXml": view_xml}
        qry = ServiceOperationQuery(
            self, "RenderListData", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def get_list_data_as_stream(
        context,
        list_full_url,
        parameters=None,
        casc_del_warn_message=None,
        custom_action=None,
        drill_down=None,
        field=None,
        field_internal_name=None,
        return_type=None,
    ):
        """
        Returns list data from the specified list url and for the specified query parameters.

        :param office365.sharepoint.client_context.ClientContext context: Client context
        :param str list_full_url: The absolute URL of the list.
        :param RenderListDataParameters parameters: The parameters to be used.
        :param str casc_del_warn_message:
        :param str custom_action:
        :param str drill_down:
        :param str field:
        :param str field_internal_name:
        :param ClientResult[dict] return_type: The return type.
        """
        if return_type is None:
            return_type = ClientResult(context, {})
        payload = {
            "listFullUrl": list_full_url,
            "parameters": parameters,
            "CascDelWarnMessage": casc_del_warn_message,
            "CustomAction": custom_action,
            "DrillDown": drill_down,
            "Field": field,
            "FieldInternalName": field_internal_name,
        }
        qry = ServiceOperationQuery(
            List(context), "GetListDataAsStream", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_onedrive_list_data_as_stream(context, parameters=None, return_type=None):
        """
        Returns list data from the specified list url and for the specified query parameters.

        :param office365.sharepoint.client_context.ClientContext context: Client context
        :param RenderListDataParameters parameters: The parameters to be used.
        :param ClientResult[dict] return_type: The return type.
        """
        if return_type is None:
            return_type = ClientResult(context, {})
        payload = {
            "parameters": parameters,
        }
        qry = ServiceOperationQuery(
            List(context),
            "GetOneDriveListDataAsStream",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    def bulk_validate_update_list_items(
        self,
        item_ids,
        form_values,
        new_document_update=True,
        checkin_comment=None,
        folder_path=None,
    ):
        """
        Validate and update multiple list items.

        :param list[int] item_ids: A collection of item Ids that need to be updated with the same formValues.
        :param dict form_values: A collection of field internal names and values for the given field.
            If the collection is empty, no update will take place.
        :param bool new_document_update: Indicates whether the list item is a document being updated after upload.
            A value of "true" means yes.
        :param str checkin_comment: The comment of check in if any. It's only applicable when the item is checked out.
        :param str folder_path: Decoded path of the folder where the items belong to. If not provided,
            the server will try to find items to update under root folder.
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(ListItemFormUpdateValue)
        )
        params = {
            "itemIds": item_ids,
            "formValues": ClientValueCollection(ListItemFormUpdateValue, form_values),
            "bNewDocumentUpdate": new_document_update,
            "checkInComment": checkin_comment,
            "folderPath": folder_path,
        }
        qry = ServiceOperationQuery(
            self, "BulkValidateUpdateListItems", None, params, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_lookup_field_choices(self, target_field_name, paging_info=None):
        """
        Retrieves the possible choices or values for a lookup field in a SharePoint list
        :param str target_field_name:
        :param str paging_info:
        """
        return_type = ClientResult(self.context, str())
        params = {"targetFieldName": target_field_name, "pagingInfo": paging_info}
        qry = ServiceOperationQuery(
            self, "GetLookupFieldChoices", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_list_item_changes_since_token(self, query):
        # type: (ChangeLogItemQuery) -> ClientResult[AnyStr]
        """
        Returns the changes made to the list since the date and time specified in the change token defined
        by the query input parameter.
        """
        return_type = ClientResult(self.context, bytes())
        payload = {"query": query}
        qry = ServiceOperationQuery(
            self, "getListItemChangesSinceToken", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def save_as_new_view(self, old_name, new_name, private_view, uri):
        """
        Overwrites a view if it already exists, creates a new view if it does not; and then extracts the
        implementation-specific filter and sort information from the URL and builds and updates the view's XML.
        Returns the URL of the view.

        :param str old_name: The name of the view the user is currently on.
        :param str new_name: The new name given by the user.
        :param bool private_view: Set to "true" to make the view private; otherwise, "false".
        :param str uri: URL that contains all the implementation-specific filter and sort information for the view.
        """
        payload = {
            "oldName": old_name,
            "newName": new_name,
            "privateView": private_view,
            "uri": uri,
        }
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "SaveAsNewView", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return self

    def save_as_template(self, file_name, name, description, save_data):
        """
        Saves the list as a template in the list template gallery and includes the option of saving with or
        without the data that is contained in the current list.

        :param bool save_data: true to save the data of the original list along with the list template; otherwise, false
        :param str description: A string that contains the description for the list template.
        :param str name: A string that contains the title for the list template.
        :param str file_name: A string that contains the file name for the list template with an .stp extension.
        """
        payload = {
            "strFileName": file_name,
            "strName": name,
            "strDescription": description,
            "bSaveData": save_data,
        }
        qry = ServiceOperationQuery(self, "saveAsTemplate", None, payload, None, None)
        self.context.add_query(qry)
        return self

    def get_item_by_unique_id(self, unique_id):
        """
        Returns the list item with the specified ID.
        :param str unique_id: The unique ID that is associated with the list item.
        """
        return ListItem(
            self.context,
            ServiceOperationPath("getItemByUniqueId", [unique_id], self.resource_path),
        )

    def get_web_dav_url(self, source_url):
        # type: (str) -> ClientResult[str]
        """
        Gets the trusted URL for opening the folder in Explorer view.
        :param str source_url: The URL of the current folder the user is in.
        """

        return_type = ClientResult(self.context, str())
        payload = {"sourceUrl": source_url}
        qry = ServiceOperationQuery(
            self, "getWebDavUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_items(self, caml_query=None):
        # type: (CamlQuery) -> ListItemCollection
        """Returns a collection of items from the list based on the specified query."""
        if not caml_query:
            caml_query = CamlQuery.create_all_items_query()
        return_type = ListItemCollection(self.context, self.items.resource_path)
        payload = {"query": caml_query}
        qry = ServiceOperationQuery(self, "GetItems", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def add_item(self, creation_information):
        # type: (ListItemCreationInformation|dict) -> ListItem
        """The recommended way to add a list item is to send a POST request to the ListItemCollection resource endpoint,
        as shown in ListItemCollection request examples."""
        return_type = ListItem(self.context, None, self)
        self.items.add_child(return_type)
        if isinstance(creation_information, dict):
            for k, v in creation_information.items():
                return_type.set_property(k, v, True)
            return_type.ensure_type_name(self)
            qry = ServiceOperationQuery(
                self, "items", None, return_type, None, return_type
            )
            self.context.add_query(qry)
        else:

            def _add_item():
                creation_information.FolderUrl = (
                    self.context.base_url + self.root_folder.serverRelativeUrl
                )
                payload = {"parameters": creation_information}
                next_qry = ServiceOperationQuery(
                    self, "addItem", None, payload, None, return_type
                )
                self.context.add_query(next_qry)

            self.root_folder.ensure_property("ServerRelativeUrl", _add_item)
        return return_type

    def create_wiki_page(self, page_name, page_content):
        """
        Creates a wiki page.
        :param str page_name:
        :param str page_content:
        """
        return_type = File(self.context)

        def _root_folder_loaded():
            page_url = self.root_folder.serverRelativeUrl + "/" + page_name
            wiki_props = WikiPageCreationInformation(page_url, page_content)
            Utility.create_wiki_page_in_context_web(
                self.context, wiki_props, return_type
            )

        self.ensure_property("RootFolder", _root_folder_loaded)
        return return_type

    def add_item_using_path(self, leaf_name, object_type, folder_url):
        """
        Adds a ListItem to an existing List.

        :param str leaf_name: Specifies the name of the list item that will be created. In the case of a
            document library, the name is equal to the filename of the list item.
        :param int object_type: Specifies the file system object type for the item that will be created.
            It MUST be either FileSystemObjectType.File or FileSystemObjectType.Folder.
        :param str ot None folder_url: Specifies the url of the folder of the new list item.
            The value MUST be either null or the decoded url value an empty string or a server-relative
            URL or an absolute URL. If the value is not null or the decoded url value not being empty string,
            the decoded url value MUST point to a location within the list.
        """
        parameters = ListItemCreationInformationUsingPath(
            leaf_name, object_type, folder_path=folder_url
        )
        return_type = ListItem(self.context)
        payload = {"parameters": parameters}
        qry = ServiceOperationQuery(
            self, "AddItemUsingPath", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def add_validate_update_item(self, create_info, form_values=None):
        """
        Adds an item to an existing list and validate the list item update values. If all fields validated successfully,
         commit all changes. If there's any exception in any of the fields, the item will not be committed.

        :param ListItemCreationInformation create_info:  Contains the information that determines how the item
            will be created.
        :param dict form_values: A collection of field internal names and values for the given field. If the collection
            is empty, no update will take place.
        """
        payload = {
            "listItemCreateInfo": create_info,
            "formValues": [
                ListItemFormUpdateValue(k, v) for k, v in form_values.items()
            ],
        }
        return_type = ClientResult(
            self.context, ClientValueCollection(ListItemFormUpdateValue)
        )
        qry = ServiceOperationQuery(
            self, "AddValidateUpdateItem", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_item_by_id(self, item_id):
        # type: (int) -> ListItem
        """Returns the list item with the specified list item identifier."""
        return ListItem(
            self.context,
            ServiceOperationPath("getItemById", [item_id], self.resource_path),
        )

    def get_item_by_url(self, url):
        # type: (str) -> ListItem
        """Returns the list item with the specified site or server relative url."""
        return_type = ListItem(self.context)
        self.items.add_child(return_type)

        def _after_loaded(item):
            [return_type.set_property(k, v, False) for k, v in item.properties.items()]

        def _get_item_by_url():
            path = os.path.join(self.root_folder.serverRelativeUrl, url)
            self.items.get_by_url(path).after_execute(_after_loaded, execute_first=True)

        self.ensure_property("RootFolder", _get_item_by_url)
        return return_type

    def get_view(self, view_id):
        """Returns the list view with the specified view identifier.
        :type view_id: str
        """
        return View(
            self.context,
            ServiceOperationPath("getView", [view_id], self.resource_path),
            self,
        )

    def get_changes(self, query=None):
        # type: (ChangeQuery) -> ChangeCollection
        """Returns the collection of changes from the change log that have occurred within the list,
           based on the specified query.

        :param ChangeQuery query: Specifies which changes to return
        """
        if query is None:
            query = ChangeQuery(list_=True)
        return_type = ChangeCollection(self.context)
        payload = {"query": query}
        qry = ServiceOperationQuery(
            self, "getChanges", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_checked_out_files(self):
        """Returns a collection of checked-out files as specified in section 3.2.5.381."""
        return_type = CheckedOutFileCollection(self.context)
        qry = ServiceOperationQuery(
            self, "GetCheckedOutFiles", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def render_list_data_as_stream(
        self, view_xml=None, render_options=None, expand_groups=None
    ):
        """Returns the data for the specified query view.

        :param str view_xml: Specifies the CAML view XML.
        :param int render_options: Specifies the type of output to return.
        :param bool expand_groups: Specifies whether to expand the grouping or not.
        """
        return_type = ClientResult(self.context, {})
        if view_xml is None:
            view_xml = "<View/>"
        payload = {
            "parameters": RenderListDataParameters(
                view_xml=view_xml,
                render_options=render_options,
                expand_groups=expand_groups,
            ),
        }
        qry = ServiceOperationQuery(
            self, "RenderListDataAsStream", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def reserve_list_item_id(self):
        # type: () -> ClientResult[int]
        """Reserves the returned list item identifier for the idempotent creation of a list item."""
        return_type = ClientResult(self.context, int())
        qry = ServiceOperationQuery(
            self, "ReserveListItemId", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_related_fields(self):
        """
        Returns a collection of lookup fields that use this list as a data source and
        that have FieldLookup.IsRelationship set to true.
        """
        return RelatedFieldCollection(
            self.context,
            ServiceOperationPath("getRelatedFields", [], self.resource_path),
        )

    def get_special_folder_url(self, folder_type, force_create, existing_folder_guid):
        """
        Gets the relative URL of the Save to OneDrive folder.

        :param int folder_type: The Save-to-OneDrive type.
        :param bool force_create: Specify true if the folder doesn't exist and SHOULD be created.
        :param str existing_folder_guid:  The GUID of the created folders that exist, if any.
        """
        payload = {
            "type": folder_type,
            "bForceCreate": force_create,
            "existingFolderGuid": existing_folder_guid,
        }
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "GetSpecialFolderUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def reset_doc_id(self):
        """"""
        from office365.sharepoint.documentmanagement.document_id import DocumentId

        def _loaded():
            doc_mng = DocumentId(self.context)
            doc_mng.reset_doc_ids_in_library(self.root_folder.serverRelativeUrl)

        self.ensure_property("RootFolder", _loaded)
        return self

    def set_exempt_from_block_download_of_non_viewable_files(self, value):
        """
        Method to set the ExemptFromBlockDownloadOfNonViewableFiles property.
        Ensures that only a tenant administrator can set the exemption.
        """
        qry = ServiceOperationQuery(
            self,
            "SetExemptFromBlockDownloadOfNonViewableFiles",
            [value],
            None,
            None,
            None,
        )
        self.context.add_query(qry)
        return self

    @property
    def id(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the list identifier."""
        return self.properties.get("Id", None)

    @property
    def additional_ux_properties(self):
        # type: () -> Optional[str]
        return self.properties.get("AdditionalUXProperties", None)

    @property
    def author(self):
        """Specifies the user who created the list."""
        return self.properties.get(
            "Author", User(self.context, ResourcePath("Author", self.resource_path))
        )

    @property
    def allow_content_types(self):
        # type: () -> Optional[bool]
        """Specifies whether the list supports content types."""
        return self.properties.get("AllowContentTypes", None)

    @property
    def allow_deletion(self):
        # type: () -> Optional[bool]
        """Specifies whether the list could be deleted."""
        return self.properties.get("AllowDeletion", None)

    @property
    def base_template(self):
        # type: () -> Optional[int]
        """Specifies the list server template of the list."""
        return self.properties.get("BaseTemplate", None)

    @property
    def base_type(self):
        # type: () -> Optional[int]
        """
        Specifies the base type of the list.
        It MUST be one of the following values: GenericList, DocumentLibrary, DiscussionBoard, Survey, or Issue.
        """
        return self.properties.get("BaseType", None)

    @property
    def browser_file_handling(self):
        # type: () -> Optional[int]
        """
        Specifies the override at the list level for the BrowserFileHandling property of the Web application.
        If the BrowserFileHandling property of the Web application is BrowserFileHandling.Strict,
        then this setting MUST be ignored. If the BrowserFileHandling property of the Web application is
        BrowserFileHandling.Permissive, then this setting MUST be respected.
        """
        return self.properties.get("BrowserFileHandling", None)

    @property
    def created(self):
        """Specifies when the list was created."""
        return self.properties.get("Created", datetime.min)

    @property
    def default_display_form_url(self):
        # type: () -> Optional[str]
        """Specifies the location of the default display form for the list."""
        return self.properties.get("DefaultDisplayFormUrl", None)

    @property
    def default_view_path(self):
        """Specifies the server-relative URL of the default view for the list."""
        return self.properties.get("DefaultViewPath", SPResPath())

    @property
    def default_view_url(self):
        # type: () -> Optional[str]
        """
        Specifies the server-relative URL of the default view for the list.
        """
        return self.properties.get("DefaultViewUrl", None)

    @property
    def crawl_non_default_views(self):
        # type: () -> Optional[bool]
        """
        Specifies whether or not the crawler indexes the non-default views of the list.
        Specify a value of true if the crawler indexes the list's non-default views; specify false if otherwise.
        """
        return self.properties.get("CrawlNonDefaultViews", None)

    @property
    def creatables_info(self):
        """
        Returns an object that describes what this list can create, and a collection of links to visit in order to
        create those things. If it can't create certain things, it contains an error message describing why.

         The consumer MUST append the encoded URL of the current page to the links returned here.
         (This page the link goes to needs it as a query parameter to function correctly.)
         The consumer SHOULD also consider appending &IsDlg=1 to the link, to remove the UI from the linked page,
         if desired.
        """
        return self.properties.get(
            "CreatablesInfo",
            CreatablesInfo(
                self.context, ResourcePath("CreatablesInfo", self.resource_path)
            ),
        )

    @property
    def current_change_token(self):
        """Gets the current change token that is used in the change log for the list."""
        return self.properties.get("CurrentChangeToken", ChangeToken())

    @property
    def data_source(self):
        """
        Specifies the data source of an external list.
        If HasExternalDataSource is "false", the server MUST return NULL.
        """
        return self.properties.get("DataSource", ListDataSource())

    @property
    def default_edit_form_url(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the URL of the edit form to use for list items in the list."""
        return self.properties.get("DefaultEditFormUrl", None)

    @property
    def default_item_open_in_browser(self):
        # type: () -> Optional[bool]
        return self.properties.get("DefaultItemOpenInBrowser", None)

    @property
    def default_item_open_use_list_setting(self):
        # type: () -> Optional[bool]
        """
        This property returns whether to use the List setting or the server wide setting for
        DefaultItemOpen (BrowserEnabledDocuments setting) in the Web application. This property can only be set to
        false, thereby using the server-wide setting. To set it to use List setting, set the value for DefaultItemOpen.
        """
        return self.properties.get("DefaultItemOpenUseListSetting", None)

    @property
    def disable_commenting(self):
        # type: () -> Optional[bool]
        return self.properties.get("DisableCommenting", None)

    @property
    def disable_grid_editing(self):
        # type: () -> Optional[bool]
        return self.properties.get("DisableGridEditing", None)

    @property
    def document_template_url(self):
        # type: () -> Optional[str]
        """Specifies the URL of the document template assigned to the list."""
        return self.properties.get("DocumentTemplateUrl", None)

    @property
    def effective_base_permissions(self):
        """
        Specifies the effective permissions on the list that are assigned to the current user.
        """
        from office365.sharepoint.permissions.base_permissions import BasePermissions

        return self.properties.get("EffectiveBasePermissions", BasePermissions())

    @property
    def effective_base_permissions_for_ui(self):
        """
        Specifies the effective base permissions for the current user as they SHOULD be displayed in user interface (UI)
        If the list is not in read-only UI mode, the value of EffectiveBasePermissionsForUI MUST be the same as the
        value of EffectiveBasePermissions (section 3.2.5.79.1.1.17). If the list is in read-only UI mode, the value
        of EffectiveBasePermissionsForUI MUST be a subset of the value of EffectiveBasePermissions.
        """
        from office365.sharepoint.permissions.base_permissions import BasePermissions

        return self.properties.get("EffectiveBasePermissionsForUI", BasePermissions())

    @property
    def enable_assign_to_email(self):
        # type: () -> Optional[bool]
        """
        Gets or sets a Boolean value specifying whether e-mail notification is enabled for the list.
        """
        return self.properties.get("EnableAssignToEmail", None)

    @property
    def enable_attachments(self):
        # type: () -> Optional[bool]
        """Specifies whether list item attachments are enabled for the list"""
        return self.properties.get("EnableAttachments", None)

    @property
    def enable_folder_creation(self):
        # type: () -> Optional[bool]
        """Specifies whether new list folders can be added to the list."""
        return self.properties.get("EnableFolderCreation", None)

    @enable_folder_creation.setter
    def enable_folder_creation(self, value):
        # type: (bool) -> None
        self.set_property("EnableFolderCreation", value, True)

    @property
    def enable_minor_versions(self):
        # type: () -> Optional[bool]
        """Specifies whether minor versions are enabled for the list."""
        return self.properties.get("EnableMinorVersions", None)

    @property
    def enable_moderation(self):
        # type: () -> Optional[bool]
        """Specifies whether content approval is enabled for the list."""
        return self.properties.get("EnableModeration", None)

    @property
    def enable_request_sign_off(self):
        # type: () -> Optional[bool]
        return self.properties.get("EnableRequestSignOff", None)

    @property
    def enable_versioning(self):
        # type: () -> Optional[bool]
        """Specifies whether content approval is enabled for the list."""
        return self.properties.get("EnableVersioning", None)

    @property
    def exclude_from_offline_client(self):
        # type: () -> Optional[bool]
        """
        This doesn't block sync but acts as a recommendation to the client to not take this list offline.
        This gets returned as part of list properties in GetListSchema soap call.
        """
        return self.properties.get("ExcludeFromOfflineClient", None)

    @property
    def exempt_from_block_download_of_non_viewable_files(self):
        # type: () -> Optional[bool]
        """
        Property indicating whether the list is exempt from the policy to block download of non-server handled files.
        This SHOULD be set using the SetExemptFromBlockDownloadOfNonViewableFiles (section 3.2.5.79.2.1.25) method.
        """
        return self.properties.get("ExemptFromBlockDownloadOfNonViewableFiles", None)

    @property
    def file_save_post_processing_enabled(self):
        # type: () -> Optional[bool]
        """
        Specifies whether or not the files in the list can be processed in asynchronous manner
        Specify a value of true if the list files can be processed asynchronously; specify false if otherwise.
        """
        return self.properties.get("FileSavePostProcessingEnabled", None)

    @property
    def force_checkout(self):
        # type: () -> Optional[bool]
        """Specifies whether check out is required when adding or editing documents in the list"""
        return self.properties.get("ForceCheckout", None)

    @property
    def has_content_assembly_templates(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("HasContentAssemblyTemplates", None)

    @property
    def has_external_data_source(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the list is an external list.
        """
        return self.properties.get("HasExternalDataSource", None)

    @property
    def has_folder_coloring_fields(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("HasFolderColoringFields", None)

    @property
    def irm_enabled(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that specifies whether Information Rights Management (IRM) is enabled for the list."""
        return self.properties.get("IrmEnabled", None)

    @property
    def irm_expire(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that specifies whether Information Rights Management (IRM) expiration is enabled
        for the list
        """
        return self.properties.get("IrmExpire", None)

    @property
    def items(self):
        # type: () -> ListItemCollection
        """Get list items"""
        return self.properties.get(
            "Items",
            ListItemCollection(self.context, ResourcePath("items", self.resource_path)),
        )

    @property
    def root_folder(self):
        # type: () -> Folder
        """Get a root folder"""
        return self.properties.get(
            "RootFolder",
            Folder(self.context, ResourcePath("RootFolder", self.resource_path)),
        )

    @property
    def fields(self):
        # type: () -> FieldCollection
        """Gets a value that specifies the collection of all fields in the list."""
        return self.properties.get(
            "Fields",
            FieldCollection(
                self.context, ResourcePath("Fields", self.resource_path), self
            ),
        )

    @property
    def subscriptions(self):
        """Gets one or more webhook subscriptions on a SharePoint list."""
        return self.properties.get(
            "Subscriptions",
            SubscriptionCollection(
                self.context, ResourcePath("Subscriptions", self.resource_path), self
            ),
        )

    @property
    def views(self):
        # type: () -> ViewCollection
        """Gets a value that specifies the collection of all public views on the list and personal views
        of the current user on the list."""
        return self.properties.get(
            "Views",
            ViewCollection(
                self.context, ResourcePath("views", self.resource_path), self
            ),
        )

    @property
    def default_view(self):
        """Gets or sets a value that specifies whether the list view is the default list view."""
        return self.properties.get(
            "DefaultView",
            View(self.context, ResourcePath("DefaultView", self.resource_path), self),
        )

    @property
    def content_types(self):
        """Gets the content types that are associated with the list."""
        return self.properties.get(
            "ContentTypes",
            ContentTypeCollection(
                self.context, ResourcePath("ContentTypes", self.resource_path), self
            ),
        )

    @property
    def content_types_enabled(self):
        # type: () -> Optional[bool]
        """Specifies whether content types are enabled for the list."""
        return self.properties.get("ContentTypesEnabled", None)

    @property
    def is_private(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the list is a private list with restricted permissions.
        True if the list is a private list, otherwise False.
        """
        return self.properties.get("IsPrivate", None)

    @property
    def is_site_assets_library(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the list is designated as a default asset location for images or other files which the
        users upload to their wiki pages.
        """
        return self.properties.get("IsSiteAssetsLibrary", None)

    @property
    def is_system_list(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the list is system list that does not contain end user data and created by system account.
        A value of True means yes.
        """
        return self.properties.get("IsSystemList", None)

    @property
    def user_custom_actions(self):
        """Gets the User Custom Actions that are associated with the list."""
        return self.properties.get(
            "UserCustomActions",
            UserCustomActionCollection(
                self.context, ResourcePath("UserCustomActions", self.resource_path)
            ),
        )

    @property
    def version_policies(self):
        """ """
        return self.properties.get(
            "VersionPolicies",
            VersionPolicyManager(
                self.context, ResourcePath("VersionPolicies", self.resource_path)
            ),
        )

    @property
    def custom_action_elements(self):
        return self.properties.get(
            "CustomActionElements", CustomActionElementCollection()
        )

    @property
    def forms(self):
        """Gets a value that specifies the collection of all list forms in the list."""
        return self.properties.get(
            "Forms",
            FormCollection(self.context, ResourcePath("forms", self.resource_path)),
        )

    @property
    def parent_web(self):
        """Gets a value that specifies the web where list resides."""
        from office365.sharepoint.webs.web import Web

        return self.properties.get(
            "ParentWeb",
            Web(self.context, ResourcePath("parentWeb", self.resource_path)),
        )

    @property
    def event_receivers(self):
        """Get Event receivers"""
        return self.properties.get(
            "EventReceivers",
            EventReceiverDefinitionCollection(
                self.context, ResourcePath("eventReceivers", self.resource_path), self
            ),
        )

    @property
    def item_count(self):
        # type: () -> Optional[int]
        """Gets a value that specifies the number of list items in the list"""
        return self.properties.get("ItemCount", None)

    @property
    def last_item_deleted_date(self):
        """
        Specifies the last time a list item was deleted from the list. It MUST return Created if no list item has
        been deleted from the list yet.
        """
        return self.properties.get("LastItemDeletedDate", datetime.min)

    @property
    def last_item_modified_date(self):
        """
        Specifies the last time a list item, field, or property of the list was modified.
        It MUST return Created if the list has not been modified.
        """
        return self.properties.get("LastItemModifiedDate", datetime.min)

    @property
    def last_item_user_modified_date(self):
        """
        Specifies when an item of the list was last modified by a non-system update. A non-system update is a change
        to a list item that is visible to end users. If no item has been created in the list, the list creation time
        is returned.
        """
        return self.properties.get("LastItemUserModifiedDate", datetime.min)

    @property
    def list_experience_options(self):
        # type: () -> Optional[int]
        """
        Gets or sets the list to new experience, classic experience or default experience set by my administrator.
        """
        return self.properties.get("ListExperienceOptions", None)

    @property
    def list_form_customized(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("ListFormCustomized", None)

    @property
    def list_item_entity_type_full_name(self):
        # type: () -> Optional[str]
        """
        Specifies the full type name of the list item.
        """
        return self.properties.get("ListItemEntityTypeFullName", None)

    @property
    def major_version_limit(self):
        # type: () -> Optional[int]
        """
        Gets the maximum number of major versions allowed for an item in a document library that uses version
        control with major versions only.
        """
        return self.properties.get("MajorVersionLimit", None)

    @property
    def list_schema_version(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("ListSchemaVersion", None)

    @property
    def major_with_minor_versions_limit(self):
        # type: () -> Optional[int]
        """
        Gets the maximum number of major versions that are allowed for an item in a document library that uses
        version control with both major and minor versions.
        """
        return self.properties.get("MajorWithMinorVersionsLimit", None)

    @property
    def no_crawl(self):
        # type: () -> Optional[bool]
        """
        Specifies that the crawler MUST NOT crawl the list.
        """
        return self.properties.get("NoCrawl", None)

    @property
    def on_quick_launch(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the list appears on the Quick Launch of the site (2). If the value is set to "true",
        the protocol server MUST set the Hidden property of the list to "false".
        """
        return self.properties.get("OnQuickLaunch", None)

    @property
    def page_render_type(self):
        # type: () -> Optional[int]
        """
        Returns the render type of the current file. If the file will render in the modern experience it will return
        ListPageRenderType.Modern. If the file will render in the classic experience, it will return the reason
        for staying in classic, as specified by ListPageRenderType enumeration (section 3.2.5.413).
        """
        return self.properties.get("PageRenderType", None)

    @property
    def parent_web_url(self):
        # type: () -> Optional[str]
        """
        Specifies the server-relative URL of the site (2) that contains the list.
        It MUST be a URL of server-relative form.
        """
        return self.properties.get("ParentWebUrl", None)

    @property
    def parser_disabled(self):
        # type: () -> Optional[str]
        """
        Specifies whether or not the document parser is enabled on the list.
        Specify a value of true if the document parser is enabled on the list; specify false if otherwise.
        """
        return self.properties.get("ParserDisabled", None)

    @property
    def read_security(self):
        # type: () -> Optional[int]
        """Gets or sets the Read security setting for the list."""
        return self.properties.get("ReadSecurity", None)

    @property
    def server_template_can_create_folders(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the list template the list is based on allows the creation of folders.
        """
        return self.properties.get("ServerTemplateCanCreateFolders", None)

    @property
    def show_hidden_fields_in_modern_form(self):
        # type: () -> Optional[bool]
        return self.properties.get("ShowHiddenFieldsInModernForm", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """Gets the displayed title for the list."""
        return self.properties.get("Title", None)

    @title.setter
    def title(self, val):
        """Sets the displayed title for the list."""
        self.set_property("Title", val)

    @property
    def default_content_approval_workflow_id(self):
        # type: () -> Optional[str]
        """
        Specifies the default workflow identifier for content approval on the list. It MUST be an empty GUID
        if there is no default content approval workflow
        """
        return self.properties.get("DefaultContentApprovalWorkflowId", None)

    @property
    def description(self):
        # type: () -> Optional[str]
        """Gets the description for the list."""
        return self.properties.get("Description", None)

    @description.setter
    def description(self, val):
        # type: (str) -> None
        """Sets the description for the list."""
        self.set_property("Description", val)

    @property
    def description_resource(self):
        """Represents the description of this list."""
        return self.properties.get(
            "DescriptionResource",
            UserResource(
                self.context, ResourcePath("DescriptionResource", self.resource_path)
            ),
        )

    @property
    def parent_web_path(self):
        """Returns the path of the parent web for the list."""
        return self.properties.get("ParentWebPath", SPResPath())

    @property
    def schema_xml(self):
        # type: () -> Optional[str]
        """Specifies the list schema of the list."""
        return self.properties.get("SchemaXml", None)

    @property
    def template_feature_id(self):
        # type: () -> Optional[str]
        """
        Specifies the feature identifier of the feature that contains the list schema for the list.
        It MUST be an empty GUID if the list schema for the list is not contained within a feature.
        """
        return self.properties.get("TemplateFeatureId", None)

    @property
    def title_resource(self):
        """Represents the title of this list."""
        return self.properties.get(
            "TitleResource",
            UserResource(
                self.context, ResourcePath("TitleResource", self.resource_path)
            ),
        )

    @property
    def validation_formula(self):
        # type: () -> Optional[str]
        """Specifies the data validation criteria for a list item."""
        return self.properties.get("ValidationFormula", None)

    @property
    def parent_collection(self):
        # type: () -> ListCollection
        return self._parent_collection

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "CreatablesInfo": self.creatables_info,
                "CurrentChangeToken": self.current_change_token,
                "ContentTypes": self.content_types,
                "CustomActionElements": self.custom_action_elements,
                "DataSource": self.data_source,
                "DescriptionResource": self.description_resource,
                "DefaultView": self.default_view,
                "DefaultViewPath": self.default_view_path,
                "EffectiveBasePermissions": self.effective_base_permissions,
                "EffectiveBasePermissionsForUI": self.effective_base_permissions_for_ui,
                "EventReceivers": self.event_receivers,
                "LastItemDeletedDate": self.last_item_deleted_date,
                "LastItemModifiedDate": self.last_item_modified_date,
                "LastItemUserModifiedDate": self.last_item_user_modified_date,
                "ParentWeb": self.parent_web,
                "ParentWebPath": self.parent_web_path,
                "RootFolder": self.root_folder,
                "TitleResource": self.title_resource,
                "UserCustomActions": self.user_custom_actions,
                "VersionPolicies": self.version_policies,
            }
            default_value = property_mapping.get(name, None)
        return super(List, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(List, self).set_property(name, value, persist_changes)
        # fallback: create a new resource path
        if self._resource_path is None:
            if name == "Id":
                self._resource_path = self.parent_collection.get_by_id(
                    value
                ).resource_path
            elif name == "Url":
                self._resource_path = ServiceOperationPath(
                    "getList", [value], self.context.web.resource_path
                )
        return self
