# coding=utf-8
import datetime
from typing import TYPE_CHECKING, AnyStr, Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.client_query import ClientQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.activities.entity import SPActivityEntity
from office365.sharepoint.activities.logger import ActivityLogger
from office365.sharepoint.alerts.collection import AlertCollection
from office365.sharepoint.authentication.acs_service_principal_info import (
    SPACSServicePrincipalInfo,
)
from office365.sharepoint.businessdata.app_bdc_catalog import AppBdcCatalog
from office365.sharepoint.changes.collection import ChangeCollection
from office365.sharepoint.changes.query import ChangeQuery
from office365.sharepoint.clientsidecomponent.hostedapps.manager import (
    HostedAppsManager,
)
from office365.sharepoint.clientsidecomponent.identifier import (
    SPClientSideComponentIdentifier,
)
from office365.sharepoint.clientsidecomponent.query_result import (
    SPClientSideComponentQueryResult,
)
from office365.sharepoint.clientsidecomponent.storage_entity import StorageEntity
from office365.sharepoint.contenttypes.collection import ContentTypeCollection
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.eventreceivers.definition_collection import (
    EventReceiverDefinitionCollection,
)
from office365.sharepoint.features.collection import FeatureCollection
from office365.sharepoint.fields.collection import FieldCollection
from office365.sharepoint.fields.datetime_field_format_type import (
    DateTimeFieldFormatType,
)
from office365.sharepoint.files.file import File
from office365.sharepoint.flows.synchronization_result import FlowSynchronizationResult
from office365.sharepoint.folders.collection import FolderCollection
from office365.sharepoint.folders.folder import Folder
from office365.sharepoint.internal.paths.web import WebPath
from office365.sharepoint.largeoperation.operation import SPLargeOperation
from office365.sharepoint.listitems.listitem import ListItem
from office365.sharepoint.lists.collection import ListCollection
from office365.sharepoint.lists.creation_information import ListCreationInformation
from office365.sharepoint.lists.document_library_information import (
    DocumentLibraryInformation,
)
from office365.sharepoint.lists.get_parameters import GetListsParameters
from office365.sharepoint.lists.list import List
from office365.sharepoint.lists.render_data_parameters import RenderListDataParameters
from office365.sharepoint.lists.template_collection import ListTemplateCollection
from office365.sharepoint.lists.template_type import ListTemplateType
from office365.sharepoint.marketplace.corporatecuratedgallery.addins.principals_response import (
    SPGetAddinPrincipalsResponse,
)
from office365.sharepoint.marketplace.corporatecuratedgallery.available_addins_response import (
    SPAvailableAddinsResponse,
)
from office365.sharepoint.marketplace.sitecollection.appcatalog.accessor import (
    SiteCollectionCorporateCatalogAccessor,
)
from office365.sharepoint.marketplace.tenant.appcatalog.accessor import (
    TenantCorporateCatalogAccessor,
)
from office365.sharepoint.navigation.navigation import Navigation
from office365.sharepoint.permissions.base_permissions import BasePermissions
from office365.sharepoint.permissions.roles.definitions.collection import (
    RoleDefinitionCollection,
)
from office365.sharepoint.permissions.securable_object import SecurableObject
from office365.sharepoint.principal.groups.collection import GroupCollection
from office365.sharepoint.principal.groups.group import Group
from office365.sharepoint.principal.users.collection import UserCollection
from office365.sharepoint.principal.users.user import User
from office365.sharepoint.pushnotifications.collection import (
    PushNotificationSubscriberCollection,
)
from office365.sharepoint.pushnotifications.subscriber import PushNotificationSubscriber
from office365.sharepoint.recyclebin.item_collection import RecycleBinItemCollection
from office365.sharepoint.sharing.external_site_option import ExternalSharingSiteOption
from office365.sharepoint.sharing.links.access_request import SharingLinkAccessRequest
from office365.sharepoint.sharing.links.data import SharingLinkData
from office365.sharepoint.sharing.object_sharing_settings import ObjectSharingSettings
from office365.sharepoint.sharing.result import SharingResult
from office365.sharepoint.sharing.shared_document_info import SharedDocumentInfo
from office365.sharepoint.sitescripts.serialization_info import (
    SiteScriptSerializationInfo,
)
from office365.sharepoint.sitescripts.serialization_result import (
    SiteScriptSerializationResult,
)
from office365.sharepoint.sitescripts.utility import SiteScriptUtility
from office365.sharepoint.translation.resource_entry import SPResourceEntry
from office365.sharepoint.translation.user_resource import UserResource
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath
from office365.sharepoint.ui.applicationpages.peoplepicker.web_service_interface import (
    ClientPeoplePickerWebServiceInterface,
)
from office365.sharepoint.usercustomactions.collection import UserCustomActionCollection
from office365.sharepoint.views.view import View
from office365.sharepoint.webparts.client.collection import ClientWebPartCollection
from office365.sharepoint.webs.calendar_type import CalendarType
from office365.sharepoint.webs.context_web_information import ContextWebInformation
from office365.sharepoint.webs.dataleakage_prevention_status_info import (
    SPDataLeakagePreventionStatusInfo,
)
from office365.sharepoint.webs.information_collection import WebInformationCollection
from office365.sharepoint.webs.modernize_homepage_result import ModernizeHomepageResult
from office365.sharepoint.webs.multilingual_settings import MultilingualSettings
from office365.sharepoint.webs.regional_settings import RegionalSettings
from office365.sharepoint.webs.template_collection import WebTemplateCollection
from office365.sharepoint.webs.theme_info import ThemeInfo

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class Web(SecurableObject):
    """
    Represents a SharePoint site. A site is a type of SecurableObject.

    A group of related webpages that is hosted by a server on the World Wide Web or an intranet.
    Each website has its own entry points, metadata, administration settings, and workflows.
    Also referred to as web site.
    """

    def __init__(self, context, resource_path=None):
        """
        Specifies the push notification subscriber over the site for the specified device app instance identifier.

        :type resource_path: ResourcePath or None
        :type context: office365.sharepoint.client_context.ClientContext
        """
        if resource_path is None:
            resource_path = WebPath("Web")
        super(Web, self).__init__(context, resource_path)
        self._web_url = None

    def __str__(self):
        return self.title

    def add_list(self, title, template_type=ListTemplateType.GenericList):
        """
        Creates a new list and adds it to the web.

        :param str title: Specifies the display name of the new list.
        :param int template_type: Specifies the list server template of the new list.
        """
        info = ListCreationInformation(title, None, template_type)
        return self.lists.add(info)

    def available_addins(self, server_relative_urls=None):
        """
        :param list[str] server_relative_urls:
        """
        payload = {"serverRelativeUrls": server_relative_urls}
        return_type = ClientResult(self.context, SPAvailableAddinsResponse())
        qry = ServiceOperationQuery(
            self, "AvailableAddins", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def add_cross_farm_message(self, message):
        # type: (str) -> ClientResult[bool]
        """
        :param str message:
        """
        payload = {"messagePayloadBase64": message}
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "AddCrossFarmMessage", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_access_request_list(self):
        """ """
        return_type = List(self.context)
        self.lists.add_child(return_type)

        def _get_access_request_list():
            return_type.set_property("Url", self.access_request_list_url)

        self.ensure_properties(["AccessRequestListUrl"], _get_access_request_list)
        return return_type

    def get_adaptive_card_extensions(self, include_errors=None, project=None):
        payload = {
            "includeErrors": include_errors,
            "project": project,
        }
        return_type = ClientResult(
            self.context, ClientValueCollection(SPClientSideComponentQueryResult)
        )
        qry = ServiceOperationQuery(
            self, "GetAdaptiveCardExtensions", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_document_by_doc_id(self, doc_id):
        """ """
        return_type = ClientResult(self.context)
        qry = ClientQuery(self.context, return_type=return_type)

        def _construct_request(request):
            request.url = "{0}/_layouts/15/DocIdRedir.aspx?ID={1}".format(
                self.context.base_url, doc_id
            )

        self.context.add_query(qry).before_query_execute(_construct_request)
        return return_type

    def get_site_script(
        self,
        include_branding=True,
        included_lists=None,
        include_links_to_exported_items=True,
        include_regional_settings=True,
    ):
        """
        Creates site script syntax from current SharePoint site.

        :param bool include_branding: Extracts the configuration of the site's branding.
        :param list[str] or None included_lists: A list of one or more lists. Each is identified by the list url.
        :param bool include_links_to_exported_items: Extracts navigation links. In order to export navigation links
            pointing to lists, the list needs to be included in the request as well.
        :param bool include_regional_settings: Extracts the site's regional settings.
        """
        result = ClientResult(self.context, SiteScriptSerializationResult())
        info = SiteScriptSerializationInfo(
            include_branding,
            included_lists,
            include_links_to_exported_items,
            include_regional_settings,
        )

        def _web_loaded():
            SiteScriptUtility.get_site_script_from_web(
                self.context, self.url, info, return_type=result
            )

        self.ensure_property("Url", _web_loaded)
        return result

    def consent_to_power_platform(self):
        """"""
        return_type = FlowSynchronizationResult(self.context)
        qry = ServiceOperationQuery(
            self, "ConsentToPowerPlatform", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_list_data_as_stream(self, path, view_xml=None):
        """Returns list data from the specified list url and for the specified query parameters.

        :param str path: A string that contains the site-relative URL for a list, for example, /Lists/Announcements.
        :param str view_xml:
        """
        if view_xml is None:
            view_xml = "<View><Query></Query></View>"
        return_type = ClientResult(self.context, {})

        def _get_list_data_as_stream():
            list_abs_url = self.url + path
            parameters = RenderListDataParameters(view_xml=view_xml)
            List.get_list_data_as_stream(
                self.context, list_abs_url, parameters, return_type=return_type
            )

        self.ensure_property("Url", _get_list_data_as_stream)
        return return_type

    def get_onedrive_list_data_as_stream(self, view_xml=None):
        """Returns list data from the specified list url and for the specified query parameters.

        :param str view_xml:
        """
        if view_xml is None:
            view_xml = RenderListDataParameters()
            view_xml.ViewXml = "<View><Query></Query></View>"
        return List.get_onedrive_list_data_as_stream(self.context, view_xml)

    def get_list_operation(self, list_id, operation_id):
        # type: (str, str) -> SPLargeOperation
        """ """
        return_type = SPLargeOperation(self.context)
        qry = ServiceOperationQuery(
            self,
            "GetListOperation",
            None,
            {"listId": list_id, "operationId": operation_id},
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_push_notification_subscriber(self, device_app_instance_id):
        """
        Specifies the push notification subscriber over the site for the specified device app instance identifier.

        :param str device_app_instance_id: Device application instance identifier.
        """
        return_type = PushNotificationSubscriber(self.context)
        qry = ServiceOperationQuery(
            self,
            "GetPushNotificationSubscriber",
            [device_app_instance_id],
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_client_side_components(self, components):
        """
        Returns the client side components for the requested components.
        Client components include data necessary to render Client Side Web Parts and Client Side Applications.

        :param list components: array of requested components, defined by id and version.
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(SPClientSideComponentIdentifier)
        )
        payload = {"components": components}
        qry = ServiceOperationQuery(
            self, "GetClientSideComponents", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_client_side_components_by_component_type(self, component_types):
        """
        :param str component_types:
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(SPClientSideComponentIdentifier)
        )
        payload = {"componentTypesString": component_types}
        qry = ServiceOperationQuery(
            self,
            "GetClientSideComponentsByComponentType",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_push_notification_subscribers_by_user(self, user):
        """
        Queries for the push notification subscribers for the site  for the specified user.

        :param str or User user: User object or login name
        """
        return_type = PushNotificationSubscriberCollection(self.context)

        def _create_and_add_query(login_name):
            """
            :type login_name: str
            """
            qry = ServiceOperationQuery(
                self,
                "GetPushNotificationSubscribersByUser",
                [login_name],
                None,
                None,
                return_type,
            )
            self.context.add_query(qry)

        if isinstance(user, User):

            def _user_loaded():
                _create_and_add_query(user.login_name)

            user.ensure_property("LoginName", _user_loaded)
        else:
            _create_and_add_query(user)
        return return_type

    @staticmethod
    def create_organization_sharing_link(context, url, is_edit_link=False):
        """Creates and returns an organization-internal link that can be used to access a document and gain permissions
           to it.

        :param office365.sharepoint.client_context.ClientContext context:
        :param str url: he URL of the site, with the path of the object in SharePoint that is represented as query
            string parameters, forSharing set to 1 if sharing, and bypass set to 1 to bypass any mobile logic.
        :param bool is_edit_link: If true, the link will allow the logged in user to edit privileges on the item.
        """
        return_type = ClientResult(context, str())
        params = {"url": url, "isEditLink": is_edit_link}
        qry = ServiceOperationQuery(
            context.web,
            "CreateOrganizationSharingLink",
            None,
            params,
            None,
            return_type,
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def destroy_organization_sharing_link(
        context, url, is_edit_link, remove_associated_sharing_link_group
    ):
        """Removes an existing organization link for an object.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str url: the URL of the site, with the path of the object in SharePoint that is represented as query
            string parameters, forSharing set to 1 if sharing, and bypass set to 1 to bypass any mobile logic.
        :param bool is_edit_link: If true, the link will allow the logged in user to edit privileges on the item.
        :param bool remove_associated_sharing_link_group: Indicates whether to remove the groups that contain the users
            who have been given access to the shared object via the sharing link
        """
        payload = {
            "url": url,
            "isEditLink": is_edit_link,
            "removeAssociatedSharingLinkGroup": remove_associated_sharing_link_group,
        }
        qry = ServiceOperationQuery(
            context.web, "DestroyOrganizationSharingLink", None, payload, None, None
        )
        qry.static = True
        context.add_query(qry)
        return context.web

    @staticmethod
    def get_context_web_information(context):
        """
        Returns an object that specifies metadata about the site

        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = ClientResult(context, ContextWebInformation())
        qry = ServiceOperationQuery(
            context.web, "GetContextWebInformation", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_web_url_from_page_url(context, page_full_url):
        """Returns the URL of the root folder for the site containing the specified URL

        :type context: office365.sharepoint.client_context.ClientContext
        :param str page_full_url: Specifies the URL from which to return the site URL.
        """
        return_type = ClientResult(context, str())
        payload = {"pageFullUrl": page_full_url}
        qry = ServiceOperationQuery(
            context.web, "GetWebUrlFromPageUrl", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    def create_default_associated_groups(
        self, user_login, user_login2, group_name_seed
    ):
        """
        Creates the default Owners, Members and Visitors SPGroups on the web.

        :param str user_login: The user logon name of the group owner.
        :param str user_login2: The secondary contact for the group.
        :param str group_name_seed: The name seed to use when creating of the full names of the default groups.
            For example, if the name seed is Contoso then the default groups will be created with the names:
            Contoso Owners, Contoso Members and Contoso Visitors. If the value of this parameter is null then the
            web title is used instead.
        """
        payload = {
            "userLogin": user_login,
            "userLogin2": user_login2,
            "groupNameSeed": group_name_seed,
        }
        qry = ServiceOperationQuery(
            self, "CreateDefaultAssociatedGroups", None, payload
        )
        qry.static = True
        self.context.add_query(qry)
        return self

    def create_group_based_environment(self):
        return_type = FlowSynchronizationResult(self.context)
        qry = ServiceOperationQuery(
            self, "CreateGroupBasedEnvironment", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_group_based_environment(self):
        return_type = FlowSynchronizationResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetGroupBasedEnvironment", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_acs_service_principals(self, app_ids=None):
        """
        List service principals
        :param list[str] app_ids:
        """
        payload = {"appIds": app_ids}
        return_type = ClientResult(
            self.context, ClientValueCollection(SPACSServicePrincipalInfo)
        )
        qry = ServiceOperationQuery(
            self, "GetACSServicePrincipals", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def sync_flow_instances(self, target_web_url):
        """
        :param str target_web_url:
        """
        return_type = FlowSynchronizationResult(self.context)
        payload = {"targetWebUrl": target_web_url}
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

    def get_all_client_side_components(self):
        """"""
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "GetAllClientSideComponents", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_addin_principals_having_permissions_in_sites(
        self, server_relative_urls=None, urls=None
    ):
        """
        :param list[str] server_relative_urls:
        :param list[str] urls:
        """
        payload = {"serverRelativeUrls": server_relative_urls, "urls": urls}
        return_type = ClientResult(self.context, SPGetAddinPrincipalsResponse())
        qry = ServiceOperationQuery(
            self,
            "GetAddinPrincipalsHavingPermissionsInSites",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_app_bdc_catalog(self):
        """
        Returns the Business Data Connectivity (BDC) MetadataCatalog for an application that gives access to the
        external content types defined in the BDC metadata model packaged by the application.<151>
        This method SHOULD be called on the Web (section 3.2.5.143) object that represents the site for the application
        and it returns the BDC MetadataCatalog deployed on the site.
        """
        return_type = AppBdcCatalog(self.context)
        qry = ServiceOperationQuery(
            self, "GetAppBdcCatalog", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_client_side_web_parts(self, project=None, include_errors=False):
        """
        It MUST return an array of 3rd party webpart components installed on this site

        :param bool include_errors: If true, webparts with errors MUST be included in the results of the request.
           If false, webparts with errors MUST be excluded in the results of the request.
        :param str project:
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(SPClientSideComponentQueryResult)
        )
        params = {"includeErrors": include_errors, "project": project}
        qry = ServiceOperationQuery(
            self, "GetClientSideWebParts", None, params, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def add_supported_ui_language(self, lcid):
        """
        Adds a supported UI language by its language identifier.
        :param int lcid: Specifies the language identifier to be added.
        """
        qry = ServiceOperationQuery(self, "AddSupportedUILanguage", {"lcid": lcid})
        self.context.add_query(qry)
        return self

    def get_lists(self, row_limit=100):
        """
        :param int row_limit: Specifies a limit for the number of lists in the query that are returned per page
        """
        return_type = ListCollection(self.context)
        payload = {"getListsParams": GetListsParameters(row_limit=row_limit)}
        qry = ServiceOperationQuery(self, "GetLists", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_sub_webs_filtered_for_current_user(self, query):
        """Returns a collection of objects that contain metadata about subsites of the current site (2) in which the
        current user is a member.

        :type query: office365.sharepoint.webs.subweb_query.SubwebQuery
        """
        return_type = WebInformationCollection(self.context)
        qry = ServiceOperationQuery(
            self,
            "getSubWebsFilteredForCurrentUser",
            {
                "nWebTemplateFilter": query.WebTemplateFilter,
                "nConfigurationFilter": query.ConfigurationFilter,
            },
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_recycle_bin_items(
        self,
        paging_info=None,
        row_limit=100,
        is_ascending=True,
        order_by=None,
        item_state=None,
    ):
        """
        Gets the recycle bin items that are based on the specified query.

        :param str paging_info: an Object that is used to obtain the next set of rows in a paged view of the Recycle Bin
        :param int row_limit: a limit for the number of items returned in the query per page.
        :param bool is_ascending: a Boolean value that specifies whether to sort in ascending order.
        :param int order_by: the column by which to order the Recycle Bin query.
        :param int item_state: Recycle Bin stage of items to return in the query.
        """
        return_type = RecycleBinItemCollection(
            self.context, self.recycle_bin.resource_path
        )
        payload = {
            "rowLimit": row_limit,
            "isAscending": is_ascending,
            "pagingInfo": paging_info,
            "orderBy": order_by,
            "itemState": item_state,
        }
        qry = ServiceOperationQuery(
            self, "GetRecycleBinItems", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_recycle_bin_items_by_query_info(
        self,
        is_ascending=True,
        item_state=None,
        order_by=None,
        paging_info=None,
        row_limit=100,
        show_only_my_items=False,
    ):
        """
        Gets the recycle bin items that are based on the specified query.

        :param str paging_info: an Object that is used to obtain the next set of rows in a paged view of the Recycle Bin
        :param int row_limit: a limit for the number of items returned in the query per page.
        :param bool is_ascending: a Boolean value that specifies whether to sort in ascending order.
        :param int order_by: the column by which to order the Recycle Bin query.
        :param int item_state: Recycle Bin stage of items to return in the query.
        :param bool show_only_my_items:
        """
        return_type = RecycleBinItemCollection(
            self.context, self.recycle_bin.resource_path
        )
        payload = {
            "rowLimit": row_limit,
            "isAscending": is_ascending,
            "pagingInfo": paging_info,
            "orderBy": order_by,
            "itemState": item_state,
            "ShowOnlyMyItems": show_only_my_items,
        }
        qry = ServiceOperationQuery(
            self, "GetRecycleBinItemsByQueryInfo", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_all_webs(self):
        """Returns a collection containing a flat list of all Web objects in the Web."""
        from office365.sharepoint.webs.collection import WebCollection

        return_type = WebCollection(self.context, self.webs.resource_path)

        def _webs_loaded():
            self._load_sub_webs_inner(self.webs, return_type)

        self.ensure_property("Webs", _webs_loaded)
        return return_type

    def _load_sub_webs_inner(self, webs, all_webs):
        """
        :type webs: office365.sharepoint.webs.collection.WebCollection
        :type all_webs: office365.sharepoint.webs.collection.WebCollection
        """
        for cur_web in webs:
            all_webs.add_child(cur_web)

            def _webs_loaded(web):
                if len(web.webs) > 0:
                    self._load_sub_webs_inner(web.webs, all_webs)

            cur_web.ensure_property("Webs", _webs_loaded, cur_web)

    def get_list_using_path(self, decoded_url):
        """
        Returns the list that is associated with the specified server-relative path.

        :param str decoded_url: Contains the site-relative path for a list, for example, /Lists/Announcements.
        """
        path = SPResPath.create_relative(self.context.base_url, decoded_url)
        return_type = List(self.context)
        self.lists.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "GetListUsingPath", path, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_news_list(self, allow_create=False):
        """
        Returns the News List on this web, if it exists. If the list does not exist, the list will be created and
        then returned if allowCreate is set to true. The News List is a hidden SP.List in which News Posts are stored.

        :param bool allow_create: Indicates whether to create the list if it does not exist on this web.
            "true" means yes.
        """
        return_type = List(self.context)
        self.lists.add_child(return_type)
        payload = {"allowCreate": allow_create}
        qry = ServiceOperationQuery(
            self, "GetNewsList", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_view_from_url(self, list_url):
        """Returns a view of a list within the site based on the specified URL.

        :param str list_url: Contains either an absolute URL or a site-relative URL of a view.
        """
        return View(
            self.context,
            ServiceOperationPath("GetViewFromUrl", [list_url], self.resource_path),
        )

    def get_view_from_path(self, decoded_url):
        """Returns a view of a list within the site based on the specified path.

        :param str decoded_url: Contains either an absolute path or a site-relative path of a view.
        """
        return View(
            self.context,
            ServiceOperationPath("GetViewFromPath", [decoded_url], self.resource_path),
        )

    def get_regional_datetime_schema(self):
        """Get DateTime Schema based on regional settings"""
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "GetRegionalDateTimeSchema", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_sharing_link_data(self, link_url):
        # type: (str) -> ClientResult[SharingLinkData]
        """
        This method determines basic information about the supplied link URL, including limited data about the object
        the link URL refers to and any additional sharing link data if the link URL is a tokenized sharing link

        :param str link_url: A URL that is either a tokenized sharing link or a canonical URL for a document
        """
        return_type = ClientResult(self.context, SharingLinkData())
        payload = {"linkUrl": link_url}
        qry = ServiceOperationQuery(
            self, "GetSharingLinkData", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def get_context_web_theme_data(context):
        # type: (ClientContext) -> ClientResult[str]
        """
        Get ThemeData for the context web.

        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = ClientResult(context, str())
        qry = ServiceOperationQuery(
            context.web, "GetContextWebThemeData", None, None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    def create_site_page(self, page_metadata):
        """Create a site page

        :param str page_metadata:
        """
        payload = {"pageMetaData": page_metadata}
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "CreateSitePage", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def create_anonymous_link(context, url, is_edit_link, return_type=None):
        """Create an anonymous link which can be used to access a document without needing to authenticate.

        :param bool is_edit_link: If true, the link will allow the guest user edit privileges on the item.
        :param str url: The URL of the site, with the path of the object in SharePoint represented as query
        string parameters
        :param office365.sharepoint.client_context.ClientContext context: client context
        :param ClientResult[str] return_type: Return type
        """
        if return_type is None:
            return_type = ClientResult(context, str())
        payload = {
            "url": str(SPResPath.create_absolute(context.base_url, url)),
            "isEditLink": is_edit_link,
        }
        qry = ServiceOperationQuery(
            context.web, "CreateAnonymousLink", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def create_anonymous_link_with_expiration(
        context, url, is_edit_link, expiration_string, return_type=None
    ):
        """
        Creates and returns an anonymous link that can be used to access a document without needing to authenticate.

        :param bool is_edit_link: If true, the link will allow the guest user edit privileges on the item.
        :param str url: The URL of the site, with the path of the object in SharePoint represented as query
        string parameters
        :param str expiration_string: A date/time string for which the format conforms to the ISO 8601:2004(E) complete
        representation for calendar date and time of day, and which represents the time and date of expiry for the
        anonymous link. Both the minutes and hour value MUST be specified for the difference between the local and
        UTC time. Midnight is represented as 00:00:00.
        :param office365.sharepoint.client_context.ClientContext context: client context
        :param ClientResult return_type: Return type
        """
        if return_type is None:
            return_type = ClientResult(context, str())
        payload = {
            "url": str(SPResPath.create_absolute(context.base_url, url)),
            "isEditLink": is_edit_link,
            "expirationString": expiration_string,
        }
        qry = ServiceOperationQuery(
            context.web,
            "CreateAnonymousLinkWithExpiration",
            None,
            payload,
            None,
            return_type,
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_object_sharing_settings(
        context, object_url, group_id=None, use_simplified_roles=None, return_type=None
    ):
        """Given a path to an object in SharePoint, this will generate a sharing settings object which contains
        necessary information for rendering sharing information

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client
        :param str object_url: A URL with one of two possible formats.
              The two possible URL formats are:
              1) The URL of the site, with the path of the object in SharePoint represented as query string parameters,
              forSharing set to 1 if sharing, and mbypass set to 1 to bypass any mobile logic
              e.g. https://contoso.com/?forSharing=1&mbypass=1&List=%7BCF908473%2D72D4%2D449D%2D8A53%2D4BD01EC54B84%7D&
              obj={CF908473-72D4-449D-8A53-4BD01EC54B84},1,DOCUMENT
              2) The URL of the SharePoint object (web, list, item) intended for sharing
              e.g. https://contoso.com/Documents/SampleFile.docx
        :param int group_id: The id value of the permissions group if adding to a group, 0 otherwise.
        :param bool use_simplified_roles: A Boolean value indicating whether to use the SharePoint
        simplified roles (Edit, View) or not.
        :param ObjectSharingSettings return_type: Return type
        """
        if return_type is None:
            return_type = ObjectSharingSettings(context)
        payload = {
            "objectUrl": object_url,
            "groupId": group_id,
            "useSimplifiedRoles": use_simplified_roles,
        }
        qry = ServiceOperationQuery(
            context.web, "GetObjectSharingSettings", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    def get_client_side_components_by_id(self, component_ids=None):
        """
        Returns the client side components for the requested component identifiers.
        Client components include data necessary to render Client Side Web Parts and Client Side Applications.

        :param list[str] component_ids: List of requested component identifiers.
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(SPClientSideComponentQueryResult)
        )
        payload = {"componentIds": StringCollection(component_ids)}
        qry = ServiceOperationQuery(
            self, "GetClientSideComponentsById", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_by_server_relative_url(self, server_relative_url):
        # type: (str) -> File
        """
        Returns the file object located at the specified server-relative URL, for example:
            - "/sites/MySite/Shared Documents/MyDocument.docx"
            - "Shared Documents/MyDocument.docx"

        :param str server_relative_url: Specifies the server-relative URL for the file.
        """
        path = SPResPath.create_relative(self.context.base_url, server_relative_url)
        return File(
            self.context,
            ServiceOperationPath(
                "getFileByServerRelativeUrl", [str(path)], self.resource_path
            ),
            self.root_folder.files,
        )

    def get_file_by_server_relative_path(self, path):
        # type: (str) -> File
        """Returns the file object located at the specified server-relative path, for example:
            - "/sites/MySite/Shared Documents/MyDocument.docx"
            - "Shared Documents/MyDocument.docx"
        Note: prefer this method over get_folder_by_server_relative_url since it supports % and # symbols in names

        :param str path: Contains the server-relative path of the file.
        """
        path = SPResPath.create_relative(self.context.base_url, path)
        return File(
            self.context,
            ServiceOperationPath(
                "getFileByServerRelativePath", path.to_json(), self.resource_path
            ),
            self.root_folder.files,
        )

    def get_folder_by_server_relative_url(self, url):
        # type: (str) -> Folder
        """Returns the folder object located at the specified server-relative URL.

        :param str url: Specifies the server-relative URL for the folder.
        """
        return Folder(
            self.context,
            ServiceOperationPath(
                "getFolderByServerRelativeUrl", [url], self.resource_path
            ),
            self.folders,
        )

    def get_folder_by_server_relative_path(self, decoded_url):
        # type: (str) -> Folder
        """Returns the folder object located at the specified server-relative URL, for example:
             - "/sites/MySite/Shared Documents"
             - "Shared Documents"
        Prefer this method over get_folder_by_server_relative_url since it supports % and # symbols

        :param str decoded_url: Contains the server-relative URL for the folder
        """
        path = SPResPath(decoded_url)
        return Folder(
            self.context,
            ServiceOperationPath(
                "getFolderByServerRelativePath", path.to_json(), self.resource_path
            ),
            self.folders,
        )

    def get_site_page_copy_to_status(self, work_item_id):
        """
        :param str work_item_id:
        """
        return_type = ClientResult(self.context, str())
        payload = {"workItemId": work_item_id}
        qry = ServiceOperationQuery(
            self, "GetSitePageCopyToStatus", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_page_move_status(self, work_item_id):
        """
        :param str work_item_id:
        """
        return_type = ClientResult(self.context, str())
        payload = {"workItemId": work_item_id}
        qry = ServiceOperationQuery(
            self, "GetSitePageMoveStatus", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ensure_folder_path(self, path):
        """
        Ensures a nested folder hierarchy exist
        :param str path: relative server URL (path) to a folder
        """
        return self.root_folder.folders.ensure_path(path)

    def ensure_edu_class_setup(self, bypass_for_automation):
        """
        :param bool bypass_for_automation:
        """
        return_type = ClientResult(self.context, bool())
        payload = {"byPassForAutomation": bypass_for_automation}
        qry = ServiceOperationQuery(
            self, "EnsureEduClassSetup", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ensure_user(self, login_name):
        """Checks whether the specified logon name belongs to a valid user of the website, and if the logon name does
        not already exist, adds it to the website.

        :param str login_name: Specifies a string that contains the login name.
        """
        return_type = User(self.context)
        self.site_users.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "EnsureUser", [login_name], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ensure_tenant_app_catalog(self, caller_id):
        """
        :param str caller_id:
        """
        return_type = ClientResult(self.context, bool())
        payload = {"callerId": caller_id}
        qry = ServiceOperationQuery(
            self, "EnsureTenantAppCatalog", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_list_by_title(self, title):
        """
        Returns the list with the specified display name.

        :param str title: Specifies the display name
        """
        return List(
            self.context,
            ServiceOperationPath("GetListByTitle", [title], self.resource_path),
        )

    def does_user_have_permissions(self, permission_mask):
        """Returns whether the current user has the given set of permissions.

        :param BasePermissions permission_mask: Specifies the set of permissions to verify.
        """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "DoesUserHavePermissions", permission_mask, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def does_push_notification_subscriber_exist(self, device_app_instance_id):
        """
        Specifies whether the push notification subscriber exists for the current user
            with the given device  app instance identifier.

        :param str device_app_instance_id: Device application instance identifier.
        """
        return_type = ClientResult(self.context, bool())
        params = {"deviceAppInstanceId": device_app_instance_id}
        qry = ServiceOperationQuery(
            self, "DoesPushNotificationSubscriberExist", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_folder_by_id(self, unique_id):
        """
        Returns the folder object with the specified GUID.

        :param str unique_id: A GUID that identifies the folder.
        """
        return Folder(
            self.context,
            ServiceOperationPath("GetFolderById", [unique_id], self.resource_path),
        )

    def get_user_by_id(self, user_id):
        """Returns the user corresponding to the specified member identifier for the current site.

        :param int user_id: Specifies the member identifier.
        """
        return User(
            self.context,
            ServiceOperationPath("getUserById", [user_id], self.resource_path),
        )

    def default_document_library(self):
        """Retrieves the default document library."""
        return List(
            self.context,
            ServiceOperationPath("defaultDocumentLibrary", None, self.resource_path),
        )

    def get_list(self, path):
        """Get list by path

        :param str path: A string that contains the site-relative URL for a list, for example, /Lists/Announcements.
        """
        safe_path = SPResPath.create_relative(self.context.base_url, path)
        return List(
            self.context,
            ServiceOperationPath("getList", [str(safe_path)], self.resource_path),
        )

    def get_changes(self, query=None):
        """Returns the collection of all changes from the change log that have occurred within the scope of the web,
        based on the specified query.

        :param office365.sharepoint.changes.query.ChangeQuery query: Specifies which changes to return
        """
        if query is None:
            query = ChangeQuery(web=True, fetch_limit=100)
        return_type = ChangeCollection(self.context)
        payload = {"query": query}
        qry = ServiceOperationQuery(
            self, "getChanges", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_available_web_templates(self, lcid=1033, do_include_cross_language=False):
        """
        Returns a collection of site templates available for the site.

        :param int lcid: Specifies the LCID of the site templates to be retrieved.
        :param bool do_include_cross_language: Specifies whether to include language-neutral site templates.
        """
        params = {"lcid": lcid, "doIncludeCrossLanguage": do_include_cross_language}
        return_type = WebTemplateCollection(
            self.context,
            ServiceOperationPath(
                "GetAvailableWebTemplates", params, self.resource_path
            ),
        )

        qry = ServiceOperationQuery(
            self, "GetAvailableWebTemplates", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def hub_site_data(self, force_refresh=False):
        """Retrieves data describing a SharePoint hub site.

        :param bool force_refresh: Default value is false. When false, the data is returned from the server's cache.
            When true, the cache is refreshed with the latest updates and then returned. Use this if you just made
            changes and need to see those changes right away.
        """
        return_type = ClientResult(self.context)
        payload = {"forceRefresh": force_refresh}
        qry = ServiceOperationQuery(
            self, "HubSiteData", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def increment_site_client_tag(self):
        """Increments the client cache control number for this site collection."""
        qry = ServiceOperationQuery(self, "IncrementSiteClientTag")
        self.context.add_query(qry)
        return self

    def apply_web_template(self, web_template):
        """
        Applies the specified site definition or site template to the website that has no template applied to it.

        :param str web_template: The name of the site definition or the file name of the site template to be applied.
        """
        qry = ServiceOperationQuery(
            self, "ApplyWebTemplate", {"webTemplate": web_template}
        )
        self.context.add_query(qry)
        return self

    def get_custom_list_templates(self):
        """Specifies the collection of custom list templates for a given site."""
        return_type = ListTemplateCollection(self.context)
        qry = ServiceOperationQuery(
            self, "GetCustomListTemplates", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_by_guest_url(self, guest_url):
        """
        Returns the file object from the guest access URL.
        :param str guest_url: The guest access URL to get the file with.
        """
        return_type = File(self.context)
        payload = {"guestUrl": guest_url}
        qry = ServiceOperationQuery(
            self, "GetFileByGuestUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_by_guest_url_extended(
        self, guest_url, ensure_access=None, password=None
    ):
        """
        Returns the file object from the tokenized sharing link URL.

        :param str guest_url: The tokenized sharing link URL for the folder.
        :param str password: This value contains the password to be supplied to a tokenized sharing link for validation.
             This value is only needed if the link requires a password before granting access and the calling user
             does not currently have perpetual access through the tokenized sharing link.
             This value MUST be set to the correct password for the tokenized sharing link for the access granting
             operation to succeed. If the tokenized sharing link does not require a password or the calling user
             already has perpetual access through the tokenized sharing link, this value will be ignored.
        :param bool ensure_access: Indicates if the request to the tokenized sharing link grants perpetual access to
            the calling user.
        """
        return_type = File(self.context)
        payload = {
            "guestUrl": guest_url,
            "requestSettings": SharingLinkAccessRequest(ensure_access, password),
        }
        qry = ServiceOperationQuery(
            self, "GetFileByGuestUrlExtended", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_by_guest_url_ensure_access(self, guest_url, ensure_access):
        """
        Returns the file object from the tokenized sharing link URL.

        :param str guest_url: The guest access URL to get the file with.
        :param bool ensure_access:  Indicates if the request to the tokenized sharing link grants perpetual access to
            the calling user. If it is set to true then the user who is requesting the file will be granted perpetual
            permissions through the tokenized sharing link.
        """
        return_type = File(self.context)
        payload = {"guestUrl": guest_url, "ensureAccess": ensure_access}
        qry = ServiceOperationQuery(
            self, "GetFileByGuestUrlEnsureAccess", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_by_linking_url(self, linking_url):
        """
        Returns the file object from the linking URL.

        :param str linking_url: The linking URL to return the file object for.
            A linking URL can be obtained from LinkingUrl.
        """
        return_type = File(self.context)
        payload = {"linkingUrl": linking_url}
        qry = ServiceOperationQuery(
            self, "GetFileByLinkingUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_by_url(self, file_url):
        """
        Returns the file object from the given URL.

        :param str file_url: The URL used to get the file object.
        """
        return_type = File(self.context)
        params = {"fileUrl": file_url}
        qry = ServiceOperationQuery(
            self, "GetFileByUrl", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_by_wopi_frame_url(self, wopi_frame_url):
        """
        Returns the file object from the WOPI frame URL.

        :param str wopi_frame_url:  The WOPI frame URL used to get the file object.
        """
        return_type = File(self.context)
        qry = ServiceOperationQuery(
            self, "GetFileByWOPIFrameUrl", [wopi_frame_url], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_folder_by_guest_url(self, guest_url):
        """
        Returns the folder object from the tokenized sharing link URL.

        :param str guest_url: The tokenized sharing link URL for the folder.
        """
        return_type = Folder(self.context, parent_collection=self.folders)
        payload = {"guestUrl": guest_url}
        qry = ServiceOperationQuery(
            self, "GetFolderByGuestUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_folder_by_guest_url_extended(
        self, guest_url, ensure_access=None, password=None
    ):
        """
        Returns the folder object from the tokenized sharing link URL.

        :param str guest_url: The tokenized sharing link URL for the folder.
        :param str password: This value contains the password to be supplied to a tokenized sharing link for validation.
             This value is only needed if the link requires a password before granting access and the calling user
             does not currently have perpetual access through the tokenized sharing link.
             This value MUST be set to the correct password for the tokenized sharing link for the access granting
             operation to succeed. If the tokenized sharing link does not require a password or the calling user
             already has perpetual access through the tokenized sharing link, this value will be ignored.
        :param bool ensure_access: Indicates if the request to the tokenized sharing link grants perpetual access to
            the calling user.
        """
        return_type = Folder(self.context, parent_collection=self.folders)
        payload = {
            "guestUrl": guest_url,
            "requestSettings": SharingLinkAccessRequest(ensure_access, password),
        }
        qry = ServiceOperationQuery(
            self, "GetFolderByGuestUrlExtended", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def parse_datetime(
        self,
        value,
        display_format=DateTimeFieldFormatType.DateTime,
        calendar_type=CalendarType.None_,
    ):
        # type: (str, int, int) -> ClientResult[str]
        """
        Returns parsed DateTime value.

        :param str value: The input is the string of a datetime that's in web's local time and in web's calendar.
           For example, the input "09/08/1430" when web's calendar was set to Hijri, the actual datetime is 07/31/2009
           in Gregorian calendar.
        :param int display_format: Int value representing SP.DateTimeFieldFormatType
        :param int calendar_type: Int value representing SP.CalendarType
        """
        return_type = ClientResult(self.context)
        payload = {
            "value": value,
            "displayFormat": display_format,
            "calendarType": calendar_type,
        }
        qry = ServiceOperationQuery(
            self, "ParseDateTime", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def share(
        self,
        user_principal_name,
        share_option=ExternalSharingSiteOption.View,
        send_email=True,
        email_subject=None,
        email_body=None,
    ):
        """
        Share a Web with user

        :param str user_principal_name: User identifier
        :param ExternalSharingSiteOption share_option: The sharing type of permission to grant on the object.
        :param bool send_email: A flag to determine if an email notification SHOULD be sent (if email is configured).
        :param str email_subject: The email subject.
        :param str email_body: The email subject.
        """

        return_type = SharingResult(self.context)

        def _share(picker_result):
            # type: (ClientResult[str]) -> None
            groups = {
                ExternalSharingSiteOption.View: self.associated_visitor_group,
                ExternalSharingSiteOption.Edit: self.associated_member_group,
                ExternalSharingSiteOption.Owner: self.associated_owner_group,
            }  # type: dict[ExternalSharingSiteOption, Group]

            picker_input = "[{0}]".format(picker_result.value)
            role_value = "group:{groupId}".format(groupId=groups[share_option].id)
            Web.share_object(
                self.context,
                self.url,
                picker_input,
                role_value,
                0,
                False,
                send_email,
                False,
                email_subject,
                email_body,
                return_type=return_type,
            )

        def _web_resolved():
            ClientPeoplePickerWebServiceInterface.client_people_picker_resolve_user(
                self.context, user_principal_name
            ).after_execute(_share)

        self.ensure_properties(
            [
                "Url",
                "AssociatedVisitorGroup",
                "AssociatedMemberGroup",
                "AssociatedOwnerGroup",
            ],
            _web_resolved,
        )
        return return_type

    def unshare(self):
        """Removes Sharing permissions on a Web"""
        return_type = SharingResult(self.context)

        def _web_initialized():
            Web.unshare_object(self.context, self.url, return_type=return_type)

        self.ensure_property("Url", _web_initialized)
        return return_type

    @staticmethod
    def get_document_libraries(context, web_full_url):
        """
        Returns the document libraries of a SharePoint site, specifically a list of objects that represents
        document library information. Document libraries that are private—picture library, catalog library,
        asset library, application list, form template or libraries—for whom the user does not have permission to view
        the items are not included.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str web_full_url: The URL of the web.
        """
        return_type = ClientResult(
            context, ClientValueCollection(DocumentLibraryInformation)
        )
        payload = {"webFullUrl": web_full_url}
        qry = ServiceOperationQuery(
            context.web, "GetDocumentLibraries", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def default_document_library_url(context, web_url):
        """
        Returns the default document library URL.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str web_url:  URL of the web.
        """
        return_type = ClientResult(context, DocumentLibraryInformation())
        payload = {
            "webUrl": web_url,
        }
        qry = ServiceOperationQuery(
            context.web,
            "DefaultDocumentLibraryUrl",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def delete_all_anonymous_links_for_object(context, url):
        """
        Removes all existing anonymous links for an object.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str url:  The URL of the object being shared, with the path of the object in SharePoint that is
             represented as query string parameters.
        """
        payload = {"url": url}
        qry = ServiceOperationQuery(
            context.web, "DeleteAllAnonymousLinksForObject", None, payload
        )
        qry.static = True
        context.add_query(qry)
        return context.web

    @staticmethod
    def delete_anonymous_link_for_object(
        context, url, is_edit_link, remove_associated_sharing_link_group
    ):
        """
        Removes an existing anonymous link for an object..

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str url:  The URL of the object being shared, with the path of the object in SharePoint that is
             represented as query string parameters.
        :param bool is_edit_link: If true, the edit link for the object will be removed. If false, the view only link
            for the object will be removed.
        :param bool remove_associated_sharing_link_group: Indicates whether to remove the groups that contain the users
        who have been given access to the shared object via the sharing link¹.
        """
        payload = {
            "url": url,
            "isEditLink": is_edit_link,
            "removeAssociatedSharingLinkGroup": remove_associated_sharing_link_group,
        }
        qry = ServiceOperationQuery(
            context.web, "DeleteAnonymousLinkForObject", None, payload
        )
        qry.static = True
        context.add_query(qry)
        return context.web

    @staticmethod
    def get_document_and_media_libraries(context, web_full_url, include_page_libraries):
        """
        Returns the document libraries of a SharePoint site, including picture, asset, and site assets libraries.
        Document libraries that are private, catalog library, application list, form template, or libraries that user
        does not have permission to view items are not inlcuded.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str web_full_url:  URL of the web.
        :param bool include_page_libraries: Indicates whether to include page libraries. A value of "true" means yes.
        """
        return_type = ClientResult(
            context, ClientValueCollection(DocumentLibraryInformation)
        )
        payload = {
            "webFullUrl": web_full_url,
            "includePageLibraries": include_page_libraries,
        }
        qry = ServiceOperationQuery(
            context.web,
            "GetDocumentAndMediaLibraries",
            None,
            payload,
            None,
            return_type,
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_sharing_link_kind(context, file_url, return_type=None):
        """
        This method determines the kind of tokenized sharing link represented by the supplied file URL.

        :param office365.sharepoint.client_context.ClientContext context:
        :param str file_url: A URL that is a tokenized sharing link for a document
        :param ClientResult or None return_type: Return object
        """
        if return_type is None:
            return_type = ClientResult(context)
        qry = ServiceOperationQuery(
            context.web,
            "GetSharingLinkKind",
            None,
            {"fileUrl": file_url},
            None,
            return_type,
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def forward_object_link(
        context, url, people_picker_input, email_subject=None, email_body=None
    ):
        """
        Shares an object in SharePoint, such as a list item or a site with no Acl changes, by sending the link.
        This is used when the user has no permission to share and cannot send access request.
        The user can use this method to send the link of the object to site members who already have permission
        to view the object.
        Returns a SharingResult object that contains completion script and page for redirection if desired.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str url: The URL of the website with the path of an object in SharePoint query string parameters.
        :param str people_picker_input: A string of JSON representing users in people picker format.
        :param str email_subject: The email subject.
        :param str email_body: The email subject.
        """
        return_type = SharingResult(context)
        payload = {
            "url": url,
            "peoplePickerInput": people_picker_input,
            "emailSubject": email_subject,
            "emailBody": email_body,
        }
        qry = ServiceOperationQuery(
            context.web, "ForwardObjectLink", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def share_object(
        context,
        url,
        people_picker_input,
        role_value=None,
        group_id=0,
        propagate_acl=False,
        send_email=True,
        include_anonymous_link_in_email=False,
        email_subject=None,
        email_body=None,
        use_simplified_roles=True,
        return_type=None,
    ):
        """
        This method shares an object in SharePoint such as a list item or site. It returns a SharingResult object
        which contains the completion script and a page to redirect to if desired.


        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str url: The URL of the website with the path of an object in SharePoint query string parameters.
        :param str role_value: The sharing role value for the type of permission to grant on the object.
        :param str people_picker_input: A string of JSON representing users in people picker format.
        :param int group_id: The ID of the group to be added. Zero if not adding to a permissions group.
        :param bool propagate_acl:  A flag to determine if permissions SHOULD be pushed to items with unique permissions
        :param bool send_email: A flag to determine if an email notification SHOULD be sent (if email is configured).
        :param bool include_anonymous_link_in_email: If an email is being sent, this determines if an anonymous link
        SHOULD be added to the message.
        :param str email_subject: The email subject.
        :param str email_body: The email subject.
        :param bool use_simplified_roles: A Boolean value indicating whether to use the SharePoint simplified roles
        (Edit, View) or not.
        :param SharingResult or None return_type: Return type
        """
        if return_type is None:
            return_type = SharingResult(context)
        payload = {
            "url": url,
            "groupId": group_id,
            "peoplePickerInput": people_picker_input,
            "roleValue": role_value,
            "includeAnonymousLinkInEmail": include_anonymous_link_in_email,
            "propagateAcl": propagate_acl,
            "sendEmail": send_email,
            "emailSubject": email_subject,
            "emailBody": email_body,
            "useSimplifiedRoles": use_simplified_roles,
        }
        qry = ServiceOperationQuery(
            context.web, "ShareObject", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def unshare_object(context, url, return_type=None):
        """
        Removes Sharing permissions on an object.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str url: A SharingResult object which contains status codes pertaining to the completion of the operation
        :param SharingResult return_type: Return type
        """
        if return_type is None:
            return_type = SharingResult(context)
        payload = {"url": url}
        qry = ServiceOperationQuery(
            context.web, "UnshareObject", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    def get_file_by_id(self, unique_id):
        """Returns the file object with the specified GUID.

        :param str unique_id: A GUID that identifies the file object.
        """
        return File(
            self.context,
            ServiceOperationPath("GetFileById", [unique_id], self.resource_path),
        )

    def get_list_item(self, str_url):
        # type: (str) -> ListItem
        """
        Returns the list item that is associated with the specified server-relative URL.

        :param str str_url: A string that contains the server-relative URL,
        for example, "/sites/MySite/Shared Documents/MyDocument.docx".
        """
        return ListItem(
            self.context,
            ServiceOperationPath("GetListItem", [str_url], self.resource_path),
        )

    def get_list_item_using_path(self, decoded_url):
        # type: (str) -> ListItem
        """
        Returns the list item that is associated with the specified server-relative path.

        :param str decoded_url: A string that contains the server-relative path,
        for example, "/sites/MySite/Shared Documents/MyDocument.docx" or "Shared Documents/MyDocument.docx".
        """
        params = SPResPath.create_relative(self.context.base_url, decoded_url)
        return ListItem(
            self.context,
            ServiceOperationPath("GetListItemUsingPath", params, self.resource_path),
        )

    def get_catalog(self, type_catalog):
        """Gets the list template gallery, site template gallery, or Web Part gallery for the Web site.

        :param int type_catalog: The type of the gallery.
        """
        return List(
            self.context,
            ServiceOperationPath("getCatalog", [type_catalog], self.resource_path),
        )

    def page_context_info(self, include_odb_settings, emit_navigation_info):
        # type: (bool, bool) -> ClientResult[AnyStr]
        """
        Return Page context info for the current list being rendered.

        :param bool include_odb_settings:
        :param bool emit_navigation_info:
        """
        return_type = ClientResult(self.context)
        payload = {
            "includeODBSettings": include_odb_settings,
            "emitNavigationInfo": emit_navigation_info,
        }
        qry = ServiceOperationQuery(
            self, "PageContextInfo", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_storage_entity(self, key):
        """
        This will return the storage entity identified by the given key

        :param str key: ID of storage entity to be returned.
        """
        return_type = StorageEntity(self.context)
        params = {
            "key": key,
        }
        qry = ServiceOperationQuery(
            self, "GetStorageEntity", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_storage_entity(self, key, value, description=None, comments=None):
        """
        This will set the storage entity identified by the given key

        :param str key: Id of the storage entity to be set.
        :param str value: Value of the storage entity to be set.
        :param str description: Description of the storage entity to be set.
        :param str comments: Comments of the storage entity to be set.
        """
        payload = {
            "key": key,
            "value": value,
            "description": description,
            "comments": comments,
        }
        qry = ServiceOperationQuery(self, "SetStorageEntity", None, payload, None)
        self.context.add_query(qry)
        return self

    def remove_storage_entity(self, key):
        """
        This will remove the storage entity identified by the given key
        :param str key: Id of the storage entity to be removed.
        """
        params = {
            "key": key,
        }
        qry = ServiceOperationQuery(self, "RemoveStorageEntity", params)
        self.context.add_query(qry)
        return self

    def register_push_notification_subscriber(
        self, device_app_instance_id, service_token
    ):
        """
        Registers the push notification subscriber for the site. If the registration already exists,
        the service token is updated with the new value.
        :param str device_app_instance_id: Device  app instance identifier.
        :param str service_token: Token provided by the notification service to the device to receive notifications.
        """
        payload = {
            "deviceAppInstanceId": device_app_instance_id,
            "serviceToken": service_token,
        }
        return_type = PushNotificationSubscriber(self.context)
        qry = ServiceOperationQuery(
            self, "RegisterPushNotificationSubscriber", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def unregister_push_notification_subscriber(self, device_app_instance_id):
        """
        Unregisters the push notification subscriber from the site
        :param str device_app_instance_id: Device  app instance identifier.
        """
        payload = {
            "deviceAppInstanceId": device_app_instance_id,
        }
        qry = ServiceOperationQuery(
            self, "UnregisterPushNotificationSubscriber", None, payload
        )
        self.context.add_query(qry)
        return self

    def remove_supported_ui_language(self, lcid):
        """
        Removes a supported UI language by its language identifier.
        :param str lcid: Specifies the language identifier to be removed.
        """
        params = {"lcid": lcid}
        qry = ServiceOperationQuery(self, "RemoveSupportedUILanguage", params)
        self.context.add_query(qry)
        return self

    def set_access_request_site_description_and_update(self, description=None):
        """
        :param str description:
        """
        payload = {"description": description}
        qry = ServiceOperationQuery(
            self, "SetAccessRequestSiteDescriptionAndUpdate", None, payload
        )
        self.context.add_query(qry)
        return self

    def set_global_nav_settings(self, title, source):
        """
        :param str title:
        :param str source:
        """
        payload = {"title": title, "source": source}
        qry = ServiceOperationQuery(self, "SetGlobalNavSettings", None, payload)
        self.context.add_query(qry)
        return self

    def sync_hub_site_theme(self):
        """"""
        qry = ServiceOperationQuery(self, "SyncHubSiteTheme")
        self.context.add_query(qry)
        return self

    def assign_document_id(self, site_prefix, enabled=True):
        """
        Assign Document IDs

        :param str site_prefix: Specify whether IDs will be automatically assigned to all documents in the
            Site Collection. Additionally, you can specify a set of 4-12 characters that will be used at the beginning
            of all IDs assigned for documents in this Site Collection, to help ensure that items in different
            Site Collections will never get the same ID. Note: A timer job will be scheduled to assign IDs to
            documents already in the Site Collection.
        :param bool enabled:
        """

        props = {
            "docid_x005f_msft_x005f_hier_x005f_siteprefix": site_prefix,
            "docid_x005f_enabled": "1",
        }
        return self.set_property("AllProperties", props).update()

    @property
    def activities(self):
        # type: () -> EntityCollection[SPActivityEntity]
        return self.properties.get(
            "Activities",
            EntityCollection(
                self.context,
                SPActivityEntity,
                ResourcePath("Activities", self.resource_path),
            ),
        )

    @property
    def activity_logger(self):
        """"""
        return self.properties.get(
            "ActivityLogger",
            ActivityLogger(
                self.context, ResourcePath("ActivityLogger", self.resource_path)
            ),
        )

    @property
    def allow_rss_feeds(self):
        # type: () -> Optional[bool]
        """
        Gets a Boolean value that specifies whether the site collection allows RSS feeds.
        """
        return self.properties.get("AllowRssFeeds", None)

    @property
    def alternate_css_url(self):
        # type: () -> Optional[str]
        """Gets the URL for an alternate cascading style sheet (CSS) to use in the website."""
        return self.properties.get("AlternateCssUrl", None)

    @property
    def app_instance_id(self):
        # type: () -> Optional[str]
        """
        Specifies the identifier of the app instance that this site (2) represents. If this site (2) does not
        represent an app instance, then this MUST specify an empty GUID.
        """
        return self.properties.get("AppInstanceId", None)

    @property
    def author(self):
        """
        Gets a user object that represents the user who created the Web site.
        """
        return self.properties.get(
            "Author", User(self.context, ResourcePath("Author", self.resource_path))
        )

    @property
    def created(self):
        # type: () -> datetime.datetime
        """Specifies when the site was created"""
        return self.properties.get("Created", datetime.datetime.min)

    @property
    def custom_master_url(self):
        # type: () -> Optional[str]
        """Gets the URL for a custom master page to apply to the Web site"""
        return self.properties.get("CustomMasterUrl", None)

    @property
    def custom_site_actions_disabled(self):
        # type: () -> Optional[bool]
        return self.properties.get("CustomSiteActionsDisabled", None)

    @property
    def description_translations(self):
        """"""
        return self.properties.get(
            "DescriptionTranslations", ClientValueCollection(SPResourceEntry)
        )

    @property
    def design_package_id(self):
        # type: () -> Optional[str]
        """Gets or sets the ID of the Design Package used in this SP.Web.

        A value of Guid.Empty will mean that the default Design Package will be used for this SP.Web.
        The default is determined by the SP.WebTemplate of this SP.Web."""
        return self.properties.get("DesignPackageId", None)

    @property
    def disable_app_views(self):
        # type: () -> Optional[bool]
        """"""
        return self.properties.get("DisableAppViews", None)

    @property
    def disable_flows(self):
        # type: () -> Optional[bool]
        """"""
        return self.properties.get("DisableFlows", None)

    @property
    def id(self):
        # type: () -> Optional[str]
        """Specifies the site identifier for the site"""
        return self.properties.get("Id", None)

    @property
    def language(self):
        # type: () -> Optional[int]
        """Specifies the language code identifier (LCID) for the language that is used on the site"""
        return self.properties.get("Language", None)

    @property
    def last_item_modified_date(self):
        # type: () -> Optional[int]
        """Specifies when an item was last modified in the site"""
        return self.properties.get("LastItemModifiedDate", datetime.datetime.min)

    @property
    def access_requests_list(self):
        """"""
        return self.properties.get(
            "AccessRequestsList",
            List(self.context, ResourcePath("AccessRequestsList", self.resource_path)),
        )

    @property
    def access_request_list_url(self):
        # type: () -> Optional[str]
        """Gets the URL of the access request list to the current site"""
        return self.properties.get("AccessRequestListUrl", None)

    @property
    def allow_designer_for_current_user(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the current user is allowed to use a designer application to customize this site
        """
        return self.properties.get("AllowDesignerForCurrentUser", None)

    @property
    def allow_master_page_editing_for_current_user(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the current user is allowed to edit the master page.
        """
        return self.properties.get("AllowMasterPageEditingForCurrentUser", None)

    @property
    def allow_revert_from_template_for_current_user(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the current user is allowed to revert the site (2) to a default site template.
        """
        return self.properties.get("AllowRevertFromTemplateForCurrentUser", None)

    @property
    def effective_base_permissions(self):
        """Specifies the effective permissions that are assigned to the current user"""
        return self.properties.get("EffectiveBasePermissions", BasePermissions())

    @property
    def enable_minimal_download(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the site will use the minimal download strategy by default.

        The minimal download strategy will use a single .aspx file (start.aspx) for your pages, with the actual URL
        encoded in the text following the hash mark ('#'). When navigating from page to page, only the changes
        between two compatible pages will be downloaded. Fewer bytes will be downloaded and the page will appear
        more quickly.
        """
        return self.properties.get("EnableMinimalDownload", None)

    @property
    def webs(self):
        """Specifies the collection of all child sites for the site"""
        from office365.sharepoint.webs.collection import WebCollection

        return self.properties.get(
            "Webs",
            WebCollection(self.context, ResourcePath("webs", self.resource_path), self),
        )

    @property
    def folders(self):
        """Specifies the collection of all first-level folders in the site"""
        return self.properties.get(
            "Folders",
            FolderCollection(
                self.context, ResourcePath("folders", self.resource_path), self
            ),
        )

    @property
    def hosted_apps(self):
        """"""
        return self.properties.get(
            "HostedApps",
            HostedAppsManager(
                self.context, ResourcePath("HostedApps", self.resource_path)
            ),
        )

    @property
    def lists(self):
        # type: () -> ListCollection
        """Specifies the collection of lists that are contained in the site available to the current user based on the
        current user's permissions."""
        return self.properties.get(
            "Lists",
            ListCollection(self.context, ResourcePath("lists", self.resource_path)),
        )

    @property
    def onedrive_shared_items(self):
        # type: () -> EntityCollection[SharedDocumentInfo]
        """"""
        return self.properties.get(
            "OneDriveSharedItems",
            EntityCollection(
                self.context,
                SharedDocumentInfo,
                ResourcePath("OneDriveSharedItems", self.resource_path),
            ),
        )

    @property
    def site_users(self):
        # type: () -> UserCollection
        """Specifies the collection of users in the site collection that contains the site"""
        return self.properties.get(
            "SiteUsers",
            UserCollection(self.context, ResourcePath("siteUsers", self.resource_path)),
        )

    @property
    def site_groups(self):
        # type: () -> GroupCollection
        """Gets the collection of groups for the site collection."""
        return self.properties.get(
            "SiteGroups",
            GroupCollection(
                self.context, ResourcePath("siteGroups", self.resource_path)
            ),
        )

    @property
    def current_user(self):
        # type: () -> User
        """Gets the current user."""
        return self.properties.get(
            "CurrentUser",
            User(self.context, ResourcePath("CurrentUser", self.resource_path)),
        )

    @property
    def parent_web(self):
        # type: () -> Web
        """Gets the parent website of the specified website."""
        return self.properties.get(
            "ParentWeb",
            Web(self.context, ResourcePath("ParentWeb", self.resource_path)),
        )

    @property
    def associated_visitor_group(self):
        # type: () -> Group
        """Gets or sets the associated visitor group of the Web site."""
        return self.properties.get(
            "AssociatedVisitorGroup",
            Group(
                self.context, ResourcePath("AssociatedVisitorGroup", self.resource_path)
            ),
        )

    @property
    def associated_owner_group(self):
        # type: () -> Group
        """Gets or sets the associated owner group of the Web site."""
        return self.properties.get(
            "AssociatedOwnerGroup",
            Group(
                self.context, ResourcePath("AssociatedOwnerGroup", self.resource_path)
            ),
        )

    @property
    def associated_member_group(self):
        # type: () -> Group
        """Gets or sets the group of users who have been given contribute permissions to the Web site."""
        return self.properties.get(
            "AssociatedMemberGroup",
            Group(
                self.context, ResourcePath("AssociatedMemberGroup", self.resource_path)
            ),
        )

    @property
    def can_modernize_homepage(self):
        """Specifies the site theme associated with the site"""
        return self.properties.get(
            "CanModernizeHomepage",
            ModernizeHomepageResult(
                self.context, ResourcePath("CanModernizeHomepage", self.resource_path)
            ),
        )

    @property
    def fields(self):
        # type: () -> FieldCollection
        """Specifies the collection of all the fields (2) in the site (2)."""
        return self.properties.get(
            "Fields",
            FieldCollection(self.context, ResourcePath("Fields", self.resource_path)),
        )

    @property
    def content_types(self):
        # type: () -> ContentTypeCollection
        """Gets the collection of content types for the Web site."""
        return self.properties.get(
            "ContentTypes",
            ContentTypeCollection(
                self.context, ResourcePath("ContentTypes", self.resource_path), self
            ),
        )

    @property
    def configuration(self):
        """
        Specifies the identifier (ID) of the site definition that was used to create the site (2). If the site (2)
        was created with a custom site template this specifies the identifier (ID) of the site definition from which
        the custom site template is derived.
        """
        return self.properties.get("Configuration", None)

    @property
    def data_leakage_prevention_status_info(self):
        return self.properties.get(
            "DataLeakagePreventionStatusInfo",
            SPDataLeakagePreventionStatusInfo(
                self.context,
                ResourcePath("DataLeakagePreventionStatusInfo", self.resource_path),
            ),
        )

    @property
    def description_resource(self):
        """A UserResource object that represents the description of this web."""
        return self.properties.get(
            "DescriptionResource",
            UserResource(
                self.context, ResourcePath("DescriptionResource", self.resource_path)
            ),
        )

    @property
    def role_definitions(self):
        # type: () -> RoleDefinitionCollection
        """Gets the collection of role definitions for the Web site."""
        return self.properties.get(
            "RoleDefinitions",
            RoleDefinitionCollection(
                self.context, ResourcePath("RoleDefinitions", self.resource_path)
            ),
        )

    @property
    def event_receivers(self):
        # type: () -> EventReceiverDefinitionCollection
        """Specifies the collection of event receiver definitions that are currently available on the Web site"""
        return self.properties.get(
            "EventReceivers",
            EventReceiverDefinitionCollection(
                self.context, ResourcePath("EventReceivers", self.resource_path), self
            ),
        )

    @property
    def client_web_parts(self):
        """
        Gets a collection of the ClientWebParts installed in this SP.Web. It can be used to get metadata of the
        ClientWebParts or render them. It is a read-only collection as ClientWebParts need to be installed in
        an app package."""
        return self.properties.get(
            "ClientWebParts",
            ClientWebPartCollection(
                self.context, ResourcePath("ClientWebParts", self.resource_path)
            ),
        )

    @property
    def features(self):
        """Get web features"""
        return self.properties.get(
            "Features",
            FeatureCollection(
                self.context, ResourcePath("Features", self.resource_path), self
            ),
        )

    @property
    def tenant_app_catalog(self):
        """Returns the tenant app catalog for the given tenant if it exists."""
        return self.properties.get(
            "TenantAppCatalog",
            TenantCorporateCatalogAccessor(
                self.context, ResourcePath("TenantAppCatalog", self.resource_path)
            ),
        )

    @property
    def site_collection_app_catalog(self):
        """Returns the site collection app catalog for the given web if it exists."""
        return self.properties.get(
            "SiteCollectionAppCatalog",
            SiteCollectionCorporateCatalogAccessor(
                self.context,
                ResourcePath("SiteCollectionAppCatalog", self.resource_path),
            ),
        )

    @property
    def web_infos(self):
        """Specifies the collection of all child sites for the site"""
        return self.properties.get(
            "WebInfos",
            WebInformationCollection(
                self.context, ResourcePath("WebInfos", self.resource_path)
            ),
        )

    @property
    def theme_info(self):
        """Specifies the site theme associated with the site"""
        return self.properties.get(
            "ThemeInfo",
            ThemeInfo(self.context, ResourcePath("ThemeInfo", self.resource_path)),
        )

    @property
    def url(self):
        # type: () -> Optional[str]
        """Gets the absolute URL for the website."""
        return self.properties.get("Url", None)

    @property
    def quick_launch_enabled(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether the Quick Launch area is enabled on the site."""
        return self.properties.get("QuickLaunchEnabled", None)

    @property
    def mega_menu_enabled(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether the Mega menu is enabled on the site."""
        return self.properties.get("MegaMenuEnabled", None)

    @quick_launch_enabled.setter
    def quick_launch_enabled(self, value):
        # type: (bool) -> None
        """Sets a value that specifies whether the Quick Launch area is enabled on the site."""
        self.set_property("QuickLaunchEnabled", value)

    @property
    def site_logo_url(self):
        # type: () -> Optional[str]
        """Gets a value that specifies Site logo url."""
        return self.properties.get("SiteLogoUrl", None)

    @property
    def list_templates(self):
        """Gets a value that specifies the collection of list definitions and list templates available for creating
        lists on the site."""
        return self.properties.get(
            "ListTemplates",
            ListTemplateCollection(
                self.context, ResourcePath("ListTemplates", self.resource_path)
            ),
        )

    @property
    def is_multilingual(self):
        # type: () -> Optional[bool]
        """Gets whether Multilingual UI is turned on for this web or not."""
        return self.properties.get("IsMultilingual", None)

    @is_multilingual.setter
    def is_multilingual(self, val):
        # type: (bool) -> None
        """
        Sets whether Multilingual UI is turned on for this web or not.
        """
        self.set_property("IsMultilingual", val)

    @property
    def multilingual_settings(self):
        """Gets a value that specifies the collection of list definitions and list templates available for creating
        lists on the site."""
        return self.properties.get(
            "MultilingualSettings",
            MultilingualSettings(
                self.context, ResourcePath("MultilingualSettings", self.resource_path)
            ),
        )

    @property
    def web_template(self):
        # type: () -> Optional[str]
        """Gets the name of the site definition or site template that was used to create the site."""
        return self.properties.get("WebTemplate", None)

    @property
    def regional_settings(self):
        """Gets the regional settings that are currently implemented on the website."""
        return self.properties.get(
            "RegionalSettings",
            RegionalSettings(
                self.context, ResourcePath("RegionalSettings", self.resource_path)
            ),
        )

    @property
    def recycle_bin(self):
        """Specifies the collection of Recycle Bin items of the Recycle Bin of the site"""
        return self.properties.get(
            "RecycleBin",
            RecycleBinItemCollection(
                self.context, ResourcePath("RecycleBin", self.resource_path)
            ),
        )

    @property
    def recycle_bin_enabled(self):
        # type: () -> Optional[bool]
        """Specifies whether the Recycle Bin is enabled."""
        return self.properties.get("RecycleBinEnabled", None)

    @property
    def related_hub_site_ids(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("RelatedHubSiteIds", None)

    @property
    def request_access_email(self):
        # type: () -> Optional[str]
        """Gets the e-mail address to which requests for access are sent."""
        return self.properties.get("RequestAccessEmail", None)

    @property
    def save_site_as_template_enabled(self):
        # type: () -> Optional[bool]
        """Specifies if the site (2) can be saved as a site template.

        A feature that creates content which is not compatible for a site template can set this value to false to
        stop users of this site (2) from generating an invalid site template.

        A feature ought to never set this value to true when it is deactivated or at any other time since another
        feature might have created content that is not compatible in a site template.

        Setting this value to false, if it was true, will result in a site template that is not supported.
        """
        return self.properties.get("SaveSiteAsTemplateEnabled", None)

    @property
    def search_box_in_navbar(self):
        # type: () -> Optional[int]
        """Gets the e-mail address to which requests for access are sent."""
        return self.properties.get("SearchBoxInNavBar", None)

    @property
    def search_box_placeholder_text(self):
        # type: () -> Optional[str]
        """Gets the placeholder text in SharePoint online search box for a given (sub) site."""
        return self.properties.get("SearchBoxPlaceholderText", None)

    @property
    def navigation(self):
        """Specifies the navigation structure on the site (2), including the Quick Launch area and the link bar."""
        return self.properties.get(
            "Navigation",
            Navigation(self.context, ResourcePath("Navigation", self.resource_path)),
        )

    @property
    def push_notification_subscribers(self):
        """Specifies the collection of push notification subscribers for the site"""
        return self.properties.get(
            "PushNotificationSubscribers",
            PushNotificationSubscriberCollection(
                self.context,
                ResourcePath("PushNotificationSubscribers", self.resource_path),
            ),
        )

    @property
    def root_folder(self):
        """Get a root folder"""
        return self.properties.get(
            "RootFolder",
            Folder(self.context, ResourcePath("RootFolder", self.resource_path)),
        )

    @property
    def alerts(self):
        # type: () -> AlertCollection
        """Gets the collection of alerts for the site or subsite."""
        return self.properties.get(
            "Alerts",
            AlertCollection(self.context, ResourcePath("Alerts", self.resource_path)),
        )

    @property
    def available_fields(self):
        # type: () -> FieldCollection
        """
        Specifies the collection of all fields available for the current scope, including those of the
        current site, as well as any parent sites.
        """
        return self.properties.get(
            "AvailableFields",
            FieldCollection(
                self.context, ResourcePath("AvailableFields", self.resource_path)
            ),
        )

    @property
    def available_content_types(self):
        # type: () -> ContentTypeCollection
        """
        Specifies the collection of all site content types that apply to the current scope,
        including those of the current site (2), as well as any parent sites.
        """
        return self.properties.get(
            "AvailableContentTypes",
            ContentTypeCollection(
                self.context, ResourcePath("AvailableContentTypes", self.resource_path)
            ),
        )

    @property
    def site_user_info_list(self):
        """
        Specifies the user information list for the site collection that contains the site
        """
        return self.properties.get(
            "SiteUserInfoList",
            List(self.context, ResourcePath("SiteUserInfoList", self.resource_path)),
        )

    @property
    def title(self):
        # type: () -> Optional[str]
        """Gets the title of the web."""
        return self.properties.get("Title", None)

    @property
    def welcome_page(self):
        # type: () -> Optional[str]
        """Specifies the URL of the Welcome page for the site"""
        return self.properties.get("WelcomePage", None)

    @property
    def supported_ui_language_ids(self):
        """Specifies the language code identifiers (LCIDs) of the languages that are enabled for the site."""
        return self.properties.get("SupportedUILanguageIds", ClientValueCollection(int))

    @property
    def ui_version(self):
        # type: () -> Optional[int]
        """Gets or sets the user interface (UI) version of the Web site."""
        return self.properties.get("UIVersion", None)

    @property
    def user_custom_actions(self):
        """Specifies the collection of user custom actions for the site"""
        return self.properties.get(
            "UserCustomActions",
            UserCustomActionCollection(
                self.context, ResourcePath("UserCustomActions", self.resource_path)
            ),
        )

    @property
    def server_relative_path(self):
        """Gets the server-relative Path of the Web."""
        return self.properties.get("ServerRelativePath", SPResPath())

    @property
    def syndication_enabled(self):
        # type: () -> Optional[bool]
        """Specifies whether the [RSS2.0] feeds are enabled on the site"""
        return self.properties.get("SyndicationEnabled", None)

    @property
    def title_resource(self):
        """A UserResource object that represents the title of this web."""
        return self.properties.get(
            "TitleResource",
            UserResource(
                self.context, ResourcePath("TitleResource", self.resource_path)
            ),
        )

    @property
    def treeview_enabled(self):
        # type: () -> Optional[bool]
        """Specifies whether the tree view is enabled on the site"""
        return self.properties.get("TreeViewEnabled", None)

    @property
    def taxonomy_list(self):
        """A special list that stores the mapping between taxonomy term IDs and their corresponding values"""
        return self.lists.get_by_title("TaxonomyHiddenList")

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "AccessRequestsList": self.access_requests_list,
                "ActivityLogger": self.activity_logger,
                "AvailableFields": self.available_fields,
                "AvailableContentTypes": self.available_content_types,
                "AssociatedOwnerGroup": self.associated_owner_group,
                "AssociatedMemberGroup": self.associated_member_group,
                "AssociatedVisitorGroup": self.associated_visitor_group,
                "ContentTypes": self.content_types,
                "ClientWebParts": self.client_web_parts,
                "CurrentUser": self.current_user,
                "DataLeakagePreventionStatusInfo": self.data_leakage_prevention_status_info,
                "DescriptionResource": self.description_resource,
                "DescriptionTranslations": self.description_translations,
                "EffectiveBasePermissions": self.effective_base_permissions,
                "EventReceivers": self.event_receivers,
                "HostedApps": self.hosted_apps,
                "LastItemModifiedDate": self.last_item_modified_date,
                "ListTemplates": self.list_templates,
                "MultilingualSettings": self.multilingual_settings,
                "OneDriveSharedItems": self.onedrive_shared_items,
                "ParentWeb": self.parent_web,
                "PushNotificationSubscribers": self.push_notification_subscribers,
                "RootFolder": self.root_folder,
                "RegionalSettings": self.regional_settings,
                "RoleDefinitions": self.role_definitions,
                "RecycleBin": self.recycle_bin,
                "SiteCollectionAppCatalog": self.site_collection_app_catalog,
                "SiteGroups": self.site_groups,
                "SiteUsers": self.site_users,
                "SiteUserInfoList": self.site_user_info_list,
                "SupportedUILanguageIds": self.supported_ui_language_ids,
                "TenantAppCatalog": self.tenant_app_catalog,
                "TitleResource": self.title_resource,
                "UserCustomActions": self.user_custom_actions,
                "WebInfos": self.web_infos,
                "ThemeInfo": self.theme_info,
            }
            default_value = property_mapping.get(name, None)
        return super(Web, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(Web, self).set_property(name, value, persist_changes)
        if name == "Url":
            self._web_url = value
            self._resource_path.patch(value)
        return self

    @property
    def resource_url(self):
        """Returns Web url"""
        orig_resource_url = super(Web, self).resource_url
        if self._web_url is not None:
            orig_resource_url = orig_resource_url.replace(
                self.context.service_root_url(), self._web_url + "/_api"
            )
        return orig_resource_url
