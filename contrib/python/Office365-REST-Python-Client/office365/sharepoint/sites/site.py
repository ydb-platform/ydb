import datetime
from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.audit.audit import Audit
from office365.sharepoint.changes.collection import ChangeCollection
from office365.sharepoint.changes.query import ChangeQuery
from office365.sharepoint.changes.token import ChangeToken
from office365.sharepoint.compliance.store_proxy import SPPolicyStoreProxy
from office365.sharepoint.compliance.tag import ComplianceTag
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.eventreceivers.definition_collection import (
    EventReceiverDefinitionCollection,
)
from office365.sharepoint.features.collection import FeatureCollection
from office365.sharepoint.lists.list import List
from office365.sharepoint.migration.job_status import SPMigrationJobStatus
from office365.sharepoint.portal.sites.icon_manager import SiteIconManager
from office365.sharepoint.principal.users.user import User
from office365.sharepoint.recyclebin.item_collection import RecycleBinItemCollection
from office365.sharepoint.sitehealth.summary import SiteHealthSummary
from office365.sharepoint.sites.azure_container_Info import (
    ProvisionedTemporaryAzureContainerInfo,
)
from office365.sharepoint.sites.copy_job_progress import CopyJobProgress
from office365.sharepoint.sites.home.site import SPHSite
from office365.sharepoint.sites.html_field_security_setting import (
    HTMLFieldSecuritySetting,
)
from office365.sharepoint.sites.upgrade_info import UpgradeInfo
from office365.sharepoint.sites.usage_info import UsageInfo
from office365.sharepoint.sites.version_policy_manager import SiteVersionPolicyManager
from office365.sharepoint.tenant.administration.sites.administrators_info import (
    SiteAdministratorsInfo,
)
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath
from office365.sharepoint.usercustomactions.collection import UserCustomActionCollection
from office365.sharepoint.webs.template_collection import WebTemplateCollection
from office365.sharepoint.webs.web import Web


class Site(Entity):
    """
    Represents a collection of sites in a Web application, including a top-level website and all its sub sites.

    A set of websites that are in the same content database, have the same owner, and share administration settings.
    A site collection can be identified by a GUID or the URL of the top-level site for the site collection.
    Each site collection contains a top-level site, can contain one or more subsites, and can have a shared
    navigational structure.
    """

    def __init__(self, context, resource_path=None):
        super(Site, self).__init__(context, ResourcePath("Site", resource_path))

    def create_migration_ingestion_job(
        self,
        g_web_id,
        azure_container_source_uri,
        azure_container_manifest_uri,
        azure_queue_report_uri,
        ingestion_task_key,
    ):
        """ """
        return_type = ClientResult(self.context, str())
        payload = {
            "gWebId": g_web_id,
            "azureContainerSourceUri": azure_container_source_uri,
            "azureContainerManifestUri": azure_container_manifest_uri,
            "azureQueueReportUri": azure_queue_report_uri,
            "ingestionTaskKey": ingestion_task_key,
        }
        qry = ServiceOperationQuery(
            self, "CreateMigrationIngestionJob", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def create_migration_job(
        self,
        g_web_id=None,
        azure_container_source_uri=None,
        azure_container_manifest_uri=None,
        azure_queue_report_uri=None,
    ):
        """ """
        return_type = ClientResult(self.context, str())
        payload = {
            "gWebId": g_web_id,
            "azureContainerSourceUri": azure_container_source_uri,
            "azureContainerManifestUri": azure_container_manifest_uri,
            "azureQueueReportUri": azure_queue_report_uri,
        }
        qry = ServiceOperationQuery(
            self, "CreateMigrationJob", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def create_preview_site(self, upgrade=None, sendemail=None):
        """
        Schedules the creation of an evaluation copy of the site collection for the purposes of evaluating an upgrade
        of the site collection to a newer version

        :param bool upgrade: If "true", the evaluation site collection MUST be upgraded when it is created.
            If "false", the evaluation site collection MUST NOT be upgraded when it is created
        :param bool sendemail: If "true", a notification email MUST be sent to the requestor and the site collection
            administrators at the completion of the creation of the evaluation site collection. If "false",
            such notification MUST NOT be sent
        """
        payload = {"upgrade": upgrade, "sendemail": sendemail}
        qry = ServiceOperationQuery(self, "CreatePreviewSPSite", None, payload)
        self.context.add_query(qry)
        return self

    def certify_site(self):
        """"""
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "CertifySite", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def delete_object(self):
        """Deletes a site"""

        def _site_resolved():
            if self.group_id == "00000000-0000-0000-0000-000000000000":
                self.context.site_manager.delete(self.id)
            else:
                self.context.group_site_manager.delete(self.url)

        self.ensure_properties(["Url", "GroupId", "Id"], _site_resolved)
        return self

    def check_is_deletable(self):
        """Check Site Is Deletable"""
        return_type = ClientResult(self.context, bool())

        def _check_is_deletable():
            SPPolicyStoreProxy.check_site_is_deletable_by_id(
                self.context, self.id, return_type
            )

        self.ensure_property("Id", _check_is_deletable)
        return return_type

    def extend_upgrade_reminder_date(self):
        """
        Extend the upgrade reminder date for this site collection, so that site collection administrators will
        not be reminded to run a site collection upgrade before the new date
        """
        qry = ServiceOperationQuery(self, "ExtendUpgradeReminderDate")
        self.context.add_query(qry)
        return self

    @staticmethod
    def from_url(url):
        """
        Initiates and returns a site instance

        :type url: str
        """
        from office365.sharepoint.client_context import ClientContext

        return ClientContext(url).site

    def get_available_tags(self):
        """ """
        return_type = ClientResult(self.context, ClientValueCollection(ComplianceTag))

        def _site_loaded():
            SPPolicyStoreProxy.get_available_tags_for_site(
                self.context, self.url, return_type
            )

        self.ensure_property("Url", _site_loaded)
        return return_type

    def get_block_download_policy_for_files_data(self):
        """ """
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "GetBlockDownloadPolicyForFilesData", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_migration_status(self):
        """"""
        return_type = EntityCollection(self.context, SPMigrationJobStatus)
        qry = ServiceOperationQuery(
            self, "GetMigrationStatus", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_copy_job_progress(self, copy_job_info=None):
        """
        :param copy_job_info: Optional copyJobInfo object.
        """
        payload = {"copyJobInfo": copy_job_info}
        return_type = ClientResult(self.context, CopyJobProgress())
        qry = ServiceOperationQuery(
            self, "GetCopyJobProgress", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_logo(self):
        """Downloads a site logo"""
        return_type = ClientResult(self.context)

        def _site_loaded():
            self.context.site_icon_manager.get_site_logo(
                self.url, return_type=return_type
            )

        self.ensure_property("Url", _site_loaded)
        return return_type

    def get_site_logo_ex(self):
        """Gets site logo image"""
        return_type = ClientResult(self.context)

        def _site_loaded():
            self.context.group_service.get_group_image(
                group_id=self.group_id, return_type=return_type
            )

        self.ensure_property("GroupId", _site_loaded)
        return return_type

    def set_site_logo(self, relative_logo_url):
        """Uploads a site logo

        :param str relative_logo_url:
        """
        site_manager = SiteIconManager(self.context)
        site_manager.set_site_logo(relative_logo_url=relative_logo_url)
        return self

    def is_comm_site(self):
        """Determines whether a site is communication site"""
        return_type = ClientResult(self.context)  # type: ClientResult[bool]

        def _site_loaded():
            SPHSite.is_comm_site(self.context, self.url, return_type)

        self.ensure_property("Url", _site_loaded)
        return return_type

    def is_valid_home_site(self):
        """Determines whether a site is landing site for your intranet."""
        return_type = ClientResult(self.context)  # type: ClientResult[bool]

        def _site_loaded():
            SPHSite.is_valid_home_site(self.context, self.url, return_type)

        self.ensure_property("Url", _site_loaded)
        return return_type

    def set_as_home_site(self):
        """Sets a site as a landing site for your intranet."""
        return_type = ClientResult(self.context)

        def _site_loaded():
            self.result = SPHSite.set_as_home_site(
                self.context, self.url, False, return_type
            )

        self.ensure_property("Url", _site_loaded)
        return return_type

    def get_changes(self, query=None):
        """Returns the collection of all changes from the change log that have occurred within the scope of the site,
        based on the specified query.

        :param office365.sharepoint.changes.query.ChangeQuery query: Specifies which changes to return
        """
        if query is None:
            query = ChangeQuery(site=True, fetch_limit=100)
        return_type = ChangeCollection(self.context)
        payload = {"query": query}
        qry = ServiceOperationQuery(
            self, "getChanges", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_recycle_bin_items(self, row_limit=100, is_ascending=True):
        """
        Returns a collection of recycle bin items based on the specified query.

        :param int row_limit: The maximum number of Recycle Bin items to retrieve.
        :param bool is_ascending: Specifies whether the Recycle Bin items are sorted in ascending order by the column
            specified in the orderBy parameter. A value of true indicates ascending order, and a value of false
            indicates descending order.
        """
        return_type = RecycleBinItemCollection(
            self.context, self.recycle_bin.resource_path
        )
        payload = {"rowLimit": row_limit, "isAscending": is_ascending}
        qry = ServiceOperationQuery(
            self, "GetRecycleBinItems", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_administrators(self):
        """Gets site collection administrators"""
        return_type = ClientResult(
            self.context, ClientValueCollection(SiteAdministratorsInfo)
        )

        def _site_loaded():
            self.context.tenant.get_site_administrators(self.id, return_type)

        self.ensure_property("Id", _site_loaded)
        return return_type

    def get_web_path(self, site_id, web_id):
        """
        :param int site_id: The site identifier
        :param int web_id: The web identifier
        """
        params = {"siteId": site_id, "webId": web_id}
        return_type = ClientResult(self.context, SPResPath())

        qry = ServiceOperationQuery(self, "GetWebPath", params, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_web_templates(self, lcid=1033, override_compat_level=0):
        """
        Returns the collection of site definitions that are available for creating
            Web sites within the site collection.

        :param int lcid: A 32-bit unsigned integer that specifies the language of the site definitions that are
            returned from the site collection.
        :param int override_compat_level: Specifies the compatibility level of the site (2)
            to return from the site collection. If this value is 0, the compatibility level of the site (2) is used.
        :return:
        """
        params = {"LCID": lcid, "overrideCompatLevel": override_compat_level}
        return_type = WebTemplateCollection(
            self.context,
            ServiceOperationPath("GetWebTemplates", params, self.resource_path),
        )

        qry = ServiceOperationQuery(
            self, "GetWebTemplates", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def invalidate(self):
        """
        Invalidates cached upgrade information about the site collection so that this information will be
        recomputed the next time it is needed
        """
        qry = ServiceOperationQuery(self, "Invalidate")
        self.context.add_query(qry)
        return self

    def needs_upgrade_by_type(self, version_upgrade, recursive):
        """
        Returns "true" if this site collection requires site collection upgrade of the specified type;
        otherwise, "false"

        :param bool version_upgrade: If "true", version-to-version site collection upgrade is requested;
            otherwise "false" for build-to-build site collection upgrade.
        :param bool recursive: If "true", child upgradable objects will be inspected; otherwise "false".
        """
        return_type = ClientResult(self.context)
        payload = {
            "versionUpgrade": version_upgrade,
            "recursive": recursive,
        }
        qry = ServiceOperationQuery(
            self, "NeedsUpgradeByType", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def join_hub_site(
        self, hub_site_id, approval_token=None, approval_correlation_id=None
    ):
        """
        Associates a site with an existing hub site.

        :param str hub_site_id: ID of the hub site to join.
        :param str approval_token:
        :param str approval_correlation_id:
        """
        payload = {
            "hubSiteId": hub_site_id,
            "approvalToken": approval_token,
            "approvalCorrelationId": approval_correlation_id,
        }
        qry = ServiceOperationQuery(self, "JoinHubSite", None, payload)
        self.context.add_query(qry)
        return self

    @staticmethod
    def get_url_by_id(context, site_id, stop_redirect=False):
        """Gets Site Url By Id

        :type context: office365.sharepoint.client_context.ClientContext
        :type site_id: str
        :type stop_redirect: bool
        """
        return_type = ClientResult(context, str())
        payload = {"id": site_id, "stopRedirect": stop_redirect}
        qry = ServiceOperationQuery(
            context.site, "GetUrlById", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_url_by_id_for_web(context, site_id, stop_redirect, web_id):
        """Gets Site Url By Id

        :type context: office365.sharepoint.client_context.ClientContext
        :type site_id: str
        :type stop_redirect: bool
        :type web_id: str
        """
        return_type = ClientResult(context)
        payload = {"id": site_id, "stopRedirect": stop_redirect, "webId": web_id}
        qry = ServiceOperationQuery(
            context.site, "GetUrlByIdForWeb", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def exists(context, url):
        """Determine whether site exists

        :type context: office365.sharepoint.client_context.ClientContext
        :param str url: The absolute url of a site.
        """
        return_type = ClientResult(context)  # type: ClientResult[bool]
        payload = {"url": url}
        qry = ServiceOperationQuery(
            context.site, "Exists", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    def is_deletable(self):
        """"""
        return_type = ClientResult(self.context, bool())

        def _is_site_deletable():
            SPPolicyStoreProxy.is_site_deletable(self.context, self.url, return_type)

        self.ensure_property("Url", _is_site_deletable)
        return return_type

    def get_catalog(self, type_catalog):
        """
        Specifies the list template gallery, site template gallery, Web Part gallery, master page gallery,
        or other galleries from the site collection, including custom galleries that are defined by users.

        :type type_catalog: int
        """
        return List(
            self.context,
            ServiceOperationPath("getCatalog", [type_catalog], self.resource_path),
        )

    def open_web(self, str_url):
        """Returns the specified Web site from the site collection.

        :param str str_url: A string that contains either the server-relative or site-relative URL of the
        Web site or of an object within the Web site. A server-relative URL begins with a forward slash ("/"),
        while a site-relative URL does not begin with a forward slash.
        """
        return_type = Web(self.context)
        qry = ServiceOperationQuery(
            self, "OpenWeb", {"strUrl": str_url}, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def open_web_by_id(self, web_id):
        """Returns the specified Web site from the site collection.

        :param str web_id: An identifier of the Web site
        """
        return_type = Web(self.context)
        qry = ServiceOperationQuery(
            self, "OpenWebById", {"gWebId": web_id}, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def provision_temporary_azure_container(self):
        """"""
        return_type = ClientResult(
            self.context, ProvisionedTemporaryAzureContainerInfo()
        )
        qry = ServiceOperationQuery(
            self, "ProvisionTemporaryAzureContainer", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def register_hub_site(self, create_info=None):
        """Registers an existing site as a hub site.

        :type create_info: HubSiteCreationInformation
        """
        qry = ServiceOperationQuery(
            self, "RegisterHubSite", None, create_info, "creationInformation", None
        )
        self.context.add_query(qry)
        return self

    def run_health_check(self, rule_id=None, repair=None, run_always=None):
        """
        Runs a health check as follows. (The health rules referenced below perform an implementation-dependent check
        on the health of a site collection.)

        :param str rule_id: Specifies the rule or rules to be run. If the value is an empty GUID, all rules are run,
             otherwise only the specified rule is run.
        :param bool repair: Specifies whether repairable rules are to be run in repair mode.
        :param bool run_always: Specifies whether the rules will be run as a result of this call or cached results
             from a previous run can be returned.
        """
        payload = {"ruleId": rule_id, "bRepair": repair, "bRunAlways": run_always}
        return_type = SiteHealthSummary(self.context)
        qry = ServiceOperationQuery(
            self, "RunHealthCheck", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def unregister_hub_site(self):
        """Disables the hub site feature on a site."""
        qry = ServiceOperationQuery(self, "UnRegisterHubSite")
        self.context.add_query(qry)
        return self

    def update_client_object_model_use_remote_apis_permission_setting(
        self, require_use_remote_apis
    ):
        """
        Sets whether the client-side object model (CSOM) requests that are made in the context of any site inside
        the site collection require UseRemoteAPIs permission.

        :param bool require_use_remote_apis: Specifies whether the client-side object model (CSOM) requests that are
            made in the context of any site inside the site collection require UseRemoteAPIs permission
        """
        payload = {"requireUseRemoteAPIs": require_use_remote_apis}
        qry = ServiceOperationQuery(
            self, "UpdateClientObjectModelUseRemoteAPIsPermissionSetting", None, payload
        )
        self.context.add_query(qry)
        return self

    @property
    def allow_create_declarative_workflow(self):
        # type: () -> Optional[bool]
        """Specifies whether a designer can be used to create declarative workflows on this site collection"""
        return self.properties.get("AllowCreateDeclarativeWorkflow", None)

    @property
    def allow_designer(self):
        # type: () -> Optional[bool]
        """
        Specifies whether a designer can be used on this site collection.
        See Microsoft.SharePoint.Client.Web.AllowDesignerForCurrentUser, which is the scalar property used
        to determine the behavior for the current user. The default, if not disabled on the Web application, is "true".
        """
        return self.properties.get("AllowDesigner", None)

    @property
    def allowed_external_domains(self):
        # type: () -> Optional[bool]
        return self.properties.get("AllowedExternalDomains", HTMLFieldSecuritySetting())

    @property
    def allow_master_page_editing(self):
        # type: () -> Optional[bool]
        """
        Specifies whether master page editing is allowed on this site collection.
        See Web.AllowMasterPageEditingForCurrentUser, which is the scalar property used to
        determine the behavior for the current user. The default, if not disabled on the Web application, is "false".
        """
        return self.properties.get("AllowMasterPageEditing", None)

    @property
    def allow_revert_from_template(self):
        # type: () -> Optional[bool]
        """
        Specifies whether this site collection can be reverted to its base template.
        See Web.AllowRevertFromTemplateForCurrentUser, which is the scalar property used to determine the behavior
        for the current user. The default, if not disabled on the Web application, is "false".
        """
        return self.properties.get("AllowRevertFromTemplate", None)

    @property
    def audit(self):
        """Enables auditing of how site collection is accessed, changed, and used."""
        return self.properties.get(
            "Audit", Audit(self.context, ResourcePath("Audit", self.resource_path))
        )

    @property
    def can_upgrade(self):
        # type: () -> Optional[bool]
        """
        Specifies whether this site collection is in an implementation-specific valid state for site collection upgrade,
         "true" if it is; otherwise, "false".
        """
        return self.properties.get("CanUpgrade", None)

    @property
    def channel_group_id(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("ChannelGroupId", None)

    @property
    def classification(self):
        # type: () -> Optional[str]
        """Gets the classification of this site."""
        return self.properties.get("Classification", None)

    @property
    def compatibility_level(self):
        # type: () -> Optional[str]
        """
        Specifies the compatibility level of the site collection for the purpose of major version level compatibility
        checks
        """
        return self.properties.get("CompatibilityLevel", None)

    @property
    def comments_on_site_pages_disabled(self):
        # type: () -> Optional[bool]
        """Indicates whether comments on site pages are disabled or not."""
        return self.properties.get("CommentsOnSitePagesDisabled", None)

    @property
    def current_change_token(self):
        """Gets the current change token that is used in the change log for the site collection."""
        return self.properties.get("CurrentChangeToken", ChangeToken())

    @property
    def disable_flows(self):
        # type: () -> Optional[bool]
        """"""
        return self.properties.get("DisableFlows", None)

    @property
    def external_sharing_tips_enabled(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that specifies whether users will be greeted with a notification bar telling them that
        the site can be shared with external users. The value is true if the notification bar is enabled; otherwise,
        it is false."""
        return self.properties.get("ExternalSharingTipsEnabled", None)

    @property
    def external_user_expiration_in_days(self):
        # type: () -> Optional[int]
        """"""
        return self.properties.get("ExternalUserExpirationInDays", None)

    @property
    def geo_location(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("GeoLocation", None)

    @property
    def group_id(self):
        # type: () -> Optional[str]
        """Group identifier"""
        return self.properties.get("GroupId", None)

    @property
    def lock_issue(self):
        # type: () -> Optional[str]
        """Specifies the comment that is used when a site collection is locked"""
        return self.properties.get("LockIssue", None)

    @property
    def root_web(self):
        # type: () -> Web
        """Get root web"""
        return self.properties.get(
            "RootWeb", Web(self.context, ResourcePath("RootWeb", self.resource_path))
        )

    @property
    def owner(self):
        # type: () -> User
        """Gets or sets the owner of the site collection. (Read-only in sandboxed solutions.)"""
        return self.properties.get(
            "Owner", User(self.context, ResourcePath("Owner", self.resource_path))
        )

    @property
    def read_only(self):
        # type: () -> Optional[bool]
        """
        Gets a Boolean value that specifies whether the site collection is read-only,
        locked, and unavailable for write access.
        """
        return self.properties.get("ReadOnly", None)

    @property
    def required_designer_version(self):
        # type: () -> Optional[str]
        """
        Specifies the required minimum version of the designer that can be used on this site collection.
        The default, if not disabled on the Web application, is "15.0.0.0".
        """
        return self.properties.get("RequiredDesignerVersion", None)

    @property
    def url(self):
        # type: () -> Optional[str]
        """Specifies the full URL of the site (2), including host name, port number and path"""
        return self.properties.get("Url", None)

    @property
    def server_relative_url(self):
        # type: () -> Optional[str]
        """Specifies the server-relative URL of the top-level site in the site collection."""
        return self.properties.get("ServerRelativeUrl", None)

    @property
    def share_by_email_enabled(self):
        # type: () -> Optional[bool]
        """
        When true, users will be able to grant permissions to guests for resources within the site collection.
        """
        return self.properties.get("ShareByEmailEnabled", None)

    @property
    def status_bar_text(self):
        # type: () -> Optional[str]
        """Gets or sets the status bar message text for this site."""
        return self.properties.get("StatusBarText", None)

    @property
    def trim_audit_log(self):
        # type: () -> Optional[bool]
        """When this flag is set for the site, the audit events are trimmed periodically."""
        return self.properties.get("TrimAuditLog", None)

    @property
    def write_locked(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("WriteLocked", None)

    @property
    def id(self):
        # type: () -> Optional[str]
        """Specifies the GUID that identifies the site collection."""
        return self.properties.get("Id", None)

    @property
    def hub_site_id(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("HubSiteId", None)

    @property
    def is_hub_site(self):
        # type: () -> Optional[bool]
        """Returns whether the specified site is a hub site"""
        return self.properties.get("IsHubSite", None)

    @property
    def server_relative_path(self):
        # type: () -> Optional[SPResPath]
        """Gets the server-relative Path of the Site."""
        return self.properties.get("ServerRelativePath", SPResPath())

    @property
    def status_bar_link(self):
        # type: () -> Optional[str]
        """Gets the status bar message link target for this site."""
        return self.properties.get("StatusBarLink", None)

    @property
    def secondary_contact(self):
        """Gets or sets the secondary contact that is used for the site collection."""
        return self.properties.get(
            "SecondaryContact",
            User(self.context, ResourcePath("SecondaryContact", self.resource_path)),
        )

    @property
    def recycle_bin(self):
        """Get recycle bin"""
        return self.properties.get(
            "RecycleBin",
            RecycleBinItemCollection(
                self.context, ResourcePath("RecycleBin", self.resource_path)
            ),
        )

    @property
    def features(self):
        # type: () -> FeatureCollection
        """Get features"""
        return self.properties.get(
            "Features",
            FeatureCollection(
                self.context, ResourcePath("Features", self.resource_path), self
            ),
        )

    @property
    def max_items_per_throttled_operation(self):
        # type: () -> Optional[int]
        """
        Specifies the maximum number of list items allowed to be returned for each retrieve request before throttling
        occurs. If throttling occurs, list items MUST NOT be returned.
        """
        return self.properties.get("MaxItemsPerThrottledOperation", None)

    @property
    def needs_b2b_upgrade(self):
        # type: () -> Optional[bool]
        """Specifies whether the site needs a Build-to-Build upgrade."""
        return self.properties.get("NeedsB2BUpgrade", None)

    @property
    def event_receivers(self):
        """
        Provides event receivers for events that occur at the scope of the site collection.
        """
        return self.properties.get(
            "EventReceivers",
            EventReceiverDefinitionCollection(
                self.context, ResourcePath("eventReceivers", self.resource_path), self
            ),
        )

    @property
    def show_url_structure(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the URL structure of this site collection is viewable.
        See Web.ShowURLStructureForCurrentUser, which is the scalar property used to determine the behavior for the
        current user. The default, if not disabled on the Web application, is "false".
        """
        return self.properties.get("ShowUrlStructure", None)

    @property
    def ui_version_configuration_enabled(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the visual upgrade UI for this site collection is displayed.
        """
        return self.properties.get("UIVersionConfigurationEnabled", None)

    @property
    def usage_info(self):
        """Provides fields used to access information regarding site collection usage."""
        return self.properties.get("UsageInfo", UsageInfo())

    @property
    def upgrade_info(self):
        """Specifies the upgrade information of this site collection."""
        return self.properties.get("UpgradeInfo", UpgradeInfo())

    @property
    def upgrade_reminder_date(self):
        """
        Specifies a date, after which site collection administrators will be reminded to upgrade the site collection.
        """
        return self.properties.get("UpgradeReminderDate", datetime.datetime.min)

    @property
    def upgrade_scheduled(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the upgrade has been scheduled. It can only be set to false by a farm administrator.
        To set it to true, set the UpgradeScheduledDate to a future time.
        """
        return self.properties.get("UpgradeScheduled", None)

    @property
    def upgrade_scheduled_date(self):
        # type: () -> Optional[datetime.datetime]
        """
        Specifies the upgrade scheduled date in UTC (Coordinated Universal Time). Only the Date part is used.
        If UpgradeScheduled is false, returns SqlDateTime.MinValue.
        """
        return self.properties.get("UpgradeScheduledDate", datetime.datetime.min)

    @property
    def upgrading(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the user will be able to share links to the documents that can be accessed without signing in.
        """
        return self.properties.get("Upgrading", None)

    @property
    def user_custom_actions(self):
        """Gets the User Custom Actions that are associated with the site."""
        return self.properties.get(
            "UserCustomActions",
            UserCustomActionCollection(
                self.context, ResourcePath("UserCustomActions", self.resource_path)
            ),
        )

    @property
    def version_policy_for_new_libraries_template(self):
        """"""
        return self.properties.get(
            "VersionPolicyForNewLibrariesTemplate",
            SiteVersionPolicyManager(
                self.context,
                ResourcePath(
                    "VersionPolicyForNewLibrariesTemplate", self.resource_path
                ),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "CurrentChangeToken": self.current_change_token,
                "EventReceivers": self.event_receivers,
                "RecycleBin": self.recycle_bin,
                "RootWeb": self.root_web,
                "SecondaryContact": self.secondary_contact,
                "UsageInfo": self.usage_info,
                "UpgradeInfo": self.upgrade_info,
                "UpgradeReminderDate": self.upgrade_reminder_date,
                "UpgradeScheduledDate": self.upgrade_scheduled_date,
                "UserCustomActions": self.user_custom_actions,
                "VersionPolicyForNewLibrariesTemplate": self.version_policy_for_new_libraries_template,
            }
            default_value = property_mapping.get(name, None)
        return super(Site, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        if name == "__siteUrl":
            super(Site, self).set_property("Url", value, False)
            self._context = self.context.clone(value)
        else:
            super(Site, self).set_property(name, value, persist_changes)
        return self
