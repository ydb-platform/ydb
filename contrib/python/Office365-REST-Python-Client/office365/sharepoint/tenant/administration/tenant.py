import time
from typing import AnyStr, Optional

from typing_extensions import Self

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.gtp.request_options import ChatGptRequestOptions
from office365.sharepoint.listitems.collection import ListItemCollection
from office365.sharepoint.listitems.listitem import ListItem
from office365.sharepoint.lists.render_data_parameters import RenderListDataParameters
from office365.sharepoint.lists.render_override_parameters import (
    RenderListDataOverrideParameters,
)
from office365.sharepoint.publishing.portal_health_status import PortalHealthStatus
from office365.sharepoint.sites.home.details import HomeSitesDetails
from office365.sharepoint.sites.site import Site
from office365.sharepoint.tenant.administration.collaboration.insights_data import (
    CollaborationInsightsData,
)
from office365.sharepoint.tenant.administration.collaboration.insights_overview import (
    CollaborationInsightsOverview,
)
from office365.sharepoint.tenant.administration.hubsites.properties import (
    HubSiteProperties,
)
from office365.sharepoint.tenant.administration.insights.onedrive_site_sharing import (
    OneDriveSiteSharingInsights,
)
from office365.sharepoint.tenant.administration.insights.top_files_sharing import (
    TopFilesSharingInsights,
)
from office365.sharepoint.tenant.administration.policies.app_billing_properties import (
    SPOAppBillingProperties,
)
from office365.sharepoint.tenant.administration.policies.content_security_configuration import (
    SPOContentSecurityPolicyConfiguration,
)
from office365.sharepoint.tenant.administration.policies.definition import (
    TenantAdminPolicyDefinition,
)
from office365.sharepoint.tenant.administration.policies.file_version_types import (
    SPOFileVersionBatchDeleteJobProgress,
    SPOFileVersionPolicySettings,
)
from office365.sharepoint.tenant.administration.powerapps.environment import (
    PowerAppsEnvironment,
)
from office365.sharepoint.tenant.administration.recent_admin_action_report import (
    RecentAdminActionReport,
)
from office365.sharepoint.tenant.administration.recent_admin_action_report_payload import (
    RecentAdminActionReportPayload,
)
from office365.sharepoint.tenant.administration.secondary_administrators_fields_data import (
    SecondaryAdministratorsFieldsData,
)
from office365.sharepoint.tenant.administration.secondary_administrators_info import (
    SecondaryAdministratorsInfo,
)
from office365.sharepoint.tenant.administration.siteinfo_for_site_picker import (
    SiteInfoForSitePicker,
)
from office365.sharepoint.tenant.administration.sites.administrators_info import (
    SiteAdministratorsInfo,
)
from office365.sharepoint.tenant.administration.sites.creation_properties import (
    SiteCreationProperties,
)
from office365.sharepoint.tenant.administration.sites.properties import SiteProperties
from office365.sharepoint.tenant.administration.sites.properties_collection import (
    SitePropertiesCollection,
)
from office365.sharepoint.tenant.administration.sites.properties_enumerable_filter import (
    SitePropertiesEnumerableFilter,
)
from office365.sharepoint.tenant.administration.spo_operation import SpoOperation
from office365.sharepoint.tenant.administration.syntex.billing_context import (
    SyntexBillingContext,
)
from office365.sharepoint.tenant.administration.types import CreatePolicyRequest
from office365.sharepoint.tenant.administration.webs.templates.collection import (
    SPOTenantWebTemplateCollection,
)
from office365.sharepoint.tenant.settings import TenantSettings


class Tenant(Entity):
    """Represents a SharePoint tenant."""

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.Online.SharePoint.TenantAdministration.Tenant"
        )
        super(Tenant, self).__init__(context, static_path)

    def accept_syntex_repository_terms_of_service(self):
        """
        Used to accept the Microsoft Syntex repository terms of service for your organization.
        This acceptance is often necessary for enabling features related to document processing or repositories
        in Microsoft Syntex.
        """
        qry = ServiceOperationQuery(self, "AcceptSyntexRepositoryTermsOfService")
        self.context.add_query(qry)
        return self

    def activate_application_billing_policy(self, billing_policy_id):
        """ """
        payload = {"billingPolicyId": billing_policy_id}
        return_type = ClientResult(self.context, SPOAppBillingProperties())
        qry = ServiceOperationQuery(
            self, "ActivateApplicationBillingPolicy", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def add_recent_admin_action_report(self):
        """Logs recent administrative actions within a SharePoint Online tenant"""
        return_type = ClientResult(self.context, RecentAdminActionReport())
        payload = {"payload": RecentAdminActionReportPayload()}
        qry = ServiceOperationQuery(
            self, "AddRecentAdminActionReport", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_corporate_catalog_site(self):
        """Retrieves Corporate Catalog Site"""
        settings = TenantSettings.current(self.context)
        return_type = Site(self.context)

        def _settings_loaded():
            return_type.set_property("__siteUrl", settings.corporate_catalog_url)

        settings.ensure_property("CorporateCatalogUrl", _settings_loaded)
        return return_type

    def get_chat_gpt_response(self):
        """"""
        return_type = ClientResult(self.context)
        payload = {"requestOptions": ChatGptRequestOptions()}
        qry = ServiceOperationQuery(
            self, "GetChatGptResponse", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def delete_policy_definition(self, item_id):
        """
        Deletes a policy definition from a Microsoft 365 tenant.
        Policy definitions may refer to specific settings related to compliance, security,
        or site governance (such as site or group creation policies).
        :param int item_id:
        """
        qry = ServiceOperationQuery(
            self, "DeletePolicyDefinition", None, {"itemId": item_id}
        )
        self.context.add_query(qry)
        return self

    def delete_recent_admin_action_report(self, report_id):
        """
        :param int report_id:
        """
        qry = ServiceOperationQuery(
            self, "DeleteRecentAdminActionReport", None, {"reportId": report_id}
        )
        self.context.add_query(qry)
        return self

    def get_spo_tenant_all_web_templates(self):
        """ """
        return_type = SPOTenantWebTemplateCollection(self.context)
        qry = ServiceOperationQuery(
            self, "GetSPOTenantAllWebTemplates", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_onedrive_site_sharing_insights(self, query_mode):
        """Retrieves insights or reports related to OneDrive for Business site sharing activities"""
        return_type = ClientResult(self.context, OneDriveSiteSharingInsights())
        payload = {"queryMode": query_mode}
        qry = ServiceOperationQuery(
            self, "GetOneDriveSiteSharingInsights", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_collaboration_insights_data(self):
        """"""
        return_type = ClientResult[CollaborationInsightsData](
            self.context, CollaborationInsightsData()
        )

        qry = ServiceOperationQuery(
            self, "GetCollaborationInsightsData", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_collaboration_insights_overview(self):
        """"""
        return_type = ClientResult[CollaborationInsightsData](
            self.context, CollaborationInsightsOverview()
        )

        qry = ServiceOperationQuery(
            self, "GetCollaborationInsightsOverview", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def render_recent_admin_actions(self):
        """ """
        return_type = ClientResult(self.context)
        payload = {
            "parameters": RenderListDataParameters(),
            "overrideParameters": RenderListDataOverrideParameters(),
        }
        qry = ServiceOperationQuery(
            self, "RenderRecentAdminActions", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_content_security_policy(self):
        """"""
        return_type = SPOContentSecurityPolicyConfiguration(self.context)
        qry = ServiceOperationQuery(
            self, "GetContentSecurityPolicy", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_spo_app_billing_policies(self):
        """ """
        return_type = ClientResult(
            self.context, ClientValueCollection(SPOAppBillingProperties)
        )
        qry = ServiceOperationQuery(
            self, "GetSPOAppBillingPolicies", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_top_files_sharing_insights(self, query_mode=None):
        """
        Retrieves a report or data about the most shared files within a SharePoint Online tenant
        :param int query_mode:
        """
        payload = {"queryMode": query_mode}
        return_type = EntityCollection(self.context, TopFilesSharingInsights)
        qry = ServiceOperationQuery(
            self, "GetTopFilesSharingInsights", payload, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_thumbnail_logo(self, site_url):
        # type: (str) -> ClientResult[AnyStr]
        """
        :param str site_url:
        """
        payload = {"siteUrl": site_url}
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetSiteThumbnailLogo", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_home_site_url(self):
        # type: () -> ClientResult[str]
        """ """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetSPHSiteUrl", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_home_sites(self):
        # type: () -> ClientResult[ClientValueCollection[HomeSitesDetails]]
        """Retrieves the Home Site that has been designated for your Microsoft 365 tenant."""
        return_type = ClientResult(
            self.context,
            ClientValueCollection(HomeSitesDetails),  # pylint: disable=E1120
        )
        qry = ServiceOperationQuery(self, "GetHomeSites", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_home_sites_details(self):
        """Retrieves detailed information about the Home Sites configured in a SharePoint Online tenant."""
        return_type = ClientResult(
            self.context, ClientValueCollection(HomeSitesDetails)
        )
        qry = ServiceOperationQuery(
            self, "GetHomeSitesDetails", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def remove_home_site(self, home_site_url):
        """
        Remove home site

        :param str home_site_url:
        """
        payload = {"homeSiteUrl": home_site_url}
        qry = ServiceOperationQuery(self, "RemoveHomeSite", None, payload)
        self.context.add_query(qry)
        return self

    def has_valid_education_license(self):
        """"""
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "HasValidEducationLicense", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def is_request_content_management_assessment_eligible(self):
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self,
            "IsRequestContentManagementAssessmentEligible",
            None,
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def is_syntex_repository_terms_of_service_accepted(self):
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self,
            "IsSyntexRepositoryTermsOfServiceAccepted",
            None,
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def export_to_csv(
        self, view_xml=None, time_zone_id=None, columns_info=None, list_name=None
    ):
        """
        Exports tenant-level data to a CSV file.
        :param str view_xml:
        :param int time_zone_id:
        :param list columns_info:
        :param str list_name:
        """
        return_type = ClientResult(self.context)
        payload = {
            "viewXml": view_xml,
            "timeZoneId": time_zone_id,
            "columnsInfo": columns_info,
            "listName": list_name,
        }
        qry = ServiceOperationQuery(
            self, "ExportToCSV", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def render_policy_report(self):
        """"""
        return_type = ClientResult(self.context, bytes())
        payload = {
            "parameters": RenderListDataParameters(),
            "overrideParameters": RenderListDataOverrideParameters(),
        }
        qry = ServiceOperationQuery(
            self, "RenderPolicyReport", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def from_url(admin_site_url):
        """
        :type admin_site_url: str
        """
        from office365.sharepoint.client_context import ClientContext

        admin_client = ClientContext(admin_site_url)
        return Tenant(admin_client)

    def get_lock_state_by_id(self, site_id):
        """
        :param str site_id: The GUID to uniquely identify a SharePoint site
        """
        return self.sites.get_lock_state_by_id(site_id)

    def hub_sites(self, site_url):
        pass

    def get_power_apps_environments(self):
        """ """
        return_type = ClientResult(
            self.context,
            ClientValueCollection(PowerAppsEnvironment),
        )
        qry = ServiceOperationQuery(
            self, "GetPowerAppsEnvironments", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_ransomware_activities(self):
        # type: () -> ClientResult[AnyStr]
        """ """
        return_type = ClientResult(self.context)
        payload = {"parameters": RenderListDataParameters()}
        qry = ServiceOperationQuery(
            self, "GetRansomwareActivities", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_ransomware_events_overview(self):
        # type: () -> ClientResult[AnyStr]
        """ """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetRansomwareEventsOverview", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_root_site_url(self):
        """ """
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self,
            "GetRootSiteUrl",
            None,
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_site_authorization_code_for_migration(self, endpoint_url):
        return_type = ClientResult(self.context, str())
        payload = {"endpointUrl": endpoint_url}
        qry = ServiceOperationQuery(
            self,
            "GetSiteAuthorizationCodeForMigration",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_site_subscription_id(self):
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self,
            "GetSiteSubscriptionId",
            None,
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_sp_list_item_count(self, list_name):
        # type: (str) -> ClientResult[int]
        """ """
        return_type = ClientResult(self.context)
        payload = {"listName": list_name}
        qry = ServiceOperationQuery(
            self, "GetSPListItemCount", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_sp_list_root_folder_properties(self, list_name):
        # type: (str) -> ClientResult[dict]
        """ """
        return_type = ClientResult(self.context)
        payload = {"listName": list_name}
        qry = ServiceOperationQuery(
            self, "GetSPListRootFolderProperties", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_spo_all_web_templates(self, culture_name=None, compatibility_level=None):
        # type: (str, int) -> SPOTenantWebTemplateCollection
        """ """
        return_type = SPOTenantWebTemplateCollection(self.context)
        payload = {
            "cultureName": culture_name,
            "compatibilityLevel": compatibility_level,
        }
        qry = ServiceOperationQuery(
            self, "GetSPOAllWebTemplates", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def check_tenant_intune_license(self):
        # type: () -> ClientResult[bool]
        """Checks whether a tenant has the Intune license."""
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "CheckTenantIntuneLicense", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def check_tenant_licenses(self, licenses):
        """
        Checks whether a tenant has the specified licenses.

        :param list[str] licenses: The list of licenses to check for.
        """
        return_type = ClientResult(self.context, bool())
        params = ClientValueCollection(str, licenses)
        qry = ServiceOperationQuery(
            self, "CheckTenantLicenses", None, params, "licenses", return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site(self, site_url):
        # type: (str) -> ListItem
        return self._aggregated_site_collections_list.items.single(
            "SiteUrl eq '{0}'".format(site_url.rstrip("/"))
        ).get()

    def get_sites_by_state(self, states=None):
        """
        :param list[int] states:
        """
        return_type = ListItemCollection(
            self.context,
            ResourcePath("items", self._aggregated_site_collections_list.resource_path),
        )
        payload = {"states": states}
        qry = ServiceOperationQuery(
            self, "GetSitesByState", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def _poll_site_status(self, site_url, polling_interval_secs):
        # type: (str, int) -> None
        states = [0, 1, 2]
        time.sleep(polling_interval_secs)

        def _after(items):
            completed = (
                len(
                    [
                        item
                        for item in items
                        if item.properties.get("SiteUrl") == site_url
                    ]
                )
                > 0
            )
            if not completed:
                self._poll_site_status(site_url, polling_interval_secs)

        self.get_sites_by_state(states).after_execute(_after, execute_first=True)

    def get_site_health_status(self, source_url):
        """
        Checks the sitehealth of a specific SharePoint site or site collection in their tenant
        :type source_url: str
        """
        result = ClientResult(self.context, PortalHealthStatus())
        params = {"sourceUrl": source_url}
        qry = ServiceOperationQuery(
            self, "GetSiteHealthStatus", None, params, None, result
        )
        self.context.add_query(qry)
        return result

    def get_site_administrators(self, site_id, return_type=None):
        """
        Gets site collection administrators

        :type site_id: str
        :type return_type: ClientResult
        """
        if return_type is None:
            return_type = ClientResult(
                self.context, ClientValueCollection(SiteAdministratorsInfo)
            )
        payload = {"siteId": site_id}
        qry = ServiceOperationQuery(
            self, "GetSiteAdministrators", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_secondary_administrators(self, site_id):
        # type: (str) -> ClientResult[ClientValueCollection[SecondaryAdministratorsInfo]]
        """
        Gets site collection administrators
        :param str site_id: Site object or identifier
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(SecondaryAdministratorsInfo)
        )
        payload = {
            "secondaryAdministratorsFieldsData": SecondaryAdministratorsFieldsData(
                site_id
            )
        }
        qry = ServiceOperationQuery(
            self, "GetSiteSecondaryAdministrators", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_site_secondary_administrators(self, site_id, emails=None, names=None):
        """
        Sets site collection administrators

        :type names: list[str] or None
        :type emails: list[str]
        :type site_id: str
        """
        payload = {
            "secondaryAdministratorsFieldsData": SecondaryAdministratorsFieldsData(
                site_id, emails, names
            )
        }
        qry = ServiceOperationQuery(
            self, "SetSiteSecondaryAdministrators", None, payload, None, None
        )
        self.context.add_query(qry)
        return self

    def register_hub_site(self, site_url):
        # type: (str) -> HubSiteProperties
        """Registers an existing site as a hub site."""
        return_type = HubSiteProperties(self.context)
        params = {"siteUrl": site_url}
        qry = ServiceOperationQuery(
            self, "RegisterHubSite", None, params, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def unregister_hub_site(self, site_url):
        # type: (str) -> Self
        """Unregisters a hub site so that it is no longer a hub site."""
        payload = {"siteUrl": site_url}
        qry = ServiceOperationQuery(
            self, "UnregisterHubSite", None, payload, None, None
        )
        self.context.add_query(qry)
        return self

    def create_policy_definition(self):
        """ """
        return_type = ClientResult(self.context, TenantAdminPolicyDefinition())
        payload = {"policyInputParameters": CreatePolicyRequest()}
        qry = ServiceOperationQuery(
            self, "CreatePolicyDefinition", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def create_site(self, url, owner, title=None):
        """Queues a site collection for creation with the specified properties.

        :param str title: Sets the new site’s title.
        :param str url: Sets the new site’s URL.
        :param str owner: Sets the login name of the owner of the new site.
        """
        return_type = SpoOperation(self.context)
        payload = {
            "siteCreationProperties": SiteCreationProperties(
                title=title, url=url, owner=owner
            )
        }
        qry = ServiceOperationQuery(
            self, "CreateSite", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def create_site_sync(self, url, owner, title=None):
        """Creates a site collection

        :param str title: Sets the new site’s title.
        :param str url: Sets the new site’s URL.
        :param str owner: Sets the login name of the owner of the new site.
        """
        return_type = Site(self.context)
        return_type.set_property("__siteUrl", url)

        def _ensure_status(op):
            # type: (SpoOperation) -> None
            if not op.is_complete:
                self._poll_site_status(url, op.polling_interval_secs)

        self.create_site(url, owner, title).after_execute(_ensure_status)
        return return_type

    def remove_site(self, site_url):
        """Deletes the site with the specified URL

        :param str site_url: A string representing the URL of the site.
        """
        return_type = SpoOperation(self.context)
        params = {"siteUrl": site_url}
        qry = ServiceOperationQuery(self, "removeSite", None, params, None, return_type)
        self.context.add_query(qry)
        return return_type

    def remove_deleted_site(self, site_url):
        """Permanently removes the specified deleted site from the recycle bin.

        :param str site_url: A string representing the URL of the site.
        """
        result = SpoOperation(self.context)
        qry = ServiceOperationQuery(
            self, "RemoveDeletedSite", [site_url], None, None, result
        )
        self.context.add_query(qry)
        return result

    def reorder_home_sites(self, home_sites_site_ids):
        """
        Reorders Home Sites within a SharePoint Online tenant
        :param list[str] home_sites_site_ids:
        """
        payload = {"homeSitesSiteIds": home_sites_site_ids}
        return_type = ClientResult(
            self.context, ClientValueCollection(HomeSitesDetails)
        )
        qry = ServiceOperationQuery(
            self, "ReorderHomeSites", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def restore_deleted_site(self, site_url):
        """Restores deleted site with the specified URL
        :param str site_url: A string representing the URL of the site.
        """
        return_type = SpoOperation(self.context)
        qry = ServiceOperationQuery(
            self, "RestoreDeletedSite", [site_url], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def restore_deleted_site_by_id(self, site_id):
        """Restores deleted site with the specified URL
        :param str site_id: A string representing the site identifier.
        """
        return_type = SpoOperation(self.context)
        qry = ServiceOperationQuery(
            self, "RestoreDeletedSiteById", [site_id], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_file_version_policy_for_library(self, site_url, list_params=None):
        """ """
        return_type = ClientResult(self.context, SPOFileVersionPolicySettings())
        payload = {"siteUrl": site_url, "listParams": list_params}
        qry = ServiceOperationQuery(
            self,
            "GetFileVersionPolicyForLibrary",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_file_version_batch_delete_job_progress_for_library(
        self, site_url, list_params
    ):
        """Gets the progress of the file version batch delete job for the specified site and list parameters."""
        return_type = ClientResult(self.context, SPOFileVersionBatchDeleteJobProgress())
        payload = {"siteUrl": site_url, "listParams": list_params}
        qry = ServiceOperationQuery(
            self,
            "GetFileVersionBatchDeleteJobProgressForLibrary",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_site_properties_by_site_id(self, site_id, include_detail=False):
        # type: (str, bool) -> SiteProperties
        """Gets the site properties for the specified site ID.

        :param str site_id: A string that represents the site identifier.
        :param bool include_detail: A Boolean value that indicates whether to include all of the SPSite properties.
        """
        return_type = SiteProperties(self.context)
        return_type.set_property("siteId", site_id, False)
        self.sites.add_child(return_type)
        payload = {"siteId": site_id, "includeDetail": include_detail}
        qry = ServiceOperationQuery(
            self, "GetSitePropertiesBySiteId", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_properties_by_url(self, url, include_detail=False):
        # type: (str, bool) -> SiteProperties
        """
         Gets the site properties for the specified URL.

        :param str url: A string that represents the site URL.
        :param bool include_detail: A Boolean value that indicates whether to include all of the SPSite properties.
        """
        return_type = SiteProperties(self.context)
        return_type.set_property("Url", url, False)
        self.sites.add_child(return_type)
        payload = {"url": url, "includeDetail": include_detail}
        qry = ServiceOperationQuery(
            self, "getSitePropertiesByUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_properties_from_sharepoint_by_filters(
        self, _filter=None, start_index=None, include_detail=False
    ):
        # type: (str, str, bool) -> SitePropertiesCollection
        """ """
        return_type = SitePropertiesCollection(self.context)
        payload = {
            "speFilter": SitePropertiesEnumerableFilter(
                _filter, start_index, include_detail
            )
        }
        qry = ServiceOperationQuery(
            self,
            "getSitePropertiesFromSharePointByFilters",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def get_billing_policy_id_for_app(self, application_id):
        """ """
        return_type = ClientResult(self.context)
        payload = {"applicationId": application_id}
        qry = ServiceOperationQuery(
            self, "GetBillingPolicyIdForApp", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def connect_site_to_hub_site_by_id(self, site_url, hub_site_id):
        # type: (str, str) -> Self
        """Connects Site to Hub Site

        :param str site_url:
        :param str hub_site_id:
        """
        params = {"siteUrl": site_url, "hubSiteId": hub_site_id}
        qry = ServiceOperationQuery(
            self, "ConnectSiteToHubSiteById", None, params, None, None
        )
        self.context.add_query(qry)
        return self

    def ensure_brand_center_feature(self):
        """Ensures that the Brand Center feature is enabled"""
        qry = ServiceOperationQuery(self, "EnsureBrandCenterFeature")
        self.context.add_query(qry)
        return self

    def export_unlicensed_one_drive_for_business_list_to_csv(self):
        """Exports a list of OneDrive for Business sites that are associated with unlicensed users into a CSV file"""
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "ExportUnlicensedOneDriveForBusinessListToCSV"
        )
        self.context.add_query(qry)
        return return_type

    def send_email(self, site_url, activity_event_json):
        # type: (str, str) -> ClientResult[bool]
        """Send Email"""
        return_type = ClientResult(self.context, bool())
        payload = {"siteUrl": site_url, "activityEventJson": activity_event_json}
        qry = ServiceOperationQuery(self, "SendEmail", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def set_copilot_promo_optin_status(self, copilot_promo_optin_enabled):
        # type: (bool) -> ClientResult[bool]
        """ """
        payload = {"copilotPromoOptInEnabled": copilot_promo_optin_enabled}
        qry = ServiceOperationQuery(self, "SetCopilotPromoOptInStatus", None, payload)
        self.context.add_query(qry)
        return self

    def set_default_view(self, view_id, list_name):
        # type: (str, str) -> Self
        """ """
        payload = {"viewId": view_id, "listName": list_name}
        qry = ServiceOperationQuery(self, "SetDefaultView", None, payload)
        self.context.add_query(qry)
        return self

    def get_file_version_policy(self):
        return_type = ClientResult(self.context, str())

        def _get_file_version_policy():
            return_type.set_property("__value", self.file_version_policy_xml)

        self.ensure_property("FileVersionPolicyXml", _get_file_version_policy)
        return return_type

    def set_file_version_policy(
        self, is_auto_trim_enabled, major_version_limit, expire_versions_after_days
    ):
        """
        Automatically delete older versions of documents after a specified number of days.
        Specify the maximum number of major versions to retain and the number of major versions
        for which all minor versions will be kept.

        :param bool is_auto_trim_enabled: If true, versions are trimmed automatically.
        :param int major_version_limit: Retains only many major versions.
        :param int expire_versions_after_days: Deletes versions older than this many days.
        """
        payload = {
            "isAutoTrimEnabled": is_auto_trim_enabled,
            "majorVersionLimit": major_version_limit,
            "expireVersionsAfterDays": expire_versions_after_days,
        }
        qry = ServiceOperationQuery(self, "SetFileVersionPolicy", None, payload)
        self.context.add_query(qry)
        return self

    def clear_file_version_policy(self):
        self.set_property("FileVersionPolicyXml", "")
        self.update()
        return self

    @property
    def app_service_principal(self):
        """ """
        from office365.sharepoint.tenant.administration.internal.appservice.principal import (
            SPOWebAppServicePrincipal,
        )

        return SPOWebAppServicePrincipal(self.context)

    @property
    def admin_settings(self):
        """Manage various tenant-level settings related to SharePoint administration"""
        from office365.sharepoint.tenant.administration.settings_service import (
            TenantAdminSettingsService,
        )

        return TenantAdminSettingsService(self.context)

    @property
    def migration_center(self):
        """ """
        from office365.sharepoint.migrationcenter.service.services import (
            MigrationCenterServices,
        )

        return MigrationCenterServices(self.context)

    @property
    def comms_messages(self):
        """ """

        from office365.sharepoint.tenant.administration.coms.messages_service_proxy import (
            Office365CommsMessagesServiceProxy,
        )

        return Office365CommsMessagesServiceProxy(self.context)

    @property
    def multi_geo(self):
        """ """

        from office365.sharepoint.multigeo.services import MultiGeoServices

        return MultiGeoServices(self.context)

    @property
    def ai_builder_enabled(self):
        # type: () -> Optional[str]
        """Gets the value if the AIBuilder settings should be shown in the tenant"""
        return self.properties.get("AIBuilderEnabled", None)

    @property
    def ai_builder_site_info_list(self):
        """"""
        return self.properties.get(
            "AIBuilderSiteInfoList", ClientValueCollection(SiteInfoForSitePicker)
        )

    @property
    def _aggregated_site_collections_list(self):
        """ """
        return self.context.web.lists.get_by_title(
            "DO_NOT_DELETE_SPLIST_TENANTADMIN_AGGREGATED_SITECOLLECTIONS"
        )

    @property
    def allow_comments_text_on_email_enabled(self):
        # type: () -> Optional[bool]
        """
        When enabled, the email notification that a user receives when is mentioned,
        includes the surrounding document context
        """
        return self.properties.get("AllowCommentsTextOnEmailEnabled", None)

    @property
    def allow_everyone_except_external_users_claim_in_private_site(self):
        # type: () -> Optional[bool]
        """
        Gets the value if EveryoneExceptExternalUsers claim is allowed or not in people picker in a private group site.
        False value means it is blocked
        """
        return self.properties.get(
            "AllowEveryoneExceptExternalUsersClaimInPrivateSite", None
        )

    @property
    def allow_editing(self):
        # type: () -> Optional[bool]
        """
        Prevents users from editing Office files in the browser and copying and pasting Office file contents
        out of the browser window.
        """
        return self.properties.get("AllowEditing", None)

    @property
    def default_content_center_site(self):
        """"""
        return self.properties.get("DefaultContentCenterSite", SiteInfoForSitePicker())

    @property
    def file_version_policy_xml(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("FileVersionPolicyXml", None)

    @property
    def information_barriers_suspension(self):
        # type: () -> Optional[bool]
        """Gets information barriers in SharePoint and OneDrive in your organization"""
        return self.properties.get("InformationBarriersSuspension", None)

    @property
    def ip_address_allow_list(self):
        # type: () -> Optional[str]
        """Configures multiple IP addresses or IP address ranges (IPv4 or IPv6), that are recognized as trusted."""
        return self.properties.get("IPAddressAllowList", None)

    @property
    def ip_address_enforcement(self):
        # type: () -> Optional[bool]
        """Determines whether access from network locations that are defined by an administrator is allowed."""
        return self.properties.get("IPAddressEnforcement", None)

    @ip_address_enforcement.setter
    def ip_address_enforcement(self, value):
        # type: (bool) -> None
        """
        Allows access from network locations that are defined by an administrator.
        Before the IPAddressEnforcement parameter is set, make sure you add a valid IPv4 or IPv6 address to the
        IPAddressAllowList parameter.
        """
        self.set_property("IPAddressEnforcement", value)

    @property
    def no_access_redirect_url(self):
        # type: () -> Optional[bool]
        """Specifies the URL of the redirected site for those site collections which have the locked state "NoAccess"""
        return self.properties.get("NoAccessRedirectUrl", None)

    @property
    def notifications_in_share_point_enabled(self):
        # type: () -> Optional[bool]
        """Enables or disables notifications in SharePoint."""
        return self.properties.get("NotificationsInSharePointEnabled", None)

    @property
    def notify_owners_when_invitations_accepted(self):
        # type: () -> Optional[bool]
        """When this parameter is set to true and when an external user accepts an invitation to a resource
        in a user's OneDrive for Business, the OneDrive for Business owner is notified by e-mail.
        """
        return self.properties.get("NotifyOwnersWhenInvitationsAccepted", None)

    @property
    def one_drive_storage_quota(self):
        # type: () -> Optional[int]
        """Gets a default OneDrive for Business storage quota for the tenant. It will be used for new OneDrive
        for Business sites created."""
        return self.properties.get("OneDriveStorageQuota", None)

    @property
    def notify_owners_when_items_reshared(self):
        # type: () -> Optional[bool]
        """When is set to true and another user re-shares a document from a
        user's OneDrive for Business, the OneDrive for Business owner is notified by e-mail.
        """
        return self.properties.get("NotifyOwnersWhenItemsReshared", None)

    @property
    def root_site_url(self):
        # type: () -> Optional[str]
        """The tenant's root site url"""
        return self.properties.get("RootSiteUrl", None)

    @property
    def sites(self):
        """Gets a collection of sites."""
        return self.properties.get(
            "sites",
            SitePropertiesCollection(
                self.context, ResourcePath("sites", self.resource_path)
            ),
        )

    @property
    def cdn_api(self):
        """ """
        from office365.sharepoint.tenant.cdn_api import TenantCdnApi

        return TenantCdnApi(self.context)

    @property
    def crawl_versions_info_provider(self):
        """Retrieves information about crawl versions for a tenant in SharePoint"""

        from office365.sharepoint.search.administration.tenant_crawl_versions_info_provider import (
            TenantCrawlVersionsInfoProvider,
        )

        return TenantCrawlVersionsInfoProvider(self.context)

    @property
    def syntex_billing_subscription_settings(self):
        """
        Manages billing and subscription details for Microsoft Syntex in SharePoint Online or Microsoft 365 environments
        """
        return self.properties.get(
            "SyntexBillingSubscriptionSettings", SyntexBillingContext()
        )

    @property
    def admin_endpoints(self):
        """ """
        from office365.sharepoint.tenant.administration.endpoints import (
            TenantAdminEndpoints,
        )

        return TenantAdminEndpoints(self.context)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.Tenant"
