from office365.directory.authentication.methods.root import AuthenticationMethodsRoot
from office365.entity import Entity
from office365.partners.partners import Partners
from office365.reports.internal.queries.create_report_query import create_report_query
from office365.reports.report import Report
from office365.reports.security.root import SecurityReportsRoot
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery


class ReportRoot(Entity):
    """Represents a container for Azure Active Directory (Azure AD) reporting resources."""

    def device_configuration_device_activity(self):
        """
        Metadata for the device configuration device activity report
        """
        return_type = ClientResult(self.context, Report())
        qry = FunctionQuery(
            self, "deviceConfigurationDeviceActivity", None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def device_configuration_user_activity(self):
        """
        Metadata for the device configuration user activity report
        """
        return_type = ClientResult(self.context, Report())
        qry = FunctionQuery(self, "deviceConfigurationUserActivity", None, return_type)
        self.context.add_query(qry)
        return return_type

    def managed_device_enrollment_failure_details(self):
        """ """
        return_type = ClientResult(self.context, Report())
        qry = FunctionQuery(
            self, "managedDeviceEnrollmentFailureDetails", None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def managed_device_enrollment_top_failures(self, period=None):
        """
        Note: The Microsoft Graph API for Intune requires an active Intune license for the tenant.
        """
        return_type = ClientResult(self.context, Report())
        qry = FunctionQuery(
            self, "managedDeviceEnrollmentTopFailures", {"period": period}, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_email_activity_counts(self, period):
        """
        Enables you to understand the trends of email activity (like how many were sent, read, and received)
        in your organization.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        return_type = ClientResult(self.context, str())
        qry = FunctionQuery(
            self, "getEmailActivityCounts", {"period": period}, return_type
        )
        self.context.add_query(qry)
        return qry.return_type

    def get_email_activity_user_counts(self, period):
        """
        Enables you to understand trends on the number of unique users who are performing email activities
        like send, read, and receive.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getEmailActivityUserCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_email_activity_user_detail(self, period):
        """
        Get details about email activity users have performed.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(
            self, "getEmailActivityUserDetail", period, return_stream=True
        )
        self.context.add_query(qry)
        return qry.return_type

    def get_email_app_usage_apps_user_counts(self, period):
        """
        Get the count of unique users per email app.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getEmailAppUsageAppsUserCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_email_app_usage_user_counts(self, period):
        """
        Get the count of unique users that connected to Exchange Online using any email app.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getEmailAppUsageUserCounts", period, True)
        self.context.add_query(qry)
        return qry.return_type

    ###
    def get_email_app_usage_user_detail(self, period):
        """
        Get details about which activities users performed on the various email apps.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getEmailAppUsageUserDetail", period, True)
        self.context.add_query(qry)
        return qry.return_type

    def get_mailbox_usage_storage(self, period):
        """
        Get the amount of storage used in your organization.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getMailboxUsageStorage", period, True)
        self.context.add_query(qry)
        return qry.return_type

    def get_m365_app_user_counts(self, period=None):
        """
        Get a report that provides the trend in the number of active users for each app (Outlook, Word, Excel,
        PowerPoint, OneNote, and Teams) in your organization.
        """
        return_type = ClientResult(self.context, bytes())
        qry = FunctionQuery(
            self, "getM365AppUserCounts", {"period": period}, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_office365_activation_counts(self):
        """Get the count of Microsoft 365 activations on desktops and devices."""
        qry = create_report_query(self, "getOffice365ActivationCounts")
        self.context.add_query(qry)
        return qry.return_type

    def get_office365_activations_user_counts(self):
        """Get the count of Microsoft 365 activations on desktops and devices."""
        qry = create_report_query(self, "getOffice365ActivationsUserCounts")
        self.context.add_query(qry)
        return qry.return_type

    def get_onedrive_activity_file_counts(self, period):
        """
        Get the number of unique, licensed users that performed file interactions against any OneDrive account.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getOneDriveActivityFileCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_onedrive_activity_user_counts(self, period):
        """
        Get the trend in the number of active OneDrive users.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getOneDriveActivityUserCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_onedrive_activity_user_detail(self, period):
        """
        Get details about OneDrive activity by user.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getOneDriveActivityUserDetail", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_onedrive_usage_file_counts(self, period):
        """
        Get the total number of files across all sites and how many are active files. A file is considered active
        if it has been saved, synced, modified, or shared within the specified time period.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getOneDriveUsageFileCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_onedrive_usage_storage(self, period):
        """
        Get the trend on the amount of storage you are using in OneDrive for Business.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getOneDriveUsageStorage", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_mailbox_usage_detail(self, period):
        """
        Get details about mailbox usage.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getMailboxUsageDetail", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_mailbox_usage_mailbox_counts(self, period):
        """
        Get the total number of user mailboxes in your organization and how many are active each day of the reporting
        period. A mailbox is considered active if the user sent or read any email.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getMailboxUsageMailboxCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_mailbox_usage_quota_status_mailbox_counts(self, period):
        """

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(
            self, "getMailboxUsageQuotaStatusMailboxCounts", period
        )
        self.context.add_query(qry)
        return qry.return_type

    def get_sharepoint_activity_pages(self, period):
        """
        Get the number of unique pages visited by users.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getSharePointActivityPages", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_sharepoint_activity_user_counts(self, period):
        """
        Get the trend in the number of active users. A user is considered active if he or she has executed a
        file activity (save, sync, modify, or share) or visited a page within the specified time period.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getSharePointActivityUserCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_sharepoint_activity_user_detail(self, period):
        """
        Get details about SharePoint activity by user.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getSharePointActivityUserDetail", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_sharepoint_site_usage_detail(self, period):
        """
        Get details about SharePoint site usage.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getSharePointSiteUsageDetail", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_sharepoint_site_usage_site_counts(self, period):
        """
        Get the trend of total and active site count during the reporting period.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(self, "getSharePointSiteUsageSiteCounts", period)
        self.context.add_query(qry)
        return qry.return_type

    def get_teams_team_counts(self, period):
        """
        Get the number of teams of a particular type in an instance of Microsoft Teams.

        :param str period: Specifies the length of time over which the report is aggregated.
            The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
            Dn where n represents the number of days over which the report is aggregated. Required.
        """
        qry = create_report_query(
            self, "getTeamsTeamCounts", period, return_stream=True
        )
        self.context.add_query(qry)
        return qry.return_type

    @property
    def authentication_methods(self):
        """Container for navigation properties for Azure AD authentication methods resources."""
        return self.properties.get(
            "authenticationMethods",
            AuthenticationMethodsRoot(
                self.context, ResourcePath("authenticationMethods", self.resource_path)
            ),
        )

    @property
    def partners(self):
        """Represents billing details for a Microsoft direct partner."""
        return self.properties.get(
            "partners",
            Partners(self.context, ResourcePath("partners", self.resource_path)),
        )

    @property
    def security(self):
        """Container for navigation properties for Azure AD authentication methods resources."""
        return self.properties.get(
            "security",
            SecurityReportsRoot(
                self.context, ResourcePath("security", self.resource_path)
            ),
        )
