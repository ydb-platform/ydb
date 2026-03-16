from typing import Optional

from office365.directory.permissions.grants.resource_specific import (
    ResourceSpecificPermissionGrant,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.teams.apps.installation import TeamsAppInstallation
from office365.teams.channels.channel import Channel
from office365.teams.channels.collection import ChannelCollection
from office365.teams.fun_settings import TeamFunSettings
from office365.teams.guest_settings import TeamGuestSettings
from office365.teams.members.settings import TeamMemberSettings
from office365.teams.messaging_settings import TeamMessagingSettings
from office365.teams.operations.async_operation import TeamsAsyncOperation
from office365.teams.schedule.schedule import Schedule
from office365.teams.summary import TeamSummary
from office365.teams.teamwork.tags.tag import TeamworkTag
from office365.teams.template import TeamsTemplate


class Team(Entity):
    """A team in Microsoft Teams is a collection of channel objects. A channel represents a topic, and therefore a
    logical isolation of discussion, within a team."""

    def __str__(self):
        return self.display_name

    def execute_query_and_wait(self):
        """
        Submit request(s) to the server and waits until operation is completed
        """

        def _loaded():
            self.operations[0].poll_for_status(status_type="succeeded")

        self.ensure_property("id", _loaded)

        self.context.execute_query()
        return self

    def delete_object(self):
        """Deletes a team"""

        def _team_loaded():
            group = self.context.groups[self.id]
            group.delete_object(False)

        self.ensure_property("id", _team_loaded)
        return self

    @property
    def fun_settings(self):
        """Settings to configure use of Giphy, memes, and stickers in the team."""
        return self.properties.get("funSettings", TeamFunSettings())

    @property
    def member_settings(self):
        """Settings to configure whether members can perform certain actions, for example,
        create channels and add bots, in the team."""
        return self.properties.get("memberSettings", TeamMemberSettings())

    @property
    def guest_settings(self):
        """Settings to configure whether guests can create, update, or delete channels in the team."""
        return self.properties.get("guestSettings", TeamGuestSettings())

    @property
    def messaging_settings(self):
        """Settings to configure messaging and mentions in the team."""
        return self.properties.get("guestSettings", TeamMessagingSettings())

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The name of the team."""
        return self.properties.get("displayName", None)

    @property
    def description(self):
        # type: () -> Optional[str]
        """An optional description for the team."""
        return self.properties.get("description", None)

    @property
    def classification(self):
        # type: () -> Optional[str]
        """An optional label. Typically describes the data or business sensitivity of the team.
        Must match one of a pre-configured set in the tenant's directory.
        """
        return self.properties.get("classification", None)

    @property
    def is_archived(self):
        # type: () -> Optional[bool]
        """Whether this team is in read-only mode."""
        return self.properties.get("isArchived", None)

    @property
    def visibility(self):
        """The visibility of the group and team. Defaults to Public."""
        return self.properties.get("visibility", None)

    @property
    def web_url(self):
        # type: () -> Optional[str]
        """A hyperlink that will go to the team in the Microsoft Teams client. This is the URL that you get when
        you right-click a team in the Microsoft Teams client and select Get link to team. This URL should be treated
        as an opaque blob, and not parsed."""
        return self.properties.get("webUrl", None)

    @property
    def created_datetime(self):
        """Timestamp at which the team was created."""
        return self.properties.get("createdDateTime", None)

    @property
    def all_channels(self):
        # type: () -> ChannelCollection
        """
        List of channels either hosted in or shared with the team (incoming channels).
        """
        return self.properties.get(
            "allChannels",
            ChannelCollection(
                self.context, ResourcePath("allChannels", self.resource_path)
            ),
        )

    @property
    def incoming_channels(self):
        # type: () -> ChannelCollection
        """List of channels shared with the team."""
        return self.properties.get(
            "incomingChannels",
            ChannelCollection(
                self.context, ResourcePath("incomingChannels", self.resource_path)
            ),
        )

    @property
    def channels(self):
        # type: () -> ChannelCollection
        """The collection of channels & messages associated with the team."""
        return self.properties.get(
            "channels",
            ChannelCollection(
                self.context, ResourcePath("channels", self.resource_path)
            ),
        )

    @property
    def group(self):
        """"""
        from office365.directory.groups.group import Group

        return self.properties.get(
            "group", Group(self.context, ResourcePath("group", self.resource_path))
        )

    @property
    def primary_channel(self):
        # type: () -> Channel
        """The general channel for the team."""
        return self.properties.get(
            "primaryChannel",
            Channel(self.context, ResourcePath("primaryChannel", self.resource_path)),
        )

    @property
    def schedule(self):
        """The schedule of shifts for this team."""
        return self.properties.get(
            "schedule",
            Schedule(self.context, ResourcePath("schedule", self.resource_path)),
        )

    @property
    def installed_apps(self):
        # type: () -> EntityCollection[TeamsAppInstallation]
        """The apps installed in this team."""
        return self.properties.get(
            "installedApps",
            EntityCollection(
                self.context,
                TeamsAppInstallation,
                ResourcePath("installedApps", self.resource_path),
            ),
        )

    @property
    def operations(self):
        # type: () -> EntityCollection[TeamsAsyncOperation]
        """The async operations that ran or are running on this team."""
        return self.properties.setdefault(
            "operations",
            EntityCollection(
                self.context,
                TeamsAsyncOperation,
                ResourcePath("operations", self.resource_path),
            ),
        )

    @property
    def permission_grants(self):
        # type: () -> EntityCollection[ResourceSpecificPermissionGrant]
        """
        List all resource-specific permission grants
        """
        return self.properties.setdefault(
            "permissionGrants",
            EntityCollection(
                self.context,
                ResourceSpecificPermissionGrant,
                ResourcePath("permissionGrants"),
            ),
        )

    @property
    def summary(self):
        """Contains summary information about the team, including number of owners, members, and guests."""
        return self.properties.get("summary", TeamSummary())

    @property
    def tenant_id(self):
        # type: () -> Optional[str]
        """The ID of the Azure Active Directory tenant."""
        return self.properties.get("tenantId", None)

    @property
    def tags(self):
        """The tags associated with the team."""
        return self.properties.get(
            "tags",
            EntityCollection(
                self.context, TeamworkTag, ResourcePath("tags", self.resource_path)
            ),
        )

    @property
    def template(self):
        """The template this team was created from"""
        return self.properties.get(
            "template",
            TeamsTemplate(self.context, ResourcePath("template", self.resource_path)),
        )

    def archive(self):
        """Archive the specified team. When a team is archived, users can no longer send or like messages on any
        channel in the team, edit the team's name, description, or other settings, or in general make most changes to
        the team. Membership changes to the team continue to be allowed."""
        qry = ServiceOperationQuery(self, "archive")
        self.context.add_query(qry)
        return self

    def unarchive(self):
        """Restore an archived team. This restores users' ability to send messages and edit the team, abiding by
        tenant and team settings."""
        qry = ServiceOperationQuery(self, "unarchive")
        self.context.add_query(qry)
        return self

    def clone(self):
        """Create a copy of a team. This operation also creates a copy of the corresponding group."""
        qry = ServiceOperationQuery(self, "clone")
        self.context.add_query(qry)
        return self

    def send_activity_notification(
        self,
        topic,
        activity_type,
        chain_id,
        preview_text,
        template_parameters,
        recipient,
    ):
        """
        Send an activity feed notification in the scope of a team.
        For more details about sending notifications and the requirements for doing so,
        see sending Teams activity notifications:
        https://docs.microsoft.com/en-us/graph/teams-send-activityfeednotifications

        :param teamworkActivityTopic topic: Topic of the notification. Specifies the resource being talked about.
        :param str activity_type: Activity type. This must be declared in the Teams app manifest.
        :param str chain_id: Optional. Used to override a previous notification. Use the same chainId in subsequent
            requests to override the previous notification.
        :param str preview_text: Preview text for the notification. Microsoft Teams will only show first 150 characters
        :param dict template_parameters: Values for template variables defined in the activity feed entry corresponding
            to activityType in Teams app manifest.
        :param dict template_parameters: Recipient of the notification.
             Only Azure AD users are supported.
        :param teamworkNotificationRecipient recipient: Recipient of the notification
        """
        payload = {
            "topic": topic,
            "activityType": activity_type,
            "chainId": chain_id,
            "previewText": preview_text,
            "templateParameters": template_parameters,
            "recipient": recipient,
        }
        qry = ServiceOperationQuery(self, "sendActivityNotification", None, payload)
        self.context.add_query(qry)
        return self

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "allChannels": self.all_channels,
                "incomingChannels": self.incoming_channels,
                "installedApps": self.installed_apps,
                "permissionGrants": self.permission_grants,
                "primaryChannel": self.primary_channel,
            }
            default_value = property_mapping.get(name, None)
        return super(Team, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(Team, self).set_property(name, value, persist_changes)
        # fallback: determine whether resource path is resolved
        if name == "id" and self._resource_path.segment == "team":
            self._resource_path = ResourcePath(value, ResourcePath("teams"))
        return self
