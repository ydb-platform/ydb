from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.teams.apps.user_scope_installation import UserScopeTeamsAppInstallation
from office365.teams.associated_info import AssociatedTeamInfo


class UserTeamwork(Entity):
    """A container for the range of Microsoft Teams functionalities that are available per user in the tenant."""

    @property
    def locale(self):
        # type: () -> Optional[str]
        """Represents the location that a user selected in Microsoft Teams and doesn't follow the Office's locale
        setting. A user's locale is represented by their preferred language and country or region.
        For example, en-us. The language component follows two-letter codes as defined in ISO 639-1,
        and the country component follows two-letter codes as defined in ISO 3166-1 alpha-2.
        """
        return self.properties.get("locale", None)

    @property
    def region(self):
        # type: () -> Optional[str]
        """Represents the region of the organization or the user. For users with multigeo licenses, the property
        contains the user's region (if available). For users without multigeo licenses, the property contains
        the organization's region.

        The region value can be any region supported by the Teams payload.
        The possible values are: Americas, Europe and MiddleEast, Asia Pacific, UAE, Australia, Brazil, Canada,
        Switzerland, Germany, France, India, Japan, South Korea, Norway, Singapore, United Kingdom, South Africa,
        Sweden, Qatar, Poland, Italy, Israel, Spain, Mexico, USGov Community Cloud, USGov Community Cloud High,
        USGov Department of Defense, and China.
        """
        return self.properties.get("region", None)

    @property
    def associated_teams(self):
        # type: () -> EntityCollection[AssociatedTeamInfo]
        """
        The apps installed in the personal scope of this user.
        """
        return self.properties.get(
            "associatedTeams",
            EntityCollection(
                self.context,
                AssociatedTeamInfo,
                ResourcePath("associatedTeams", self.resource_path),
            ),
        )

    @property
    def installed_apps(self):
        # type: () -> EntityCollection[UserScopeTeamsAppInstallation]
        """
        The apps installed in the personal scope of this user.
        """
        return self.properties.get(
            "installedApps",
            EntityCollection(
                self.context,
                UserScopeTeamsAppInstallation,
                ResourcePath("installedApps", self.resource_path),
            ),
        )

    def send_activity_notification(
        self, topic, activity_type, chain_id, preview_text, template_parameters=None
    ):
        """
        Send an activity feed notification in the scope of a team. For more details about sending notifications
        and the requirements for doing so, see sending Teams activity notifications.

        :param TeamworkActivityTopic topic: Topic of the notification. Specifies the resource being talked about.
        :param str activity_type: Activity type. This must be declared in the Teams app manifest.
        :param int chain_id: Optional. Used to override a previous notification. Use the same chainId in subsequent
            requests to override the previous notification.
        :param ItemBody preview_text: Preview text for the notification. Microsoft Teams will only show first
            150 characters.
        :param dict template_parameters: Values for template variables defined in the activity feed entry corresponding
            to activityType in Teams app manifest.
        """
        payload = {
            "topic": topic,
            "activityType": activity_type,
            "chainId": chain_id,
            "previewText": preview_text,
            "templateParameters": template_parameters,
        }
        qry = ServiceOperationQuery(self, "sendActivityNotification", None, payload)
        self.context.add_query(qry)
        return self

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "associatedTeams": self.associated_teams,
                "installedApps": self.installed_apps,
            }
            default_value = property_mapping.get(name, None)
        return super(UserTeamwork, self).get_property(name, default_value)
