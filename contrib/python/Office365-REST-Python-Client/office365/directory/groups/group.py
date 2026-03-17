import json
from datetime import datetime
from typing import Optional

from office365.directory.applications.roles.assignment_collection import (
    AppRoleAssignmentCollection,
)
from office365.directory.extensions.extension import Extension
from office365.directory.groups.assigned_label import AssignedLabel
from office365.directory.licenses.assigned_license import AssignedLicense
from office365.directory.licenses.processing_state import LicenseProcessingState
from office365.directory.object import DirectoryObject
from office365.directory.object_collection import DirectoryObjectCollection
from office365.directory.permissions.grants.resource_specific import (
    ResourceSpecificPermissionGrant,
)
from office365.directory.profile_photo import ProfilePhoto
from office365.entity_collection import EntityCollection
from office365.onedrive.drives.drive import Drive
from office365.onenote.onenote import Onenote
from office365.outlook.calendar.events.collection import EventCollection
from office365.outlook.mail.conversation import Conversation
from office365.outlook.mail.conversation_thread import ConversationThread
from office365.planner.group import PlannerGroup
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.teams.team import Team


class Group(DirectoryObject):
    """Represents an Azure Active Directory (Azure AD) group, which can be an Office 365 group, or a security group."""

    def __repr__(self):
        return self.display_name or self.id or self.entity_type_name

    def renew(self):
        """
        Renews a group's expiration. When a group is renewed, the group expiration is extended by the number
        of days defined in the policy.
        """
        qry = ServiceOperationQuery(self, "renew")
        self.context.add_query(qry)
        return self

    def add_favorite(self):
        """Add the group to the list of the current user's favorite groups. Supported for Microsoft 365 groups only."""
        qry = ServiceOperationQuery(self, "addFavorite")
        self.context.add_query(qry)
        return self

    def check_granted_permissions_for_app(self):
        """"""
        return_type = EntityCollection(self.context, ResourceSpecificPermissionGrant)
        qry = ServiceOperationQuery(
            self, "checkGrantedPermissionsForApp", return_type=return_type
        )
        self.context.add_query(qry)
        return return_type

    def remove_favorite(self):
        """
        Remove the group from the list of the current user's favorite groups. Supported for Microsoft 365 groups only.
        """
        qry = ServiceOperationQuery(self, "removeFavorite")
        self.context.add_query(qry)
        return self

    def reset_unseen_count(self):
        """
        Reset the unseenCount of all the posts that the current user has not seen since their last visit.
        Supported for Microsoft 365 groups only.
        """
        qry = ServiceOperationQuery(self, "resetUnseenCount")
        self.context.add_query(qry)
        return self

    def subscribe_by_mail(self):
        """Calling this method will enable the current user to receive email notifications for this group,
        about new posts, events, and files in that group. Supported for Microsoft 365 groups only.
        """
        qry = ServiceOperationQuery(self, "subscribeByMail")
        self.context.add_query(qry)
        return self

    def unsubscribe_by_mail(self):
        """Calling this method will prevent the current user from receiving email notifications for this group
        about new posts, events, and files in that group. Supported for Microsoft 365 groups only.
        """
        qry = ServiceOperationQuery(self, "unsubscribeByMail")
        self.context.add_query(qry)
        return self

    def add_team(self):
        """Create a new team under a group."""
        qry = ServiceOperationQuery(self, "team", None, self.team, None, self.team)

        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.method = HttpMethod.Put
            request.set_header("Content-Type", "application/json")
            request.data = json.dumps(request.data)

        self.context.add_query(qry).before_query_execute(_construct_request, once=False)
        return self.team

    def delete_object(self, permanent_delete=False):
        """
        :param permanent_delete: Permanently deletes the group from directory
        :type permanent_delete: bool

        """
        super(Group, self).delete_object()
        if permanent_delete:
            deleted_item = self.context.directory.deleted_groups[self.id]
            deleted_item.delete_object()
        return self

    @property
    def assigned_labels(self):
        # type: () -> ClientValueCollection[AssignedLabel]
        """The list of sensitivity label pairs (label ID, label name) associated with a Microsoft 365 group."""
        return self.properties.get(
            "assignedLabels", ClientValueCollection(AssignedLabel)
        )

    @property
    def classification(self):
        # type: () -> Optional[str]
        """Describes a classification for the group (such as low, medium or high business impact). Valid values for
        this property are defined by creating a ClassificationList setting value, based on the template definition.
        """
        return self.properties.get("classification", None)

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """
        The display name for the group. This property is required when a group is created and cannot be cleared during
        updates. Maximum length is 256 characters.

        Returned by default. Supports $filter (eq, ne, not, ge, le, in, startsWith, and eq on null values), $search,
        and $orderby.
        """
        return self.properties.get("displayName", None)

    @property
    def group_types(self):
        """
        Specifies the group type and its membership.

        If the collection contains Unified, the group is a Microsoft 365 group; otherwise, it's either a security group
        or distribution group. For details, see groups overview.

        If the collection includes DynamicMembership, the group has dynamic membership; otherwise, membership is static.

        Returned by default. Supports $filter (eq, not).
        """
        return self.properties.get("groupTypes", StringCollection())

    @property
    def has_members_with_license_errors(self):
        # type: () -> Optional[bool]
        """
        Indicates whether there are members in this group that have license errors from its group-based license
        assignment.

        This property is never returned on a GET operation. You can use it as a $filter argument to get groups that
        have members with license errors (that is, filter for this property being true)
        """
        return self.properties.get("hasMembersWithLicenseErrors", None)

    @property
    def is_assignable_to_role(self):
        # type: () -> Optional[bool]
        """
        Indicates whether this group can be assigned to an Azure Active Directory role or not. Optional.

        This property can only be set while creating the group and is immutable. If set to true, the securityEnabled
        property must also be set to true, visibility must be Hidden, and the group cannot be a dynamic group
        (that is, groupTypes cannot contain DynamicMembership).

        Only callers in Global Administrator and Privileged Role Administrator roles can set this property.
        The caller must also be assigned the RoleManagement.ReadWrite.Directory permission to set this property or
        update the membership of such groups. For more, see Using a group to manage Azure AD role assignments
        """
        return self.properties.get("isAssignableToRole", None)

    @property
    def license_processing_state(self):
        """Indicates status of the group license assignment to all members of the group. Default value is false.
        Read-only. Possible values: QueuedForProcessing, ProcessingInProgress, and ProcessingComplete.
        """
        return self.properties.get("licenseProcessingState", LicenseProcessingState())

    @property
    def mail(self):
        # type: () -> Optional[str]
        """
        The SMTP address for the group, for example, "serviceadmins@contoso.onmicrosoft.com".
        """
        return self.properties.get("mail", None)

    @property
    def mail_enabled(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the group is mail-enabled. Required.
        """
        return self.properties.get("mailEnabled", None)

    @property
    def mail_nickname(self):
        # type: () -> Optional[str]
        """
        The mail alias for the group, unique for Microsoft 365 groups in the organization. Maximum length is 64
        characters.
        """
        return self.properties.get("mailNickname", None)

    @property
    def on_premises_domain_name(self):
        # type: () -> Optional[str]
        return self.properties.get("onPremisesDomainName", None)

    @property
    def conversations(self):
        # type: () -> EntityCollection[Conversation]
        """The group's conversations."""
        return self.properties.get(
            "conversations",
            EntityCollection(
                self.context,
                Conversation,
                ResourcePath("conversations", self.resource_path),
            ),
        )

    @property
    def created_datetime(self):
        """Timestamp of when the group was created."""
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def extensions(self):
        # type: () -> EntityCollection[Extension]
        """
        The collection of open extensions defined for the group
        """
        return self.properties.get(
            "extensions",
            EntityCollection(
                self.context, Extension, ResourcePath("extensions", self.resource_path)
            ),
        )

    @property
    def members(self):
        """Users and groups that are members of this group."""
        return self.properties.get(
            "members",
            DirectoryObjectCollection(
                self.context, ResourcePath("members", self.resource_path)
            ),
        )

    @property
    def transitive_members(self):
        """
        Get a list of the group's members. A group can have members, devices, organizational contacts,
        and other groups as members. This operation is transitive and returns a flat list of all nested members.
        """
        return self.properties.get(
            "transitiveMembers",
            DirectoryObjectCollection(
                self.context, ResourcePath("transitiveMembers", self.resource_path)
            ),
        )

    @property
    def transitive_member_of(self):
        """
        Get groups that the group is a member of. This operation is transitive and will also include all groups that
        this groups is a nested member of. Unlike getting a user's Microsoft 365 groups, this returns all
        types of groups, not just Microsoft 365 groups.
        """
        return self.properties.get(
            "transitiveMemberOf",
            DirectoryObjectCollection(
                self.context, ResourcePath("transitiveMemberOf", self.resource_path)
            ),
        )

    @property
    def threads(self):
        # type: () -> EntityCollection[ConversationThread]
        """The group's conversation threads"""
        return self.properties.get(
            "threads",
            EntityCollection(
                self.context,
                ConversationThread,
                ResourcePath("threads", self.resource_path),
            ),
        )

    @property
    def owners(self):
        """The owners of the group."""
        return self.properties.get(
            "owners",
            DirectoryObjectCollection(
                self.context, ResourcePath("owners", self.resource_path)
            ),
        )

    @property
    def drives(self):
        # type: () -> EntityCollection[Drive]
        """
        The group's drives. Read-only.
        """
        return self.properties.get(
            "drives",
            EntityCollection(
                self.context, Drive, ResourcePath("drives", self.resource_path)
            ),
        )

    @property
    def sites(self):
        """
        The list of SharePoint sites in this group. Access the default site with /sites/root.
        """
        from office365.onedrive.sites.sites_with_root import SitesWithRoot

        return self.properties.get(
            "sites",
            SitesWithRoot(self.context, ResourcePath("sites", self.resource_path)),
        )

    @property
    def events(self):
        # type: () -> EventCollection
        """Get an event collection or an event."""
        return self.properties.get(
            "events",
            EventCollection(self.context, ResourcePath("events", self.resource_path)),
        )

    @property
    def app_role_assignments(self):
        """Get an event collection or an appRoleAssignments."""
        return self.properties.get(
            "appRoleAssignments",
            AppRoleAssignmentCollection(
                self.context, ResourcePath("appRoleAssignments", self.resource_path)
            ),
        )

    @property
    def onenote(self):
        """Represents the Onenote services available to a group."""
        return self.properties.get(
            "onenote",
            Onenote(self.context, ResourcePath("onenote", self.resource_path)),
        )

    @property
    def planner(self):
        """The plannerGroup resource provide access to Planner resources for a group."""
        return self.properties.get(
            "planner",
            PlannerGroup(self.context, ResourcePath("planner", self.resource_path)),
        )

    @property
    def permission_grants(self):
        # type: () -> EntityCollection[ResourceSpecificPermissionGrant]
        """List permissions that have been granted to apps to access the group."""
        return self.properties.setdefault(
            "permissionGrants",
            EntityCollection(
                self.context,
                ResourceSpecificPermissionGrant,
                ResourcePath("permissionGrants"),
            ),
        )

    @property
    def photo(self):
        """The group's profile photo"""
        return self.properties.get(
            "photo",
            ProfilePhoto(self.context, ResourcePath("photo", self.resource_path)),
        )

    @property
    def team(self):
        """The team associated with this group."""
        return self.properties.setdefault(
            "team", Team(self.context, ResourcePath(self.id, ResourcePath("teams")))
        )

    @property
    def assigned_licenses(self):
        """
        The licenses that are assigned to the group.
        Returned only on $select. Supports $filter (eq).Read-only.
        """
        return self.properties.get(
            "assignedLicenses", ClientValueCollection(AssignedLicense)
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "assignedLabels": self.assigned_labels,
                "appRoleAssignments": self.app_role_assignments,
                "assignedLicenses": self.assigned_licenses,
                "createdDateTime": self.created_datetime,
                "groupTypes": self.group_types,
                "licenseProcessingState": self.license_processing_state,
                "permissionGrants": self.permission_grants,
                "transitiveMembers": self.transitive_members,
                "transitiveMemberOf": self.transitive_member_of,
            }
            default_value = property_mapping.get(name, None)
        return super(Group, self).get_property(name, default_value)
