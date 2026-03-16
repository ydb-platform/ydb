from typing import Optional

from office365.entity import Entity
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery


class GroupLifecyclePolicy(Entity):
    """
    Represents a lifecycle policy for a Microsoft 365 group. A group lifecycle policy allows administrators
    to set an expiration period for groups. For example, after 180 days, a group expires.
    When a group reaches its expiration, owners of the group are required to renew their group within a time interval
    defined by the administrator. Once renewed, the group expiration is extended by the number of days defined
    in the policy. For example, the group's new expiration is 180 days after renewal.
    If the group is not renewed, it expires and is deleted.
    The group can be restored within a period of 30 days from deletion.
    """

    def add_group(self, group_id):
        """
        Adds specific groups to a lifecycle policy. This action limits the group lifecycle policy to a set of groups
        only if the managedGroupTypes property of groupLifecyclePolicy is set to Selected.

        :param str group_id: The identifier of the group to remove from the policy.
        """
        return_type = ClientResult[bool](self.context)
        payload = {"groupId": group_id}
        qry = ServiceOperationQuery(self, "addGroup", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def remove_group(self, group_id):
        # type: (str) -> ClientResult[bool]
        """
        Removes a group from a lifecycle policy.
        :param str group_id: The identifier of the group to add to the policy.
        """
        return_type = ClientResult(self.context)
        payload = {"groupId": group_id}
        qry = ServiceOperationQuery(
            self, "removeGroup", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def alternate_notification_emails(self):
        # type: () -> Optional[str]
        """
        List of email address to send notifications for groups without owners.
        Multiple email address can be defined by separating email address with a semicolon.
        """
        return self.properties.get("alternateNotificationEmails", None)

    @property
    def group_lifetime_in_days(self):
        # type: () -> Optional[int]
        """
        Number of days before a group expires and needs to be renewed. Once renewed, the group expiration is extended
        by the number of days defined.
        """
        return self.properties.get("groupLifetimeInDays", None)

    @property
    def managed_group_types(self):
        # type: () -> Optional[str]
        """The group type for which the expiration policy applies. Possible values are All, Selected or None."""
        return self.properties.get("managedGroupTypes", None)
