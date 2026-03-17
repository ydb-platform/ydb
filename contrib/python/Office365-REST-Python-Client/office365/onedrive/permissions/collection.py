from typing import TYPE_CHECKING

from office365.directory.permissions.identity import Identity
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.permissions.permission import Permission
from office365.runtime.queries.create_entity import CreateEntityQuery

if TYPE_CHECKING:
    from office365.directory.applications.application import Application
    from office365.directory.groups.group import Group
    from office365.directory.users.user import User
    from office365.intune.devices.device import Device


class PermissionCollection(EntityCollection[Permission]):
    """Permission's collection"""

    def __init__(self, context, resource_path=None):
        super(PermissionCollection, self).__init__(context, Permission, resource_path)

    def add(self, roles, identity=None, identity_type=None):
        # type: (list[str], Application|User|Group|Device|str, str) -> Permission
        """
        Create a new permission object.

        :param list[str] roles: Permission types
        :param Application or User or Device or User or Group or str identity: Identity object or identifier
        :param str identity_type: Identity type
        """

        return_type = Permission(self.context)
        self.add_child(return_type)

        known_identities = {
            "application": self.context.applications,
            "user": self.context.users,
            "device": self.context.device_app_management,
            "group": self.context.groups,
        }

        if isinstance(identity, Entity):
            identity_type = type(identity).__name__.lower()
        else:
            if identity_type is None:
                raise ValueError(
                    "Identity type is a mandatory when identity identifier is specified"
                )
            known_identity = known_identities.get(identity_type, None)
            if known_identity is None:
                raise ValueError("Unknown identity type")
            identity = known_identity[identity]

        def _add():
            payload = {
                "roles": roles,
                "grantedToIdentities": [
                    {
                        identity_type: Identity(
                            display_name=identity.display_name, _id=identity.id
                        )
                    }
                ],
            }

            qry = CreateEntityQuery(self, payload, return_type)
            self.context.add_query(qry)

        identity.ensure_properties(["displayName"], _add)
        return return_type

    def delete_all(self):
        """Remove all access to resource"""

        def _after_loaded(return_type):
            # type: (PermissionCollection) -> None
            for permission in return_type:
                permission.delete_object()

        self.get().after_execute(_after_loaded)
        return self
