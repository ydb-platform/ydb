import logging

from tableauserverclient.server import RequestFactory
from tableauserverclient.models import TableauItem, PermissionsRule

from .endpoint import Endpoint
from .exceptions import MissingRequiredFieldError

from typing import Callable, TYPE_CHECKING, Optional, Union

from tableauserverclient.helpers.logging import logger

if TYPE_CHECKING:
    from ..server import Server
    from ..request_options import RequestOptions


class _PermissionsEndpoint(Endpoint):
    """Adds permission model to another endpoint

    Tableau permissions model is identical between objects, but they are nested under
    the parent object endpoint (i.e. permissions for workbooks are under
    /workbooks/:id/permission).  This class is meant to be instantiated inside a
    parent endpoint which has these supported endpoints
    """

    def __init__(self, parent_srv: "Server", owner_baseurl: Callable[[], str]) -> None:
        super().__init__(parent_srv)

        # owner_baseurl is the baseurl of the parent.  The MUST be a lambda
        # since we don't know the full site URL until we sign in.  If
        # populated without, we will get a sign-in error
        self.owner_baseurl = owner_baseurl

    def __str__(self):
        return f"<PermissionsEndpoint baseurl={self.owner_baseurl}>"

    def update(self, resource: TableauItem, permissions: list[PermissionsRule]) -> list[PermissionsRule]:
        url = f"{self.owner_baseurl()}/{resource.id}/permissions"
        update_req = RequestFactory.Permission.add_req(permissions)
        response = self.put_request(url, update_req)
        permissions = PermissionsRule.from_response(response.content, self.parent_srv.namespace)
        logger.info(f"Updated permissions for resource {resource.id}: {permissions}")

        return permissions

    def delete(self, resource: TableauItem, rules: Union[PermissionsRule, list[PermissionsRule]]):
        # Delete is the only endpoint that doesn't take a list of rules
        # so let's fake it to keep it consistent
        # TODO that means we need error handling around the call
        if isinstance(rules, PermissionsRule):
            rules = [rules]

        for rule in rules:
            for capability, mode in rule.capabilities.items():
                "/permissions/groups/group-id/capability-name/capability-mode"
                url = "{}/{}/permissions/{}/{}/{}/{}".format(
                    self.owner_baseurl(),
                    resource.id,
                    rule.grantee.tag_name + "s",
                    rule.grantee.id,
                    capability,
                    mode,
                )

                logger.debug(f"Removing {mode} permission for capability {capability}")

                self.delete_request(url)

            logger.info(f"Deleted permission for {rule.grantee.tag_name} {rule.grantee.id} item {resource.id}")

    def populate(self, item: TableauItem):
        if not item.id:
            error = "Server item is missing ID. Item must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def permission_fetcher():
            return self._get_permissions(item)

        item._set_permissions(permission_fetcher)
        logger.info(f"Populated permissions for item (ID: {item.id})")

    def _get_permissions(self, item: TableauItem, req_options: Optional["RequestOptions"] = None):
        url = f"{self.owner_baseurl()}/{item.id}/permissions"
        server_response = self.get_request(url, req_options)
        permissions = PermissionsRule.from_response(server_response.content, self.parent_srv.namespace)
        logger.info(f"Permissions for resource {item.id}: {permissions}")

        return permissions
