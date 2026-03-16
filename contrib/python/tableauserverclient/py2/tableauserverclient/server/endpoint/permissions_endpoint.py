import logging

from .. import RequestFactory, PermissionsRule

from .endpoint import Endpoint
from .exceptions import MissingRequiredFieldError


logger = logging.getLogger(__name__)


class _PermissionsEndpoint(Endpoint):
    """ Adds permission model to another endpoint

    Tableau permissions model is identical between objects but they are nested under
    the parent object endpoint (i.e. permissions for workbooks are under
    /workbooks/:id/permission).  This class is meant to be instantated inside a
    parent endpoint which has these supported endpoints
    """

    def __init__(self, parent_srv, owner_baseurl):
        super(_PermissionsEndpoint, self).__init__(parent_srv)

        # owner_baseurl is the baseurl of the parent.  The MUST be a lambda
        # since we don't know the full site URL until we sign in.  If
        # populated without, we will get a sign-in error
        self.owner_baseurl = owner_baseurl

    def update(self, resource, permissions):
        url = '{0}/{1}/permissions'.format(self.owner_baseurl(), resource.id)
        update_req = RequestFactory.Permission.add_req(permissions)
        response = self.put_request(url, update_req)
        permissions = PermissionsRule.from_response(response.content,
                                                    self.parent_srv.namespace)
        logger.info('Updated permissions for resource {0}'.format(resource.id))

        return permissions

    def delete(self, resource, rules):
        # Delete is the only endpoint that doesn't take a list of rules
        # so let's fake it to keep it consistent
        # TODO that means we need error handling around the call
        if isinstance(rules, PermissionsRule):
            rules = [rules]

        for rule in rules:
            for capability, mode in rule.capabilities.items():
                "              /permissions/groups/group-id/capability-name/capability-mode"
                url = '{0}/{1}/permissions/{2}/{3}/{4}/{5}'.format(
                    self.owner_baseurl(),
                    resource.id,
                    rule.grantee.tag_name + 's',
                    rule.grantee.id,
                    capability,
                    mode)

                logger.debug('Removing {0} permission for capabilty {1}'.format(
                    mode, capability))

                self.delete_request(url)

            logger.info('Deleted permission for {0} {1} item {2}'.format(
                rule.grantee.tag_name,
                rule.grantee.id,
                resource.id))

    def populate(self, item):
        if not item.id:
            error = "Server item is missing ID. Item must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def permission_fetcher():
            return self._get_permissions(item)

        item._set_permissions(permission_fetcher)
        logger.info('Populated permissions for item (ID: {0})'.format(item.id))

    def _get_permissions(self, item, req_options=None):
        url = "{0}/{1}/permissions".format(self.owner_baseurl(), item.id)
        server_response = self.get_request(url, req_options)
        permissions = PermissionsRule.from_response(server_response.content,
                                                    self.parent_srv.namespace)

        return permissions
