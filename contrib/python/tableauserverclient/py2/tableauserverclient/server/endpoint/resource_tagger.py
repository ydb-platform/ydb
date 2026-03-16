from .endpoint import Endpoint
from .exceptions import EndpointUnavailableError, ServerResponseError
from .. import RequestFactory
from ...models.tag_item import TagItem
import logging
import copy

logger = logging.getLogger('tableau.endpoint.resource_tagger')


class _ResourceTagger(Endpoint):
    # Add new tags to resource
    def _add_tags(self, baseurl, resource_id, tag_set):
        url = "{0}/{1}/tags".format(baseurl, resource_id)
        add_req = RequestFactory.Tag.add_req(tag_set)

        try:
            server_response = self.put_request(url, add_req)
            return TagItem.from_response(server_response.content, self.parent_srv.namespace)
        except ServerResponseError as e:
            if e.code == "404003":
                error = "Adding tags to this resource type is only available with REST API version 2.6 and later."
                raise EndpointUnavailableError(error)
            raise  # Some other error

    # Delete a resource's tag by name
    def _delete_tag(self, baseurl, resource_id, tag_name):
        url = "{0}/{1}/tags/{2}".format(baseurl, resource_id, tag_name)

        try:
            self.delete_request(url)
        except ServerResponseError as e:
            if e.code == "404003":
                error = "Deleting tags from this resource type is only available with REST API version 2.6 and later."
                raise EndpointUnavailableError(error)

    # Remove and add tags to match the resource item's tag set
    def update_tags(self, baseurl, resource_item):
        if resource_item.tags != resource_item._initial_tags:
            add_set = resource_item.tags - resource_item._initial_tags
            remove_set = resource_item._initial_tags - resource_item.tags
            for tag in remove_set:
                self._delete_tag(baseurl, resource_item.id, tag)
            if add_set:
                resource_item.tags = self._add_tags(baseurl, resource_item.id, add_set)
            resource_item._initial_tags = copy.copy(resource_item.tags)
        logger.info('Updated tags to {0}'.format(resource_item.tags))
