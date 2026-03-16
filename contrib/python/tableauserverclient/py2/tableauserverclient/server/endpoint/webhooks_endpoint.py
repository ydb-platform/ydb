from .endpoint import Endpoint, api
from ...models import WebhookItem, PaginationItem
from .. import RequestFactory

import logging

logger = logging.getLogger('tableau.endpoint.webhooks')


class Webhooks(Endpoint):
    def __init__(self, parent_srv):
        super(Webhooks, self).__init__(parent_srv)

    @property
    def baseurl(self):
        return "{0}/sites/{1}/webhooks".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    @api(version="3.6")
    def get(self, req_options=None):
        logger.info('Querying all Webhooks on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        all_webhook_items = WebhookItem.from_response(server_response.content, self.parent_srv.namespace)
        pagination_item = PaginationItem.from_single_page_list(all_webhook_items)
        return all_webhook_items, pagination_item

    @api(version="3.6")
    def get_by_id(self, webhook_id):
        if not webhook_id:
            error = "Webhook ID undefined."
            raise ValueError(error)
        logger.info('Querying single webhook (ID: {0})'.format(webhook_id))
        url = "{0}/{1}".format(self.baseurl, webhook_id)
        server_response = self.get_request(url)
        return WebhookItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.6")
    def delete(self, webhook_id):
        if not webhook_id:
            error = "Webhook ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, webhook_id)
        self.delete_request(url)
        logger.info('Deleted single webhook (ID: {0})'.format(webhook_id))

    @api(version="3.6")
    def create(self, webhook_item):
        url = self.baseurl
        create_req = RequestFactory.Webhook.create_req(webhook_item)
        server_response = self.post_request(url, create_req)
        new_webhook = WebhookItem.from_response(server_response.content, self.parent_srv.namespace)[0]

        logger.info('Created new webhook (ID: {0})'.format(new_webhook.id))
        return new_webhook

    @api(version="3.6")
    def test(self, webhook_id):
        if not webhook_id:
            error = "Webhook ID undefined."
            raise ValueError(error)
        url = "{0}/{1}/test".format(self.baseurl, webhook_id)
        testOutcome = self.get_request(url)
        logger.info('Testing webhook (ID: {0} returned {1})'.format(webhook_id, testOutcome))
        return testOutcome
