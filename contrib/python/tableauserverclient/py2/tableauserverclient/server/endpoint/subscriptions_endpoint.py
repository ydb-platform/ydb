from .endpoint import Endpoint, api
from .. import RequestFactory, SubscriptionItem, PaginationItem

import logging

logger = logging.getLogger('tableau.endpoint.subscriptions')


class Subscriptions(Endpoint):
    @property
    def baseurl(self):
        return "{0}/sites/{1}/subscriptions".format(self.parent_srv.baseurl,
                                                    self.parent_srv.site_id)

    @api(version='2.3')
    def get(self, req_options=None):
        logger.info('Querying all subscriptions for the site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)

        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_subscriptions = SubscriptionItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_subscriptions, pagination_item

    @api(version='2.3')
    def get_by_id(self, subscription_id):
        if not subscription_id:
            error = "No Subscription ID provided"
            raise ValueError(error)
        logger.info("Querying a single subscription by id ({})".format(subscription_id))
        url = "{}/{}".format(self.baseurl, subscription_id)
        server_response = self.get_request(url)
        return SubscriptionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version='2.3')
    def create(self, subscription_item):
        if not subscription_item:
            error = "No Susbcription provided"
            raise ValueError(error)
        logger.info("Creating a subscription ({})".format(subscription_item))
        url = self.baseurl
        create_req = RequestFactory.Subscription.create_req(subscription_item)
        server_response = self.post_request(url, create_req)
        return SubscriptionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version='2.3')
    def delete(self, subscription_id):
        if not subscription_id:
            error = "Subscription ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, subscription_id)
        self.delete_request(url)
        logger.info('Deleted subscription (ID: {0})'.format(subscription_id))
