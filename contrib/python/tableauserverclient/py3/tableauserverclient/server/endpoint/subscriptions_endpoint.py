import logging

from .endpoint import Endpoint, api
from .exceptions import MissingRequiredFieldError
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import SubscriptionItem, PaginationItem

from tableauserverclient.helpers.logging import logger

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..request_options import RequestOptions


class Subscriptions(Endpoint):
    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/subscriptions"

    @api(version="2.3")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[SubscriptionItem], PaginationItem]:
        logger.info("Querying all subscriptions for the site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)

        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_subscriptions = SubscriptionItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_subscriptions, pagination_item

    @api(version="2.3")
    def get_by_id(self, subscription_id: str) -> SubscriptionItem:
        if not subscription_id:
            error = "No Subscription ID provided"
            raise ValueError(error)
        logger.info(f"Querying a single subscription by id ({subscription_id})")
        url = f"{self.baseurl}/{subscription_id}"
        server_response = self.get_request(url)
        return SubscriptionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.3")
    def create(self, subscription_item: SubscriptionItem) -> SubscriptionItem:
        if not subscription_item:
            error = "No Susbcription provided"
            raise ValueError(error)
        logger.info(f"Creating a subscription ({subscription_item})")
        url = self.baseurl
        create_req = RequestFactory.Subscription.create_req(subscription_item)
        server_response = self.post_request(url, create_req)
        return SubscriptionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.3")
    def delete(self, subscription_id: str) -> None:
        if not subscription_id:
            error = "Subscription ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{subscription_id}"
        self.delete_request(url)
        logger.info(f"Deleted subscription (ID: {subscription_id})")

    @api(version="2.3")
    def update(self, subscription_item: SubscriptionItem) -> SubscriptionItem:
        if not subscription_item.id:
            error = "Subscription item missing ID. Subscription must be retrieved from server first."
            raise MissingRequiredFieldError(error)
        url = f"{self.baseurl}/{subscription_item.id}"
        update_req = RequestFactory.Subscription.update_req(subscription_item)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated subscription item (ID: {subscription_item.id})")
        return SubscriptionItem.from_response(server_response.content, self.parent_srv.namespace)[0]
