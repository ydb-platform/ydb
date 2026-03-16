import logging

from .endpoint import Endpoint, api
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import WebhookItem, PaginationItem

from tableauserverclient.helpers.logging import logger

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..server import Server
    from ..request_options import RequestOptions


class Webhooks(Endpoint):
    def __init__(self, parent_srv: "Server") -> None:
        super().__init__(parent_srv)

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/webhooks"

    @api(version="3.6")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[WebhookItem], PaginationItem]:
        """
        Returns a list of all webhooks on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#list_webhooks_for_site

        Parameters
        ----------
        req_options : Optional[RequestOptions]
            Filter and sorting options for the request.

        Returns
        -------
        tuple[list[WebhookItem], PaginationItem]
            A tuple of the list of webhooks and pagination item
        """
        logger.info("Querying all Webhooks on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        all_webhook_items = WebhookItem.from_response(server_response.content, self.parent_srv.namespace)
        pagination_item = PaginationItem.from_single_page_list(all_webhook_items)
        return all_webhook_items, pagination_item

    @api(version="3.6")
    def get_by_id(self, webhook_id: str) -> WebhookItem:
        """
        Returns information about a specified Webhook.

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#get_webhook

        Parameters
        ----------
        webhook_id : str
            The ID of the webhook to query.

        Returns
        -------
        WebhookItem
            An object containing information about the webhook.
        """
        if not webhook_id:
            error = "Webhook ID undefined."
            raise ValueError(error)
        logger.info(f"Querying single webhook (ID: {webhook_id})")
        url = f"{self.baseurl}/{webhook_id}"
        server_response = self.get_request(url)
        return WebhookItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.6")
    def delete(self, webhook_id: str) -> None:
        """
        Deletes a specified webhook.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#delete_webhook

        Parameters
        ----------
        webhook_id : str
            The ID of the webhook to delete.

        Returns
        -------
        None
        """
        if not webhook_id:
            error = "Webhook ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{webhook_id}"
        self.delete_request(url)
        logger.info(f"Deleted single webhook (ID: {webhook_id})")

    @api(version="3.6")
    def create(self, webhook_item: WebhookItem) -> WebhookItem:
        """
        Creates a new webhook on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#create_webhook

        Parameters
        ----------
        webhook_item : WebhookItem
            The webhook item to create.

        Returns
        -------
        WebhookItem
            An object containing information about the created webhook
        """
        url = self.baseurl
        create_req = RequestFactory.Webhook.create_req(webhook_item)
        server_response = self.post_request(url, create_req)
        new_webhook = WebhookItem.from_response(server_response.content, self.parent_srv.namespace)[0]

        logger.info(f"Created new webhook (ID: {new_webhook.id})")
        return new_webhook

    @api(version="3.6")
    def test(self, webhook_id: str):
        """
        Tests the specified webhook. Sends an empty payload to the configured
        destination URL of the webhook and returns the response from the server.
        This is useful for testing, to ensure that things are being sent from
        Tableau and received back as expected.

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#test_webhook

        Parameters
        ----------
        webhook_id : str
            The ID of the webhook to test.

        Returns
        -------
        XML Response

        """
        if not webhook_id:
            error = "Webhook ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{webhook_id}/test"
        testOutcome = self.get_request(url)
        logger.info(f"Testing webhook (ID: {webhook_id} returned {testOutcome})")
        return testOutcome
