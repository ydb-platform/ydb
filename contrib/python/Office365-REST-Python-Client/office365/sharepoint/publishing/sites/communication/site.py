from office365.runtime.client_result import ClientResult
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.publishing.sites.communication.creation_request import (
    CommunicationSiteCreationRequest,
)
from office365.sharepoint.publishing.sites.communication.creation_response import (
    CommunicationSiteCreationResponse,
)


class CommunicationSite(Entity):
    """Represents a Communication Site."""

    def create(self, title, site_url, description=None):
        """
        Initiates creation of a Communication Site.

        - If the SiteStatus returns 1, the Communication Site is in the process of being created asynchronously.

        - If the SiteStatus returns 2 and the SiteUrl returns a non-empty, non-null value, the site was created
        synchronously and is available at the specified URL.

        - If the SiteStatus returns 2 and the SiteUrl returns an empty or null value, the site already exists but is
        inaccessible for some reason, such as being "locked".

        - If the SiteStatus returns 3 or 0, the Communication site failed to be created.

        :param str site_url: Site url
        :param str title: Site title
        :param str description: Site description
        """
        request = CommunicationSiteCreationRequest(title, site_url, description)
        return_type = ClientResult(self.context, CommunicationSiteCreationResponse())
        qry = ServiceOperationQuery(
            self, "Create", None, {"request": request}, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_status(self, site_url):
        """
        Retrieves the status of creation of a Communication site.

        If the SiteStatus returned is 0, then no work item for a site with the specified URL was found, and no site was
        found with the specified URL. This could mean either that a creation attempt hasn’t started yet, or that it
        failed with a “non-retryable” exception and did not preserve a work item for further attempts.

        If the SiteStatus returns 1, the Communication Site is in the process of being created asynchronously.

        If the SiteStatus returns 2 and the SiteUrl returns a non-empty, non-null value, the site was created
        synchronously and is available at the specified URL.

        If the SiteStatus returns 2 and the SiteUrl returns an empty or null value, the site already exists but
        is inaccessible for some reason, such as being “locked”.

        If the SiteStatus returns 3 or 0, the Communication site failed to be created.
        """
        response = ClientResult(self.context, CommunicationSiteCreationResponse())
        qry = ServiceOperationQuery(
            self, "Status", None, {"url": site_url}, None, response
        )

        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.method = HttpMethod.Get
            request.url += "?url='{0}'".format(site_url)

        self.context.add_query(qry).before_query_execute(_construct_request)
        return response

    def enable(self, design_package_id):
        qry = ServiceOperationQuery(
            self, "Enable", None, {"designPackageId": design_package_id}
        )
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "SP.Publishing.CommunicationSite"
