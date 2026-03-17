from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.publishing.pages.coauth_state import SitePageCoAuthState
from office365.sharepoint.publishing.pages.dependency_metadata import (
    SitePageDependencyMetadata,
)
from office365.sharepoint.publishing.pages.fields_data import SitePageFieldsData
from office365.sharepoint.publishing.pages.metadata import SitePageMetadata
from office365.sharepoint.translation.status_collection import (
    TranslationStatusCollection,
)


class SharePagePreviewByEmailFieldsData(ClientValue):
    """This class contains the information used by SharePagePreviewByEmail method"""

    def __init__(self, message=None, recipient_emails=None):
        """
        :param str message:
        :param list[str] recipient_emails:
        """
        self.message = message
        self.recipientEmails = StringCollection(recipient_emails)

    @property
    def entity_type_name(self):
        return "SP.Publishing.SharePagePreviewByEmailFieldsData"


class SitePage(SitePageMetadata):
    """Represents a Site Page."""

    def checkout_page(self):
        """Checks out the current Site Page if it is available to be checked out."""
        qry = ServiceOperationQuery(self, "CheckoutPage", None, None, None, self)
        self.context.add_query(qry)
        return self

    def copy(self):
        """Creates a copy of the current Site Page and returns the resulting new SitePage."""
        qry = ServiceOperationQuery(self, "Copy")
        self.context.add_query(qry)
        return self

    def discard_page(self):
        """Discards the current checked out version of the Site Page.  Returns the resulting SitePage after discard."""
        qry = ServiceOperationQuery(self, "DiscardPage", return_type=self)
        self.context.add_query(qry)
        return self

    def ensure_title_resource(self):
        """"""
        qry = ServiceOperationQuery(self, "EnsureTitleResource")
        self.context.add_query(qry)
        return self

    def get_dependency_metadata(self):
        """ """
        return_type = ClientResult(
            self.context, ClientValueCollection(SitePageDependencyMetadata)
        )
        qry = ServiceOperationQuery(
            self, "GetDependencyMetadata", return_type=return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_version(self, version_id):
        """ """
        return_type = SitePage(self.context, self.resource_path)
        qry = ServiceOperationQuery(
            self, "GetVersion", [version_id], None, None, return_type
        )
        self.context.add_query(qry)

    def save_page(
        self, title, canvas_content=None, banner_image_url=None, topic_header=None
    ):
        """
        Updates the current Site Page with the provided pageStream content.

        :param str title: The title of Site Page
        :param str canvas_content:
        :param str banner_image_url:
        :param str topic_header:
        """
        payload = SitePageFieldsData(
            title=title,
            canvas_content=canvas_content,
            banner_image_url=banner_image_url,
            topic_header=topic_header,
        )
        qry = ServiceOperationQuery(self, "SavePage", None, payload, "pageStream")
        self.context.add_query(qry)
        return self

    def save_draft(
        self, title, canvas_content=None, banner_image_url=None, topic_header=None
    ):
        """
        Updates the Site Page with the provided sitePage metadata and checks in a minor version if the page library
        has minor versions enabled.

        :param str title: The title of Site Page
        :param str canvas_content:
        :param str banner_image_url:
        :param str topic_header:
        """
        payload = SitePageFieldsData(
            title=title,
            canvas_content=canvas_content,
            banner_image_url=banner_image_url,
            topic_header=topic_header,
        )
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "SaveDraft", None, payload, "sitePage", return_type
        )
        self.context.add_query(qry)
        return return_type

    def save_page_as_draft(
        self, title, canvas_content=None, banner_image_url=None, topic_header=None
    ):
        """
        Updates the Site Page with the provided pageStream content and checks in a minor version if the page library
        has minor versions enabled.

        :param str title: The title of Site Page. At least Title property needs to be provided
        :param str canvas_content:
        :param str banner_image_url:
        :param str topic_header:
        """
        payload = SitePageFieldsData(
            title=title,
            canvas_content=canvas_content,
            banner_image_url=banner_image_url,
            topic_header=topic_header,
        )
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "SavePageAsDraft", None, payload, "pageStream", return_type
        )
        self.context.add_query(qry)
        return return_type

    def save_page_as_template(self):
        """ """
        return_type = SitePage(self.context)
        qry = ServiceOperationQuery(
            self, "SavePageAsTemplate", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def save_page_co_auth(self, page_stream):
        """ """
        return_type = ClientResult(self.context, SitePageCoAuthState())
        payload = {"pageStream": page_stream}
        qry = ServiceOperationQuery(
            self, "SavePageCoAuth", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def demote_from_news(self):
        """
        Updates the promoted state of the site page to 0. On success MUST return true.
        If the site page already has promoted state as 0, MUST return true. If the site page is not checked out
        to the current user,
        the server MUST throw Microsoft.SharePoint.Client.ClientServiceException with ErrorInformation.HttpStatusCode
        set to 409.
        """
        result = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "DemoteFromNews", None, None, None, result)
        self.context.add_query(qry)
        return result

    def promote_to_news(self):
        """
        Updates the promoted state of the site page to 1 if the site page has not been published yet.
        Updates the promoted state of the site page to 2 if the site page has already been published.
        If the site page already has promoted state set to 1 or 2, MUST return true.
        If the site page is not checked out to the current users,
        the server MUST throw Microsoft.SharePoint.Client.ClientServiceException with ErrorInformation.HttpStatusCode
        set to 409.
        """
        result = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "PromoteToNews", None, None, None, result)
        self.context.add_query(qry)
        return result

    def publish(self):
        """
        Publishes a major version of the current Site Page.  Returns TRUE on success, FALSE otherwise.
        """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(self, "Publish", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def schedule_publish(self, publish_start_date):
        """
        Schedules the page publication for a certain date
        :param datetime.datetime publish_start_date: The pending publication scheduled date
        """
        payload = SitePageFieldsData(publish_start_date=publish_start_date)
        result = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "SchedulePublish", None, payload, "sitePage", result
        )
        self.context.add_query(qry)
        return result

    def share_page_preview_by_email(self, message, recipient_emails):
        """
        :param str message:
        :param list[str] recipient_emails:
        """
        payload = SharePagePreviewByEmailFieldsData(message, recipient_emails)
        qry = ServiceOperationQuery(self, "SharePagePreviewByEmail", None, payload)
        self.context.add_query(qry)
        return self

    def start_co_auth(self):
        """"""
        return_type = SitePage(self.context, self.resource_path)
        qry = ServiceOperationQuery(self, "StartCoAuth", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def canvas_content(self):
        # type: () -> Optional[str]
        """Gets the CanvasContent1 for the current Site Page"""
        return self.properties.get("CanvasContent1", None)

    @property
    def language(self):
        # type: () -> Optional[str]
        """Gets the Language for the Site Page."""
        return self.properties.get("Language", None)

    @canvas_content.setter
    def canvas_content(self, value):
        # type: (str) -> None
        """Sets the CanvasContent1 for the current Site Page"""
        self.set_property("CanvasContent1", value)

    @property
    def layout_web_parts_content(self):
        # type: () -> Optional[str]
        """Gets the LayoutWebPartsContent field for the current Site Page."""
        return self.properties.get("LayoutWebpartsContent", None)

    @layout_web_parts_content.setter
    def layout_web_parts_content(self, value):
        # type: (str) -> None
        """Sets the LayoutWebPartsContent field for the current Site Page."""
        self.set_property("LayoutWebpartsContent", value)

    @property
    def translations(self):
        # type: () -> TranslationStatusCollection
        return self.properties.get(
            "Translations",
            TranslationStatusCollection(
                self.context, ResourcePath("Translations", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        # type: () -> str
        return "SP.Publishing.SitePage"
