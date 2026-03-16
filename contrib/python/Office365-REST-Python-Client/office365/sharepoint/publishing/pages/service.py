from typing import TYPE_CHECKING, Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.administration.orgassets.org_assets import OrgAssets
from office365.sharepoint.clientsidecomponent.query_result import (
    SPClientSideComponentQueryResult,
)
from office365.sharepoint.entity import Entity
from office365.sharepoint.files.file import File
from office365.sharepoint.publishing.file_picker_options import FilePickerOptions
from office365.sharepoint.publishing.pages.collection import SitePageCollection
from office365.sharepoint.publishing.pages.page import SitePage
from office365.sharepoint.publishing.primary_city_time import PrimaryCityTime
from office365.sharepoint.publishing.sites.communication.site import CommunicationSite

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class SitePageService(Entity):
    """Represents a set of APIs to use for managing site pages."""

    def __init__(self, context, resource_path=None):
        """Represents a set of APIs to use for managing site pages."""
        if resource_path is None:
            resource_path = ResourcePath("SP.Publishing.SitePageService")
        super(SitePageService, self).__init__(context, resource_path)

    @property
    def pages(self):
        # type: () -> SitePageCollection
        """Gets the SitePageCollection for the current web."""
        return self.properties.get(
            "pages",
            SitePageCollection(self.context, ResourcePath("pages", self.resource_path)),
        )

    @property
    def communication_site(self):
        # type: () -> CommunicationSite
        """Gets a CommunicationSite for the current web."""
        return self.properties.get(
            "CommunicationSite",
            CommunicationSite(
                self.context, ResourcePath("CommunicationSite", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        return "SP.Publishing.SitePageService"

    def create_page(self, title, language=None):
        # type: (str, Optional[str]) -> SitePage
        """Create a new sitePage in the site pages list in a site.
        :param str title: The title of Site Page
        :param str language: The language of the Site Page.
        """

        def _page_created(return_type):
            # type: (SitePage) -> None

            def _draft_saved(result):
                # type: (ClientResult[bool]) -> None
                return_type.get()

            return_type.save_draft(title=title).after_execute(
                _draft_saved, execute_first=True
            )

        return self.pages.add().after_execute(_page_created)

    def create_and_publish_page(self, title):
        """
        Create and publish a new sitePage in the site pages list in a site.
        :param str title: The title of Site Page
        """

        def _page_created(return_type):
            # type: (SitePage) -> None

            def _page_published(result):
                # type: (ClientResult[bool]) -> None
                pass

            return_type.publish().after_execute(_page_published)

        return self.create_page(title).after_execute(_page_created)

    def can_create_page(self):
        """
        Checks if the current user has permission to create a site page on the site pages document library.
        MUST return true if the user has permission to create a site page, otherwise MUST return false.
        """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "CanCreatePage", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def can_create_promoted_page(self):
        """
        Checks if the current user has permission to create a site page on the site pages document library.
        MUST return true if the user has permission to create a site page, otherwise MUST return false.
        """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "CanCreatePromotedPage", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def get_current_user_memberships(context, scenario=None):
        # type: (ClientContext, Optional[str]) -> ClientResult[StringCollection]
        return_type = ClientResult(context, StringCollection())
        svc = SitePageService(context)
        qry = ServiceOperationQuery(
            svc, "GetCurrentUserMemberships", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_time_zone(context, city_name):
        # type: (ClientContext, str) -> PrimaryCityTime
        """
        Gets time zone data for specified city.
        :param office365.sharepoint.client_context.ClientContext context:
        :param str city_name: The name of the city.
        """
        return_type = PrimaryCityTime(context)
        binding_type = SitePageService(context)
        params = {"cityName": city_name}
        qry = ServiceOperationQuery(
            binding_type, "GetTimeZone", params, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def compute_file_name(context, title):
        # type: (ClientContext, str) -> ClientResult[str]
        """
        :param office365.sharepoint.client_context.ClientContext context: Client context
        :param str title: The title of the page.
        """
        return_type = ClientResult(context)
        binding_type = SitePageService(context)
        params = {"title": title}
        qry = ServiceOperationQuery(
            binding_type, "ComputeFileName", params, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_available_full_page_applications(
        context, include_errors=None, project=None
    ):
        return_type = ClientResult(
            context, ClientValueCollection(SPClientSideComponentQueryResult)
        )
        params = {"includeErrors": include_errors, "project": project}
        qry = ServiceOperationQuery(
            SitePageService(context),
            "GetAvailableFullPageApplications",
            None,
            params,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def is_file_picker_external_image_search_enabled(context):
        # type: (ClientContext) -> ClientResult[bool]
        return_type = ClientResult(context)
        binding_type = SitePageService(context)
        qry = ServiceOperationQuery(
            binding_type,
            "IsFilePickerExternalImageSearchEnabled",
            None,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def org_assets(context):
        # type: (ClientContext) -> ClientResult[OrgAssets]
        return_type = ClientResult(context, OrgAssets())
        svc = SitePageService(context)
        qry = ServiceOperationQuery(
            svc, "OrgAssets", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def file_picker_tab_options(context):
        # type: (ClientContext) -> ClientResult[FilePickerOptions]
        return_type = ClientResult(context, FilePickerOptions())
        svc = SitePageService(context)
        qry = ServiceOperationQuery(
            svc, "FilePickerTabOptions", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    def add_image(self, page_name, image_file_name, image_stream):
        """
        Adds an image to the site assets library of the current web.
        Returns a File object ([MS-CSOMSPT] section 3.2.5.64) that represents the image.

        :param str image_stream: The image stream.
        :param str image_file_name: Indicates the file name of the image to be added.
        :param str page_name: Indicates the name of that site page that the image is to be used in.
        :return: File
        """
        return_type = File(self.context)
        params = {
            "pageName": page_name,
            "imageFileName": image_file_name,
            "imageStream": image_stream,
        }
        qry = ServiceOperationQuery(
            self, "AddImage", params, None, None, return_type, True
        )
        self.context.add_query(qry)
        return return_type

    def add_image_from_external_url(
        self, page_name, image_file_name, external_url, sub_folder_name, page_id
    ):
        """
        Adds an image to the site assets library of the current web.
        Returns a File object ([MS-CSOMSPT] section 3.2.5.64) that represents the image.

        :param str image_file_name: Indicates the file name of the image to be added.
        :param str page_name: Indicates the name of that site page that the image is to be used in.
        :param str external_url:
        :param str sub_folder_name:
        :param str page_id:
        """
        return_type = File(self.context)
        params = {
            "pageName": page_name,
            "imageFileName": image_file_name,
            "externalUrl": external_url,
            "subFolderName": sub_folder_name,
            "pageId": page_id,
        }
        qry = ServiceOperationQuery(
            self, "AddImageFromExternalUrl", params, None, None, return_type
        )
        qry.static = True
        self.context.add_query(qry)
        return return_type

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "CommunicationSite": self.communication_site,
            }
            default_value = property_mapping.get(name, None)
        return super(SitePageService, self).get_property(name, default_value)
