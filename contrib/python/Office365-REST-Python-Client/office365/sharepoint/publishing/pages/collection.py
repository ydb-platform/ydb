from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.publishing.pages.metadata_collection import (
    SitePageMetadataCollection,
)
from office365.sharepoint.publishing.pages.page import SitePage
from office365.sharepoint.translation.status_collection import (
    TranslationStatusCollection,
)


class SitePageCollection(SitePageMetadataCollection[SitePage]):
    def __init__(self, context, resource_path=None):
        """Specifies a collection of site pages."""
        if resource_path is None:
            resource_path = ResourcePath("SP.Publishing.SitePageCollection")
        super(SitePageCollection, self).__init__(context, SitePage, resource_path)

    def add(self):
        """Adds Site Page"""
        return_type = SitePage(self.context)
        return_type.set_property("Title", "", True)
        qry = CreateEntityQuery(self, return_type, return_type)
        self.context.add_query(qry)
        self.add_child(return_type)
        return return_type

    def create_app_page(self, web_part_data=None):
        """
        :param str web_part_data:
        """
        return_type = ClientResult(self.context)
        payload = {"webPartDataAsJson": web_part_data}
        qry = ServiceOperationQuery(
            self, "CreateAppPage", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_translations(self, source_item_id=None):
        """
        :param str source_item_id:
        """
        return_type = TranslationStatusCollection(self.context)
        qry = ServiceOperationQuery(
            self, "GetTranslations", [source_item_id], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def is_site_page(self, url):
        """
        Indicates whether a specific item is a modern site page.
        :param str url: URL of the SitePage to be checked.
        """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(self, "IsSitePage", [url], None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_page_column_state(self, url):
        """
        Determines whether a specific SitePage is a single or multicolumn page.

        :param str url: URL of the SitePage for which to return state.
        """
        return_type = ClientResult(self.context, int())
        qry = ServiceOperationQuery(
            self, "GetPageColumnState", [url], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_by_url(self, url):
        """Gets the site page with the specified server relative url.

        :param str url: Specifies the server relative url of the site page.
        """
        return SitePage(
            self.context, ServiceOperationPath("GetByUrl", [url], self.resource_path)
        )

    def get_by_name(self, name):
        """Gets the site page with the specified file name.
        :param str name: Specifies the name of the site page.
        """
        return self.single("FileName eq '{0}'".format(name))

    def templates(self):
        """"""
        return SitePageMetadataCollection(
            self.context,
            SitePage,
            ServiceOperationPath("Templates", None, self.resource_path),
        )

    def update_full_page_app(self, server_relative_url, web_part_data_as_json):
        """
        :param str server_relative_url:
        :param str web_part_data_as_json:
        """
        payload = {
            "serverRelativeUrl": server_relative_url,
            "webPartDataAsJson": web_part_data_as_json,
        }
        qry = ServiceOperationQuery(self, "UpdateFullPageApp", None, payload)
        self.context.add_query(qry)
        return self
