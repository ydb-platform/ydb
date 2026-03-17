from office365.runtime.client_value import ClientValue


class DocumentLibraryInformation(ClientValue):
    """Specifies the information for a document library on a site."""

    def __init__(
        self,
        title=None,
        absolute_url=None,
        server_relative_url=None,
        drive_id=None,
        from_cross_farm=None,
        is_default_document_library=None,
    ):
        """
        :param str title:Identifies the title of the document library
        :param str absolute_url: Absolute Url of the document library.
        :param str server_relative_url: Identifies the server-relative URL of the document library.
        :param str drive_id:
        :param bool from_cross_farm:
        :param bool is_default_document_library:
        """
        self.Title = title
        self.AbsoluteUrl = absolute_url
        self.ServerRelativeUrl = server_relative_url
        self.DriveId = drive_id
        self.FromCrossFarm = from_cross_farm
        self.IsDefaultDocumentLibrary = is_default_document_library
