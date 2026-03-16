from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class UploadSession(ClientValue):
    """The UploadSession resource provides information about how to upload large files to OneDrive, OneDrive for
    Business, or SharePoint document libraries."""

    def __init__(
        self, upload_url=None, expiration_datetime=None, next_expected_ranges=None
    ):
        """
        :param str upload_url: The URL endpoint that accepts PUT requests for byte ranges of the file.
        :param datetime expiration_datetime: The date and time in UTC that the upload session will expire.
            The complete file must be uploaded before this expiration time is reached.
        :param list[str] next_expected_ranges: A collection of byte ranges that the server is missing for the file.
            These ranges are zero indexed and of the format "start-end" (e.g. "0-26" to indicate the first 27 bytes
            of the file). When uploading files as Outlook attachments, instead of a collection of ranges,
            this property always indicates a single value "{start}", the location in the file where the next upload
            should begin.
        """
        super(UploadSession, self).__init__()
        self.uploadUrl = upload_url
        self.expirationDateTime = expiration_datetime
        self.nextExpectedRanges = StringCollection(next_expected_ranges)
