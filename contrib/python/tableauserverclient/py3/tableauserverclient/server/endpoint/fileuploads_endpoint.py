from .endpoint import Endpoint, api
from tableauserverclient import datetime_helpers as datetime
from tableauserverclient.helpers.logging import logger

from tableauserverclient.config import BYTES_PER_MB, config
from tableauserverclient.models import FileuploadItem
from tableauserverclient.server import RequestFactory


class Fileuploads(Endpoint):
    def __init__(self, parent_srv):
        super().__init__(parent_srv)

    @property
    def baseurl(self):
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/fileUploads"

    @api(version="2.0")
    def initiate(self):
        url = self.baseurl
        server_response = self.post_request(url, "")
        fileupload_item = FileuploadItem.from_response(server_response.content, self.parent_srv.namespace)
        upload_id = fileupload_item.upload_session_id
        logger.info(f"Initiated file upload session (ID: {upload_id})")
        return upload_id

    @api(version="2.0")
    def append(self, upload_id, data, content_type):
        url = f"{self.baseurl}/{upload_id}"
        server_response = self.put_request(url, data, content_type)
        logger.info(f"Uploading a chunk to session (ID: {upload_id})")
        return FileuploadItem.from_response(server_response.content, self.parent_srv.namespace)

    def _read_chunks(self, file):
        file_opened = False
        try:
            file_content = open(file, "rb")
            file_opened = True
        except TypeError:
            file_content = file

        try:
            while True:
                chunked_content = file_content.read(config.CHUNK_SIZE_MB * BYTES_PER_MB)
                if not chunked_content:
                    break
                yield chunked_content
        finally:
            if file_opened:
                file_content.close()

    def upload(self, file):
        upload_id = self.initiate()
        for chunk in self._read_chunks(file):
            logger.debug(f"{datetime.timestamp()} processing chunk...")
            request, content_type = RequestFactory.Fileupload.chunk_req(chunk)
            logger.debug(f"{datetime.timestamp()} created chunk request")
            fileupload_item = self.append(upload_id, request, content_type)
            logger.info(f"\t{datetime.timestamp()} Published {fileupload_item.file_size}MB")
        logger.info(f"File upload finished (ID: {upload_id})")
        return upload_id
