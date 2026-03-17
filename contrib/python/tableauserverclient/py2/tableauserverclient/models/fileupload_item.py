import xml.etree.ElementTree as ET


class FileuploadItem(object):
    def __init__(self):
        self._file_size = None
        self._upload_session_id = None

    @property
    def upload_session_id(self):
        return self._upload_session_id

    @property
    def file_size(self):
        return self._file_size

    @classmethod
    def from_response(cls, resp, ns):
        parsed_response = ET.fromstring(resp)
        fileupload_elem = parsed_response.find('.//t:fileUpload', namespaces=ns)
        fileupload_item = cls()
        fileupload_item._upload_session_id = fileupload_elem.get('uploadSessionId', None)
        fileupload_item._file_size = fileupload_elem.get('fileSize', None)
        return fileupload_item
