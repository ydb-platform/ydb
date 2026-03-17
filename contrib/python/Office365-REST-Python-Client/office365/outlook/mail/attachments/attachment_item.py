import os

from office365.runtime.client_value import ClientValue


class AttachmentItem(ClientValue):
    """Represents attributes of an item to be attached."""

    def __init__(self, attachment_type=None, name=None, size=None):
        """
        :param int attachment_type:
        :param str name:
        :param int size:
        """
        super(AttachmentItem, self).__init__()
        self.attachmentType = attachment_type
        self.name = name
        self.size = size

    @staticmethod
    def create_file(path):
        file_name = os.path.basename(path)
        file_size = os.stat(path).st_size
        from office365.outlook.mail.attachments.attachment_type import AttachmentType

        return AttachmentItem(
            attachment_type=AttachmentType.file, name=file_name, size=file_size
        )
