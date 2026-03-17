from office365.delta_collection import DeltaCollection
from office365.outlook.mail.folders.folder import MailFolder


class MailFolderCollection(DeltaCollection[MailFolder]):
    def __init__(self, context, resource_path=None):
        super(MailFolderCollection, self).__init__(context, MailFolder, resource_path)
