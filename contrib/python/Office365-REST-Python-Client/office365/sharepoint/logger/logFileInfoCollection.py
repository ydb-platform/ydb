from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.logger.logFileInfo import LogFileInfo


class LogFileInfoCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(LogFileInfoCollection, self).__init__(context, LogFileInfo, resource_path)
