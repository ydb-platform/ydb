from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class SiteScriptSerializationResult(ClientValue):
    def __init__(self, json=None, warnings=None):
        """
        :param str json:
        :param list[str] warnings:
        """
        self.JSON = json
        self.Warnings = StringCollection(warnings)
