from abc import ABCMeta


class ODataJsonFormat(object):
    """OData JSON format"""

    def __init__(self, metadata_level=None):
        # type: (str) -> None
        self.metadata_level = metadata_level

    __metaclass__ = ABCMeta

    @property
    def metadata_type(self):
        raise NotImplementedError

    @property
    def collection(self):
        raise NotImplementedError

    @property
    def collection_next(self):
        raise NotImplementedError

    @property
    def collection_delta(self):
        raise NotImplementedError

    @property
    def etag(self):
        raise NotImplementedError

    @property
    def value_tag(self):
        raise NotImplementedError

    @property
    def media_type(self):
        # type: () -> str
        """Gets media type"""
        raise NotImplementedError

    @property
    def include_control_information(self):
        # type: () -> bool
        """Determines whether control information that is represented as annotations should be included in payload"""
        raise NotImplementedError
