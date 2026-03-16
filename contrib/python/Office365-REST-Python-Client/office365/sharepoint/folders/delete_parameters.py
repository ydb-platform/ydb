from office365.runtime.client_value import ClientValue


class FolderDeleteParameters(ClientValue):
    def __init__(self, bypass_shared_lock=None, delete_if_empty=None, etag_match=None):
        """
        :param bool bypass_shared_lock:
        :param bool delete_if_empty:
        :param bool etag_match:
        """
        self.BypassSharedLock = bypass_shared_lock
        self.DeleteIfEmpty = delete_if_empty
        self.ETagMatch = etag_match
