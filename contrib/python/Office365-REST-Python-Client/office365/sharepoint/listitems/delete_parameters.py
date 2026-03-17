from office365.runtime.client_value import ClientValue


class ListItemDeleteParameters(ClientValue):
    def __init__(self, bypass_shared_lock=None):
        """
        :param bool bypass_shared_lock:
        """
        self.BypassSharedLock = bypass_shared_lock
