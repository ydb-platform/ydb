from office365.runtime.client_value import ClientValue


class ListItemUpdateParameters(ClientValue):

    def __init__(self, bypass_quota_check=None, bypass_shared_lock=None):
        self.BypassQuotaCheck = bypass_quota_check
        self.BypassSharedLock = bypass_shared_lock
