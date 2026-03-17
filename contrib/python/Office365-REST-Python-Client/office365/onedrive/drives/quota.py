from office365.runtime.client_value import ClientValue


class Quota(ClientValue):
    """
    The quota resource provides details about space constraints on a drive resource.
    In OneDrive Personal, the values reflect the total/used unified storage quota across multiple Microsoft services.
    """

    def __init__(self, deleted=None, remaining=None, state=None):
        """
        :param int deleted: Total space consumed by files in the recycle bin, in bytes. Read-only.
        :param int remaining: Total space remaining before reaching the quota limit, in bytes. Read-only.
        :param str state: Enumeration value that indicates the state of the storage space. Read-only.
        """
        self.deleted = deleted
        self.remaining = remaining
        self.state = state
