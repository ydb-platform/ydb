from office365.runtime.client_value import ClientValue


class SitePageCoAuthState(ClientValue):

    def __init__(
        self,
        action=None,
        has_reached_minor_versions_limit=None,
        is_new_session=None,
        is_partition_flushed=None,
        lock_action=None,
        lock_duration=None,
        overwrite_existing_version=None,
        shared_lock_id=None,
    ):
        self.Action = action
        self.HasReachedMinorVersionsLimit = has_reached_minor_versions_limit
        self.IsNewSession = is_new_session
        self.IsPartitionFlushed = is_partition_flushed
        self.LockAction = lock_action
        self.LockDuration = lock_duration
        self.OverwriteExistingVersion = overwrite_existing_version
        self.SharedLockId = shared_lock_id

    @property
    def entity_type_name(self):
        return "SP.Publishing.SitePageCoAuthState"
