class Lease(object):
    """
    A lease.

    :ivar id: ID of the lease
    :ivar ttl: time to live for this lease
    """

    def __init__(self, lease_id, ttl, etcd_client=None):
        self.id = lease_id
        self.ttl = ttl

        self.etcd_client = etcd_client

    def _get_lease_info(self):
        return self.etcd_client.get_lease_info(self.id)

    def revoke(self):
        """Revoke this lease."""
        self.etcd_client.revoke_lease(self.id)

    def refresh(self):
        """Refresh the time to live for this lease."""
        return list(self.etcd_client.refresh_lease(self.id))

    @property
    def remaining_ttl(self):
        return self._get_lease_info().TTL

    @property
    def granted_ttl(self):
        return self._get_lease_info().grantedTTL

    @property
    def keys(self):
        return self._get_lease_info().keys
