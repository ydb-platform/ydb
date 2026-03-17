class Member(object):
    """
    A member of the etcd cluster.

    :ivar id: ID of the member
    :ivar name: human-readable name of the member
    :ivar peer_urls: list of URLs the member exposes to the cluster for
                     communication
    :ivar client_urls: list of URLs the member exposes to clients for
                       communication
    """

    def __init__(self, id, name, peer_urls, client_urls, etcd_client=None):
        self.id = id
        self.name = name
        self.peer_urls = peer_urls
        self.client_urls = client_urls
        self._etcd_client = etcd_client

    def __str__(self):
        return ('Member {id}: peer urls: {peer_urls}, client '
                'urls: {client_urls}'.format(id=self.id,
                                             peer_urls=self.peer_urls,
                                             client_urls=self.client_urls))

    def remove(self):
        """Remove this member from the cluster."""
        self._etcd_client.remove_member(self.id)

    def update(self, peer_urls):
        """
        Update the configuration of this member.

        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        """
        self._etcd_client.update_member(self.id, peer_urls)

    @property
    def active_alarms(self):
        """Get active alarms of the member.

        :returns: Alarms
        """
        return self._etcd_client.list_alarms(member_id=self.id)
