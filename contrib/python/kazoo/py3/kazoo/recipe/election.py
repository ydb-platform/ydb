"""ZooKeeper Leader Elections

:Maintainer: None
:Status: Unknown

"""
from kazoo.exceptions import CancelledError


class Election(object):
    """Kazoo Basic Leader Election

    Example usage with a :class:`~kazoo.client.KazooClient` instance::

        zk = KazooClient()
        zk.start()
        election = zk.Election("/electionpath", "my-identifier")

        # blocks until the election is won, then calls
        # my_leader_function()
        election.run(my_leader_function)

    """

    def __init__(self, client, path, identifier=None):
        """Create a Kazoo Leader Election

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The election path to use.
        :param identifier: Name to use for this lock contender. This
                           can be useful for querying to see who the
                           current lock contenders are.

        """
        self.lock = client.Lock(path, identifier)

    def run(self, func, *args, **kwargs):
        """Contend for the leadership

        This call will block until either this contender is cancelled
        or this contender wins the election and the provided leadership
        function subsequently returns or fails.

        :param func: A function to be called if/when the election is
                     won.
        :param args: Arguments to leadership function.
        :param kwargs: Keyword arguments to leadership function.

        """
        if not callable(func):
            raise ValueError("leader function is not callable")

        try:
            with self.lock:
                func(*args, **kwargs)

        except CancelledError:
            pass

    def cancel(self):
        """Cancel participation in the election

        .. note::

            If this contender has already been elected leader, this
            method will not interrupt the leadership function.

        """
        self.lock.cancel()

    def contenders(self):
        """Return an ordered list of the current contenders in the
        election

        .. note::

            If the contenders did not set an identifier, it will appear
            as a blank string.

        """
        return self.lock.contenders()
