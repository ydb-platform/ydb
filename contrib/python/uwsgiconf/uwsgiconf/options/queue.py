from ..base import OptionsGroup


class Queue(OptionsGroup):
    """Queue.

    At the low level it is a simple block-based shared array,
    with two optional counters, one for stack-style, LIFO usage,
    the other one for FIFO.

    http://uwsgi-docs.readthedocs.io/en/latest/Queue.html

    """

    def enable(self, size, *, block_size=None, store=None, store_sync_interval=None):
        """Enables shared queue of the given size.

        :param int size: Queue size.

        :param int block_size: Block size in bytes. Default: 8 KiB.

        :param str store: Persist the queue into file.

        :param int store_sync_interval: Store sync interval in master cycles (usually seconds).

        """
        self._set('queue', size)
        self._set('queue-blocksize', block_size)
        self._set('queue-store', store)
        self._set('queue-store-sync', store_sync_interval)

        return self._section
