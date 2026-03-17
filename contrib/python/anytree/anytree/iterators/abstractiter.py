class AbstractIter:
    # pylint: disable=R0205
    """
    Iterate over tree starting at `node`.

    Base class for all iterators.

    Keyword Args:
        filter_: function called with every `node` as argument, `node` is returned if `True`.
        stop: stop iteration at `node` if `stop` function returns `True` for `node`.
        maxlevel (int): maximum descending in the node hierarchy.
    """

    def __init__(self, node, filter_=None, stop=None, maxlevel=None):
        self.node = node
        self.filter_ = filter_
        self.stop = stop
        self.maxlevel = maxlevel
        self.__iter = None

    def __init(self):
        node = self.node
        maxlevel = self.maxlevel
        filter_ = self.filter_ or AbstractIter.__default_filter
        stop = self.stop or AbstractIter.__default_stop
        children = [] if AbstractIter._abort_at_level(1, maxlevel) else AbstractIter._get_children([node], stop)
        return self._iter(children, filter_, stop, maxlevel)

    @staticmethod
    def __default_filter(node):
        # pylint: disable=W0613
        return True

    @staticmethod
    def __default_stop(node):
        # pylint: disable=W0613
        return False

    def __iter__(self):
        return self

    def __next__(self):
        if self.__iter is None:
            self.__iter = self.__init()
        return next(self.__iter)

    @staticmethod
    def _iter(children, filter_, stop, maxlevel):
        raise NotImplementedError

    @staticmethod
    def _abort_at_level(level, maxlevel):
        return maxlevel is not None and level > maxlevel

    @staticmethod
    def _get_children(children, stop):
        return [child for child in children if not stop(child)]
