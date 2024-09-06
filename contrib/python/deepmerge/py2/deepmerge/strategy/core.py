from ..exception import StrategyNotFound, InvalidMerge
from ..compat import string_type

STRATEGY_END = object()


class StrategyList(object):

    NAME = None

    def __init__(self, strategy_list):
        if not isinstance(strategy_list, list):
            strategy_list = [strategy_list]
        self._strategies = [self._expand_strategy(s) for s in strategy_list]

    @classmethod
    def _expand_strategy(cls, strategy):
        """
        :param strategy: string or function

        If the strategy is a string, attempt to resolve it
        among the built in strategies.

        Otherwise, return the value, implicitly assuming it's a function.
        """
        if isinstance(strategy, string_type):
            method_name = "strategy_{0}".format(strategy)
            if not hasattr(cls, method_name):
                raise StrategyNotFound(strategy)
            return getattr(cls, method_name)
        return strategy

    def __call__(self, *args, **kwargs):
        for s in self._strategies:
            ret_val = s(*args, **kwargs)
            if ret_val is not STRATEGY_END:
                return ret_val
        raise InvalidMerge(self.NAME, args, kwargs)
