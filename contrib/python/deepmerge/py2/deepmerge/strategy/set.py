from .core import StrategyList


class SetStrategies(StrategyList):
    """
    Contains the strategies provided for sets.
    """

    NAME = "set"

    @staticmethod
    def strategy_union(config, path, base, nxt):
        """
        use all values in either base or nxt.
        """
        return base | nxt

    @staticmethod
    def strategy_intersect(config, path, base, nxt):
        """
        use all values in both base and nxt.
        """
        return base & nxt

    @staticmethod
    def strategy_override(config, path, base, nxt):
        """
        use the set nxt.
        """
        return nxt
