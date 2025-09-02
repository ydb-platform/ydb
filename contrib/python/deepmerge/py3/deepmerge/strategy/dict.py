from .core import StrategyList


class DictStrategies(StrategyList):
    """
    Contains the strategies provided for dictionaries.

    """

    NAME = "dict"

    @staticmethod
    def strategy_merge(config, path, base, nxt):
        """
        for keys that do not exists,
        use them directly. if the key exists
        in both dictionaries, attempt a value merge.
        """
        for k, v in nxt.items():
            if k not in base:
                base[k] = v
            else:
                base[k] = config.value_strategy(path + [k], base[k], v)
        return base

    @staticmethod
    def strategy_override(config, path, base, nxt):
        """
        move all keys in nxt into base, overriding
        conflicts.
        """
        return nxt
