from .strategy.list import ListStrategies
from .strategy.dict import DictStrategies
from .strategy.set import SetStrategies
from .strategy.type_conflict import TypeConflictStrategies
from .strategy.fallback import FallbackStrategies


class Merger(object):
    """
    :param type_strategies, List[Tuple]: a list of (Type, Strategy) pairs
           that should be used against incoming types. For example: (dict, "override").
    """

    PROVIDED_TYPE_STRATEGIES = {
        list: ListStrategies,
        dict: DictStrategies,
        set: SetStrategies,
    }

    def __init__(self, type_strategies, fallback_strategies, type_conflict_strategies):
        self._fallback_strategy = FallbackStrategies(fallback_strategies)

        expanded_type_strategies = []
        for typ, strategy in type_strategies:
            if typ in self.PROVIDED_TYPE_STRATEGIES:
                strategy = self.PROVIDED_TYPE_STRATEGIES[typ](strategy)
            expanded_type_strategies.append((typ, strategy))
        self._type_strategies = expanded_type_strategies

        self._type_conflict_strategy = TypeConflictStrategies(type_conflict_strategies)

    def merge(self, base, nxt):
        return self.value_strategy([], base, nxt)

    def type_conflict_strategy(self, *args):
        return self._type_conflict_strategy(self, *args)

    def value_strategy(self, path, base, nxt):
        for typ, strategy in self._type_strategies:
            if isinstance(base, typ) and isinstance(nxt, typ):
                return strategy(self, path, base, nxt)
        if not (isinstance(base, type(nxt)) or isinstance(nxt, type(base))):
            return self.type_conflict_strategy(path, base, nxt)
        return self._fallback_strategy(self, path, base, nxt)
