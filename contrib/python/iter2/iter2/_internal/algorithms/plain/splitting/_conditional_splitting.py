import typing as tp

from ....lib.functions import Predicate

from ..groupping import group_and_collect_values_globally_with_same_computable_key


# ---

def split_by_condition[Item](
    iterable: tp.Iterable[Item],
    predicate: Predicate[Item],
) -> tp.Tuple[tp.List[Item], tp.List[Item]]:
    mapping = group_and_collect_values_globally_with_same_computable_key(iterable, key_fn=predicate)
    return (
        mapping.get(True, []),
        mapping.get(False, []),
    )
