from functools import lru_cache
from typing import List, Optional


from .. import classification, classification_loop

from .base import OptionType


def get_rec_label(obj: OptionType, recommended_range: List[OptionType]) -> str:
    if isinstance(obj, str):
        return f'Recommended range: {recommended_range}'
    elif isinstance(obj, float):
        return f'Recommended range: [{recommended_range[0]:.2f}, {recommended_range[-1]:.2f}]'
    elif isinstance(obj, bool) and len(recommended_range) == 1:
        return f'Recommended: {recommended_range[0]}'
    else:
        return f'Recommended range: [{recommended_range[0]}, {recommended_range[-1]}]'


@lru_cache(maxsize=None)
def get_overlap(overlap: str, confidence: Optional[float] = None) -> classification_loop.Overlap:
    spl = overlap.split('-')
    if len(spl) == 1:
        ovr = int(overlap)
        assert ovr >= 1
        return classification_loop.StaticOverlap(ovr)
    min_overlap, max_overlap = map(int, spl)
    assert 0 < min_overlap <= max_overlap <= 100
    if min_overlap == max_overlap:
        return classification_loop.StaticOverlap(min_overlap)
    return classification_loop.DynamicOverlap(min_overlap, max_overlap, confidence)


def overlap_is_static(overlap: str) -> bool:
    return isinstance(get_overlap(overlap, 0.5), classification_loop.StaticOverlap)


def get_agg_algorithm(agg: str) -> classification.AggregationAlgorithm:
    return {
        'DS': classification.AggregationAlgorithm.DAWID_SKENE,
        'MV': classification.AggregationAlgorithm.MAJORITY_VOTE,
        'ML': classification.AggregationAlgorithm.MAX_LIKELIHOOD,
    }[agg]
