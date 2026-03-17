from __future__ import annotations

from datetime import datetime
import numbers
import threading
from typing import cast
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TYPE_CHECKING

from optuna.distributions import BaseDistribution
from optuna.distributions import CategoricalDistribution
from optuna.distributions import FloatDistribution
from optuna.distributions import IntDistribution
from optuna.trial import FrozenTrial
from optuna.trial import TrialState


if TYPE_CHECKING:
    SearchSpaceSetT = Set[Tuple[str, BaseDistribution]]
    SearchSpaceListT = List[Tuple[str, BaseDistribution]]


def get_cached_extra_study_property(
    in_memory_cache: InMemoryCache, study_id: int, trials: list[FrozenTrial]
) -> tuple[SearchSpaceListT, SearchSpaceListT, list[tuple[str, bool]], bool]:
    with in_memory_cache._cached_extra_study_property_cache_lock:
        cached_extra_study_property = in_memory_cache._cached_extra_study_property_cache.get(
            study_id, None
        )
        if cached_extra_study_property is None:
            cached_extra_study_property = _CachedExtraStudyProperty()
        cached_extra_study_property.update(trials)
        in_memory_cache._cached_extra_study_property_cache[study_id] = cached_extra_study_property
        return (
            cached_extra_study_property.intersection_search_space,
            cached_extra_study_property.union_search_space,
            cached_extra_study_property.union_user_attrs,
            cached_extra_study_property.has_intermediate_values,
        )


class InMemoryCache:
    def __init__(self) -> None:
        self._cached_extra_study_property_cache: dict[int, "_CachedExtraStudyProperty"] = {}
        self._cached_extra_study_property_cache_lock = threading.Lock()
        self._trials_cache: dict[int, list[FrozenTrial]] = {}
        self._trials_cache_lock = threading.Lock()
        self._trials_last_fetched_at: dict[int, datetime] = {}

    def clear(self) -> None:
        with self._cached_extra_study_property_cache_lock:
            self._cached_extra_study_property_cache.clear()
        with self._trials_cache_lock:
            self._trials_cache.clear()
            self._trials_last_fetched_at.clear()


class _CachedExtraStudyProperty:
    def __init__(self) -> None:
        self._cursor: int = -1
        self._intersection_search_space: Optional[SearchSpaceSetT] = None
        self._union_search_space: SearchSpaceSetT = set()
        self._union_user_attrs: dict[str, bool] = {}  # attr_name: is_sortable (= is_number)
        self.has_intermediate_values: bool = False

    @property
    def intersection_search_space(self) -> SearchSpaceListT:
        if self._intersection_search_space is None:
            return []
        intersection = list(self._intersection_search_space)
        intersection.sort(key=lambda x: x[0])
        return intersection

    @property
    def union_search_space(self) -> SearchSpaceListT:
        union = list(self._union_search_space)
        union.sort(key=lambda x: x[0])
        return union

    @property
    def union_user_attrs(self) -> list[tuple[str, bool]]:
        union = [(name, is_sortable) for name, is_sortable in self._union_user_attrs.items()]
        sorted(union, key=lambda x: x[0])
        return union

    def update(self, trials: list[FrozenTrial]) -> None:
        next_cursor = self._cursor
        for trial in reversed(trials):
            if self._cursor > trial.number:
                break

            if not trial.state.is_finished():
                next_cursor = trial.number

            self._update_user_attrs(trial)
            if trial.state != TrialState.FAIL:
                self._update_intermediate_values(trial)
                self._update_search_space(trial)

        self._cursor = next_cursor

    def _update_user_attrs(self, trial: FrozenTrial) -> None:
        current_user_attrs = {
            k: not isinstance(v, bool) and isinstance(v, numbers.Real)
            for k, v in trial.user_attrs.items()
        }
        for attr_name, current_is_sortable in current_user_attrs.items():
            is_sortable = self._union_user_attrs.get(attr_name)
            if is_sortable is None:
                self._union_user_attrs[attr_name] = current_is_sortable
            elif is_sortable and not current_is_sortable:
                self._union_user_attrs[attr_name] = False

    def _update_intermediate_values(self, trial: FrozenTrial) -> None:
        if not self.has_intermediate_values and len(trial.intermediate_values) > 0:
            self.has_intermediate_values = True

    def _update_search_space(self, trial: FrozenTrial) -> None:
        def _is_same_float_distribution(d1: BaseDistribution, d2: BaseDistribution) -> bool:
            return (
                isinstance(d1, FloatDistribution)
                and isinstance(d2, FloatDistribution)
                and d1.step == d2.step
                and d1.log == d2.log
            )

        def _is_same_int_distribution(d1: BaseDistribution, d2: BaseDistribution) -> bool:
            return (
                isinstance(d1, IntDistribution)
                and isinstance(d2, IntDistribution)
                and d1.step == d2.step
                and d1.log == d2.log
            )

        def _is_same_categorical_distribution(d1: BaseDistribution, d2: BaseDistribution) -> bool:
            return isinstance(d1, CategoricalDistribution) and isinstance(
                d2, CategoricalDistribution
            )

        new_union_search_space: SearchSpaceSetT = set(
            [(n, d) for n, d in self._union_search_space]
        )
        for n1, d1 in trial.distributions.items():
            for n2, d2 in self._union_search_space:
                if n1 == n2 and _is_same_float_distribution(d1, d2):
                    d1, d2 = cast(FloatDistribution, d1), cast(FloatDistribution, d2)
                    new_float_search_space = (
                        n1,
                        FloatDistribution(
                            low=min(d1.low, d2.low),
                            high=max(d1.high, d2.high),
                            step=d1.step,
                            log=d1.log,
                        ),
                    )
                    new_union_search_space.remove((n2, d2))
                    new_union_search_space.add(new_float_search_space)
                    break
                elif n1 == n2 and _is_same_int_distribution(d1, d2):
                    d1, d2 = cast(IntDistribution, d1), cast(IntDistribution, d2)
                    new_int_search_space = (
                        n1,
                        IntDistribution(
                            low=min(d1.low, d2.low),
                            high=max(d1.high, d2.high),
                            step=d1.step,
                            log=d1.log,
                        ),
                    )
                    new_union_search_space.remove((n2, d2))
                    new_union_search_space.add(new_int_search_space)
                    break
                elif n1 == n2 and _is_same_categorical_distribution(d1, d2):
                    d1, d2 = cast(CategoricalDistribution, d1), cast(CategoricalDistribution, d2)
                    new_categorical_search_space = (
                        n1,
                        CategoricalDistribution(choices=list(set(d1.choices + d2.choices))),
                    )
                    new_union_search_space.remove((n2, d2))
                    new_union_search_space.add(new_categorical_search_space)
                    break
            else:
                new_union_search_space.add((n1, d1))
        self._union_search_space = new_union_search_space

        if self._intersection_search_space is None:
            self._intersection_search_space = set([(n, d) for n, d in trial.distributions.items()])
        else:
            new_intersection_search_space: SearchSpaceSetT = set()
            for n1, d1 in trial.distributions.items():
                for n2, d2 in self._intersection_search_space:
                    if n1 == n2 and _is_same_float_distribution(d1, d2):
                        d1, d2 = cast(FloatDistribution, d1), cast(FloatDistribution, d2)
                        new_float_search_space = (
                            n1,
                            FloatDistribution(
                                low=min(d1.low, d2.low),
                                high=max(d1.high, d2.high),
                                step=d1.step,
                                log=d1.log,
                            ),
                        )
                        new_intersection_search_space.add(new_float_search_space)
                        break
                    elif n1 == n2 and _is_same_int_distribution(d1, d2):
                        d1, d2 = cast(IntDistribution, d1), cast(IntDistribution, d2)
                        new_int_search_space = (
                            n1,
                            IntDistribution(
                                low=min(d1.low, d2.low),
                                high=max(d1.high, d2.high),
                                step=d1.step,
                                log=d1.log,
                            ),
                        )
                        new_intersection_search_space.add(new_int_search_space)
                        break
                    elif n1 == n2 and _is_same_categorical_distribution(d1, d2):
                        d1, d2 = (
                            cast(CategoricalDistribution, d1),
                            cast(CategoricalDistribution, d2),
                        )
                        new_categorical_search_space = (
                            n1,
                            CategoricalDistribution(choices=list(set(d1.choices + d2.choices))),
                        )
                        new_intersection_search_space.add(new_categorical_search_space)
                        break
            self._intersection_search_space = new_intersection_search_space
