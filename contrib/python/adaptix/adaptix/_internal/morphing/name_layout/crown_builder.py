import math
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import groupby
from typing import Generic, TypeVar, Union, cast

from ..model.crown_definitions import (
    BaseDictCrown,
    BaseListCrown,
    CrownPath,
    DictExtraPolicy,
    InpDictCrown,
    InpListCrown,
    LeafBaseCrown,
    LeafInpCrown,
    LeafOutCrown,
    ListExtraPolicy,
    OutDictCrown,
    OutListCrown,
    Sieve,
)
from .base import KeyPath, PathsTo

LeafCr = TypeVar("LeafCr", bound=LeafBaseCrown)
DictCr = TypeVar("DictCr", bound=BaseDictCrown)
ListCr = TypeVar("ListCr", bound=BaseListCrown)


@dataclass
class PathWithLeaf(Generic[LeafCr]):
    path: CrownPath
    leaf: LeafCr


PathedLeaves = Sequence[PathWithLeaf[LeafCr]]


class BaseCrownBuilder(ABC, Generic[LeafCr, DictCr, ListCr]):
    def __init__(self, paths_to_leaves: PathsTo[LeafCr]):
        self._paths_to_leaves = paths_to_leaves
        self._paths_to_order = {path: i for i, path in enumerate(paths_to_leaves)}

    def build_empty_crown(self, *, as_list: bool) -> Union[DictCr, ListCr]:
        if as_list:
            return self._make_list_crown(current_path=(), paths_with_leaves=[])
        return self._make_dict_crown(current_path=(), paths_with_leaves=[])

    def build_crown(self) -> Union[DictCr, ListCr]:
        paths_with_leaves = [PathWithLeaf(path, leaf) for path, leaf in self._paths_to_leaves.items()]
        paths_with_leaves.sort(key=lambda x: x.path)
        return cast(Union[DictCr, ListCr], self._build_crown(paths_with_leaves, 0))

    def _build_crown(self, paths_with_leaves: PathedLeaves[LeafCr], path_offset: int) -> Union[LeafCr, DictCr, ListCr]:
        if not paths_with_leaves:
            raise ValueError

        try:
            first = paths_with_leaves[0].path[path_offset]
        except IndexError:
            if len(paths_with_leaves) != 1:
                raise ValueError
            return paths_with_leaves[0].leaf

        if isinstance(first, str):
            return self._make_dict_crown(paths_with_leaves[0].path[:path_offset], paths_with_leaves)
        if isinstance(first, int):
            return self._make_list_crown(paths_with_leaves[0].path[:path_offset], paths_with_leaves)
        raise RuntimeError

    def _get_dict_crown_map(
        self,
        current_path: KeyPath,
        paths_with_leaves: PathedLeaves[LeafCr],
    ) -> Mapping[str, Union[LeafCr, DictCr, ListCr]]:
        dict_crown_map = {
            key: self._build_crown(list(path_group), len(current_path) + 1)
            for key, path_group in groupby(paths_with_leaves, lambda x: x.path[len(current_path)])
        }
        sorted_keys = sorted(
            dict_crown_map,
            key=lambda key: self._paths_to_order.get((*current_path, key), math.inf),
        )
        return {key: dict_crown_map[key] for key in sorted_keys}

    @abstractmethod
    def _make_dict_crown(self, current_path: KeyPath, paths_with_leaves: PathedLeaves[LeafCr]) -> DictCr:
        ...

    def _get_list_crown_map(
        self,
        current_path: KeyPath,
        paths_with_leaves: PathedLeaves[LeafCr],
    ) -> Sequence[Union[LeafCr, DictCr, ListCr]]:
        grouped_paths = [
            list(grouped_paths)
            for key, grouped_paths in groupby(paths_with_leaves, lambda x: x.path[len(current_path)])
        ]
        if paths_with_leaves and len(grouped_paths) != cast(int, paths_with_leaves[-1].path[len(current_path)]) + 1:
            raise ValueError(f"Found gaps in list mapping at {current_path}")
        return tuple(
            self._build_crown(path_group, len(current_path) + 1)
            for path_group in grouped_paths
        )

    @abstractmethod
    def _make_list_crown(self, current_path: KeyPath, paths_with_leaves: PathedLeaves[LeafCr]) -> ListCr:
        ...


class InpCrownBuilder(BaseCrownBuilder[LeafInpCrown, InpDictCrown, InpListCrown]):
    def __init__(self, extra_policies: PathsTo[DictExtraPolicy], paths_to_leaves: PathsTo[LeafInpCrown]):
        self.extra_policies = extra_policies
        super().__init__(paths_to_leaves)

    def _make_dict_crown(self, current_path: KeyPath, paths_with_leaves: PathedLeaves[LeafInpCrown]) -> InpDictCrown:
        return InpDictCrown(
            map=self._get_dict_crown_map(current_path, paths_with_leaves),
            extra_policy=self.extra_policies[current_path],
        )

    def _make_list_crown(self, current_path: KeyPath, paths_with_leaves: PathedLeaves[LeafInpCrown]) -> InpListCrown:
        return InpListCrown(
            map=self._get_list_crown_map(current_path, paths_with_leaves),
            extra_policy=cast(ListExtraPolicy, self.extra_policies[current_path]),
        )


class OutCrownBuilder(BaseCrownBuilder[LeafOutCrown, OutDictCrown, OutListCrown]):
    def __init__(self, path_to_sieves: PathsTo[Sieve], paths_to_leaves: PathsTo[LeafOutCrown]):
        self.path_to_sieves = path_to_sieves
        super().__init__(paths_to_leaves)

    def _make_dict_crown(self, current_path: KeyPath, paths_with_leaves: PathedLeaves[LeafOutCrown]) -> OutDictCrown:
        key_to_sieve: dict[str, Sieve] = {}
        for leaf_with_path in paths_with_leaves:
            sieve = self.path_to_sieves.get(leaf_with_path.path[:len(current_path) + 1])
            if sieve is not None:
                key_to_sieve[cast(str, leaf_with_path.path[len(current_path)])] = sieve

        return OutDictCrown(
            map=self._get_dict_crown_map(current_path, paths_with_leaves),
            sieves=key_to_sieve,
        )

    def _make_list_crown(self, current_path: KeyPath, paths_with_leaves: PathedLeaves[LeafOutCrown]) -> OutListCrown:
        return OutListCrown(
            map=self._get_list_crown_map(current_path, paths_with_leaves),
        )
