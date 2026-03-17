from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, List, Optional, TypedDict, Union, cast

from annet.storage import Query


FIELD_VALUE_SEPARATOR = ":"
ALLOWED_GLOB_GROUPS = ["site", "tag", "role", "device_type", "status", "tenant", "asset_tag"]


class Filter(TypedDict, total=False):
    site: list[str]
    tag: list[str]
    role: list[str]
    name: list[str]
    device_type: list[str]
    status: list[str]
    tenant: list[str]
    asset_tag: list[str]


@dataclass
class NetboxQuery(Query):
    query: List[str]

    @classmethod
    def new(
        cls,
        query: Union[str, Iterable[str]],
        hosts_range: Optional[slice] = None,
    ) -> "NetboxQuery":
        if hosts_range is not None:
            raise ValueError("host_range is not supported")
        return cls(query=list(query))

    @property
    def globs(self):
        # We process every query host as a glob
        return self.query

    def parse_query(self) -> Filter:
        query_groups = parse_query(self.globs)
        return cast(Filter, query_groups)

    def is_empty(self) -> bool:
        return len(self.query) == 0

    def is_host_query(self) -> bool:
        if not self.globs:
            return False
        for q in self.globs:
            if FIELD_VALUE_SEPARATOR in q:
                return False
        return True


def parse_query(query: list[str]) -> dict[str, list[str]]:
    query_groups = defaultdict(list)
    for q in query:
        if FIELD_VALUE_SEPARATOR in q:
            glob_type, param = q.split(FIELD_VALUE_SEPARATOR, 2)
            if glob_type not in ALLOWED_GLOB_GROUPS:
                raise Exception(f"unknown query type: '{glob_type}'")
            if not param:
                raise Exception(f"empty param for '{glob_type}'")
            query_groups[glob_type].append(param)
        else:
            query_groups["name"].append(q)
    return dict(query_groups)
