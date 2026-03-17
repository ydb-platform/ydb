import abc
from dataclasses import dataclass, field
import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Optional, Set, Tuple

import toloka.client as toloka

from .common import Worker
from ..utils import and_, or_


# assignment provides us moment in time and other info
@dataclass
class Human(Worker):
    user_id: str
    assignment_id: str

    def __init__(self, assignment: toloka.Assignment):
        self.user_id = assignment.user_id
        self.assignment_id = assignment.id

    @property
    def id(self) -> str:
        return self.user_id


class RegionCodes:
    UNITED_STATES = 84
    GERMANY = 96
    UNITED_KINGDOM = 102
    NETHERLANDS = 118
    FINLAND = 123
    FRANCE = 124
    SWEDEN = 127
    BELARUS = 149
    KAZAKHSTAN = 159
    AZERBAIJAN = 167
    ISRAEL = 181
    UKRAINE = 187
    SPAIN = 204
    ITALY = 205
    RUSSIA = 225
    TURKEY = 983
    UZBEKISTAN = 171


lang_to_default_regions = {
    'AZ': {RegionCodes.AZERBAIJAN},
    'DE': {RegionCodes.GERMANY},
    'EN': {RegionCodes.UNITED_STATES, RegionCodes.UNITED_KINGDOM},
    'ES': {RegionCodes.SPAIN},
    'FI': {RegionCodes.FINLAND},
    'FR': {RegionCodes.FRANCE},
    'HE': {RegionCodes.ISRAEL},
    'IT': {RegionCodes.ITALY},
    'KK': {RegionCodes.KAZAKHSTAN},
    'NL': {RegionCodes.NETHERLANDS},
    'RU': {RegionCodes.RUSSIA, RegionCodes.BELARUS, RegionCodes.UKRAINE},
    'SV': {RegionCodes.SWEDEN},
    'TR': {RegionCodes.TURKEY},
    'UZ': {RegionCodes.UZBEKISTAN},
}


@dataclass
class LanguageRequirement:
    lang: str
    verified: bool = False

    def __eq__(self, other):
        return self.lang == other.lang and self.verified == other.verified

    def __lt__(self, other):
        if self.lang == other.lang:
            return self.verified < other.verified
        return self.lang < other.lang

    def __hash__(self):
        return hash((self.lang, self.verified))


@dataclass
class HumanFilter:
    @abc.abstractmethod
    def to_toloka_filter(self) -> Optional[toloka.filter.FilterCondition]:
        ...


@dataclass
class ExpertFilter(HumanFilter):
    skills: List[toloka.Skill]

    def to_toloka_filter(self) -> toloka.filter.FilterCondition:
        return toloka.filter.FilterOr([(toloka.filter.Skill(skill.id) > 0) for skill in self.skills])


@dataclass
class BaseWorkerFilter(HumanFilter, abc.ABC):
    training_score: Optional[int]

    def __post_init__(self):
        assert self.training_score is None or (
            isinstance(self.training_score, int) and (0 <= self.training_score <= 100)
        )


# Actually Toloka allows to make arbitrary filters with AND/OR expressions. Currently, in our code we work with
# specific filters with predefined AND/OR structure, but during lzy serialization we store filters as unstructured
# Toloka object, so we have possibility to make more complex filters in future without loss of backward compatibility.
@dataclass
class WorkerFilter(BaseWorkerFilter):
    @dataclass
    class Params(HumanFilter):
        langs: Set[LanguageRequirement] = field(default_factory=set)
        regions: Set[int] = field(default_factory=set)  # empty means all regions
        age_range: Tuple[Optional[int], Optional[int]] = (18, None)
        client_types: Set[toloka.filter.ClientType.ClientType] = field(default_factory=set)  # empty means all types

        def __post_init__(self):
            assert len(self.langs) == len({lang.lang for lang in self.langs})

        def to_toloka_filter(self) -> Optional[toloka.filter.FilterCondition]:
            filters = []
            if self.langs:
                lang_filters = []
                for lang in sorted(self.langs, key=lambda req: req.lang):  # sorted for determinism
                    kwargs = {}
                    if lang.verified:
                        kwargs['verified'] = True  # we cannot pass verified=False parameter to toloka-kit Languages
                    lang_filters.append(toloka.filter.Languages.in_(lang.lang, **kwargs))
                filters.append(and_(lang_filters))
            if self.regions:
                regions = sorted(self.regions)  # sorted for determinism
                filters.append(or_([toloka.filter.RegionByPhone.in_(code) for code in regions]))
            today = datetime.datetime.today()
            min_age, max_age = self.age_range
            if min_age:
                filters.append(toloka.filter.DateOfBirth < int((today - relativedelta(years=min_age)).timestamp()))
            if max_age:
                filters.append(toloka.filter.DateOfBirth > int((today - relativedelta(years=max_age)).timestamp()))
            if self.client_types:
                client_types = sorted(list(self.client_types), key=lambda i: i.value)  # sorted for determinism
                filters.append(or_([toloka.filter.ClientType == t for t in client_types]))
            return and_(filters)

    filters: List[Params]

    def to_toloka_filter(self) -> Optional[toloka.filter.FilterCondition]:
        return or_([f.to_toloka_filter() for f in self.filters])


@dataclass
class CustomWorkerFilter(BaseWorkerFilter):
    filter: toloka.filter.FilterCondition

    def to_toloka_filter(self) -> toloka.filter.FilterCondition:
        return self.filter
