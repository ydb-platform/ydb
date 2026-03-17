from dataclasses import dataclass
from typing import List, Optional

from pure_protobuf.dataclasses_ import field, message, one_of, part
from pure_protobuf.oneof import OneOf_
import toloka.client as toloka

from ... import worker
from .common import ProtobufSerializer, T, deserialize_one_of_field, func_registry, load_func
from .toloka import TolokaSkill, TolokaFilterCondition


@message
@dataclass
class ExpertFilter(ProtobufSerializer[worker.ExpertFilter]):
    skills: List[TolokaSkill] = field(1)

    @staticmethod
    def serialize(obj: worker.ExpertFilter) -> 'ExpertFilter':
        return ExpertFilter([TolokaSkill.serialize(skill) for skill in obj.skills])

    def deserialize(self) -> worker.ExpertFilter:
        return worker.ExpertFilter([skill.deserialize() for skill in self.skills])


@message
@dataclass
class LanguageRequirement(ProtobufSerializer[worker.LanguageRequirement]):
    lang: str = field(1)
    verified: bool = field(2)

    @staticmethod
    def serialize(obj: worker.LanguageRequirement) -> 'LanguageRequirement':
        return LanguageRequirement(lang=obj.lang, verified=obj.verified)

    def deserialize(self) -> worker.LanguageRequirement:
        return worker.LanguageRequirement(lang=self.lang, verified=self.verified)


@message
@dataclass
class BaseWorkerFilter(ProtobufSerializer[worker.BaseWorkerFilter]):
    training_score: Optional[int] = field(1, default=None)

    @staticmethod
    def serialize(obj: worker.BaseWorkerFilter) -> 'BaseWorkerFilter':
        return BaseWorkerFilter(training_score=obj.training_score)

    def deserialize(self) -> worker.BaseWorkerFilter:
        return worker.BaseWorkerFilter(training_score=self.training_score)


@message
@dataclass
class WorkerFilterParams(ProtobufSerializer[worker.WorkerFilter.Params]):
    langs: List[LanguageRequirement] = field(1, default=list)
    regions: List[int] = field(2, default_factory=list)
    age_min: Optional[int] = field(3, default=None)
    age_max: Optional[int] = field(4, default=None)
    client_types: List[str] = field(5, default_factory=list)

    @staticmethod
    def serialize(obj: worker.WorkerFilter.Params) -> 'WorkerFilterParams':
        return WorkerFilterParams(
            langs=[LanguageRequirement.serialize(lang) for lang in sorted(obj.langs)],
            regions=list(obj.regions),
            age_min=obj.age_range[0],
            age_max=obj.age_range[1],
            client_types=[t.value for t in obj.client_types],
        )

    def deserialize(self) -> worker.WorkerFilter.Params:
        return worker.WorkerFilter.Params(
            langs={lang.deserialize() for lang in self.langs},
            regions=set(self.regions),
            age_range=(self.age_min, self.age_max),
            client_types={toloka.filter.ClientType.ClientType(t) for t in self.client_types},
        )


@message
@dataclass
class WorkerFilter(BaseWorkerFilter, ProtobufSerializer[worker.WorkerFilter]):
    filters: List[WorkerFilterParams] = field(100, default_factory=list)

    @staticmethod
    def serialize(obj: worker.WorkerFilter) -> 'WorkerFilter':
        base_filter = BaseWorkerFilter.serialize(obj)
        return WorkerFilter(
            training_score=base_filter.training_score, filters=[WorkerFilterParams.serialize(f) for f in obj.filters]
        )

    def deserialize(self) -> worker.WorkerFilter:
        base_filter = super(WorkerFilter, self).deserialize()
        return worker.WorkerFilter(
            training_score=base_filter.training_score,
            filters=[f.deserialize() for f in self.filters],
        )


@message
@dataclass
class CustomWorkerFilter(BaseWorkerFilter, ProtobufSerializer[worker.CustomWorkerFilter]):
    filter: Optional[TolokaFilterCondition] = field(100, default=None)

    @staticmethod
    def serialize(obj: worker.CustomWorkerFilter) -> 'CustomWorkerFilter':
        base_filter = BaseWorkerFilter.serialize(obj)
        return CustomWorkerFilter(
            training_score=base_filter.training_score,
            filter=TolokaFilterCondition.serialize(obj.filter) if obj.filter else None,
        )

    def deserialize(self) -> worker.CustomWorkerFilter:
        base_filter = super(CustomWorkerFilter, self).deserialize()
        return worker.CustomWorkerFilter(
            training_score=base_filter.training_score,
            filter=TolokaFilterCondition.deserialize(self.filter) if self.filter else None,
        )


@message
@dataclass
class HumanFilter(ProtobufSerializer[worker.HumanFilter]):
    filter: OneOf_ = one_of(
        expert=part(ExpertFilter, 1),
        worker=part(WorkerFilter, 2),
        custom_worker=part(CustomWorkerFilter, 3),
    )

    @staticmethod
    def serialize(obj: worker.HumanFilter) -> 'HumanFilter':
        f = HumanFilter()
        if isinstance(obj, worker.ExpertFilter):
            f.filter.expert = ExpertFilter.serialize(obj)
        elif isinstance(obj, worker.WorkerFilter):
            f.filter.worker = WorkerFilter.serialize(obj)
        elif isinstance(obj, worker.CustomWorkerFilter):
            f.filter.custom_worker = CustomWorkerFilter.serialize(obj)
        else:
            raise ValueError(f'unexpected filter type: {type(obj)}')
        return f

    def deserialize(self) -> worker.HumanFilter:
        return deserialize_one_of_field(self.filter)


@message
@dataclass
class Human(ProtobufSerializer[worker.Human]):
    user_id: str = field(1)
    assignment_id: str = field(2)

    @staticmethod
    def serialize(obj: worker.Human) -> 'Human':
        return Human(user_id=obj.user_id, assignment_id=obj.assignment_id)

    def deserialize(self) -> worker.Human:
        fake_assignment = toloka.Assignment(id=self.assignment_id, user_id=self.user_id)  # TODO: ugly hack
        return worker.Human(fake_assignment)


@message
@dataclass
class Model(ProtobufSerializer[worker.Model]):
    name: str = field(1)
    func: str = field(2)

    @staticmethod
    def serialize(obj: worker.Model) -> 'Model':
        return Model(name=obj.name, func=func_registry[obj.func])

    def deserialize(self) -> worker.Model:
        return worker.Model(name=self.name, func=load_func(self.func))


@message
@dataclass
class Worker(ProtobufSerializer[worker.Worker]):
    worker: OneOf_ = one_of(
        human=part(Human, 1),
        model=part(Model, 2),
    )

    @staticmethod
    def serialize(obj: T) -> 'Worker':
        w = Worker()
        if isinstance(obj, worker.Human):
            w.worker.human = Human.serialize(obj)
        elif isinstance(obj, worker.Model):
            w.worker.model = Model.serialize(obj)
        else:
            raise ValueError(f'unexpected worker type: {type(obj)}')
        return w

    def deserialize(self) -> T:
        return deserialize_one_of_field(self.worker)
