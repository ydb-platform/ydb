#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import TypeVar, Generic, TYPE_CHECKING, Optional
from abc import ABC, abstractmethod
from .const import NULL_PTR_NAME, MIN_EXPORT_VERSION
from .hdr import AcisHeader

if TYPE_CHECKING:
    from .entities import AcisEntity
    from ezdxf.math import Vec3


T = TypeVar("T", bound="AbstractEntity")


class AbstractEntity(ABC):
    """Unified query interface for SAT and SAB data."""

    name: str
    id: int = -1

    def __str__(self):
        return f"{self.name}"

    @property
    def is_null_ptr(self) -> bool:
        """Returns ``True`` if this entity is the ``NULL_PTR`` entity."""
        return self.name == NULL_PTR_NAME


class DataLoader(ABC):
    """Data loading interface to create high level AcisEntity data from low
    level AbstractEntity representation.

    """

    version: int = MIN_EXPORT_VERSION

    @abstractmethod
    def has_data(self) -> bool:
        pass

    @abstractmethod
    def read_int(self, skip_sat: Optional[int] = None) -> int:
        """There are sometimes additional int values in SAB files which are
        not present in SAT files, maybe reference counters e.g. vertex, coedge.
        """
        pass

    @abstractmethod
    def read_double(self) -> float:
        pass

    @abstractmethod
    def read_interval(self) -> float:
        pass

    @abstractmethod
    def read_vec3(self) -> tuple[float, float, float]:
        pass

    @abstractmethod
    def read_bool(self, true: str, false: str) -> bool:
        pass

    @abstractmethod
    def read_str(self) -> str:
        pass

    @abstractmethod
    def read_ptr(self) -> AbstractEntity:
        pass

    @abstractmethod
    def read_transform(self) -> list[float]:
        pass


class DataExporter(ABC):
    version: int = MIN_EXPORT_VERSION

    @abstractmethod
    def write_int(self, value: int, skip_sat=False) -> None:
        """There are sometimes additional int values in SAB files which are
        not present in SAT files, maybe reference counters e.g. vertex, coedge.
        """
        pass

    @abstractmethod
    def write_double(self, value: float) -> None:
        pass

    @abstractmethod
    def write_interval(self, value: float) -> None:
        pass

    @abstractmethod
    def write_loc_vec3(self, value: Vec3) -> None:
        pass

    @abstractmethod
    def write_dir_vec3(self, value: Vec3) -> None:
        pass

    @abstractmethod
    def write_bool(self, value: bool, true: str, false: str) -> None:
        pass

    @abstractmethod
    def write_str(self, value: str) -> None:
        pass

    @abstractmethod
    def write_literal_str(self, value: str) -> None:
        pass

    @abstractmethod
    def write_ptr(self, entity: AcisEntity) -> None:
        pass

    @abstractmethod
    def write_transform(self, data: list[str]) -> None:
        pass


class AbstractBuilder(Generic[T]):
    header: AcisHeader
    bodies: list[T]
    entities: list[T]

    def reorder_records(self) -> None:
        if len(self.entities) == 0:
            return
        header: list[T] = []
        entities: list[T] = []
        for e in self.entities:
            if e.name == "body":
                header.append(e)
            elif e.name == "asmheader":
                header.insert(0, e)  # has to be the first record
            else:
                entities.append(e)
        self.entities = header + entities

    def reset_ids(self, start: int = 0) -> None:
        for num, entity in enumerate(self.entities, start=start):
            entity.id = num

    def clear_ids(self) -> None:
        for entity in self.entities:
            entity.id = -1


class EntityExporter(Generic[T]):
    def __init__(self, header: AcisHeader):
        self.header = header
        self.version = header.version
        self._exported_entities: dict[int, T] = {}
        if self.header.has_asm_header:
            self.export(self.header.asm_header())

    def export_records(self) -> list[T]:
        return list(self._exported_entities.values())

    @abstractmethod
    def make_record(self, entity: AcisEntity) -> T:
        pass

    @abstractmethod
    def make_data_exporter(self, record: T) -> DataExporter:
        pass

    def get_record(self, entity: AcisEntity) -> T:
        assert not entity.is_none
        return self._exported_entities[id(entity)]

    def export(self, entity: AcisEntity):
        if entity.is_none:
            raise TypeError("invalid NONE_REF entity given")
        self._make_all_records(entity)
        self._export_data(entity)

    def _has_record(self, entity: AcisEntity) -> bool:
        return id(entity) in self._exported_entities

    def _add_record(self, entity: AcisEntity, record: T) -> None:
        assert not entity.is_none
        self._exported_entities[id(entity)] = record

    def _make_all_records(self, entity: AcisEntity):
        def add(e: AcisEntity) -> bool:
            if not e.is_none and not self._has_record(e):
                self._add_record(e, self.make_record(e))
                return True
            return False

        entities = [entity]
        while entities:
            next_entity = entities.pop(0)
            add(next_entity)
            for sub_entity in next_entity.entities():
                if add(sub_entity):
                    entities.append(sub_entity)

    def _export_data(self, entity: AcisEntity):
        def _export_record(e: AcisEntity):
            if id(e) not in done:
                done.add(id(e))
                record = self.get_record(e)
                if not e.attributes.is_none:
                    record.attributes = self.get_record(e.attributes)  # type: ignore
                e.export(self.make_data_exporter(record))
                return True
            return False

        entities = [entity]
        done: set[int] = set()
        while entities:
            next_entity = entities.pop(0)
            _export_record(next_entity)
            for sub_entity in next_entity.entities():
                if _export_record(sub_entity):
                    entities.append(sub_entity)
