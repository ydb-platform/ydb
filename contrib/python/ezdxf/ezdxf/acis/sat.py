#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import (
    Any,
    Sequence,
    Iterator,
    Union,
    List,
    TYPE_CHECKING,
    Optional,
)
from typing_extensions import TypeAlias
import math
from datetime import datetime

from . import const
from .const import ParsingError, InvalidLinkStructure
from .hdr import AcisHeader
from .abstract import (
    AbstractEntity,
    AbstractBuilder,
    DataLoader,
    DataExporter,
    EntityExporter,
)

if TYPE_CHECKING:
    from .entities import AcisEntity
    from ezdxf.math import Vec3

SatRecord: TypeAlias = List[str]


class SatEntity(AbstractEntity):
    """Low level representation of an ACIS entity (node)."""

    def __init__(
        self,
        name: str,
        attr_ptr: str = "$-1",
        id: int = -1,
        data: Optional[list[Any]] = None,
    ):
        self.name = name
        self.attr_ptr = attr_ptr
        self.id = id
        self.data: list[Any] = data if data is not None else []
        self.attributes: "SatEntity" = None  # type: ignore

    def __str__(self):
        return f"{self.name}({self.id})"


NULL_PTR = SatEntity("null-ptr", "$-1", -1, tuple())  # type: ignore


def new_entity(
    name: str,
    attributes=NULL_PTR,
    id=-1,
    data: Optional[list[Any]] = None,
) -> SatEntity:
    """Factory to create new ACIS entities.

    Args:
        name: entity type
        attributes: reference to the entity attributes or :attr:`NULL_PTR`.
        id: unique entity id as integer or -1
        data: generic data container as list

    """
    e = SatEntity(name, "$-1", id, data)
    e.attributes = attributes
    return e


def is_ptr(s: str) -> bool:
    """Returns ``True`` if the string `s` represents an entity pointer."""
    return len(s) > 0 and s[0] == "$"


def resolve_str_pointers(entities: list[SatEntity]) -> list[SatEntity]:
    def ptr(s: str) -> SatEntity:
        num = int(s[1:])
        if num == -1:
            return NULL_PTR
        return entities[num]

    for entity in entities:
        entity.attributes = ptr(entity.attr_ptr)
        entity.attr_ptr = "$-1"
        data = []
        for token in entity.data:
            if is_ptr(token):
                data.append(ptr(token))
            else:
                data.append(token)
        entity.data = data
    return entities


class SatDataLoader(DataLoader):
    def __init__(self, data: list[Any], version: int):
        self.version = version
        self.data = data
        self.index = 0

    def has_data(self) -> bool:
        return self.index <= len(self.data)

    def read_int(self, skip_sat: Optional[int] = None) -> int:
        if skip_sat is not None:
            return skip_sat

        entry = self.data[self.index]
        try:
            value = int(entry)
        except ValueError:
            raise ParsingError(f"expected integer, got {entry}")
        self.index += 1
        return value

    def read_double(self) -> float:
        entry = self.data[self.index]
        try:
            value = float(entry)
        except ValueError:
            raise ParsingError(f"expected double, got {entry}")
        self.index += 1
        return value

    def read_interval(self) -> float:
        finite = self.read_bool("F", "I")
        if finite:
            return self.read_double()
        return math.inf

    def read_vec3(self) -> tuple[float, float, float]:
        x = self.read_double()
        y = self.read_double()
        z = self.read_double()
        return x, y, z

    def read_bool(self, true: str, false: str) -> bool:
        value = self.data[self.index]
        if value == true:
            self.index += 1
            return True
        elif value == false:
            self.index += 1
            return False
        raise ParsingError(
            f"expected bool string '{true}' or '{false}', got {value}"
        )

    def read_str(self) -> str:
        value = self.data[self.index]
        if self.version < const.Features.AT or value.startswith("@"):
            self.index += 2
            return self.data[self.index - 1]
        raise ParsingError(f"expected string, got {value}")

    def read_ptr(self) -> AbstractEntity:
        entity = self.data[self.index]
        if isinstance(entity, AbstractEntity):
            self.index += 1
            return entity
        raise ParsingError(f"expected pointer, got {type(entity)}")

    def read_transform(self) -> list[float]:
        # 4th column is not stored
        # Read only the matrix values which contain all information needed,
        # the additional data are only hints for the kernel how to process
        # the data (rotation, reflection, scaling, shearing).
        return [self.read_double() for _ in range(12)]


class SatBuilder(AbstractBuilder):
    """Low level data structure to manage ACIS SAT data files."""

    def __init__(self) -> None:
        self.header = AcisHeader()
        self.bodies: list[SatEntity] = []
        self.entities: list[SatEntity] = []
        self._export_mapping: dict[int, SatEntity] = {}

    def dump_sat(self) -> list[str]:
        """Returns the text representation of the ACIS file as list of strings
        without line endings.

        Raise:
            InvalidLinkStructure: referenced ACIS entity is not stored in
                the :attr:`entities` storage

        """
        self.reorder_records()
        self.header.n_entities = len(self.bodies) + int(
            self.header.has_asm_header
        )
        if self.header.version == 700:
            self.header.n_records = 0  # ignored for old versions
        else:
            self.header.n_records = len(self.entities)
        data = self.header.dumps()
        data.extend(build_str_records(self.entities, self.header.version))
        data.append(self.header.sat_end_marker())
        return data

    def set_entities(self, entities: list[SatEntity]) -> None:
        """Reset entities and bodies list. (internal API)"""
        self.bodies = [e for e in entities if e.name == "body"]
        self.entities = entities


class SatExporter(EntityExporter[SatEntity]):
    def make_record(self, entity: AcisEntity) -> SatEntity:
        record = SatEntity(entity.type, id=entity.id)
        record.attributes = NULL_PTR
        return record

    def make_data_exporter(self, record: SatEntity) -> DataExporter:
        return SatDataExporter(self, record.data)

    def dump_sat(self) -> list[str]:
        builder = SatBuilder()
        builder.header = self.header
        builder.set_entities(self.export_records())
        return builder.dump_sat()


def build_str_records(entities: list[SatEntity], version: int) -> Iterator[str]:
    def ptr_str(e: SatEntity) -> str:
        if e is NULL_PTR:
            return "$-1"
        try:
            return f"${entities.index(e)}"
        except ValueError:
            raise InvalidLinkStructure(f"entity {str(e)} not in record storage")

    for entity in entities:
        tokens = [entity.name]
        tokens.append(ptr_str(entity.attributes))
        if version >= 700:
            tokens.append(str(entity.id))
        for data in entity.data:
            if isinstance(data, SatEntity):
                tokens.append(ptr_str(data))
            else:
                tokens.append(str(data))
        tokens.append("#")
        yield " ".join(tokens)


def parse_header_str(s: str) -> Iterator[str]:
    num = ""
    collect = 0
    token = ""
    for c in s.rstrip():
        if collect > 0:
            token += c
            collect -= 1
            if collect == 0:
                yield token
                token = ""
        elif c == "@":
            continue
        elif c in "0123456789":
            num += c
        elif c == " " and num:
            collect = int(num)
            num = ""


def parse_header(data: Sequence[str]) -> tuple[AcisHeader, Sequence[str]]:
    header = AcisHeader()
    tokens = data[0].split()
    header.version = int(tokens[0])
    try:
        header.n_records = int(tokens[1])
        header.n_entities = int(tokens[2])
        header.flags = int(tokens[3])
    except (IndexError, ValueError):
        pass
    tokens = list(parse_header_str(data[1]))
    try:
        header.product_id = tokens[0]
        header.acis_version = tokens[1]
    except IndexError:
        pass

    if len(tokens) > 2:
        try:  # Sat Jan  1 10:00:00 2022
            header.creation_date = datetime.strptime(tokens[2], const.DATE_FMT)
        except ValueError:
            pass
    tokens = data[2].split()
    try:
        header.units_in_mm = float(tokens[0])
    except (IndexError, ValueError):
        pass
    return header, data[3:]


def _filter_records(data: Sequence[str]) -> Iterator[str]:
    for line in data:
        if line.startswith(const.END_OF_ACIS_DATA_SAT) or line.startswith(
            const.BEGIN_OF_ACIS_HISTORY_DATA
        ):
            return
        yield line


def merge_record_strings(data: Sequence[str]) -> Iterator[str]:
    merged_data = " ".join(_filter_records(data))
    for record in merged_data.split("#"):
        record = record.strip()
        if record:
            yield record


def parse_records(data: Sequence[str]) -> list[SatRecord]:
    expected_seq_num = 0
    records: list[SatRecord] = []
    for line in merge_record_strings(data):
        tokens: SatRecord = line.split()
        first_token = tokens[0].strip()
        if first_token.startswith("-"):
            num = -int(first_token)
            if num != expected_seq_num:
                raise ParsingError(
                    "non-continuous sequence numbers not supported"
                )
            tokens.pop(0)
        records.append(tokens)
        expected_seq_num += 1
    return records


def build_entities(
    records: Sequence[SatRecord], version: int
) -> list[SatEntity]:
    entities: list[SatEntity] = []
    for record in records:
        name = record[0]
        attr = record[1]
        id_ = -1
        if version >= 700:
            id_ = int(record[2])
            data = record[3:]
        else:
            data = record[2:]
        entities.append(SatEntity(name, attr, id_, data))
    return entities


def parse_sat(s: Union[str, Sequence[str]]) -> SatBuilder:
    """Returns the :class:`SatBuilder` for the ACIS :term:`SAT` file content
    given as string or list of strings.

    Raises:
        ParsingError: invalid or unsupported ACIS data structure

    """
    data: Sequence[str]
    if isinstance(s, str):
        data = s.splitlines()
    else:
        data = s
    if not isinstance(data, Sequence):
        raise TypeError("expected as string or a sequence of strings")
    builder = SatBuilder()
    header, data = parse_header(data)
    builder.header = header
    records = parse_records(data)
    entities = build_entities(records, header.version)
    builder.set_entities(resolve_str_pointers(entities))
    return builder


class SatDataExporter(DataExporter):
    def __init__(self, exporter: SatExporter, data: list[Any]):
        self.version = exporter.version
        self.exporter = exporter
        self.data = data

    def write_int(self, value: int, skip_sat=False) -> None:
        """There are sometimes additional int values in SAB files which are
        not present in SAT files, maybe reference counters e.g. vertex, coedge.
        """
        if not skip_sat:
            self.data.append(str(value))

    def write_double(self, value: float) -> None:
        self.data.append(f"{value:g}")

    def write_interval(self, value: float) -> None:
        if math.isinf(value):
            self.data.append("I")  # infinite
        else:
            self.data.append("F")  # finite
            self.write_double(value)

    def write_loc_vec3(self, value: Vec3) -> None:
        self.write_double(value.x)
        self.write_double(value.y)
        self.write_double(value.z)

    def write_dir_vec3(self, value: Vec3) -> None:
        self.write_double(value.x)
        self.write_double(value.y)
        self.write_double(value.z)

    def write_bool(self, value: bool, true: str, false: str) -> None:
        self.data.append(true if value else false)

    def write_str(self, value: str) -> None:
        self.data.append(f"@{len(value)}")
        self.data.append(str(value))

    def write_literal_str(self, value: str) -> None:
        self.write_str(value)  # just for SAB files important

    def write_ptr(self, entity: AcisEntity) -> None:
        record = NULL_PTR
        if not entity.is_none:
            record = self.exporter.get_record(entity)
        self.data.append(record)

    def write_transform(self, data: list[str]) -> None:
        self.data.extend(data)
