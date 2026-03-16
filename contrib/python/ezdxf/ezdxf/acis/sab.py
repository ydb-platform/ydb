#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import (
    NamedTuple,
    Any,
    Sequence,
    Iterator,
    Union,
    Iterable,
    cast,
    TYPE_CHECKING,
    List,
    Tuple,
    Optional,
)
from typing_extensions import TypeAlias
import math
import struct
from datetime import datetime

from ezdxf.math import Vec3
from . import const
from .const import ParsingError, Tags, InvalidLinkStructure
from .hdr import AcisHeader
from .abstract import (
    AbstractEntity,
    DataLoader,
    AbstractBuilder,
    DataExporter,
    EntityExporter,
)

if TYPE_CHECKING:
    from .entities import AcisEntity


class Token(NamedTuple):
    """Named tuple to store tagged value tokens of the SAB format."""

    tag: int
    value: Any

    def __str__(self):
        return f"(0x{self.tag:02x}, {str(self.value)})"


SabRecord: TypeAlias = List[Token]


class Decoder:
    def __init__(self, data: bytes):
        self.data = data
        self.index: int = 0

    @property
    def has_data(self) -> bool:
        return self.index < len(self.data)

    def read_header(self) -> AcisHeader:
        header = AcisHeader()
        for signature in const.SIGNATURES:
            if self.data.startswith(signature):
                self.index = len(signature)
                break
        else:
            raise ParsingError("not a SAB file")
        header.version = self.read_int()
        header.n_records = self.read_int()
        header.n_entities = self.read_int()
        header.flags = self.read_int()
        header.product_id = self.read_str_tag()
        header.acis_version = self.read_str_tag()
        date = self.read_str_tag()
        header.creation_date = datetime.strptime(date, const.DATE_FMT)
        header.units_in_mm = self.read_double_tag()
        # tolerances are ignored
        _ = self.read_double_tag()  # res_tol
        _ = self.read_double_tag()  # nor_tol
        return header

    def forward(self, count: int):
        pos = self.index
        self.index += count
        return pos

    def read_byte(self) -> int:
        pos = self.forward(1)
        return self.data[pos]

    def read_bytes(self, count: int) -> bytes:
        pos = self.forward(count)
        return self.data[pos : pos + count]

    def read_int(self) -> int:
        pos = self.forward(4)
        values = struct.unpack_from("<i", self.data, pos)[0]
        return values

    def read_float(self) -> float:
        pos = self.forward(8)
        return struct.unpack_from("<d", self.data, pos)[0]

    def read_floats(self, count: int) -> Sequence[float]:
        pos = self.forward(8 * count)
        return struct.unpack_from(f"<{count}d", self.data, pos)

    def read_str(self, length) -> str:
        text = self.read_bytes(length)
        return text.decode()

    def read_str_tag(self) -> str:
        tag = self.read_byte()
        if tag != Tags.STR:
            raise ParsingError("string tag (7) not found")
        return self.read_str(self.read_byte())

    def read_double_tag(self) -> float:
        tag = self.read_byte()
        if tag != Tags.DOUBLE:
            raise ParsingError("double tag (6) not found")
        return self.read_float()

    def read_record(self) -> SabRecord:
        def entity_name():
            return "-".join(entity_type)

        values: SabRecord = []
        entity_type: list[str] = []
        subtype_level: int = 0
        while True:
            if not self.has_data:
                if values:
                    token = values[0]
                    if token.value in const.DATA_END_MARKERS:
                        return values
                raise ParsingError("pre-mature end of data")
            tag = self.read_byte()
            if tag == Tags.INT:
                values.append(Token(tag, self.read_int()))
            elif tag == Tags.DOUBLE:
                values.append(Token(tag, self.read_float()))
            elif tag == Tags.STR:
                values.append(Token(tag, self.read_str(self.read_byte())))
            elif tag == Tags.POINTER:
                values.append(Token(tag, self.read_int()))
            elif tag == Tags.BOOL_TRUE:
                values.append(Token(tag, True))
            elif tag == Tags.BOOL_FALSE:
                values.append(Token(tag, False))
            elif tag == Tags.LITERAL_STR:
                values.append(Token(tag, self.read_str(self.read_int())))
            elif tag == Tags.ENTITY_TYPE_EX:
                entity_type.append(self.read_str(self.read_byte()))
            elif tag == Tags.ENTITY_TYPE:
                entity_type.append(self.read_str(self.read_byte()))
                values.append(Token(tag, entity_name()))
                entity_type.clear()
            elif tag == Tags.LOCATION_VEC:
                values.append(Token(tag, self.read_floats(3)))
            elif tag == Tags.DIRECTION_VEC:
                values.append(Token(tag, self.read_floats(3)))
            elif tag == Tags.ENUM:
                values.append(Token(tag, self.read_int()))
            elif tag == Tags.UNKNOWN_0x17:
                values.append(Token(tag, self.read_float()))
            elif tag == Tags.SUBTYPE_START:
                subtype_level += 1
                values.append(Token(tag, subtype_level))
            elif tag == Tags.SUBTYPE_END:
                values.append(Token(tag, subtype_level))
                subtype_level -= 1
            elif tag == Tags.RECORD_END:
                return values
            else:
                raise ParsingError(
                    f"unknown SAB tag: 0x{tag:x} ({tag}) in entity '{values[0].value}'"
                )

    def read_records(self) -> Iterator[SabRecord]:
        while True:
            try:
                if self.has_data:
                    yield self.read_record()
                else:
                    return
            except IndexError:
                return


class SabEntity(AbstractEntity):
    """Low level representation of an ACIS entity (node)."""

    def __init__(
        self,
        name: str,
        attr_ptr: int = -1,
        id: int = -1,
        data: Optional[SabRecord] = None,
    ):
        self.name = name
        self.attr_ptr = attr_ptr
        self.id = id
        self.data: SabRecord = data if data is not None else []
        self.attributes: "SabEntity" = None  # type: ignore

    def __str__(self):
        return f"{self.name}({self.id})"


NULL_PTR = SabEntity(const.NULL_PTR_NAME, -1, -1, tuple())  # type: ignore


class SabDataLoader(DataLoader):
    def __init__(self, data: SabRecord, version: int):
        self.version = version
        self.data = data
        self.index = 0

    def has_data(self) -> bool:
        return self.index <= len(self.data)

    def read_int(self, skip_sat: Optional[int] = None) -> int:
        token = self.data[self.index]
        if token.tag == Tags.INT:
            self.index += 1
            return cast(int, token.value)
        raise ParsingError(f"expected int token, got {token}")

    def read_double(self) -> float:
        token = self.data[self.index]
        if token.tag == Tags.DOUBLE:
            self.index += 1
            return cast(float, token.value)
        raise ParsingError(f"expected double token, got {token}")

    def read_interval(self) -> float:
        finite = self.read_bool("F", "I")
        if finite:
            return self.read_double()
        return math.inf

    def read_vec3(self) -> tuple[float, float, float]:
        token = self.data[self.index]
        if token.tag in (Tags.LOCATION_VEC, Tags.DIRECTION_VEC):
            self.index += 1
            return cast(Tuple[float, float, float], token.value)
        raise ParsingError(f"expected vector token, got {token}")

    def read_bool(self, true: str, false: str) -> bool:
        token = self.data[self.index]
        if token.tag == Tags.BOOL_TRUE:
            self.index += 1
            return True
        elif token.tag == Tags.BOOL_FALSE:
            self.index += 1
            return False
        raise ParsingError(f"expected bool token, got {token}")

    def read_str(self) -> str:
        token = self.data[self.index]
        if token.tag in (Tags.STR, Tags.LITERAL_STR):
            self.index += 1
            return cast(str, token.value)
        raise ParsingError(f"expected str token, got {token}")

    def read_ptr(self) -> AbstractEntity:
        token = self.data[self.index]
        if token.tag == Tags.POINTER:
            self.index += 1
            return cast(AbstractEntity, token.value)
        raise ParsingError(f"expected pointer token, got {token}")

    def read_transform(self) -> list[float]:
        # Transform matrix is stored as literal string like in the SAT format!
        # 4th column is not stored
        # Read only the matrix values which contain all information needed,
        # the additional data are only hints for the kernel how to process
        # the data (rotation, reflection, scaling, shearing).
        values = self.read_str().split(" ")
        return [float(v) for v in values[:12]]


class SabBuilder(AbstractBuilder):
    """Low level data structure to manage ACIS SAB data files."""

    def __init__(self) -> None:
        self.header = AcisHeader()
        self.bodies: list[SabEntity] = []
        self.entities: list[SabEntity] = []

    def dump_sab(self) -> bytes:
        """Returns the SAB representation of the ACIS file as bytes."""
        self.reorder_records()
        self.header.n_entities = len(self.bodies) + int(
            self.header.has_asm_header
        )
        self.header.n_records = 0  # is always 0
        self.header.flags = 12  # important for 21800 - meaning unknown
        data: list[bytes] = [self.header.dumpb()]
        encoder = Encoder()
        for record in build_sab_records(self.entities):
            encoder.write_record(record)
        data.extend(encoder.buffer)
        data.append(self.header.sab_end_marker())
        return b"".join(data)

    def set_entities(self, entities: list[SabEntity]) -> None:
        """Reset entities and bodies list. (internal API)"""
        self.bodies = [e for e in entities if e.name == "body"]
        self.entities = entities


class SabExporter(EntityExporter[SabEntity]):
    def make_record(self, entity: AcisEntity) -> SabEntity:
        record = SabEntity(entity.type, id=entity.id)
        record.attributes = NULL_PTR
        return record

    def make_data_exporter(self, record: SabEntity) -> DataExporter:
        return SabDataExporter(self, record.data)

    def dump_sab(self) -> bytes:
        builder = SabBuilder()
        builder.header = self.header
        builder.set_entities(self.export_records())
        return builder.dump_sab()


def build_entities(
    records: Iterable[SabRecord], version: int
) -> Iterator[SabEntity]:
    for record in records:
        assert record[0].tag == Tags.ENTITY_TYPE, "invalid entity-name tag"
        name = record[0].value  # 1. entity-name
        if name in const.DATA_END_MARKERS:
            yield SabEntity(name)
            return
        assert record[1].tag == Tags.POINTER, "invalid attribute pointer tag"
        attr = record[1].value  # 2. attribute record pointer
        id_ = -1
        if version >= 700:
            assert record[2].tag == Tags.INT, "invalid id tag"
            id_ = record[2].value  # 3. int id
            data = record[3:]
        else:
            data = record[2:]
        yield SabEntity(name, attr, id_, data)


def resolve_pointers(entities: list[SabEntity]) -> list[SabEntity]:
    def ptr(num: int) -> SabEntity:
        if num == -1:
            return NULL_PTR
        return entities[num]

    for entity in entities:
        entity.attributes = ptr(entity.attr_ptr)
        entity.attr_ptr = -1
        for index, token in enumerate(entity.data):
            if token.tag == Tags.POINTER:
                entity.data[index] = Token(token.tag, ptr(token.value))
    return entities


def parse_sab(data: Union[bytes, bytearray]) -> SabBuilder:
    """Returns the :class:`SabBuilder` for the ACIS :term:`SAB` file content
    given as string or list of strings.

    Raises:
        ParsingError: invalid or unsupported ACIS data structure

    """
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("expected bytes, bytearray")
    builder = SabBuilder()
    decoder = Decoder(data)
    builder.header = decoder.read_header()
    entities = list(
        build_entities(decoder.read_records(), builder.header.version)
    )
    builder.set_entities(resolve_pointers(entities))
    return builder


class SabDataExporter(DataExporter):
    def __init__(self, exporter: SabExporter, data: list[Token]):
        self.version = exporter.version
        self.exporter = exporter
        self.data = data

    def write_int(self, value: int, skip_sat=False) -> None:
        """There are sometimes additional int values in SAB files which are
        not present in SAT files, maybe reference counters e.g. vertex, coedge.
        """
        self.data.append(Token(Tags.INT, value))

    def write_double(self, value: float) -> None:
        self.data.append(Token(Tags.DOUBLE, value))

    def write_interval(self, value: float) -> None:
        if math.isinf(value):
            self.data.append(Token(Tags.BOOL_FALSE, False))  # infinite "I"
        else:
            self.data.append(Token(Tags.BOOL_TRUE, True))  # finite "F"
            self.write_double(value)

    def write_loc_vec3(self, value: Vec3) -> None:
        self.data.append(Token(Tags.LOCATION_VEC, value))

    def write_dir_vec3(self, value: Vec3) -> None:
        self.data.append(Token(Tags.DIRECTION_VEC, value))

    def write_bool(self, value: bool, true: str, false: str) -> None:
        if value:
            self.data.append(Token(Tags.BOOL_TRUE, True))
        else:
            self.data.append(Token(Tags.BOOL_FALSE, False))

    def write_str(self, value: str) -> None:
        self.data.append(Token(Tags.STR, value))

    def write_literal_str(self, value: str) -> None:
        self.data.append(Token(Tags.LITERAL_STR, value))

    def write_ptr(self, entity: AcisEntity) -> None:
        record = NULL_PTR
        if not entity.is_none:
            record = self.exporter.get_record(entity)
        self.data.append(Token(Tags.POINTER, record))

    def write_transform(self, data: list[str]) -> None:
        # The last space is important!
        self.write_literal_str(" ".join(data) + " ")


def encode_entity_type(name: str) -> list[Token]:
    if name == const.NULL_PTR_NAME:
        raise InvalidLinkStructure(
            f"invalid record type: {const.NULL_PTR_NAME}"
        )
    parts = name.split("-")
    tokens = [Token(Tags.ENTITY_TYPE_EX, part) for part in parts[:-1]]
    tokens.append(Token(Tags.ENTITY_TYPE, parts[-1]))
    return tokens


def encode_entity_ptr(entity: SabEntity, entities: list[SabEntity]) -> Token:
    if entity.is_null_ptr:
        return Token(Tags.POINTER, -1)
    try:
        return Token(Tags.POINTER, entities.index(entity))
    except ValueError:
        raise InvalidLinkStructure(
            f"entity {str(entity)} not in record storage"
        )


def build_sab_records(entities: list[SabEntity]) -> Iterator[SabRecord]:
    for entity in entities:
        record: list[Token] = []
        record.extend(encode_entity_type(entity.name))
        # 1. attribute record pointer
        record.append(encode_entity_ptr(entity.attributes, entities))
        # 2. int id
        record.append(Token(Tags.INT, entity.id))
        for token in entity.data:
            if token.tag == Tags.POINTER:
                record.append(encode_entity_ptr(token.value, entities))
            elif token.tag == Tags.ENTITY_TYPE:
                record.extend(encode_entity_type(token.value))
            else:
                record.append(token)
        yield record


END_OF_RECORD = bytes([Tags.RECORD_END.value])
TRUE_RECORD = bytes([Tags.BOOL_TRUE.value])
FALSE_RECORD = bytes([Tags.BOOL_FALSE.value])
SUBTYPE_START_RECORD = bytes([Tags.SUBTYPE_START.value])
SUBTYPE_END_RECORD = bytes([Tags.SUBTYPE_END.value])
SAB_ENCODING = "utf8"


class Encoder:
    def __init__(self) -> None:
        self.buffer: list[bytes] = []

    def write_record(self, record: SabRecord) -> None:
        for token in record:
            self.write_token(token)
        self.buffer.append(END_OF_RECORD)

    def write_token(self, token: Token) -> None:
        tag = token.tag
        if tag in (Tags.INT, Tags.POINTER, Tags.ENUM):
            assert isinstance(token.value, int)
            self.buffer.append(struct.pack("<Bi", tag, token.value))
        elif tag == Tags.DOUBLE:
            assert isinstance(token.value, float)
            self.buffer.append(struct.pack("<Bd", tag, token.value))
        elif tag == Tags.STR:
            assert isinstance(token.value, str)
            data = token.value.encode(encoding=SAB_ENCODING)
            self.buffer.append(struct.pack("<BB", tag, len(data)) + data)
        elif tag == Tags.LITERAL_STR:
            assert isinstance(token.value, str)
            data = token.value.encode(encoding=SAB_ENCODING)
            self.buffer.append(struct.pack("<Bi", tag, len(data)) + data)
        elif tag in (Tags.ENTITY_TYPE, Tags.ENTITY_TYPE_EX):
            assert isinstance(token.value, str)
            data = token.value.encode(encoding=SAB_ENCODING)
            self.buffer.append(struct.pack("<BB", tag, len(data)) + data)
        elif tag in (Tags.LOCATION_VEC, Tags.DIRECTION_VEC):
            v = token.value
            assert isinstance(v, Vec3)
            self.buffer.append(struct.pack("<B3d", tag, v.x, v.y, v.z))
        elif tag == Tags.BOOL_TRUE:
            self.buffer.append(TRUE_RECORD)
        elif tag == Tags.BOOL_FALSE:
            self.buffer.append(FALSE_RECORD)
        elif tag == Tags.SUBTYPE_START:
            self.buffer.append(SUBTYPE_START_RECORD)
        elif tag == Tags.SUBTYPE_END:
            self.buffer.append(SUBTYPE_END_RECORD)
        else:
            raise ValueError(f"invalid tag in token: {token}")
