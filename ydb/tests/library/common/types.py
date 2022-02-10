#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs

from enum import unique, Enum, IntEnum
from ydb.tests.library.common.generators import int_between, one_of, float_in, string_with_length, actor_id


@unique
class DeltaTypes(IntEnum):
    AddTable = 1,
    DropTable = 2,
    AddColumn = 3,
    DropColumn = 4,
    AddColumnToKey = 5,
    AddColumnToFamily = 6,
    AddFamily = 7,
    UpdateExecutorInfo = 8,
    SetCompactionPolicy = 9,
    SetRoom = 10,
    SetFamily = 11,
    SetRedo = 12,
    SetTable = 13


@unique
class PDiskCategory(IntEnum):
    ROT = 0
    SSD = 1


@unique
class FailDomainType(IntEnum):
    DC = 10
    Room = 20
    Rack = 30
    Body = 40


@unique
class VDiskCategory(IntEnum):
    Default = 0
    Test1 = 1
    Test2 = 2
    Test3 = 3
    Log = 10
    Extra2 = 22
    Extra3 = 23


def _erasure_type(id_, min_fail_domains, min_alive_replicas):
    return id_, min_fail_domains, min_alive_replicas


@unique
class Erasure(Enum):
    NONE = _erasure_type(id_=0, min_fail_domains=1, min_alive_replicas=1)
    MIRROR_3 = _erasure_type(id_=1, min_fail_domains=4, min_alive_replicas=1)
    BLOCK_3_1 = _erasure_type(id_=2, min_fail_domains=5, min_alive_replicas=4)
    STRIPE_3_1 = _erasure_type(id_=3, min_fail_domains=5, min_alive_replicas=4)
    BLOCK_4_2 = _erasure_type(id_=4, min_fail_domains=8, min_alive_replicas=6)
    BLOCK_3_2 = _erasure_type(id_=5, min_fail_domains=7, min_alive_replicas=5)
    STRIPE_4_2 = _erasure_type(id_=6, min_fail_domains=8, min_alive_replicas=6)
    STRIPE_3_2 = _erasure_type(id_=7, min_fail_domains=7, min_alive_replicas=5)
    MIRROR_3_2 = _erasure_type(id_=8, min_fail_domains=5, min_alive_replicas=3)
    MIRROR_3_DC = _erasure_type(id_=9, min_fail_domains=3, min_alive_replicas=3)
    MIRROR_3OF4 = _erasure_type(id_=18, min_fail_domains=8, min_alive_replicas=6)

    def __init__(self, id_, min_fail_domains, min_alive_replicas):
        self.__id = id_
        self.__min_fail_domains = min_fail_domains
        self.__min_alive_replicas = min_alive_replicas

    def __str__(self):
        return self.name.replace("_", "-").lower()

    def __repr__(self):
        return self.__str__()

    def __int__(self):
        return self.__id

    @property
    def min_fail_domains(self):
        return self.__min_fail_domains

    @property
    def min_alive_replicas(self):
        return self.__min_alive_replicas

    @staticmethod
    def from_string(string_):
        string_ = string_.upper()
        string_ = string_.replace("-", "_")
        return Erasure[string_]

    @staticmethod
    def from_int(species):
        for candidate in list(Erasure):
            if int(candidate) == species:
                return candidate
        raise ValueError("No valid candidate found")

    @staticmethod
    def common_used():
        return (
            Erasure.NONE, Erasure.BLOCK_4_2, Erasure.MIRROR_3_DC,
            Erasure.MIRROR_3
        )


@unique
class TabletStates(IntEnum):
    Created = 0,
    ResolveStateStorage = 1,
    Candidate = 2,
    BlockBlobStorage = 3,
    RebuildGraph = 4,
    WriteZeroEntry = 5,
    Restored = 6,
    Discover = 7,
    Lock = 8,
    Dead = 9,
    Active = 10

    @staticmethod
    def from_int(val):
        for tablet_state in list(TabletStates):
            if int(tablet_state) == val:
                return tablet_state
        return None


def _tablet_type(id_, magic, is_unique=False, service_name=None):
    """
    Convenient wrapper for TabletTypes enum

    :return: tuple of all arguments
    """
    return id_, magic, is_unique, service_name


@unique
class TabletTypes(Enum):
    TX_DUMMY = _tablet_type(8, 999)
    RTMR_PARTITION = _tablet_type(10, 999)
    KEYVALUEFLAT = _tablet_type(12, 999)

    KESUS = _tablet_type(29, 999)
    PERSQUEUE = _tablet_type(20, 999)
    FLAT_DATASHARD = _tablet_type(18, 999, service_name='DATASHARD')
    BLOCKSTORE_VOLUME = _tablet_type(25, 999)
    BLOCKSTORE_PARTITION = _tablet_type(26, 999)

    FLAT_TX_COORDINATOR = _tablet_type(13, 0x800001, service_name='TX_COORDINATOR')
    TX_MEDIATOR = _tablet_type(5, 0x810001, service_name='TX_MEDIATOR')
    TX_ALLOCATOR = _tablet_type(23, 0x820001, service_name='TX_ALLOCATOR')
    FLAT_TX_PROXY = _tablet_type(17, 0x820001, service_name='TX_PROXY')

    FLAT_HIVE = _tablet_type(14, 0xA001, is_unique=True, service_name='HIVE')
    FLAT_SCHEMESHARD = _tablet_type(16, 0x8587a0, is_unique=True, service_name='FLAT_TX_SCHEMESHARD')

    CMS = _tablet_type(21, 0x2000, is_unique=True, service_name='CMS')
    NODE_BROKER = _tablet_type(22, 0x2001, is_unique=True, service_name='NODE_BROKER')
    TENANT_SLOT_BROKER = _tablet_type(27, 0x2002, is_unique=True, service_name='TENANT_SLOT_BROKER')
    CONSOLE = _tablet_type(28, 0x2003, is_unique=True, service_name='CONSOLE')
    FLAT_BS_CONTROLLER = _tablet_type(15, 0x1001, is_unique=True, service_name='BS_CONTROLLER')

    USER_TYPE_START = _tablet_type(0xFF, 0)
    TYPE_INVALID = _tablet_type(0xFFFFFFFF, 0)

    def __init__(self, id_, magic, is_unique=False, service_name=None):
        self.__id = id_
        self.__magic_preset = magic
        self.__is_unique = is_unique
        self.__service_name = service_name

    def __int__(self):
        return self.__id

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    @property
    def service_name(self):
        """
        :return: Name of the logging service for this tablet type
        """
        if self.__service_name is None:
            return self.name
        return self.__service_name

    def tablet_id_for(self, index, domain_id=1):
        if self.__is_unique:
            return domain_id << 56 | self.__magic_preset

        raw_tablet_id = self.__magic_preset + index
        return (domain_id << 56) | (domain_id << 44) | (raw_tablet_id & 0x00000FFFFFFFFFFF)

    @staticmethod
    def from_int(val):
        for tablet_type in list(TabletTypes):
            if tablet_type.__id == val:
                return tablet_type
        return TabletTypes.TYPE_INVALID


def bool_converter(astr):
    if isinstance(astr, bool):
        return astr
    if astr.lower() == 'true':
        return True
    else:
        return False


class AbstractTypeEnum(Enum):

    def __str__(self):
        return self._name_

    def __repr__(self):
        return self.__str__()

    def as_obj(self, value):
        return self.to_obj_converter(value)

    @property
    def idn(self):
        return self._idn_

    @classmethod
    def from_int(cls, idn):
        for v in list(cls):
            if idn == v._idn_:
                return v
        raise AssertionError('There is no PType with value = ' + str(idn))


def from_bytes(val):
    try:
        return codecs.decode(val, 'utf8')
    except (UnicodeEncodeError, TypeError):
        return val


def _ptype_from(idn, generator, to_obj_converter=str, proto_field='Bytes', min_value=None, max_value=None):
    return idn, generator, to_obj_converter, proto_field, min_value, max_value


# noinspection PyTypeChecker
@unique
class PType(AbstractTypeEnum):
    Int32 = _ptype_from(1, int_between(-2 ** 31, 2 ** 31), int, proto_field='Int32', min_value=-2 ** 31, max_value=2 ** 31 - 1)
    Uint32 = _ptype_from(2, int_between(0, 2 ** 32), int, proto_field='Uint32', min_value=0, max_value=2 ** 32 - 1)
    Int64 = _ptype_from(3, int_between(-2 ** 63, 2 ** 63), int, proto_field='Int64', min_value=-2**63, max_value=2 ** 63 - 1)
    Uint64 = _ptype_from(4, int_between(0, 2 ** 64 - 1), int, proto_field='Uint64', min_value=0, max_value=2 ** 64 - 1)
    Byte = _ptype_from(5, int_between(0, 255), int, min_value=0, max_value=255)
    Bool = _ptype_from(6, one_of([True, False]), bool_converter, proto_field='Bool', min_value=True, max_value=False)

    Double = _ptype_from(32, float_in(-100, 100), float, proto_field='Double')
    Float = _ptype_from(33, float_in(-100, 100), float, proto_field='Float')

    # Rework Pair later
    PairUi64Ui64 = _ptype_from(257, int_between(0, 2 ** 64 - 1), int)

    String = _ptype_from(4097, string_with_length(4), str)
    SmallBoundedString = _ptype_from(4113, string_with_length(4), str)
    LargeBoundedString = _ptype_from(4114, string_with_length(500), str)

    Utf8 = _ptype_from(4608, string_with_length(500), from_bytes)

    ActorID = _ptype_from(8193, actor_id(), str)

    def __init__(self, idn, generator, to_obj_converter, proto_field, min_value, max_value):
        self._idn_ = idn
        self.generator = generator
        self.to_obj_converter = to_obj_converter
        self._proto_field = proto_field
        self.__min_value = min_value
        self.__max_value = max_value

    @property
    def max_value(self):
        return self.__max_value

    @property
    def min_value(self):
        return self.__min_value

    @staticmethod
    def from_string(string_):
        if isinstance(string_, PType):
            return string_
        return PType[string_]
