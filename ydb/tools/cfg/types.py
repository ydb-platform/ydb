#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import enum


def py3_ensure_list(iterable):
    if sys.version_info[0] >= 3:
        return list(iterable)
    return iterable


def py3_ensure_str(value):
    if sys.version_info[0] >= 3 and isinstance(value, bytes):
        return value.decode()
    return value


def py3_ensure_bytes(value):
    if sys.version_info[0] >= 3 and isinstance(value, str):
        return value.encode()
    return value


@enum.unique
class LogLevels(enum.IntEnum):
    EMERG = (0,)
    ALERT = (1,)
    CRIT = (2,)
    ERROR = (3,)
    WARN = (4,)
    NOTICE = (5,)
    INFO = (6,)
    DEBUG = (7,)
    TRACE = 8


def _tablet_type(id_, magic, is_unique=False, service_name=None):
    return id_, magic, is_unique, service_name


@enum.unique
class TabletTypes(enum.Enum):
    TX_MEDIATOR = _tablet_type(5, 0x810001, service_name='TX_MEDIATOR')
    TX_DUMMY = _tablet_type(8, 999)
    RTMR_PARTITION = _tablet_type(10, 999)
    KEYVALUEFLAT = _tablet_type(12, 999)
    FLAT_TX_COORDINATOR = _tablet_type(13, 0x800001, service_name='TX_COORDINATOR')
    FLAT_HIVE = _tablet_type(14, 0xA001, is_unique=True, service_name='HIVE')
    FLAT_BS_CONTROLLER = _tablet_type(15, 0x1001, is_unique=True, service_name='BS_CONTROLLER')
    FLAT_SCHEMESHARD = _tablet_type(16, 0x1000008587A0, is_unique=True, service_name='FLAT_TX_SCHEMESHARD')
    TX_ALLOCATOR = _tablet_type(23, 0x820001, service_name='TX_ALLOCATOR')
    FLAT_TX_PROXY = _tablet_type(17, 0x820001, service_name='TX_PROXY')
    FLAT_DATASHARD = _tablet_type(18, 999, service_name='DATASHARD')
    PERSQUEUE = _tablet_type(20, 999)

    CMS = _tablet_type(21, 0x2000, is_unique=True, service_name='CMS')
    NODE_BROKER = _tablet_type(22, 0x2001, is_unique=True, service_name='NODE_BROKER')
    TENANT_SLOT_BROKER = _tablet_type(27, 0x2002, is_unique=True, service_name='TENANT_SLOT_BROKER')
    CONSOLE = _tablet_type(28, 0x2003, is_unique=True, service_name='CONSOLE')

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
            if int(tablet_type) == val:
                return tablet_type
        return TabletTypes.TYPE_INVALID


@enum.unique
class TabletTypesFixed(enum.Enum):
    TX_MEDIATOR = _tablet_type(5, 0x810001, service_name='TX_MEDIATOR')
    TX_DUMMY = _tablet_type(8, 999)
    RTMR_PARTITION = _tablet_type(10, 999)
    KEYVALUEFLAT = _tablet_type(12, 999)
    FLAT_TX_COORDINATOR = _tablet_type(13, 0x800001, service_name='TX_COORDINATOR')
    FLAT_HIVE = _tablet_type(14, 0xA001, is_unique=True, service_name='HIVE')
    FLAT_BS_CONTROLLER = _tablet_type(15, 0x1001, is_unique=True, service_name='BS_CONTROLLER')
    FLAT_SCHEMESHARD = _tablet_type(16, 0x8587A0, is_unique=True, service_name='FLAT_TX_SCHEMESHARD')
    TX_ALLOCATOR = _tablet_type(23, 0x820001, service_name='TX_ALLOCATOR')
    FLAT_TX_PROXY = _tablet_type(17, 0x820001, service_name='TX_PROXY')
    FLAT_DATASHARD = _tablet_type(18, 999, service_name='DATASHARD')
    PERSQUEUE = _tablet_type(20, 999)

    CMS = _tablet_type(21, 0x2000, is_unique=True, service_name='CMS')
    NODE_BROKER = _tablet_type(22, 0x2001, is_unique=True, service_name='NODE_BROKER')
    TENANT_SLOT_BROKER = _tablet_type(27, 0x2002, is_unique=True, service_name='TENANT_SLOT_BROKER')
    CONSOLE = _tablet_type(28, 0x2003, is_unique=True, service_name='CONSOLE')

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

    def tablet_id_for(self, index, state_storage_id=1, domain_id=0):
        if self.__is_unique:
            return state_storage_id << 56 | self.__magic_preset

        raw_tablet_id = self.__magic_preset + index
        return (state_storage_id << 56) | (domain_id << 44) | raw_tablet_id

    @staticmethod
    def from_int(val):
        for tablet_type in list(TabletTypes):
            if int(tablet_type) == val:
                return tablet_type
        return TabletTypes.TYPE_INVALID


def _erasure_type(id_, min_fail_domains, min_alive_replicas):
    return id_, min_fail_domains, min_alive_replicas


@enum.unique
class PDiskCategory(enum.IntEnum):
    ROT = 0
    SSD = 1
    NVME = 2

    @staticmethod
    def from_string(value):
        for category in list(PDiskCategory):
            if str(category) == value:
                return category
        raise ValueError('Invalid PDiskCategory')

    def __str__(self):
        return self.name

    @staticmethod
    def all_pdisk_category_names():
        return py3_ensure_list(map(str, list(PDiskCategory)))

    @staticmethod
    def all_categories():
        return (
            PDiskCategory.SSD,
            PDiskCategory.ROT,
            PDiskCategory.NVME,
        )


@enum.unique
class FailDomainType(enum.IntEnum):
    DC = 10
    Room = 20
    Rack = 30
    Body = 40
    Disk = 256

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def all_fail_domain_type_names():
        return py3_ensure_list(map(str, list(FailDomainType)))

    @staticmethod
    def from_string(value):
        if isinstance(value, FailDomainType):
            return value
        for fail_domain in list(FailDomainType):
            if str(fail_domain) == value:
                return fail_domain

        raise ValueError("Invalid FailDomainType")

    @staticmethod
    def is_body_fail_domain(actual_faildomain):
        return str(actual_faildomain) in {str(FailDomainType.Body), str(FailDomainType.Disk)}


DistinctionLevels = {
    FailDomainType.DC: (10, 20, 10, 20),
    FailDomainType.Room: (10, 20, 10, 30),
    FailDomainType.Rack: (10, 20, 10, 40),
    FailDomainType.Body: (10, 20, 10, 50),
    FailDomainType.Disk: (10, 20, 10, 256),
}


@enum.unique
class NodeType(enum.IntEnum):
    STORAGE = 1
    COMPUTE = 2
    HYBRID = 3

    @staticmethod
    def all_node_type_names():
        return map(str, list(NodeType))

    def __str__(self):
        return self.name.upper()

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def from_string(value):
        if isinstance(value, NodeType):
            return value
        for node_type in list(NodeType):
            if str(node_type) == value:
                return int(node_type)

        raise ValueError("Invalid NodeType " + repr(value))


@enum.unique
class VDiskCategory(enum.IntEnum):
    Default = 0
    Test1 = 1
    Test2 = 2
    Test3 = 3
    Log = 10
    LocalMode = 11
    Extra2 = 22
    Extra3 = 23
    Extra4 = 24
    Extra5 = 25

    INVALID_CATEGORY = 1000

    def __str__(self):
        return self.name

    @staticmethod
    def from_string(st):
        for category in list(VDiskCategory):
            if category.name == st:
                return category
        return VDiskCategory.INVALID_CATEGORY


@enum.unique
class Erasure(enum.Enum):
    NONE = _erasure_type(id_=0, min_fail_domains=1, min_alive_replicas=1)
    MIRROR_3 = _erasure_type(id_=1, min_fail_domains=4, min_alive_replicas=1)
    BLOCK_4_2 = _erasure_type(id_=4, min_fail_domains=8, min_alive_replicas=6)
    MIRROR_3_DC = _erasure_type(id_=9, min_fail_domains=3, min_alive_replicas=3)
    MIRROR_3OF4 = _erasure_type(id_=18, min_fail_domains=8, min_alive_replicas=1)

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
    def from_string(value):
        if isinstance(value, Erasure):
            return value
        value = value.upper()
        value = value.replace("-", "_")
        return Erasure[value]

    @staticmethod
    def all_erasure_type_names():
        return py3_ensure_list(map(str, list(Erasure)))
