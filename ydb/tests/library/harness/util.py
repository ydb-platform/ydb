# -*- coding: utf-8 -*-
import enum


@enum.unique
class LogLevels(enum.IntEnum):
    EMERG = 0,
    ALERT = 1,
    CRIT = 2,
    ERROR = 3,
    WARN = 4,
    NOTICE = 5,
    INFO = 6,
    DEBUG = 7,
    TRACE = 8

    @staticmethod
    def from_string(val):
        names = []
        for x in list(LogLevels):
            names.append(x.name)
            if val == x.name:
                return x
        raise ValueError("Invalid LogLevel: valid are %s" % names)


def _pdisk_state(id_, is_valid_state):
    return id_, is_valid_state


@enum.unique
class PDiskState(enum.Enum):
    Initial = _pdisk_state(0, is_valid_state=True)
    InitialFormatRead = _pdisk_state(1, is_valid_state=True)
    InitialFormatReadError = _pdisk_state(2, is_valid_state=False)
    InitialSysLogRead = _pdisk_state(3, is_valid_state=True)
    InitialSysLogReadError = _pdisk_state(4, is_valid_state=False)
    InitialSysLogParseError = _pdisk_state(5, is_valid_state=False)
    InitialCommonLogRead = _pdisk_state(6, is_valid_state=True)
    InitialCommonLogReadError = _pdisk_state(7, is_valid_state=False)
    InitialCommonLogParseError = _pdisk_state(8, is_valid_state=False)
    CommonLoggerInitError = _pdisk_state(9, is_valid_state=False)
    Normal = _pdisk_state(10, is_valid_state=True)
    OpenFileError = _pdisk_state(11, is_valid_state=False)
    ChunkQuotaError = _pdisk_state(12, is_valid_state=False)
    DeviceIoError = _pdisk_state(13, is_valid_state=False)
    Stopped = _pdisk_state(14, is_valid_state=False)

    def __init__(self, id_, is_valid_state):
        self.__id = id_
        self.__is_valid_state = is_valid_state

    def __int__(self):
        return self.__id

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    @property
    def is_valid_state(self):
        return self.__is_valid_state

    @staticmethod
    def from_int(val):
        for x in list(PDiskState):
            if val == int(x):
                return x
        raise ValueError()

    @staticmethod
    def from_string(val):
        for x in list(PDiskState):
            if val == x.name:
                return x
        raise ValueError()
