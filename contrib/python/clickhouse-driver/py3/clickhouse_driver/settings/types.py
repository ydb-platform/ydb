from ..util.helpers import asbool
from ..varint import write_varint
from ..writer import write_binary_str


class SettingType(object):
    @classmethod
    def write(cls, value, buf):
        raise NotImplementedError


class SettingUInt64(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_varint(int(value), buf)


class SettingBool(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_varint(asbool(value), buf)


class SettingString(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_binary_str(value, buf)


class SettingChar(SettingType):
    @classmethod
    def write(cls, value, buf):
        write_binary_str(value[0], buf)


class SettingFloat(SettingType):
    @classmethod
    def write(cls, value, buf):
        """
        Float is written in string representation.
        """
        write_binary_str(str(value), buf)


class SettingMaxThreads(SettingUInt64):
    @classmethod
    def write(cls, value, buf):
        if value == 'auto':
            value = 0
        super(SettingMaxThreads, cls).write(value, buf)
