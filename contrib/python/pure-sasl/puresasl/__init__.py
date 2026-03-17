__version__ = '0.6.2'
__version_info__ = (0, 6, 2)


class SASLError(Exception):
    """
    Typically represents a user error in configuration or usage of the
    SASL client or mechanism.
    """
    pass


class SASLProtocolException(Exception):
    """
    Raised when an error occurs while communicating with the SASL server
    or the client and server fail to agree on negotiated properties such
    as quality of protection.
    """
    pass


class SASLWarning(Warning):
    """
    Emitted in potentially fatal circumstances.
    """
    pass


class QOP(object):

    AUTH = b'auth'
    AUTH_INT = b'auth-int'
    AUTH_CONF = b'auth-conf'

    all = (AUTH, AUTH_INT, AUTH_CONF)

    bit_map = {1: AUTH, 2: AUTH_INT, 4: AUTH_CONF}

    name_map = dict((bit, name) for name, bit in bit_map.items())

    @classmethod
    def names_from_bitmask(cls, byt):
        return set(name for bit, name in cls.bit_map.items() if bit & byt)

    @classmethod
    def flag_from_name(cls, name):
        return cls.name_map[name]
