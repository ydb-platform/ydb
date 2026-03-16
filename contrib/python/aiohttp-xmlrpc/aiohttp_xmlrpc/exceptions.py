from lxml import etree

from .common import py2xml


__all__ = (
    "XMLRPCError", "ApplicationError", "InvalidCharacterError", "ParseError", "ServerError",
    "SystemError", "TransportError", "UnsupportedEncodingError",
)


class XMLRPCError(Exception):
    code = -32500

    @property
    def message(self):
        return self.args[0]

    @property
    def name(self):
        return self.__class__.__name__

    def __repr__(self):
        return "<[{0.code}] {0.name}({0.message})>".format(self)


class ParseError(XMLRPCError):
    code = -32700


class UnsupportedEncodingError(ParseError):
    code = -32701


class InvalidCharacterError(ParseError):
    code = -32702


class ServerError(XMLRPCError):
    code = -32603


class InvalidData(ServerError):
    code = -32600


class MethodNotFound(ServerError):
    code = -32601


class InvalidArguments(ServerError):
    code = -32602


class ApplicationError(XMLRPCError):
    code = -32500


class SystemError(XMLRPCError):
    code = -32400


class TransportError(XMLRPCError):
    code = -32300


__EXCEPTION_CODES = {
    -32000: Exception,
    XMLRPCError.code: XMLRPCError,
    ParseError.code: ParseError,
    UnsupportedEncodingError.code: UnsupportedEncodingError,
    InvalidCharacterError.code: InvalidCharacterError,
    ServerError.code: ServerError,
    InvalidData.code: InvalidData,
    MethodNotFound.code: MethodNotFound,
    InvalidArguments.code: InvalidArguments,
    ApplicationError.code: ApplicationError,
    SystemError.code: SystemError,
    TransportError.code: TransportError,
}

__EXCEPTION_TYPES = {value: key for key, value in __EXCEPTION_CODES.items()}


def register_exception(exception_type: BaseException, code: int):
    code = int(code)
    if not issubclass(exception_type, BaseException):
        raise TypeError("Exception must be instance of Base exception")

    if code in __EXCEPTION_CODES:
        raise ValueError("Exception with code %s already registered" % code)

    if exception_type in __EXCEPTION_TYPES:
        raise ValueError("Exception type %r already registered" % exception_type)

    __EXCEPTION_CODES[code] = exception_type
    __EXCEPTION_TYPES[exception_type] = code


def xml2py_exception(code: int, fault: str, default_exc_class=XMLRPCError):
    if code not in __EXCEPTION_CODES:
        exc = default_exc_class(fault)
        exc.code = code
        return exc

    exc = __EXCEPTION_CODES[code]
    return exc(fault)


@py2xml.register(Exception)
def _(value):
    code, reason = __EXCEPTION_TYPES[Exception], repr(value)

    for klass in value.__class__.__mro__:
        if klass in __EXCEPTION_TYPES:
            code = __EXCEPTION_TYPES[klass]
            break

    struct = etree.Element("struct")

    for key, value in (("faultCode", code), ("faultString", reason)):
        member = etree.Element("member")
        struct.append(member)

        key_el = etree.Element("name")
        key_el.text = key
        member.append(key_el)

        value_el = etree.Element("value")
        value_el.append(py2xml(value))
        member.append(value_el)

    # struct.attrib['error'] = '1'
    return struct
