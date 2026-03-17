import binascii
import re
import typing
from binascii import a2b_base64, b2a_base64
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from io import BytesIO
from typing import Generic, Optional, TypeVar, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..common import Dumper, Loader
from ..feature_requirement import HAS_PY_311, HAS_SELF_TYPE
from ..provider.essential import CannotProvide, Mediator
from ..provider.loc_stack_filtering import P, create_loc_stack_checker
from ..provider.loc_stack_tools import find_owner_with_field
from ..provider.located_request import LocatedRequest, for_predicate
from ..special_cases_optimization import as_is_stub
from .json_schema.definitions import JSONSchema
from .json_schema.request_cls import JSONSchemaRequest
from .json_schema.schema_model import JSONSchemaBuiltinFormat, JSONSchemaType
from .load_error import FormatMismatchLoadError, TypeLoadError, ValueLoadError
from .provider_template import DumperProvider, JSONSchemaProvider, MorphingProvider
from .request_cls import DumperRequest, LoaderRequest, StrictCoercionRequest


class IsoFormatProvider(MorphingProvider):
    _CLS_TO_JSON_FORMAT = {
        time: JSONSchemaBuiltinFormat.TIME,
        date: JSONSchemaBuiltinFormat.DATE,
        datetime: JSONSchemaBuiltinFormat.DATE_TIME,
    }

    def __init__(self, cls: type[Union[date, time]]):
        self._cls = cls
        self._loc_stack_checker = create_loc_stack_checker(cls)

    def __repr__(self):
        return f"{type(self)}(cls={self._cls})"

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(self._make_loader)

    def _make_loader(self):
        raw_loader = self._cls.fromisoformat

        def isoformat_loader(data):
            try:
                return raw_loader(data)
            except TypeError:
                raise TypeLoadError(str, data)
            except ValueError:
                raise ValueLoadError("Invalid isoformat string", data)
        return isoformat_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        return self._cls.isoformat

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.STRING, format=self._CLS_TO_JSON_FORMAT[self._cls])


@for_predicate(datetime)
class DatetimeFormatProvider(MorphingProvider):
    def __init__(self, fmt: str):
        self._fmt = fmt

    def __repr__(self):
        return f"{type(self)}(fmt={self._fmt})"

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(self._make_loader)

    def _make_loader(self):
        fmt = self._fmt

        def datetime_format_loader(data):
            try:
                return datetime.strptime(data, fmt)  # noqa: DTZ007
            except ValueError:
                raise FormatMismatchLoadError(fmt, data)
            except TypeError:
                raise TypeLoadError(str, data)

        return datetime_format_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        fmt = self._fmt

        def datetime_format_dumper(data: datetime):
            return data.strftime(fmt)

        return datetime_format_dumper

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.STRING)


@for_predicate(datetime)
class DatetimeTimestampProvider(MorphingProvider):
    def __init__(self, tz: Optional[timezone]):
        self._tz = tz

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(self._make_loader)

    def _make_loader(self):
        tz = self._tz

        def datetime_timestamp_loader(data):
            try:
                return datetime.fromtimestamp(data, tz=tz)
            except TypeError:
                raise TypeLoadError(Union[int, float], data)
            except ValueError:
                raise ValueLoadError("Unexpected value", data)
            except OverflowError:
                raise ValueLoadError(
                    "Timestamp is out of the range of supported values",
                    data,
                )

        return datetime_timestamp_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        def datetime_timestamp_dumper(data: datetime):
            return data.timestamp()
        return datetime_timestamp_dumper

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.NUMBER)


@for_predicate(date)
class DateTimestampProvider(MorphingProvider):
    def _is_pydatetime(self) -> bool:
        try:
            import _pydatetime
        except ImportError:
            return False
        else:
            if datetime is _pydatetime:
                return True

        return False

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(self._make_loader)

    def _make_loader(self):
        def date_timestamp_loader(data):
            try:
                # Pure-Python implementation and C-extension implementation
                # of datetime.date.fromtimestamp module works differently with a None arg.
                # See https://github.com/python/cpython/issues/120268 for more details.
                if data is None:
                    raise TypeLoadError(Union[int, float], data)

                return date.fromtimestamp(data)  # noqa: DTZ012
            except TypeError:
                raise TypeLoadError(Union[int, float], data)
            except ValueError:
                raise ValueLoadError("Unexpected value", data)
            except OverflowError:
                raise ValueLoadError(
                    "Timestamp is out of the range of supported values",
                    data,
                )

        def pydate_timestamp_loader(data):
            try:
                return date.fromtimestamp(data)  # noqa: DTZ012
            except TypeError:
                raise TypeLoadError(Union[int, float], data)
            except OverflowError:
                raise ValueLoadError(
                    "Timestamp is out of the range of supported values",
                    data,
                )

        return pydate_timestamp_loader if self._is_pydatetime() else date_timestamp_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        def date_timestamp_dumper(data: date):
            dt = datetime(
                year=data.year,
                month=data.month,
                day=data.day,
                tzinfo=timezone.utc,
            )
            return dt.timestamp()
        return date_timestamp_dumper

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.NUMBER)


@for_predicate(timedelta)
class SecondsTimedeltaProvider(MorphingProvider):
    _OK_TYPES = (int, float, Decimal)

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(self._make_loader)

    def _make_loader(self):
        ok_types = self._OK_TYPES

        def timedelta_loader(data):
            if type(data) not in ok_types:
                raise TypeLoadError(Union[int, float, Decimal], data)
            return timedelta(seconds=int(data), microseconds=int(data % 1 * 10 ** 6))

        return timedelta_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        return timedelta.total_seconds

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.NUMBER)


def none_loader(data):
    if data is None:
        return None  # noqa: RET501
    raise TypeLoadError(None, data)


@for_predicate(None)
class NoneProvider(MorphingProvider):
    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return none_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return as_is_stub

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.NULL)


class _Base64DumperMixin(DumperProvider):
    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        def bytes_base64_dumper(data):
            return b2a_base64(data, newline=False).decode("ascii")
        return bytes_base64_dumper


class _Base64JSONSchemaMixin(JSONSchemaProvider):
    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.STRING, content_encoding="base64")


B64_PATTERN = re.compile(b"[A-Za-z0-9+/]*={0,2}")


@for_predicate(bytes)
class BytesBase64Provider(_Base64DumperMixin, _Base64JSONSchemaMixin, MorphingProvider):
    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(self._make_loader)

    def _make_loader(self):
        def bytes_base64_loader(data):
            try:
                encoded = data.encode("ascii")
            except AttributeError:
                raise TypeLoadError(str, data)

            if not B64_PATTERN.fullmatch(encoded):
                raise ValueLoadError("Bad base64 string", data)

            try:
                return a2b_base64(encoded)
            except binascii.Error as e:
                raise ValueLoadError(str(e), data)
        return bytes_base64_loader


@for_predicate(BytesIO)
class BytesIOBase64Provider(_Base64JSONSchemaMixin, MorphingProvider):
    _BYTES_PROVIDER = BytesBase64Provider()

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(
            self._make_loader,
            loader=self._BYTES_PROVIDER.provide_loader(mediator, request),
        )

    def _make_loader(self, loader: Loader):
        def bytes_io_base64_loader(data):
            return BytesIO(loader(data))
        return bytes_io_base64_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        def bytes_io_base64_dumper(data: BytesIO):
            return b2a_base64(data.getvalue(), newline=False).decode("ascii")
        return bytes_io_base64_dumper


@for_predicate(typing.IO[bytes])
class IOBytesBase64Provider(BytesIOBase64Provider, _Base64JSONSchemaMixin, MorphingProvider):
    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return mediator.cached_call(self._make_dumper)

    def _make_dumper(self):
        def io_bytes_base64_dumper(data: typing.IO[bytes]):
            if data.seekable():
                data.seek(0)

            return b2a_base64(data.read(), newline=False).decode("ascii")
        return io_bytes_base64_dumper


@for_predicate(bytearray)
class BytearrayBase64Provider(_Base64DumperMixin, _Base64JSONSchemaMixin, MorphingProvider):
    _BYTES_PROVIDER = BytesBase64Provider()

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        bytes_loader = self._BYTES_PROVIDER.provide_loader(
            mediator,
            replace(request, loc_stack=request.loc_stack.replace_last_type(bytes)),
        )

        return mediator.cached_call(
            self._make_loader,
            loader=bytes_loader,
        )

    def _make_loader(self, loader: Loader):
        def bytearray_base64_loader(data):
            return bytearray(loader(data))
        return bytearray_base64_loader


def _regex_dumper(data: re.Pattern):
    return data.pattern


@for_predicate(re.Pattern)
class RegexPatternProvider(MorphingProvider):
    def __init__(self, flags: re.RegexFlag = re.RegexFlag(0)):
        self.flags = flags

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        return mediator.cached_call(self._make_loader)

    def _make_loader(self):
        flags = self.flags
        re_compile = re.compile

        def regex_loader(data):
            if not isinstance(data, str):
                raise TypeLoadError(str, data)

            try:
                return re_compile(data, flags)
            except re.error as e:
                raise ValueLoadError(str(e), data)

        return regex_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return _regex_dumper

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.STRING, format=JSONSchemaBuiltinFormat.REGEX)


T = TypeVar("T")


class ScalarProvider(MorphingProvider, Generic[T]):
    def __init__(
        self,
        target: type[T],
        strict_coercion_loader: Loader[T],
        lax_coercion_loader: Loader[T],
        dumper: Dumper[T],
        json_schema: JSONSchema,
    ):
        self._target = target
        self._loc_stack_checker = create_loc_stack_checker(target)
        self._strict_coercion_loader = strict_coercion_loader
        self._lax_coercion_loader = lax_coercion_loader
        self._dumper = dumper
        self._json_schema = json_schema

    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        strict_coercion = mediator.mandatory_provide(StrictCoercionRequest(loc_stack=request.loc_stack))
        return mediator.cached_call(
            self._make_loader,
            strict_coercion=strict_coercion,
        )

    def _make_loader(self, *, strict_coercion: bool):
        return self._strict_coercion_loader if strict_coercion else self._lax_coercion_loader

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return self._dumper

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return self._json_schema


def int_strict_coercion_loader(data):
    if type(data) is int:
        return data
    raise TypeLoadError(int, data)


def int_lax_coercion_loader(data):
    try:
        return int(data)
    except ValueError as e:
        e_str = str(e)
        if e_str.startswith("invalid literal"):
            raise ValueLoadError("Bad string format", data)
        raise ValueLoadError(e_str, data)
    except TypeError:
        raise TypeLoadError(Union[int, float, str], data)


INT_PROVIDER = ScalarProvider(
    target=int,
    strict_coercion_loader=int_strict_coercion_loader,
    lax_coercion_loader=int_lax_coercion_loader,
    dumper=as_is_stub,
    json_schema=JSONSchema(type=JSONSchemaType.INTEGER),
)


def float_strict_coercion_loader(data):
    if type(data) in (float, int):
        return float(data)
    raise TypeLoadError(Union[float, int], data)


def float_lax_coercion_loader(data):
    try:
        return float(data)
    except ValueError as e:
        e_str = str(e)
        if e_str.startswith("could not convert string"):
            raise ValueLoadError("Bad string format", data)
        raise ValueLoadError(e_str, data)
    except TypeError:
        raise TypeLoadError(Union[int, float, str], data)


FLOAT_PROVIDER = ScalarProvider(
    target=float,
    strict_coercion_loader=float_strict_coercion_loader,
    lax_coercion_loader=float_lax_coercion_loader,
    dumper=as_is_stub,
    json_schema=JSONSchema(type=JSONSchemaType.NUMBER),
)


def str_strict_coercion_loader(data):
    if type(data) is str:
        return data
    raise TypeLoadError(str, data)


STR_PROVIDER = ScalarProvider(
    target=str,
    strict_coercion_loader=str_strict_coercion_loader,
    lax_coercion_loader=str,
    dumper=as_is_stub,
    json_schema=JSONSchema(type=JSONSchemaType.STRING),
)


def bool_strict_coercion_loader(data):
    if type(data) is bool:
        return data
    raise TypeLoadError(bool, data)


BOOL_PROVIDER = ScalarProvider(
    target=bool,
    strict_coercion_loader=bool_strict_coercion_loader,
    lax_coercion_loader=bool,
    dumper=as_is_stub,
    json_schema=JSONSchema(type=JSONSchemaType.BOOLEAN),
)


def decimal_strict_coercion_loader(data):
    if type(data) is str:
        try:
            return Decimal(data)
        except InvalidOperation:
            raise ValueLoadError("Bad string format", data)
    if type(data) is Decimal:
        return data
    raise TypeLoadError(Union[str, Decimal], data)


def decimal_lax_coercion_loader(data):
    try:
        return Decimal(data)
    except InvalidOperation:
        raise ValueLoadError("Bad string format", data)
    except TypeError:
        raise TypeLoadError(Union[str, Decimal], data)
    except ValueError as e:
        raise ValueLoadError(str(e), data)


DECIMAL_PROVIDER = ScalarProvider(
    target=Decimal,
    strict_coercion_loader=decimal_strict_coercion_loader,
    lax_coercion_loader=decimal_lax_coercion_loader,
    dumper=Decimal.__str__,
    json_schema=JSONSchema(type=JSONSchemaType.STRING),
)


def fraction_strict_coercion_loader(data):
    if type(data) in (str, Fraction):
        try:
            return Fraction(data)
        except ValueError:
            raise ValueLoadError("Bad string format", data)
    raise TypeLoadError(Union[str, Fraction], data)


def fraction_lax_coercion_loader(data):
    try:
        return Fraction(data)
    except TypeError:
        raise TypeLoadError(Union[str, Fraction], data)
    except ValueError as e:
        str_e = str(e)
        if str_e.startswith("Invalid literal"):
            raise ValueLoadError("Bad string format", data)
        raise ValueLoadError(str(e), data)


FRACTION_PROVIDER = ScalarProvider(
    target=Fraction,
    strict_coercion_loader=fraction_strict_coercion_loader,
    lax_coercion_loader=fraction_lax_coercion_loader,
    dumper=Fraction.__str__,
    json_schema=JSONSchema(type=JSONSchemaType.STRING),
)


def complex_strict_coercion_loader(data):
    if type(data) in (str, complex):
        try:
            return complex(data)
        except ValueError:
            raise ValueLoadError("Bad string format", data)
    raise TypeLoadError(Union[str, complex], data)


def complex_lax_coercion_loader(data):
    try:
        return complex(data)
    except TypeError:
        raise TypeLoadError(Union[str, complex], data)
    except ValueError:
        raise ValueLoadError("Bad string format", data)


COMPLEX_PROVIDER = ScalarProvider(
    target=complex,
    strict_coercion_loader=complex_strict_coercion_loader,
    lax_coercion_loader=complex_lax_coercion_loader,
    dumper=complex.__str__,
    json_schema=JSONSchema(type=JSONSchemaType.STRING),
)


def zone_info_coercion_loader(data):
    try:
        return ZoneInfo(data)
    except ZoneInfoNotFoundError:
        raise ValueLoadError("ZoneInfo with key is not found", data)
    except TypeError:
        raise TypeLoadError(str, data)


ZONE_INFO_PROVIDER = ScalarProvider(
    target=ZoneInfo,
    strict_coercion_loader=zone_info_coercion_loader,
    lax_coercion_loader=zone_info_coercion_loader,
    dumper=lambda zone_info: zone_info.key,
    json_schema=JSONSchema(type=JSONSchemaType.STRING),
)


@for_predicate(typing.Self if HAS_SELF_TYPE else ~P.ANY)
class SelfTypeProvider(MorphingProvider):
    def _substituting_provide(self, mediator: Mediator, request: LocatedRequest):
        try:
            owner_loc, _field_loc = find_owner_with_field(request.loc_stack)
        except ValueError:
            raise CannotProvide(
                "Owner type is not found",
                is_terminal=True,
                is_demonstrative=True,
            ) from None

        return mediator.delegating_provide(
            replace(
                request,
                loc_stack=request.loc_stack.replace_last_type(owner_loc.type),
            ),
        )

    def provide_loader(self, mediator: Mediator[Loader], request: LoaderRequest) -> Loader:
        return self._substituting_provide(mediator, request)

    def provide_dumper(self, mediator: Mediator[Dumper], request: DumperRequest) -> Dumper:
        return self._substituting_provide(mediator, request)

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return self._substituting_provide(mediator, request)


@for_predicate(typing.LiteralString if HAS_PY_311 else ~P.ANY)
class LiteralStringProvider(MorphingProvider):
    def provide_loader(self, mediator: Mediator, request: LoaderRequest) -> Loader:
        strict_coercion = mediator.mandatory_provide(StrictCoercionRequest(loc_stack=request.loc_stack))
        return str_strict_coercion_loader if strict_coercion else str

    def provide_dumper(self, mediator: Mediator, request: DumperRequest) -> Dumper:
        return as_is_stub

    def _generate_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        return JSONSchema(type=JSONSchemaType.STRING)
