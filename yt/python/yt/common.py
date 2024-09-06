try:
    from yt.packages.six import iteritems, PY3, text_type, binary_type, string_types
    from yt.packages.six.moves import map as imap
except ImportError:
    from six import iteritems, PY3, text_type, binary_type, string_types
    from six.moves import map as imap

import yt.json_wrapper as json

try:
    from library.python.prctl import prctl
except ImportError:
    prctl = None

# Fix for thread unsafety of datetime module.
# See http://bugs.python.org/issue7980 for more details.
import _strptime  # noqa

# Python3 compatibility
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping
import datetime
from itertools import chain
from functools import wraps

import calendar
import copy
import ctypes
import errno
import functools
import inspect
import os
import re
import signal
import socket
import sys
import time
import types
import string
import warnings

# Standard YT time representation

YT_DATETIME_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%fZ"

YT_NULL_TRANSACTION_ID = "0-0-0-0"


# Deprecation stuff.
class YtDeprecationWarning(DeprecationWarning):
    """Custom warnings category, because built-in category is ignored by default."""


warnings.simplefilter("default", category=YtDeprecationWarning)

DEFAULT_DEPRECATION_MESSAGE = "{0} is deprecated and will be removed in the next major release, " \
                              "use {1} instead"

ERROR_TEXT_MATCHING_DEPRECATION_MESSAGE = "Matching errors by their messages using string patterns is highly " \
                                          "discouraged. It is recommended to use contains_code(code) method instead. " \
                                          "If there is no suitable error code for your needs, ask yt@ for creating one."


def declare_deprecated(functional_name, alternative_name, condition=None, message=None):
    if condition or condition is None:
        message = get_value(message, DEFAULT_DEPRECATION_MESSAGE.format(functional_name, alternative_name))
        warnings.warn(message, YtDeprecationWarning)


def deprecated_with_message(message):
    def function_decorator(func):
        @wraps(func)
        def deprecated_function(*args, **kwargs):
            warnings.warn(message, YtDeprecationWarning)
            return func(*args, **kwargs)
        return deprecated_function
    return function_decorator


def deprecated(alternative):
    def function_decorator(func):
        warn_message = DEFAULT_DEPRECATION_MESSAGE.format(func.__name__, alternative)
        return deprecated_with_message(warn_message)(func)
    return function_decorator


def get_fqdn():
    fqdn = socket.getfqdn()
    if fqdn == "localhost.localdomain":
        fqdn = "localhost"
    return fqdn


class YtError(Exception):
    """Base class for all YT errors."""
    def __init__(self, message="", code=1, inner_errors=None, attributes=None):
        self.message = message
        self.code = code
        self.inner_errors = inner_errors if inner_errors is not None else []
        self.attributes = attributes if attributes else {}
        if "host" not in self.attributes:
            self.attributes["host"] = self._get_fqdn()
        if "datetime" not in self.attributes:
            self.attributes["datetime"] = datetime_to_string(utcnow())

    def simplify(self):
        """Transforms error (with inner errors) to standard python dict."""
        result = {"message": self.message, "code": self.code}
        if self.attributes:
            result["attributes"] = self.attributes
        if self.inner_errors:
            result["inner_errors"] = []
            for error in self.inner_errors:
                result["inner_errors"].append(
                    error.simplify() if isinstance(error, YtError) else
                    error)
        return result

    @classmethod
    def from_dict(cls, dict_):
        """Restores YtError instance from standard python dict. Reverses simplify()."""
        inner_errors = [cls.from_dict(inner) for inner in dict_.get("inner_errors", [])]
        return cls(message=dict_["message"], code=dict_["code"], attributes=dict_.get("attributes"),
                   inner_errors=inner_errors)

    def find_matching_error(self, code=None, predicate=None):
        """
        Find a suberror contained in the error (possibly the error itself) which is either:
        - having error code equal to `code';
        - or satisfying custom predicate `predicate'.

        Exactly one condition should be specified.

        Returns either first error matching the condition or None if no matching found.
        """

        if sum(argument is not None for argument in (code, predicate)) != 1:
            raise ValueError("Exactly one condition should be specified")

        if code is not None:
            predicate = lambda error: int(error.code) == code  # noqa

        def find_recursive(error):
            # error may be Python dict; if so, transform it to YtError.
            if not isinstance(error, YtError):
                error = YtError(**error)

            if predicate(error):
                return error
            for inner_error in error.inner_errors:
                inner_result = find_recursive(inner_error)
                if inner_result:
                    return inner_result
            return None

        return find_recursive(self)

    def contains_code(self, code):
        """Check if error or one of its inner errors contains specified error code."""
        return self.find_matching_error(code=code) is not None

    def _contains_text(self, text):
        """Inner method, do not call explicitly."""
        return self.find_matching_error(predicate=lambda error: text in error.message) is not None

    @deprecated_with_message(ERROR_TEXT_MATCHING_DEPRECATION_MESSAGE)
    def contains_text(self, text):
        """
        Check if error or one of its inner errors contains specified substring in message.

        It is not recommended to use this helper; consider using contains_code instead.
        If the error you are seeking is not distinguishable by code, please send a message to yt@
        and we will fix that.
        """

        return self._contains_text(text)

    def _matches_regexp(self, pattern):
        """Inner method, do not call explicitly."""
        return self.find_matching_error(predicate=lambda error: re.match(pattern, error.message) is not None) is not None

    @deprecated_with_message(ERROR_TEXT_MATCHING_DEPRECATION_MESSAGE)
    def matches_regexp(self, pattern):
        """
        Check if error message or one of its inner error messages matches given regexp.

        It is not recommended to use this helper; consider using contains_code instead.
        If the error you are seeking is not distinguishable by code, please send a message to yt@
        and we will fix that.
        """

        return self._matches_regexp(pattern)

    def __str__(self):
        return format_error(self)

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            _pretty_format_messages_flat(self))

    @staticmethod
    def _get_fqdn():
        if not hasattr(YtError, "_cached_fqdn"):
            YtError._cached_fqdn = get_fqdn()
        return YtError._cached_fqdn

    # Error differentiation methods.
    def is_retriable_archive_error(self):
        """
            Operation progress in Cypress is outdated or some attributes are archive only
            while archive request failed
        """
        return self.contains_code(1911)

    def is_resolve_error(self):
        """Resolution error."""
        return self.contains_code(500)

    def is_already_exists(self):
        """Already exists."""
        return self.contains_code(501)

    def is_access_denied(self):
        """Access denied."""
        return self.contains_code(901)

    def is_account_limit_exceeded(self):
        """Access denied."""
        return self.contains_code(902)

    def is_concurrent_transaction_lock_conflict(self):
        """Deprecated! Transaction lock conflict."""
        return self.contains_code(402)

    def is_cypress_transaction_lock_conflict(self):
        """Transaction lock conflict."""
        return self.contains_code(402)

    def is_tablet_transaction_lock_conflict(self):
        """Transaction lock conflict."""
        return self.contains_code(1700)

    @deprecated(alternative='use is_request_queue_size_limit_exceeded')
    def is_request_rate_limit_exceeded(self):
        """Request rate limit exceeded."""
        return self.contains_code(904)

    def is_safe_mode_enabled(self):
        """Safe mode enabled."""
        return self.contains_code(906)

    def is_request_queue_size_limit_exceeded(self):
        """Request rate limit exceeded."""
        return self.contains_code(108) or self.contains_code(904)

    def is_rpc_unavailable(self):
        """Rpc unavailable."""
        return self.contains_code(105)

    def is_master_communication_error(self):
        """Master communication error."""
        return self.contains_code(712)

    def is_chunk_unavailable(self):
        """Chunk unavailable."""
        return self.contains_code(716)

    def is_request_timed_out(self):
        """Request timed out."""
        return self.contains_code(3)

    def is_concurrent_operations_limit_reached(self):
        """Too many concurrent operations."""
        return self.contains_code(202)

    def is_master_disconnected(self):
        """Master disconnected error."""
        return self.contains_code(218)

    def is_no_such_transaction(self):
        """No such transaction."""
        return self.contains_code(11000)

    def is_no_such_job(self):
        """No such job."""
        return self.contains_code(203)

    def is_no_such_operation(self):
        """No such operation."""
        return self.contains_code(1915)

    def is_shell_exited(self):
        """Shell exited."""
        return self.contains_code(1800) or self.contains_code(1801)

    def is_no_such_service(self):
        """No such service."""
        return self.contains_code(102)

    def is_transport_error(self):
        """Transport error."""
        return self.contains_code(100)

    def is_tablet_in_intermediate_state(self):
        """Tablet is in intermediate state."""
        # TODO(ifsmirnov) migrate to error code, YT-10993
        return self._matches_regexp("Tablet .* is in state .*")

    def is_no_such_tablet(self):
        """No such tablet."""
        return self.contains_code(1701)

    def is_tablet_not_mounted(self):
        """Tablet is not mounted."""
        return self.contains_code(1702)

    def is_no_such_cell(self):
        """No such cell."""
        return self.contains_code(1721)

    def is_all_target_nodes_failed(self):
        """Failed to write chunk since all target nodes have failed."""
        return self.contains_code(700)

    def is_no_such_attribute(self, attributes_list=None):
        """Operation attribute is not supported."""
        if attributes_list is None:
            pred_new = lambda err: err.code == 1920  # noqa
        else:
            pred_new = lambda err: (err.attributes.get("attribute_name") in attributes_list) and (err.code == 1920)  # noqa
        pred_old = lambda err: ("Attribute" in err.message) and ("is not allowed" in err.message)  # noqa
        # COMPAT: remove old version
        return self.find_matching_error(predicate=pred_new) or self.find_matching_error(predicate=pred_old)

    def is_row_is_blocked(self):
        """Row is blocked."""
        return self.contains_code(1712)

    def is_blocked_row_wait_timeout(self):
        """Timed out waiting on blocked row."""
        return self.contains_code(1713)

    def is_chunk_not_preloaded(self):
        """Chunk data is not preloaded yet."""
        return self.contains_code(1735)

    def is_no_in_sync_replicas(self):
        """No in-sync replicas found."""
        return self.contains_code(1736)

    def is_already_present_in_group(self):
        """Member is already present in group."""
        return self.contains_code(908)

    def is_prohibited_cross_cell_copy(self):
        """Cross-cell "copy"/"move" command is explicitly disabled."""
        return self.contains_code(1002)

    def is_sequoia_retriable_error(self):
        """Probably lock conflict in Sequoia tables."""
        return self.contains_code(6002)


class YtResponseError(YtError):
    """Represents an error in YT response."""
    def __init__(self, underlying_error):
        super(YtResponseError, self).__init__()
        self.message = "Received response with error"
        self._underlying_error = underlying_error
        self.inner_errors = [self._underlying_error]

    # Common response error properties.
    @property
    def params(self):
        return self.attributes.get("params")

    # HTTP response interface.
    @property
    def url(self):
        """ Returns url for HTTP response error"""
        return self.attributes.get("url")

    @property
    def headers(self):
        """ COMPAT: Returns request headers for HTTP response error"""
        return self.attributes.get("request_headers")

    @property
    def error(self):
        """ COMPAT: Returns underlying error"""
        return self._underlying_error

    @property
    def request_headers(self):
        """ Returns request headers for HTTP response error"""
        return self.attributes.get("request_headers")

    @property
    def response_headers(self):
        """ Returns response headers for HTTP response error"""
        return self.attributes.get("response_headers")

    def __reduce__(self):
        return (_reconstruct_yt_response_error, (type(self), self.message, self.attributes, self._underlying_error, self.inner_errors))


def _reconstruct_yt_response_error(error_class, message, attributes, underlying_error, inner_errors):
    error = error_class(underlying_error)
    error.message = message
    error.inner_errors = inner_errors
    error.attributes = attributes
    return error


class PrettyPrintableDict(dict):
    pass


def _pretty_format_escape(value):
    def escape(char):
        if char in string.printable:
            return char
        return "\\x{0:02x}".format(ord(char))
    value = value.replace("\n", "\\n").replace("\t", "\\t")
    try:
        value.encode("utf-8")
        return value
    except UnicodeDecodeError:
        return "".join(imap(escape, value))


def _pretty_format_attribute(name, value, attribute_length_limit):
    name = to_native_str(name)
    if isinstance(value, PrettyPrintableDict):
        value = json.dumps(value, indent=2)
        value = value.replace("\n", "\n" + " " * (15 + 1 + 4))
    else:
        # YsonStringProxy attribute formatting.
        if hasattr(value, "_bytes"):
            value = value._bytes
        else:
            if isinstance(value, string_types):
                value = to_native_str(value)
            else:
                value = str(value)
            value = _pretty_format_escape(value)
        if attribute_length_limit is not None and len(value) > attribute_length_limit:
            value = value[:attribute_length_limit] + "...message truncated..."
    return " " * 4 + "%-15s %s" % (name, value)


def _pretty_simplify_error(error):
    if isinstance(error, YtError):
        error = error.simplify()
    elif isinstance(error, (Exception, KeyboardInterrupt)):
        error = {"code": 1, "message": str(error)}
    return error


def _pretty_extract_messages(error, depth=0):
    """
    YtError -> [(depth: int, message: str), ...], in tree order.
    """
    error = _pretty_simplify_error(error)

    if not error.get("attributes", {}).get("transparent", False):
        yield (depth, to_native_str(error["message"]))
        depth += 1

    for inner_error in error.get("inner_errors", []):
        for subitem in _pretty_extract_messages(inner_error, depth=depth):
            yield subitem


def _pretty_format_messages_flat(error):
    prev_depth = 0
    result = []
    for depth, message in _pretty_extract_messages(error):
        if depth > prev_depth:
            result.append(" ")
            result.append("(" * (depth - prev_depth))
        elif prev_depth > depth:
            result.append(")" * (prev_depth - depth))
        elif result:
            result.append(", ")
        result.append(repr(message))
        prev_depth = depth

    result.append(")" * prev_depth)
    return "".join(result)


def _pretty_format_messages(error, indent=0, indent_step=4):
    result = []
    for depth, message in _pretty_extract_messages(error):
        result.append("{indent}{message}".format(
            indent=" " * (indent + depth * indent_step),
            message=message))

    return "\n".join(result)


def _pretty_format_full_errors(error, attribute_length_limit):
    error = _pretty_simplify_error(error)

    lines = []
    if "message" in error:
        lines.append(to_native_str(error["message"]))

    if "code" in error and int(error["code"]) != 1:
        lines.append(_pretty_format_attribute(
            "code", error["code"], attribute_length_limit=attribute_length_limit))

    attributes = error.get("attributes", {})

    origin_keys = ["host", "datetime"]
    origin_cpp_keys = ["pid", "tid", "fid"]
    if all(key in attributes for key in origin_keys):
        date = attributes["datetime"]
        if isinstance(date, datetime.datetime):
            date = date.strftime("%y-%m-%dT%H:%M:%S.%fZ")
        value = "{0} on {1}".format(attributes["host"], date)
        if all(key in attributes for key in origin_cpp_keys):
            value += " (pid %(pid)d, tid %(tid)x, fid %(fid)x)" % attributes
        lines.append(_pretty_format_attribute(
            "origin", value, attribute_length_limit=attribute_length_limit))

    location_keys = ["file", "line"]
    if all(key in attributes for key in location_keys):
        lines.append(_pretty_format_attribute(
            "location",
            "%(file)s:%(line)d" % attributes,
            attribute_length_limit=attribute_length_limit))

    for key, value in iteritems(attributes):
        if key in origin_keys or key in location_keys or key in origin_cpp_keys:
            continue
        lines.append(_pretty_format_attribute(
            key, value, attribute_length_limit=attribute_length_limit))

    result = (" " * 4 + "\n").join(lines)
    if "inner_errors" in error:
        for inner_error in error["inner_errors"]:
            result += "\n" + _pretty_format_full_errors(
                inner_error, attribute_length_limit=attribute_length_limit)

    return result


def _pretty_format(error, attribute_length_limit=None):
    return "{}\n\n***** Details:\n{}\n".format(
        _pretty_format_messages(error),
        _pretty_format_full_errors(error, attribute_length_limit=attribute_length_limit))


def _pretty_format_fake(error, attribute_length_limit=None):
    return _pretty_format(error, attribute_length_limit)


def _pretty_format_for_logging(error, attribute_length_limit=None):
    return _pretty_format_full_errors(error, attribute_length_limit=attribute_length_limit).replace("\n", "\\n")


def format_error(error, attribute_length_limit=300):
    return _pretty_format(error, attribute_length_limit)


def join_exceptions(*args):
    result = []
    for exception in args:
        if isinstance(exception, tuple):
            result += exception
        else:
            result.append(exception)
    return tuple(result)


def which(name, flags=os.X_OK, custom_paths=None):
    """Return list of files in system paths with given name."""
    # TODO: check behavior when dealing with symlinks
    result = []
    paths = os.environ.get("PATH", "").split(os.pathsep)
    if custom_paths is not None:
        paths = custom_paths + paths
    for dir in paths:
        path = os.path.join(dir, name)
        if os.access(path, flags):
            result.append(path)
    return result


def unlist(list):
    try:
        return list[0] if len(list) == 1 else list
    except TypeError:  # cannot calculate len
        return list


def require(condition, exception_func):
    if not condition:
        raise exception_func()


def update_inplace(object, patch):
    """Apply patch to object inplace"""
    if isinstance(patch, Mapping) and isinstance(object, Mapping):
        for key, value in iteritems(patch):
            if key in object:
                object[key] = update_inplace(object[key], value)
            else:
                object[key] = value
    elif isinstance(patch, list) and isinstance(object, list):
        for index, value in enumerate(patch):
            if index < len(object):
                object[index] = update_inplace(object[index], value)
            else:
                object.append(value)
    else:
        object = patch
    return object


def update(object, patch):
    """Apply patch to object without modifying original object or patch"""
    if patch is None:
        return copy.deepcopy(object)
    elif object is None:
        return copy.deepcopy(patch)
    else:
        return update_inplace(copy.deepcopy(object), patch)


def flatten(obj, list_types=(list, tuple, set, frozenset, types.GeneratorType)):
    """Create flat list from all elements."""
    if isinstance(obj, list_types):
        return list(chain(*imap(flatten, obj)))
    return [obj]


def update_from_env(variables):
    """Update variables dict from environment."""
    for key, value in iteritems(os.environ):
        prefix = "YT_"
        if not key.startswith(prefix):
            continue

        key = key[len(prefix):]
        if key not in variables:
            continue

        var_type = type(variables[key])
        # Using int we treat "0" as false, "1" as "true"
        if var_type == bool:
            try:
                value = int(value)
            except:  # noqa
                pass
        # None type is treated as str
        if isinstance(None, var_type):
            var_type = str

        variables[key] = var_type(value)


def get_value(value, default):
    if value is None:
        return default
    else:
        return value


def filter_dict(predicate, dictionary):
    return dict([(k, v) for (k, v) in iteritems(dictionary) if predicate(k, v)])


def set_pdeathsig(signum=None):
    if sys.platform.startswith("linux"):
        if signum is None:
            signum = signal.SIGTERM
        if prctl:
            prctl.set_pdeathsig(signum)
        else:
            ctypes.cdll.LoadLibrary("libc.so.6")
            libc = ctypes.CDLL("libc.so.6")
            PR_SET_PDEATHSIG = 1
            libc.prctl(PR_SET_PDEATHSIG, signum)


def remove_file(path, force=False):
    try:
        os.remove(path)
    except OSError:
        if not force:
            raise


def makedirp(path):
    try:
        os.makedirs(path)
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise


def touch(path):
    if not os.path.exists(path):
        makedirp(os.path.dirname(path))
        with open(path, "w"):
            pass


def date_string_to_datetime(date):
    return datetime.datetime.strptime(date, YT_DATETIME_FORMAT_STRING)


def date_string_to_timestamp(date):
    return calendar.timegm(date_string_to_datetime(date).timetuple())


def date_string_to_timestamp_mcs(time_str):
    dt = date_string_to_datetime(time_str)
    return int(calendar.timegm(dt.timetuple()) * (10 ** 6) + dt.microsecond)


def datetime_to_string(date, is_local=False):
    if is_local:
        date = datetime.datetime.utcfromtimestamp(time.mktime(date.timetuple()))
    return date.strftime(YT_DATETIME_FORMAT_STRING)


def utcnow():
    if sys.version_info >= (3, 12, ):
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    else:
        return datetime.datetime.utcnow()


def make_non_blocking(fd):
    # Use local import to support Windows.
    import fcntl
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def to_native_str(string, encoding="utf-8", errors="strict"):
    if not PY3 and isinstance(string, text_type):
        return string.encode(encoding)
    if PY3 and isinstance(string, binary_type):
        return string.decode(encoding, errors=errors)
    return string


def copy_docstring_from(documented_function):
    """Decorator that copies docstring from one function to another.

    :param documented_function: function that provides docstring.

    Usage::

        def foo(...):
            "USEFUL DOCUMENTATION"
            ...

        @copy_docstring_from(foo)
        def bar(...)
            # docstring will be copied from `foo' function
            ...
    """
    return functools.wraps(documented_function, assigned=("__doc__",), updated=())


def is_process_alive(pid):
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        elif err.errno == errno.EPERM:
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    return True


def uuid_to_parts(guid):
    id_parts = guid.split("-")
    id_hi = int(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = int(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo


def parts_to_uuid(id_hi, id_lo):
    guid = id_lo << 64 | id_hi
    mask = 0xFFFFFFFF

    parts = []
    for i in range(4):
        parts.append((guid & mask) >> (i * 32))
        mask <<= 32

    return "-".join(reversed(["{:x}".format(part) for part in parts]))


# TODO(asaitgalin): Remove copy-paste from YP.
def underscore_case_to_camel_case(str):
    result = []
    first = True
    upper = True
    for c in str:
        if c == "_":
            upper = True
        else:
            if upper:
                if c not in string.ascii_letters and not first:
                    result.append("_")
                c = c.upper()
            result.append(c)
            upper = False
        first = False
    return "".join(result)


class WaitFailed(Exception):
    pass


def wait(predicate, error_message=None, iter=None, sleep_backoff=None, timeout=None, ignore_exceptions=False):
    # 30 seconds by default
    if sleep_backoff is None:
        sleep_backoff = 0.3

    last_exception = None
    if ignore_exceptions:
        def check_predicate():
            try:
                return predicate(), None
            # Do not catch BaseException because pytest exceptions are inherited from it
            # pytest.fail raises exception inherited from BaseException.
            except Exception as ex:
                return False, ex
    else:
        def check_predicate():
            return predicate(), None

    if timeout is None:
        if iter is None:
            iter = 100
        index = 0
        while index < iter:
            result, last_exception = check_predicate()
            if result:
                return
            index += 1
            time.sleep(sleep_backoff)
    else:
        start_time = datetime.datetime.now()
        while datetime.datetime.now() - start_time < datetime.timedelta(seconds=timeout):
            result, last_exception = check_predicate()
            if result:
                return
            time.sleep(sleep_backoff)

    if inspect.isfunction(error_message):
        error_message = error_message()
    if error_message is None:
        error_message = "Wait failed"

    error_message += f" (timeout = {timeout if timeout is not None else iter * sleep_backoff}"
    if last_exception is not None:
        error_message += f", exception = {last_exception}"
    error_message += ")"
    raise WaitFailed(error_message)
