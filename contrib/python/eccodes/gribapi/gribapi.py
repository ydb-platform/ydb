#
# (C) Copyright 2017- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

"""
@package gribapi
@brief This package is the \b Python 3 interface to ecCodes.
It offers almost one to one bindings to the C API functions.

The Python 3 interface to ecCodes uses the <a href="http://numpy.scipy.org/"><b>NumPy</b></a> package
as the container of choice for the possible arrays of values that can be encoded/decoded in and from a message.
Numpy is a package used for scientific computing in Python and an efficient container for generic data.

@em Requirements:

    - Python 3.9 or higher
    - NumPy

"""

import os
import sys
from functools import wraps

import numpy as np

from gribapi.errors import GribInternalError

from . import errors
from .bindings import ENC
from .bindings import __version__ as bindings_version  # noqa
from .bindings import ffi, lib, library_path

try:
    type(file)
except NameError:
    import io

    file = io.IOBase
    long = int

KEYTYPES = {1: int, 2: float, 3: str, 4: bytes}

CODES_PRODUCT_ANY = 0
""" Generic product kind """
CODES_PRODUCT_GRIB = 1
""" GRIB product kind """
CODES_PRODUCT_BUFR = 2
""" BUFR product kind """
CODES_PRODUCT_METAR = 3
""" METAR product kind """
CODES_PRODUCT_GTS = 4
""" GTS product kind """
CODES_PRODUCT_TAF = 5
""" TAF product kind """

# Constants for 'missing'
GRIB_MISSING_DOUBLE = -1e100
GRIB_MISSING_LONG = 2147483647

# Constants for GRIB nearest neighbour
GRIB_NEAREST_SAME_GRID = 1 << 0
GRIB_NEAREST_SAME_DATA = 1 << 1
GRIB_NEAREST_SAME_POINT = 1 << 2

# Constants for feature selection
CODES_FEATURES_ALL = 0
CODES_FEATURES_ENABLED = 1
CODES_FEATURES_DISABLED = 2


# ECC-1029: Disable function-arguments type-checking unless
# environment variable is defined and equal to 1
enable_type_checks = os.environ.get("ECCODES_PYTHON_ENABLE_TYPE_CHECKS") == "1"


# Function-arguments type-checking decorator
# inspired from http://code.activestate.com/recipes/454322-type-checking-decorator/
# modified to support multiple allowed types and all types in the same decorator call
# This returns a decorator. _params_ is the dict with the type specs
def require(**_params_):
    """
    The actual decorator. Receives the target function in _func_
    """

    def check_types(_func_, _params_=_params_):
        if not enable_type_checks:
            return _func_

        @wraps(_func_)
        # The wrapper function. Replaces the target function and receives its args
        def modified(*args, **kw):
            arg_names = _func_.__code__.co_varnames
            # argnames, varargs, kwargs, defaults = inspect.getargspec(_func_)
            kw.update(zip(arg_names, args))
            for name, allowed_types in _params_.items():
                param = kw[name]
                if isinstance(allowed_types, type):
                    allowed_types = (allowed_types,)
                assert any(
                    [isinstance(param, type1) for type1 in allowed_types]
                ), "Parameter '%s' should be of type %s (instead of %s)" % (
                    name,
                    " or ".join([t.__name__ for t in allowed_types]),
                    type(param).__name__,
                )
            return _func_(**kw)

        return modified

    return check_types


# @cond
class Bunch(dict):
    """
    The collector of a bunch of named stuff :).
    """

    def __init__(self, **kw):
        dict.__init__(self, kw)
        self.__dict__.update(kw)

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self.__dict__[key] = value

    def __setattr__(self, key, value):
        dict.__setitem__(self, key, value)
        self.__dict__[key] = value

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        del self.__dict__[key]

    def __delattr__(self, key):
        dict.__delitem__(self, key)
        del self.__dict__[key]

    def __str__(self):
        state = [
            "%s=%r" % (attribute, value) for (attribute, value) in self.__dict__.items()
        ]
        return "\n".join(state)


# @endcond


def err_last(func):
    @wraps(func)
    def wrapper(*args):
        err = ffi.new("int *")
        args += (err,)
        retval = func(*args)
        return err[0], retval

    return wrapper


def get_handle(msgid):
    h = ffi.cast("grib_handle*", msgid)
    if h == ffi.NULL:
        raise errors.NullHandleError(f"get_handle: Bad message ID {msgid}")
    return h


def put_handle(handle):
    if handle == ffi.NULL:
        raise errors.NullHandleError("put_handle: Bad message ID (handle is NULL)")
    return int(ffi.cast("size_t", handle))


def get_multi_handle(msgid):
    return ffi.cast("grib_multi_handle*", msgid)


def put_multi_handle(handle):
    return int(ffi.cast("size_t", handle))


def get_index(indexid):
    return ffi.cast("grib_index*", indexid)


def put_index(indexh):
    return int(ffi.cast("size_t", indexh))


def get_iterator(iterid):
    return ffi.cast("grib_iterator*", iterid)


def put_iterator(iterh):
    return int(ffi.cast("size_t", iterh))


def get_grib_keys_iterator(iterid):
    return ffi.cast("grib_keys_iterator*", iterid)


def put_grib_keys_iterator(iterh):
    return int(ffi.cast("size_t", iterh))


def get_bufr_keys_iterator(iterid):
    return ffi.cast("bufr_keys_iterator*", iterid)


def put_bufr_keys_iterator(iterh):
    return int(ffi.cast("size_t", iterh))


# @cond
@require(errid=int)
def GRIB_CHECK(errid):
    """
    Utility function checking the ecCodes error code and raising
    an error if that was set.

    @param errid  the C interface error id to check
    @exception CodesInternalError
    """
    if errid:
        errors.raise_grib_error(errid)


# @endcond


@require(fileobj=file)
def gts_new_from_file(fileobj, headers_only=False):
    """
    @brief Load in memory a GTS message from a file.

    The message can be accessed through its id and will be available\n
    until @ref codes_release is called.\n

    @param fileobj        python file object
    @param headers_only   whether or not to load the message with the headers only
    @return               id of the GTS loaded in memory or None
    @exception CodesInternalError
    """

    err, h = err_last(lib.codes_handle_new_from_file)(
        ffi.NULL, fileobj, CODES_PRODUCT_GTS
    )
    if err:
        if err == lib.GRIB_END_OF_FILE:
            return None
        else:
            GRIB_CHECK(err)
            return None
    if h == ffi.NULL:
        return None
    else:
        return put_handle(h)


@require(fileobj=file)
def metar_new_from_file(fileobj, headers_only=False):
    """
    @brief Load in memory a METAR message from a file.

    The message can be accessed through its id and will be available\n
    until @ref codes_release is called.\n

    @param fileobj        python file object
    @param headers_only   whether or not to load the message with the headers only
    @return               id of the METAR loaded in memory or None
    @exception CodesInternalError
    """

    err, h = err_last(lib.codes_handle_new_from_file)(
        ffi.NULL, fileobj, CODES_PRODUCT_METAR
    )
    if err:
        if err == lib.GRIB_END_OF_FILE:
            return None
        else:
            GRIB_CHECK(err)
            return None
    if h == ffi.NULL:
        return None
    else:
        return put_handle(h)


@require(fileobj=file, product_kind=int)
def codes_new_from_file(fileobj, product_kind, headers_only=False):
    """
    @brief Load in memory a message from a file for a given product.

    The message can be accessed through its id and will be available\n
    until @ref codes_release is called.\n

    \b Examples: \ref get_product_kind.py "get_product_kind.py"

    @param fileobj        python file object
    @param product_kind   one of CODES_PRODUCT_GRIB, CODES_PRODUCT_BUFR, CODES_PRODUCT_METAR or CODES_PRODUCT_GTS
    @param headers_only   whether or not to load the message with the headers only
    @return               id of the message loaded in memory or None
    @exception CodesInternalError
    """
    if product_kind == CODES_PRODUCT_GRIB:
        return grib_new_from_file(fileobj, headers_only)
    if product_kind == CODES_PRODUCT_BUFR:
        return bufr_new_from_file(fileobj, headers_only)
    if product_kind == CODES_PRODUCT_METAR:
        return metar_new_from_file(fileobj, headers_only)
    if product_kind == CODES_PRODUCT_GTS:
        return gts_new_from_file(fileobj, headers_only)
    if product_kind == CODES_PRODUCT_ANY:
        return any_new_from_file(fileobj, headers_only)
    raise ValueError("Invalid product kind %d" % product_kind)


@require(fileobj=file)
def any_new_from_file(fileobj, headers_only=False):
    """
    @brief Load in memory a message from a file.

    The message can be accessed through its id and will be available\n
    until @ref codes_release is called.\n

    \b Examples: \ref grib_get_keys.py "grib_get_keys.py"

    @param fileobj        python file object
    @param headers_only   whether or not to load the message with the headers only
    @return               id of the message loaded in memory or None
    @exception CodesInternalError
    """
    err, h = err_last(lib.codes_handle_new_from_file)(
        ffi.NULL, fileobj, CODES_PRODUCT_ANY
    )
    if err:
        if err == lib.GRIB_END_OF_FILE:
            return None
        else:
            GRIB_CHECK(err)
            return None
    if h == ffi.NULL:
        return None
    else:
        return put_handle(h)


@require(fileobj=file)
def bufr_new_from_file(fileobj, headers_only=False):
    """
    @brief Load in memory a BUFR message from a file.

    The message can be accessed through its id and will be available\n
    until @ref codes_release is called.\n

    \b Examples: \ref bufr_get_keys.py "bufr_get_keys.py"

    @param fileobj        python file object
    @param headers_only   whether or not to load the message with the headers only
    @return               id of the BUFR loaded in memory or None
    @exception CodesInternalError
    """
    err, h = err_last(lib.codes_handle_new_from_file)(
        ffi.NULL, fileobj, CODES_PRODUCT_BUFR
    )
    if err:
        if err == lib.GRIB_END_OF_FILE:
            return None
        else:
            GRIB_CHECK(err)
            return None
    if h == ffi.NULL:
        return None
    else:
        return put_handle(h)


@require(fileobj=file)
def grib_new_from_file(fileobj, headers_only=False):
    """
    @brief Load in memory a GRIB message from a file.

    The message can be accessed through its gribid and will be available\n
    until @ref codes_release is called.\n

    The message can be loaded headers only by using the headers_only argument.
    Default is to have the headers only option set to off (False). If set to on (True),
    data values will be skipped. This will result in a significant performance gain
    if one is only interested in browsing through messages to retrieve metadata.
    Any attempt to retrieve data values keys when in the headers only mode will
    result in a key not found error.

    \b Examples: \ref grib_get_keys.py "grib_get_keys.py"

    @param fileobj        python file object
    @param headers_only   whether or not to load the message with the headers only
    @return               id of the grib loaded in memory or None
    @exception CodesInternalError
    """

    err, h = err_last(lib.codes_handle_new_from_file)(
        ffi.NULL, fileobj, CODES_PRODUCT_GRIB
    )
    if err:
        if err == lib.GRIB_END_OF_FILE:
            return None
        else:
            GRIB_CHECK(err)
            return None
    if h == ffi.NULL:
        return None
    else:
        return put_handle(h)


@require(fileobj=file)
def grib_count_in_file(fileobj):
    """
    @brief Count the messages in a file.

    \b Examples: \ref count_messages.py "count_messages.py"

    @param fileobj  python file object
    @return         number of messages in the file
    @exception CodesInternalError
    """
    num_p = ffi.new("int*")
    err = lib.grib_count_in_file(ffi.NULL, fileobj, num_p)
    GRIB_CHECK(err)
    return num_p[0]


def grib_multi_support_on():
    """
    @brief Turn on the support for multiple fields in a single GRIB message.

    @exception CodesInternalError
    """
    lib.grib_multi_support_on(ffi.NULL)


def grib_multi_support_off():
    """
    @brief Turn off the support for multiple fields in a single GRIB message.

    @exception CodesInternalError
    """
    lib.grib_multi_support_off(ffi.NULL)


@require(fileobj=file)
def grib_multi_support_reset_file(fileobj):
    """
    @brief Reset file handle in multiple field support mode
    """
    context = lib.grib_context_get_default()
    lib.grib_multi_support_reset_file(context, fileobj)


@require(msgid=int)
def grib_release(msgid):
    """
    @brief Free the memory for the message referred to by msgid.

    \b Examples: \ref grib_get_keys.py "grib_get_keys.py"

    @param msgid      id of the message loaded in memory
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    GRIB_CHECK(lib.grib_handle_delete(h))


@require(msgid=int, key=str)
def grib_get_string(msgid, key):
    """
    @brief Get the string value of a key from a message.

    @param msgid       id of the message loaded in memory
    @param key         key name
    @return            string value of key
    @exception CodesInternalError
    """
    length = grib_get_string_length(msgid, key)

    h = get_handle(msgid)
    values = ffi.new("char[]", length)
    length_p = ffi.new("size_t *", length)
    err = lib.grib_get_string(h, key.encode(ENC), values, length_p)
    GRIB_CHECK(err)
    return _decode_bytes(values, length_p[0])


@require(msgid=int, key=str, value=str)
def grib_set_string(msgid, key, value):
    """
    @brief Set the value for a string key in a message.

    @param msgid      id of the message loaded in memory
    @param key        key name
    @param value      string value
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    bvalue = value.encode(ENC)
    length_p = ffi.new("size_t *", len(bvalue))
    GRIB_CHECK(lib.grib_set_string(h, key.encode(ENC), bvalue, length_p))


def grib_gribex_mode_on():
    """
    @brief Turn on the compatibility mode with GRIBEX.

    @exception CodesInternalError
    """
    lib.grib_gribex_mode_on(ffi.NULL)


def grib_gribex_mode_off():
    """
    @brief Turn off the compatibility mode with GRIBEX.

    @exception CodesInternalError
    """
    lib.grib_gribex_mode_off(ffi.NULL)


@require(msgid=int, fileobj=file)
def grib_write(msgid, fileobj):
    """
    @brief Write a message to a file.

    \b Examples: \ref grib_set_keys.py "grib_set_keys.py"

    @param msgid      id of the message loaded in memory
    @param fileobj    python file object
    @exception CodesInternalError
    """
    msg_bytes = grib_get_message(msgid)
    fileobj.write(msg_bytes)
    fileobj.flush()


@require(multigribid=int, fileobj=file)
def grib_multi_write(multigribid, fileobj):
    """
    @brief Write a multi-field GRIB message to a file.

    \b Examples: \ref grib_multi_write.py "grib_multi_write.py"

    @param multigribid      id of the multi-field grib loaded in memory
    @param fileobj          python file object
    @exception CodesInternalError
    """
    mh = get_multi_handle(multigribid)
    GRIB_CHECK(lib.grib_multi_handle_write(mh, fileobj))


@require(ingribid=int, startsection=int, multigribid=int)
def grib_multi_append(ingribid, startsection, multigribid):
    """
    @brief Append a single-field GRIB message to a multi-field GRIB message.

    Only the sections with section number greather or equal "startsection"
    are copied from the input single message to the multi-field output grib.

    \b Examples: \ref grib_multi_write.py "grib_multi_write.py"

    @param ingribid      id of the input single-field GRIB
    @param startsection  starting from startsection (included) all the sections are copied
                         from the input single grib to the output multi-field grib
    @param multigribid   id of the output multi-field GRIB
    @exception CodesInternalError
    """
    h = get_handle(ingribid)
    mh = get_multi_handle(multigribid)
    GRIB_CHECK(lib.grib_multi_handle_append(h, startsection, mh))


@require(msgid=int, key=str)
def grib_get_offset(msgid, key):
    """
    @brief Get the byte offset of a key. If several keys of the same name
    are present, the offset of the last one is returned

    @param msgid      id of the message loaded in memory
    @param key        name of the key
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    offset_p = ffi.new("size_t*")
    err = lib.grib_get_offset(h, key.encode(ENC), offset_p)
    GRIB_CHECK(err)
    return offset_p[0]


@require(msgid=int, key=str)
def grib_get_size(msgid, key):
    """
    @brief Get the size of a key. Return 1 for scalar keys and >1 for array keys

    \b Examples: \ref grib_get_keys.py "grib_get_keys.py",\ref count_messages.py "count_messages.py"

    @param msgid      id of the message loaded in memory
    @param key        name of the key
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    size_p = ffi.new("size_t*")
    err = lib.grib_get_size(h, key.encode(ENC), size_p)
    GRIB_CHECK(err)
    return size_p[0]


@require(msgid=int, key=str)
def grib_get_string_length(msgid, key):
    """
    @brief Get the length of the string version of a key.

    @param msgid      id of the message loaded in memory
    @param key        name of the key
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    size = ffi.new("size_t *")
    err = lib.grib_get_length(h, key.encode(ENC), size)
    GRIB_CHECK(err)
    return size[0]


@require(iterid=int)
def grib_skip_computed(iterid):
    """
    @brief Skip the computed keys in a keys iterator.

    The computed keys are not coded in the message, they are computed
    from other keys.

    @see grib_keys_iterator_new,grib_keys_iterator_next,grib_keys_iterator_delete

    @param iterid      keys iterator id
    @exception CodesInternalError
    """
    gki = get_grib_keys_iterator(iterid)
    lib.grib_keys_iterator_set_flags(gki, lib.GRIB_KEYS_ITERATOR_SKIP_COMPUTED)


@require(iterid=int)
def grib_skip_coded(iterid):
    """
    @brief Skip the coded keys in a keys iterator.

    The coded keys are actually coded in the message.

    @see grib_keys_iterator_new,grib_keys_iterator_next,grib_keys_iterator_delete

    @param iterid      keys iterator id
    @exception CodesInternalError
    """
    gki = get_grib_keys_iterator(iterid)
    lib.grib_keys_iterator_set_flags(gki, lib.GRIB_KEYS_ITERATOR_SKIP_CODED)


@require(iterid=int)
def grib_skip_edition_specific(iterid):
    """
    @brief Skip the edition specific keys in a keys iterator.

    @see grib_keys_iterator_new,grib_keys_iterator_next,grib_keys_iterator_delete

    @param iterid      keys iterator id
    @exception CodesInternalError
    """
    gki = get_grib_keys_iterator(iterid)
    lib.grib_keys_iterator_set_flags(gki, lib.GRIB_KEYS_ITERATOR_SKIP_EDITION_SPECIFIC)


@require(iterid=int)
def grib_skip_duplicates(iterid):
    """
    @brief Skip the duplicate keys in a keys iterator.

    @see grib_keys_iterator_new,grib_keys_iterator_next,grib_keys_iterator_delete

    @param iterid      keys iterator id
    @exception CodesInternalError
    """
    gki = get_grib_keys_iterator(iterid)
    lib.grib_keys_iterator_set_flags(gki, lib.GRIB_KEYS_ITERATOR_SKIP_DUPLICATES)


@require(iterid=int)
def grib_skip_read_only(iterid):
    """
    @brief Skip the read_only keys in a keys iterator.

    Read only keys cannot be set.

    @see grib_keys_iterator_new,grib_keys_iterator_next,grib_keys_iterator_delete

    @param iterid      keys iterator id
    @exception CodesInternalError
    """
    gki = get_grib_keys_iterator(iterid)
    lib.grib_keys_iterator_set_flags(gki, lib.GRIB_KEYS_ITERATOR_SKIP_READ_ONLY)


@require(iterid=int)
def grib_skip_function(iterid):
    """
    @brief Skip the function keys in a keys iterator.

    @see grib_keys_iterator_new,grib_keys_iterator_next,grib_keys_iterator_delete

    @param iterid      keys iterator id
    @exception CodesInternalError
    """
    gki = get_grib_keys_iterator(iterid)
    lib.grib_keys_iterator_set_flags(gki, lib.GRIB_KEYS_ITERATOR_SKIP_FUNCTION)


@require(gribid=int, mode=int)
def grib_iterator_new(gribid, mode):
    """
    @brief Create a new geoiterator for the given GRIB message, using its geometry and values.

    The geoiterator can be used to go through all the geopoints in a GRIB message and
    retrieve the values corresponding to those geopoints.

    \b Examples: \ref grib_iterator.py "grib_iterator.py"

    @param gribid  id of the GRIB loaded in memory
    @param mode    flags for future use
    @return        geoiterator id
    """
    h = get_handle(gribid)
    err, iterid = err_last(lib.grib_iterator_new)(h, mode)
    GRIB_CHECK(err)
    return put_iterator(iterid)


@require(iterid=int)
def grib_iterator_delete(iterid):
    """
    @brief Delete a geoiterator and free memory.

    \b Examples: \ref grib_iterator.py "grib_iterator.py"

    @param iterid  geoiterator id
    @exception CodesInternalError
    """
    ih = get_iterator(iterid)
    GRIB_CHECK(lib.grib_iterator_delete(ih))


@require(iterid=int)
def grib_iterator_next(iterid):
    """
    @brief Retrieve the next value from a geoiterator.

    \b Examples: \ref grib_iterator.py "grib_iterator.py"

    @param    iterid geoiterator id
    @return   tuple with the latitude, longitude and value
    @exception CodesInternalError
    """
    iterh = get_iterator(iterid)
    lat_p = ffi.new("double*")
    lon_p = ffi.new("double*")
    value_p = ffi.new("double*")
    retval = lib.grib_iterator_next(iterh, lat_p, lon_p, value_p)
    if retval == 0:
        # No more data available. End of iteration
        return []
    else:
        return (lat_p[0], lon_p[0], value_p[0])


@require(msgid=int)
def grib_keys_iterator_new(msgid, namespace=None):
    """
    @brief Create a new iterator on the keys.

    The keys iterator can be navigated to give all the key names which
    can then be used to get or set the key values with \ref grib_get or
    \ref grib_set.
    The set of keys returned can be controlled with the input variable
    namespace or using the functions
    \ref grib_skip_read_only, \ref grib_skip_duplicates,
    \ref grib_skip_coded,\ref grib_skip_computed.
    If namespace is a non empty string only the keys belonging to
    that namespace are returned. Example namespaces are "ls" (to get the same
    default keys as the grib_ls) and "mars" to get the keys used by mars.

    \b Examples: \ref grib_iterator.py "grib_iterator.py"

    @param msgid       id of the message loaded in memory
    @param namespace   the namespace of the keys to search for (all the keys if None)
    @return            keys iterator id to be used in the keys iterator functions
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    bnamespace = ffi.NULL if namespace is None else namespace.encode(ENC)
    iterid = lib.grib_keys_iterator_new(h, 0, bnamespace)
    return put_grib_keys_iterator(iterid)


@require(iterid=int)
def grib_keys_iterator_next(iterid):
    """
    @brief Advance to the next keys iterator value.

    \b Examples: \ref grib_keys_iterator.py "grib_keys_iterator.py"

    @param iterid      keys iterator id created with @ref grib_keys_iterator_new
    @exception CodesInternalError
    """
    kih = get_grib_keys_iterator(iterid)
    res = lib.grib_keys_iterator_next(kih)
    # res is 0 or 1
    return res


@require(iterid=int)
def grib_keys_iterator_delete(iterid):
    """
    @brief Delete a keys iterator and free memory.

    \b Examples: \ref grib_keys_iterator.py "grib_keys_iterator.py"

    @param iterid      keys iterator id created with @ref grib_keys_iterator_new
    @exception CodesInternalError
    """
    kih = get_grib_keys_iterator(iterid)
    lib.grib_keys_iterator_delete(kih)


@require(iterid=int)
def grib_keys_iterator_get_name(iterid):
    """
    @brief Get the name of a key from a keys iterator.

    \b Examples: \ref grib_keys_iterator.py "grib_keys_iterator.py"

    @param iterid    keys iterator id created with @ref grib_keys_iterator_new
    @return          key name to be retrieved
    @exception CodesInternalError
    """
    kih = get_grib_keys_iterator(iterid)
    name = lib.grib_keys_iterator_get_name(kih)
    return ffi.string(name).decode(ENC)


@require(iterid=int)
def grib_keys_iterator_rewind(iterid):
    """
    @brief Rewind a keys iterator.

    @param iterid      keys iterator id created with @ref grib_keys_iterator_new
    @exception CodesInternalError
    """
    gki = get_grib_keys_iterator(iterid)
    GRIB_CHECK(lib.grib_keys_iterator_rewind(gki))


# BUFR keys iterator
@require(bufrid=int)
def codes_bufr_keys_iterator_new(bufrid):
    """
    @brief Create a new iterator on the BUFR keys.

    The keys iterator can be navigated to give all the key names which
    can then be used to get or set the key values with \ref codes_get or
    \ref codes_set.

    \b Examples: \ref bufr_keys_iterator.py "bufr_keys_iterator.py"

    @param bufrid   id of the BUFR message loaded in memory
    @return         keys iterator id to be used in the bufr keys iterator functions
    @exception CodesInternalError
    """
    h = get_handle(bufrid)
    bki = lib.codes_bufr_keys_iterator_new(h, 0)
    if bki == ffi.NULL:
        raise errors.InvalidKeysIteratorError(
            f"BUFR keys iterator failed bufrid={bufrid}"
        )
    return put_bufr_keys_iterator(bki)


@require(iterid=int)
def codes_bufr_keys_iterator_next(iterid):
    """
    @brief Advance to the next BUFR keys iterator value.

    \b Examples: \ref bufr_keys_iterator.py "bufr_keys_iterator.py"

    @param iterid      keys iterator id created with @ref codes_bufr_keys_iterator_new
    @exception CodesInternalError
    """
    bki = get_bufr_keys_iterator(iterid)
    res = lib.codes_bufr_keys_iterator_next(bki)
    # res is 0 or 1
    return res


@require(iterid=int)
def codes_bufr_keys_iterator_delete(iterid):
    """
    @brief Delete a BUFR keys iterator and free memory.

    \b Examples: \ref bufr_keys_iterator.py "bufr_keys_iterator.py"

    @param iterid      keys iterator id created with @ref codes_bufr_keys_iterator_new
    @exception CodesInternalError
    """
    bki = get_bufr_keys_iterator(iterid)
    GRIB_CHECK(lib.codes_bufr_keys_iterator_delete(bki))


@require(iterid=int)
def codes_bufr_keys_iterator_get_name(iterid):
    """
    @brief Get the name of a key from a BUFR keys iterator.

    \b Examples: \ref bufr_keys_iterator.py "bufr_keys_iterator.py"

    @param iterid   keys iterator id created with @ref codes_bufr_keys_iterator_new
    @return         key name to be retrieved
    @exception CodesInternalError
    """
    bki = get_bufr_keys_iterator(iterid)
    name = lib.codes_bufr_keys_iterator_get_name(bki)
    return ffi.string(name).decode(ENC)


@require(iterid=int)
def codes_bufr_keys_iterator_rewind(iterid):
    """
    @brief Rewind a BUFR keys iterator.

    @param iterid      keys iterator id created with @ref codes_bufr_keys_iterator_new
    @exception CodesInternalError
    """
    bki = get_bufr_keys_iterator(iterid)
    GRIB_CHECK(lib.codes_bufr_keys_iterator_rewind(bki))


@require(msgid=int, key=str)
def grib_get_long(msgid, key):
    """
    @brief Get the value of a key in a message as an integer.

    @param msgid       id of the message loaded in memory
    @param key         key name
    @return            value of key as int
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    value_p = ffi.new("long*")
    err = lib.grib_get_long(h, key.encode(ENC), value_p)
    GRIB_CHECK(err)
    return value_p[0]


@require(msgid=int, key=str)
def grib_get_double(msgid, key):
    """
    @brief Get the value of a key in a message as a float.

    @param msgid      id of the message loaded in memory
    @param key        key name
    @return           value of key as float
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    value_p = ffi.new("double*")
    err = lib.grib_get_double(h, key.encode(ENC), value_p)
    GRIB_CHECK(err)
    return value_p[0]


@require(msgid=int, key=str, value=(int, float, np.float16, np.float32, np.int64, str))
def grib_set_long(msgid, key, value):
    """
    @brief Set the integer value for a key in a message.

    A TypeError exception will be thrown if value cannot be represented
    as an integer.

    @param msgid      id of the message loaded in memory
    @param key        key name
    @param value      value to set
    @exception CodesInternalError,TypeError
    """
    try:
        value = int(value)
    except (ValueError, TypeError):
        raise TypeError("Invalid type")

    if value > sys.maxsize:
        raise ValueError("Value too large")

    h = get_handle(msgid)
    GRIB_CHECK(lib.grib_set_long(h, key.encode(ENC), value))


@require(msgid=int, key=str, value=(int, float, np.float16, np.float32, str))
def grib_set_double(msgid, key, value):
    """
    @brief Set the double value for a key in a message.

    A TypeError exception will be thrown if value cannot be represented
    as a float.

    @param msgid       id of the message loaded in memory
    @param key         key name
    @param value       float value to set
    @exception CodesInternalError,TypeError
    """
    try:
        value = float(value)
    except (ValueError, TypeError):
        raise TypeError("Invalid type")
    h = get_handle(msgid)
    GRIB_CHECK(lib.grib_set_double(h, key.encode(ENC), value))


@require(samplename=str, product_kind=int)
def codes_new_from_samples(samplename, product_kind):
    """
    @brief Create a new valid message from a sample for a given product.

    The available samples are picked up from the directory pointed to
    by the environment variable ECCODES_SAMPLES_PATH.
    To know where the samples directory is run the codes_info tool.\n

    \b Examples: \ref grib_samples.py "grib_samples.py"

    @param samplename     name of the sample to be used
    @param product_kind   CODES_PRODUCT_GRIB or CODES_PRODUCT_BUFR
    @return               id of the message loaded in memory
    @exception CodesInternalError
    """
    if product_kind == CODES_PRODUCT_GRIB:
        return grib_new_from_samples(samplename)
    if product_kind == CODES_PRODUCT_BUFR:
        return codes_bufr_new_from_samples(samplename)
    if product_kind == CODES_PRODUCT_ANY:
        return codes_any_new_from_samples(samplename)
    raise ValueError("Invalid product kind %d" % product_kind)


@require(samplename=str)
def grib_new_from_samples(samplename):
    """
    @brief Create a new valid GRIB message from a sample.

    The available samples are picked up from the directory pointed to
    by the environment variable ECCODES_SAMPLES_PATH.
    To know where the samples directory is run the codes_info tool.\n

    \b Examples: \ref grib_samples.py "grib_samples.py"

    @param samplename   name of the sample to be used
    @return             id of the message loaded in memory
    @exception CodesInternalError
    """
    h = lib.grib_handle_new_from_samples(ffi.NULL, samplename.encode(ENC))
    if h == ffi.NULL:
        raise errors.FileNotFoundError(f"grib_new_from_samples failed: {samplename}")
    return put_handle(h)


@require(samplename=str)
def codes_bufr_new_from_samples(samplename):
    """
    @brief Create a new valid BUFR message from a sample.

    The available samples are picked up from the directory pointed to
    by the environment variable ECCODES_SAMPLES_PATH.
    To know where the samples directory is run the codes_info tool.\n

    \b Examples: \ref bufr_copy_data.py "bufr_copy_data.py"

    @param samplename   name of the BUFR sample to be used
    @return             id of the message loaded in memory
    @exception CodesInternalError
    """
    h = lib.codes_bufr_handle_new_from_samples(ffi.NULL, samplename.encode(ENC))
    if h == ffi.NULL:
        raise errors.FileNotFoundError(f"bufr_new_from_samples failed: {samplename}")
    return put_handle(h)


@require(samplename=str)
def codes_any_new_from_samples(samplename):
    """
    @brief Create a new valid message from a sample.

    The available samples are picked up from the directory pointed to
    by the environment variable ECCODES_SAMPLES_PATH.
    To know where the samples directory is run the codes_info tool.\n

    @param samplename   name of the sample to be used
    @return             id of the message loaded in memory
    @exception CodesInternalError
    """
    h = lib.codes_handle_new_from_samples(ffi.NULL, samplename.encode(ENC))
    if h == ffi.NULL:
        raise errors.FileNotFoundError(f"any_new_from_samples failed: {samplename}")
    return put_handle(h)


@require(msgid_src=int, msgid_dst=int)
def codes_bufr_copy_data(msgid_src, msgid_dst):
    """
    @brief Copy data values from a BUFR message msgid_src to another message msgid_dst

    Copies all the values in the data section that are present in the same position
    in the data tree and with the same number of values to the output handle.

    @param msgid_src   id of the message from which the data are copied
    @param msgid_dst   id of the message to which the data are copied
    @return            id of new message
    @exception CodesInternalError
    """
    h_src = get_handle(msgid_src)
    h_dst = get_handle(msgid_dst)
    err = lib.codes_bufr_copy_data(h_src, h_dst)
    GRIB_CHECK(err)
    return msgid_dst


@require(msgid_src=int)
def grib_clone(msgid_src, headers_only=False):
    r"""
    @brief Create a copy of a message.

    Create a copy of a given message (\em msgid_src) resulting in a new
    message in memory (\em msgid_dest) identical to the original one.
    If the headers_only option is enabled, the clone will not contain
    the Bitmap and Data sections

    \b Examples: \ref grib_clone.py "grib_clone.py"

    @param msgid_src    id of message to be cloned
    @param headers_only whether or not to clone the message with the headers only
    @return             id of clone
    @exception CodesInternalError
    """
    h_src = get_handle(msgid_src)
    if headers_only:
        h_dest = lib.grib_handle_clone_headers_only(h_src)
    else:
        h_dest = lib.grib_handle_clone(h_src)
    if h_dest == ffi.NULL:
        raise errors.MessageInvalidError("clone failed")
    return put_handle(h_dest)


@require(msgid=int, key=str)
def grib_set_double_array(msgid, key, inarray):
    """
    @brief Set the value of the key to a double array.

    The input array can be a numpy.ndarray or a python sequence like tuple, list, array, ...

    The elements of the input sequence need to be convertible to a double.

    @param msgid    id of the message loaded in memory
    @param key      key name
    @param inarray  tuple,list,array,numpy.ndarray
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    length = len(inarray)
    if isinstance(inarray, np.ndarray):
        nd = inarray
        # ECC-1555
        length = inarray.size
        if length > 0:
            if not isinstance(nd[0], float):
                # ECC-1042: input array of integers
                nd = nd.astype(float)
        # ECC-1007: Could also call numpy.ascontiguousarray
        if not inarray.flags["C_CONTIGUOUS"]:
            nd = nd.copy(order="C")
        a = ffi.cast("double*", nd.ctypes.data)
    else:
        a = inarray

    GRIB_CHECK(lib.grib_set_double_array(h, key.encode(ENC), a, length))


@require(msgid=int, key=str)
def grib_get_double_array(msgid, key):
    """
    @brief Get the value of the key as a NumPy array of doubles.

    @param msgid   id of the message loaded in memory
    @param key     key name
    @return        numpy.ndarray
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    nval = grib_get_size(msgid, key)
    length_p = ffi.new("size_t*", nval)
    arr = np.empty((nval,), dtype="float64")
    vals_p = ffi.cast("double *", arr.ctypes.data)
    err = lib.grib_get_double_array(h, key.encode(ENC), vals_p, length_p)
    GRIB_CHECK(err)
    return arr


@require(msgid=int, key=str)
def grib_get_float_array(msgid, key):
    """
    @brief Get the value of the key as a NumPy array of floats.

    @param msgid   id of the message loaded in memory
    @param key     key name
    @return        numpy.ndarray
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    nval = grib_get_size(msgid, key)
    length_p = ffi.new("size_t*", nval)
    arr = np.empty((nval,), dtype="float32")
    vals_p = ffi.cast("float *", arr.ctypes.data)
    err = lib.grib_get_float_array(h, key.encode(ENC), vals_p, length_p)
    GRIB_CHECK(err)
    return arr


# See ECC-1246
def _decode_bytes(binput, maxlen=None):
    if maxlen:
        a_str = ffi.string(binput, maxlen)
    else:
        a_str = ffi.string(binput)
    # Check for a MISSING value i.e., each character has all its bits=1
    if all(x == 255 for x in a_str):
        return ""
    # Replace with a suitable replacement character rather than throw an exception
    return a_str.decode(ENC, "replace")


@require(msgid=int, key=str)
def grib_get_string_array(msgid, key):
    """
    @brief Get the value of the key as a list of strings.

    @param msgid   id of the message loaded in memory
    @param key     key name
    @return        list
    @exception CodesInternalError
    """
    length = grib_get_string_length(msgid, key)
    size = grib_get_size(msgid, key)
    h = get_handle(msgid)
    values_keepalive = [ffi.new("char[]", length) for _ in range(size)]
    values = ffi.new("char*[]", values_keepalive)
    size_p = ffi.new("size_t *", size)
    err = lib.grib_get_string_array(h, key.encode(ENC), values, size_p)
    GRIB_CHECK(err)
    return [_decode_bytes(values[i]) for i in range(size_p[0])]


@require(msgid=int, key=str)
def grib_set_string_array(msgid, key, inarray):
    """
    @brief Set the value of the key to a string array.

    The input array can be a python sequence like tuple, list, array, ...

    The elements of the input sequence need to be convertible to a string.

    @param msgid   id of the message loaded in memory
    @param key     key name
    @param inarray tuple,list,array
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    size = len(inarray)
    # See https://cffi.readthedocs.io/en/release-1.3/using.html
    values_keepalive = [ffi.new("char[]", s.encode(ENC)) for s in inarray]
    values_p = ffi.new("const char *[]", values_keepalive)
    GRIB_CHECK(lib.grib_set_string_array(h, key.encode(ENC), values_p, size))


@require(msgid=int, key=str)
def grib_set_long_array(msgid, key, inarray):
    """
    @brief Set the value of the key to an integer array.

    The input array can be a numpy.ndarray or a python sequence like tuple, list, array, ...

    The elements of the input sequence need to be convertible to an int.

    @param msgid       id of the message loaded in memory
    @param key         key name
    @param inarray     tuple,list,python array,numpy.ndarray
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    if isinstance(inarray, np.ndarray):
        inarray = inarray.tolist()
    GRIB_CHECK(lib.grib_set_long_array(h, key.encode(ENC), inarray, len(inarray)))


@require(msgid=int, key=str)
def grib_get_long_array(msgid, key):
    """
    @brief Get the integer array of values for a key from a message.

    @param msgid      id of the message loaded in memory
    @param key        key name
    @return           numpy.ndarray
    @exception CodesInternalError
    """

    # See ECC-1113
    sizeof_long = ffi.sizeof("long")
    dataType = "int64"
    if sizeof_long == 4:
        dataType = "int32"

    h = get_handle(msgid)
    nval = grib_get_size(msgid, key)
    length_p = ffi.new("size_t*", nval)
    arr = np.empty((nval,), dtype=dataType)
    vals_p = ffi.cast("long *", arr.ctypes.data)
    err = lib.grib_get_long_array(h, key.encode(ENC), vals_p, length_p)
    GRIB_CHECK(err)
    return arr


def grib_multi_new():
    """
    @brief Create a new multi-field GRIB message and return its id.

    \b Examples: \ref grib_multi_write.py "grib_multi_write.py"

    @return  id of the multi-field message
    @exception CodesInternalError
    """
    mgid = lib.grib_multi_handle_new(ffi.NULL)
    if mgid == ffi.NULL:
        raise errors.InvalidGribError("GRIB multi new failed")
    return put_multi_handle(mgid)


@require(gribid=int)
def grib_multi_release(gribid):
    """
    @brief Release a multi-field message from memory.

    \b Examples: \ref grib_multi_write.py "grib_multi_write.py"

    @param gribid    id of the multi-field we want to release the memory for
    @exception CodesInternalError
    """
    mh = get_multi_handle(gribid)
    GRIB_CHECK(lib.grib_multi_handle_delete(mh))


@require(gribid_src=int, namespace=str, gribid_dest=int)
def grib_copy_namespace(gribid_src, namespace, gribid_dest):
    """
    @brief Copy the value of all the keys belonging to a namespace from the source message
    to the destination message.

    @param gribid_src     id of source message
    @param gribid_dest    id of destination message
    @param namespace      namespace to be copied
    @exception CodesInternalError
    """
    h_src = get_handle(gribid_src)
    h_dest = get_handle(gribid_dest)
    GRIB_CHECK(lib.grib_copy_namespace(h_src, namespace.encode(ENC), h_dest))


@require(filename=str, keys=(tuple, list))
def grib_index_new_from_file(filename, keys):
    """
    @brief Create a new index from a file.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param filename   path of the file to index on
    @param keys       sequence of keys to index on.
                      The type of the key can be explicitly declared appending
                      :l for long (or alternatively :i),
                      :d for double,
                      :s for string to the key name.
    @return           index id
    @exception CodesInternalError
    """
    ckeys = ",".join(keys)
    err, iid = err_last(lib.grib_index_new_from_file)(
        ffi.NULL, filename.encode(ENC), ckeys.encode(ENC)
    )
    GRIB_CHECK(err)
    return put_index(iid)


@require(indexid=int, filename=str)
def grib_index_add_file(indexid, filename):
    """
    @brief Add a file to an index.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid    id of the index to add the file to
    @param filename   path of the file to be added to index
    @exception CodesInternalError
    """
    iid = get_index(indexid)
    err = lib.grib_index_add_file(iid, filename.encode(ENC))
    GRIB_CHECK(err)


@require(indexid=int)
def grib_index_release(indexid):
    """
    @brief Delete an index.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file.
    @exception CodesInternalError
    """
    ih = get_index(indexid)
    lib.grib_index_delete(ih)


@require(indexid=int, key=str)
def grib_index_get_size(indexid, key):
    """
    @brief Get the number of distinct values for the index key.
    The key must belong to the index.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid    id of an index created from a file. The index must have been created on the given key.
    @param key        key for which the number of values is computed
    @return           number of distinct values for key in index
    @exception CodesInternalError
    """
    ih = get_index(indexid)
    size_p = ffi.new("size_t*")
    err = lib.grib_index_get_size(ih, key.encode(ENC), size_p)
    GRIB_CHECK(err)
    return size_p[0]


@require(indexid=int, key=str)
def grib_index_get_long(indexid, key):
    """
    @brief Get the distinct values of the key in argument contained in the index.
    The key must belong to the index.

    This function is used when the type of the key was explicitly defined as long or when the native type of
    the key is long.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file. The index must have been created with the key in argument.
    @param key       key for which the values are returned
    @return          tuple with values of key in index
    @exception CodesInternalError
    """
    nval = grib_index_get_size(indexid, key)
    ih = get_index(indexid)

    values_p = ffi.new("long[]", nval)
    size_p = ffi.new("size_t *", nval)
    err = lib.grib_index_get_long(ih, key.encode(ENC), values_p, size_p)
    GRIB_CHECK(err)
    return tuple(int(values_p[i]) for i in range(size_p[0]))


@require(indexid=int, key=str)
def grib_index_get_string(indexid, key):
    """
    @brief Get the distinct values of the key in argument contained in the index.
    The key must belong to the index.

    This function is used when the type of the key was explicitly defined as string or when the native type of
    the key is string.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file. The index must have been created with the key in argument.
    @param key       key for which the values are returned
    @return          tuple with values of key in index
    @exception CodesInternalError
    """
    nval = grib_index_get_size(indexid, key)
    ih = get_index(indexid)
    max_val_size = 1024
    values_keepalive = [ffi.new("char[]", max_val_size) for _ in range(nval)]
    values_p = ffi.new("const char *[]", values_keepalive)
    size_p = ffi.new("size_t *", max_val_size)
    err = lib.grib_index_get_string(ih, key.encode(ENC), values_p, size_p)
    GRIB_CHECK(err)
    return tuple(ffi.string(values_p[i]).decode(ENC) for i in range(size_p[0]))


@require(indexid=int, key=str)
def grib_index_get_double(indexid, key):
    """
    @brief Get the distinct values of the key in argument contained in the index.
    The key must belong to the index.

    This function is used when the type of the key was explicitly defined as double or when the native type
    of the key is double.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid  id of an index created from a file. The index must have been created with the key in argument.
    @param key      key for which the values are returned
    @return         tuple with values of key in index
    @exception CodesInternalError
    """
    nval = grib_index_get_size(indexid, key)
    ih = get_index(indexid)

    values_p = ffi.new("double[]", nval)
    size_p = ffi.new("size_t *", nval)
    err = lib.grib_index_get_double(ih, key.encode(ENC), values_p, size_p)
    GRIB_CHECK(err)
    return tuple(values_p[i] for i in range(size_p[0]))


@require(indexid=int, key=str, value=int)
def grib_index_select_long(indexid, key, value):
    """
    @brief Select the message subset with key==value.
    The value is an integer.

    The key must have been created with integer type or have integer as native type if the type
    was not explicitly defined in the index creation.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file. The index must have been created with the key in argument.
    @param key       key to be selected
    @param value     value of the key to select
    @exception CodesInternalError
    """
    iid = get_index(indexid)
    GRIB_CHECK(lib.grib_index_select_long(iid, key.encode(ENC), value))


@require(indexid=int, key=str, value=float)
def grib_index_select_double(indexid, key, value):
    """
    @brief Select the message subset with key==value.
    The value is a double.

    The key must have been created with integer type or have integer as native type if the type was
    not explicitly defined in the index creation.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file. The index must have been created with the key in argument.
    @param key       key to be selected
    @param value     value of the key to select
    @exception CodesInternalError
    """
    iid = get_index(indexid)
    GRIB_CHECK(lib.grib_index_select_double(iid, key.encode(ENC), value))


@require(indexid=int, key=str, value=str)
def grib_index_select_string(indexid, key, value):
    """
    @brief Select the message subset with key==value.
    The value is an integer.

    The key must have been created with string type or have string as native type if the type
    was not explicitly defined in the index creation.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file. The index must have been created with the key in argument.
    @param key       key to be selected
    @param value     value of the key to select
    @exception CodesInternalError
    """
    ih = get_index(indexid)
    GRIB_CHECK(lib.grib_index_select_string(ih, key.encode(ENC), value.encode(ENC)))


@require(indexid=int)
def grib_new_from_index(indexid):
    """
    @brief Create a new handle from an index after having selected the key values.

    All the keys belonging to the index must be selected before calling this function.
    Successive calls to this function will return all the handles compatible with the constraints
    defined selecting the values of the index keys.

    The message can be accessed through its gribid and will be available until @ref grib_release is called.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file.
    @return          id of the message loaded in memory or None if end of index
    @exception CodesInternalError
    """
    ih = get_index(indexid)
    err, h = err_last(lib.grib_handle_new_from_index)(ih)

    if h == ffi.NULL or err == lib.GRIB_END_OF_INDEX:
        return None
    elif err:
        GRIB_CHECK(err)
        return None
    else:
        return put_handle(h)


@require(msgid=int)
def grib_get_message_size(msgid):
    """
    @brief Get the size of a coded message.

    @param msgid     id of the message loaded in memory
    @return          size in bytes of the message
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    size_p = ffi.new("size_t*")
    err = lib.grib_get_message_size(h, size_p)
    GRIB_CHECK(err)
    return size_p[0]


@require(msgid=int)
def grib_get_message_offset(msgid):
    """
    @brief Get the offset of a coded message.

    @param msgid    id of the message loaded in memory
    @return         offset in bytes of the message
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    offset_p = ffi.new("long int*")
    err = lib.grib_get_message_offset(h, offset_p)
    GRIB_CHECK(err)
    return offset_p[0]


@require(msgid=int, key=str, index=int)
def grib_get_double_element(msgid, key, index):
    """
    @brief Get as double the i-th element of the "key" array.

    @param msgid       id of the message loaded in memory
    @param key         the key to be searched
    @param index       zero based index of value to retrieve
    @return            value
    @exception CodesInternalError

    """
    h = get_handle(msgid)
    value_p = ffi.new("double*")
    err = lib.grib_get_double_element(h, key.encode(ENC), index, value_p)
    GRIB_CHECK(err)
    return value_p[0]


@require(msgid=int, key=str, indexes=(list, tuple))
def grib_get_double_elements(msgid, key, indexes):
    """
    @brief Get as double array the elements of the "key" array whose indexes are listed in the input array.

    @param msgid       id of the message loaded in memory
    @param key         the key to be searched
    @param indexes     list or tuple of indexes
    @return            numpy.ndarray
    @exception CodesInternalError

    """
    nidx = len(indexes)
    h = get_handle(msgid)
    i_p = ffi.new("int[]", indexes)
    value_p = ffi.new("double[]", nidx)
    err = lib.grib_get_double_elements(h, key.encode(ENC), i_p, nidx, value_p)
    GRIB_CHECK(err)
    return [float(v) for v in value_p]


@require(msgid=int, key=str)
def grib_get_elements(msgid, key, indexes):
    """
    @brief Retrieve the elements of the key array for the indexes specified in the input.

    @param msgid      id of the message loaded in memory
    @param key        the key to be searched
    @param indexes    single index or a list of indexes
    @return           numpy.ndarray containing the values of key for the given indexes
    @exception CodesInternalError
    """
    try:
        iter(indexes)
    except TypeError:
        indexes = (indexes,)

    return grib_get_double_elements(msgid, key, indexes)


@require(msgid=int, key=str)
def grib_set_missing(msgid, key):
    """
    @brief Set as missing the value for a key in a GRIB message.

    It can be used to set a missing value in the GRIB header but not in
    the data values.

    \b Examples: \ref grib_set_missing.py "grib_set_missing.py"

    @param  msgid     id of the message loaded in memory
    @param  key       key name
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    GRIB_CHECK(lib.grib_set_missing(h, key.encode(ENC)))


@require(gribid=int)
def grib_set_key_vals(gribid, key_vals):
    """
    Set the values for several keys at once in a grib message.

    @param gribid      id of the grib loaded in memory
    @param key_vals    can be a string, list/tuple or dictionary.
                       If a string, format must be "key1=val1,key2=val2"
                       If a list, it must contain strings of the form "key1=val1"
    @exception         GribInternalError
    """
    if len(key_vals) == 0:
        raise errors.InvalidKeyValueError("Empty key/values argument")
    key_vals_str = ""
    if isinstance(key_vals, str):
        # Plain string. We need to do a DEEP copy so as not to change the original
        key_vals_str = "".join(key_vals)
    elif isinstance(key_vals, (list, tuple)):
        # A list of key=val strings
        for kv in key_vals:
            if not isinstance(kv, str):
                raise TypeError("Invalid list/tuple element type '%s'" % kv)
            if "=" not in str(kv):
                raise GribInternalError("Invalid list/tuple element format '%s'" % kv)
            if len(key_vals_str) > 0:
                key_vals_str += ","
            key_vals_str += kv
    elif isinstance(key_vals, dict):
        # A dictionary mapping keys to values
        for key in key_vals.keys():
            if len(key_vals_str) > 0:
                key_vals_str += ","
            key_vals_str += key + "=" + str(key_vals[key])
    else:
        raise TypeError("Invalid argument type")

    h = get_handle(gribid)
    values = ffi.new("grib_values[]", 1024)
    count_p = ffi.new("int*", 1000)
    err = lib.parse_keyval_string(
        ffi.NULL, key_vals_str.encode(ENC), 1, lib.GRIB_TYPE_UNDEFINED, values, count_p
    )
    GRIB_CHECK(err)
    err = lib.grib_set_values(h, values, count_p[0])
    GRIB_CHECK(err)


@require(msgid=int, key=str)
def grib_is_missing(msgid, key):
    """
    @brief Check if the value of a key is MISSING.

    The value of a key is considered as MISSING when all the bits assigned to it
    are set to 1. This is different from the actual key missing from the grib message.
    The value of a key MISSING has a special significance and that can be read about
    in the WMO documentation.

    @param msgid      id of the message loaded in memory
    @param key        key name
    @return           0->not missing, 1->missing
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    err, value = err_last(lib.grib_is_missing)(h, key.encode(ENC))
    GRIB_CHECK(err)
    return value


@require(msgid=int, key=str)
def grib_is_defined(msgid, key):
    """
    @brief Check if a key is defined (exists)
    @param msgid      id of the message loaded in memory
    @param key        key name
    @return           0->not defined, 1->defined
    @exception        GribInternalError
    """
    h = get_handle(msgid)
    return lib.grib_is_defined(h, key.encode(ENC))


@require(gribid=int, inlat=(int, float), inlon=(int, float))
def grib_find_nearest(gribid, inlat, inlon, is_lsm=False, npoints=1):
    """
    @brief Find the nearest grid point or the nearest four grid points to a given latitude/longitude.

    The number of nearest points returned can be controled through the npoints function argument.

    \b Examples: \ref grib_nearest.py "grib_nearest.py"

    @param gribid     id of the GRIB message loaded in memory
    @param inlat      latitude of the point
    @param inlon      longitude of the point
    @param is_lsm     True if the nearest land point is required otherwise False.
    @param npoints    1 or 4 nearest grid points
    @return           (npoints*(outlat,outlon,value,dist,index))
    @exception CodesInternalError
    """
    h = get_handle(gribid)
    inlats_p = ffi.new("double*", inlat)
    inlons_p = ffi.new("double*", inlon)

    if npoints == 1:
        outlats_p = ffi.new("double[]", 1)
        outlons_p = ffi.new("double[]", 1)
        values_p = ffi.new("double[]", 1)
        distances_p = ffi.new("double[]", 1)
        indexes_p = ffi.new("int[]", 1)
        num_input_points = 1
        # grib_nearest_find_multiple always returns ONE nearest neighbour
        err = lib.grib_nearest_find_multiple(
            h,
            is_lsm,
            inlats_p,
            inlons_p,
            num_input_points,
            outlats_p,
            outlons_p,
            values_p,
            distances_p,
            indexes_p,
        )
        GRIB_CHECK(err)
    elif npoints == 4:
        outlats_p = ffi.new("double[]", npoints)
        outlons_p = ffi.new("double[]", npoints)
        values_p = ffi.new("double[]", npoints)
        distances_p = ffi.new("double[]", npoints)
        indexes_p = ffi.new("int[]", npoints)
        size = ffi.new("size_t *")
        err, nid = err_last(lib.grib_nearest_new)(h)
        GRIB_CHECK(err)
        flags = 0
        err = lib.grib_nearest_find(
            nid,
            h,
            inlat,
            inlon,
            flags,
            outlats_p,
            outlons_p,
            values_p,
            distances_p,
            indexes_p,
            size,
        )
        GRIB_CHECK(err)
        GRIB_CHECK(lib.grib_nearest_delete(nid))
    else:
        raise ValueError("Invalid value for npoints. Expecting 1 or 4.")

    result = []
    for i in range(npoints):
        result.append(
            Bunch(
                lat=outlats_p[i],
                lon=outlons_p[i],
                value=values_p[i],
                distance=distances_p[i],
                index=indexes_p[i],
            )
        )

    return tuple(result)


@require(gribid=int, is_lsm=bool)
def grib_find_nearest_multiple(gribid, is_lsm, inlats, inlons):
    """
    @brief Find the nearest point of a set of points whose latitudes and longitudes are given in
    the inlats, inlons arrays respectively

    @param gribid     id of the GRIB message loaded in memory
    @param is_lsm     True if the nearest land point is required otherwise False.
    @param inlats     latitudes of the points to search for
    @param inlons     longitudes of the points to search for
    @return           (npoints*(outlat,outlon,value,dist,index))
    @exception CodesInternalError
    """
    h = get_handle(gribid)
    npoints = len(inlats)
    if len(inlons) != npoints:
        raise ValueError(
            "grib_find_nearest_multiple: input arrays inlats and inlons must have the same length"
        )

    inlats_p = ffi.new("double[]", inlats)
    inlons_p = ffi.new("double[]", inlons)

    outlats_p = ffi.new("double[]", npoints)
    outlons_p = ffi.new("double[]", npoints)
    values_p = ffi.new("double[]", npoints)
    distances_p = ffi.new("double[]", npoints)
    indexes_p = ffi.new("int[]", npoints)

    # Note: grib_nearest_find_multiple always returns ONE nearest neighbour
    err = lib.grib_nearest_find_multiple(
        h,
        is_lsm,
        inlats_p,
        inlons_p,
        npoints,
        outlats_p,
        outlons_p,
        values_p,
        distances_p,
        indexes_p,
    )
    GRIB_CHECK(err)
    result = []
    for i in range(npoints):
        result.append(
            Bunch(
                lat=outlats_p[i],
                lon=outlons_p[i],
                value=values_p[i],
                distance=distances_p[i],
                index=indexes_p[i],
            )
        )

    return tuple(result)


@require(msgid=int, key=str)
def grib_get_native_type(msgid, key):
    """
    @brief Retrieve the native type of a key.

    Possible values can be int, float or str.

    @param msgid   id of the message loaded in memory
    @param key     key we want to find out the type for
    @return        type of key given as input or None if not determined
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    itype_p = ffi.new("int*")
    err = lib.grib_get_native_type(h, key.encode(ENC), itype_p)
    GRIB_CHECK(err)
    if itype_p[0] in KEYTYPES:
        return KEYTYPES[itype_p[0]]
    else:
        return None


@require(msgid=int, key=str)
def grib_get(msgid, key, ktype=None):
    r"""
    @brief Get the value of a key in a message.

    The type of value returned depends on the native type of the requested key.
    The type of value returned can be forced by using the type argument of the
    function. The ktype argument can be int, float, str or bytes.

    The \em msgid references a message loaded in memory.

    \b Examples: \ref grib_get_keys.py "grib_get_keys.py", \ref grib_print_data.py "grib_print_data.py"

    @see grib_new_from_file, grib_release, grib_set

    @param msgid      id of the message loaded in memory
    @param key        key name
    @param ktype      the type we want the output in, native type if not specified
    @return           scalar value of key as int, float or str
    @exception CodesInternalError
    """
    if not key:
        raise ValueError("Invalid key name")

    if ktype is None:
        ktype = grib_get_native_type(msgid, key)

    result = None
    if ktype is int:
        result = grib_get_long(msgid, key)
    elif ktype is float:
        result = grib_get_double(msgid, key)
    elif ktype is str:
        result = grib_get_string(msgid, key)
    elif ktype is bytes:
        result = grib_get_string(msgid, key)

    return result


@require(msgid=int, key=str)
def grib_get_array(msgid, key, ktype=None):
    """
    @brief Get the contents of an array key.

    The type of the array returned depends on the native type of the requested key.
    For numeric data, the output array will be stored in a NumPy ndarray.
    The type of value returned can be forced by using the ktype argument of the function.
    The ktype argument can be int, float, float32, float64, str or bytes.

    @param msgid  id of the message loaded in memory
    @param key    the key to get the value for
    @param ktype  the type we want the output in, native type if not specified
    @return       numpy.ndarray or None
    @exception CodesInternalError
    """
    if ktype is None:
        ktype = grib_get_native_type(msgid, key)

    # ECC-2086
    if ktype is bytes and key == "bitmap":
        return grib_get_long_array(msgid, key)

    result = None
    if ktype is int:
        result = grib_get_long_array(msgid, key)
    elif ktype is float or ktype is np.float64:
        result = grib_get_double_array(msgid, key)
    elif ktype is np.float32:
        result = grib_get_float_array(msgid, key)
    elif ktype is str:
        result = grib_get_string_array(msgid, key)
    elif ktype is bytes:
        result = grib_get_string_array(msgid, key)

    return result


@require(gribid=int)
def grib_get_values(gribid, ktype=float):
    """
    @brief Retrieve the contents of the 'values' key for a GRIB message.

    A NumPy ndarray containing the values in the GRIB message is returned.

    \b Examples: \ref grib_print_data.py "grib_print_data.py", \ref grib_samples.py "grib_samples.py"

    @param gribid    id of the GRIB loaded in memory
    @param ktype     data type of the result: numpy.float32 or numpy.float64
    @return          numpy.ndarray
    @exception CodesInternalError
    """
    result = None

    if ktype is np.float32:
        result = grib_get_float_array(gribid, "values")
    elif ktype is np.float64 or ktype is float:
        result = grib_get_double_array(gribid, "values")
    else:
        raise TypeError(
            f"Unsupported data type {ktype}. Supported data types are numpy.float32 and numpy.float64"
        )

    return result


@require(gribid=int)
def grib_get_data(gribid):
    """
    @brief Get array containing latitude/longitude and data values.

    @param gribid   id of the GRIB loaded in memory
    @return         lat/lon/value list. Each list element is a dict
    """
    npoints = grib_get(gribid, "numberOfDataPoints")
    outlats_p = ffi.new("double[]", npoints)
    outlons_p = ffi.new("double[]", npoints)
    values_p = ffi.new("double[]", npoints)
    h = get_handle(gribid)
    err = lib.grib_get_data(h, outlats_p, outlons_p, values_p)
    GRIB_CHECK(err)
    result = []
    for i in range(npoints):
        result.append(Bunch(lat=outlats_p[i], lon=outlons_p[i], value=values_p[i]))

    return tuple(result)


@require(gribid=int)
def grib_set_values(gribid, values):
    """
    @brief Set the contents of the 'values' key for a GRIB message.

    The input array can be a numpy.ndarray or a python sequence like tuple, list, array, ...

    The elements of the input sequence need to be convertible to a double.

    \b Examples: \ref grib_clone.py "grib_clone.py", \ref grib_samples.py "grib_samples.py"

    @param gribid   id of the GRIB loaded in memory
    @param values   array of values to set as tuple, list, array or numpy.ndarray
    """
    grib_set_double_array(gribid, "values", values)


@require(msgid=int, key=str)
def grib_set(msgid, key, value):
    """
    @brief Set the value for a scalar key in a message.

    The input value can be a python int, float or str.

    \b Examples: \ref grib_set_keys.py "grib_set_keys.py"

    @see grib_new_from_file, grib_release, grib_get

    @param msgid      id of the message loaded in memory
    @param key        key name
    @param value      scalar value to set for key
    @exception CodesInternalError
    """
    if isinstance(value, (int, np.int64)):
        grib_set_long(msgid, key, value)
    elif isinstance(value, (float, np.float16, np.float32, np.float64)):
        grib_set_double(msgid, key, value)
    elif isinstance(value, str):
        grib_set_string(msgid, key, value)
    # elif hasattr(value, "__iter__"):
    #    # The value passed in is iterable; i.e. a list or array etc
    #    grib_set_array(msgid, key, value)
    else:
        hint = ""
        if hasattr(value, "__iter__"):
            hint = " (Hint: for array keys use codes_set_array(msgid, key, value))"
        raise GribInternalError(
            "Invalid type of value when setting key '%s'%s." % (key, hint)
        )


@require(msgid=int, key=str)
def grib_set_array(msgid, key, value):
    """
    @brief Set the value for an array key in a message.

    Examples of array keys:
    "values" - data values
    "pl" - list of number of points for each latitude in a reduced grid
    "pv" - list of vertical levels

    The input array can be a numpy.ndarray or a python sequence like tuple, list, array, ...

    @param msgid       id of the message loaded in memory
    @param key         key name
    @param value       array to set for key
    @exception CodesInternalError
    """
    val0 = None
    try:
        val0 = value[0]
    except TypeError:
        pass

    if isinstance(val0, (float, np.float16, np.float32, np.float64)):
        grib_set_double_array(msgid, key, value)
    elif isinstance(val0, str):
        grib_set_string_array(msgid, key, value)
    else:
        try:
            int(val0)
        except (ValueError, TypeError):
            raise GribInternalError(
                "Invalid type of value when setting key '%s'." % key
            )
        grib_set_long_array(msgid, key, value)


@require(indexid=int, key=str)
def grib_index_get(indexid, key, ktype=str):
    """
    @brief Get the distinct values of an index key.
    The key must belong to the index.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file. The index must have been created on the given key.
    @param key       key for which the values are returned
    @param ktype     the type we want the output in (int, float or str), str if not specified
    @return          array of values
    @exception CodesInternalError
    """
    # Cannot get the native type of a key from an index
    # so right now the default is str. The user can overwrite
    # the type but there is no way right now to do it automatically.

    # if ktype is None:
    #     ktype = grib_get_native_type(indexid,key)

    result = None
    if ktype is int:
        result = grib_index_get_long(indexid, key)
    elif ktype is float:
        result = grib_index_get_double(indexid, key)
    elif ktype is str:
        result = grib_index_get_string(indexid, key)

    return result


@require(indexid=int, key=str)
def grib_index_select(indexid, key, value):
    """
    @brief Select the message subset with key==value.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid   id of an index created from a file. The index must have been created with the key in argument.
    @param key       key to be selected
    @param value     value of the key to select
    @exception CodesInternalError
    """
    if isinstance(value, int):
        grib_index_select_long(indexid, key, value)
    elif isinstance(value, float):
        grib_index_select_double(indexid, key, value)
    elif isinstance(value, str):
        grib_index_select_string(indexid, key, value)
    else:
        raise GribInternalError("Invalid type of value when setting key '%s'." % key)


@require(indexid=int, filename=str)
def grib_index_write(indexid, filename):
    """
    @brief Write an index to a file for later reuse.

    An index can be loaded back from an index file with \ref grib_index_read.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param indexid    id of the index
    @param filename   path of file to save the index to
    @exception CodesInternalError
    """
    ih = get_index(indexid)
    GRIB_CHECK(lib.grib_index_write(ih, filename.encode(ENC)))


@require(filename=str)
def grib_index_read(filename):
    """
    @brief Loads an index previously saved with \ref grib_index_write to a file.

    \b Examples: \ref grib_index.py "grib_index.py"

    @param filename    path of file to load the index from
    @return            id of the loaded index
    @exception CodesInternalError
    """
    err, ih = err_last(lib.grib_index_read)(ffi.NULL, filename.encode(ENC))
    GRIB_CHECK(err)
    return put_index(ih)


@require(flag=bool)
def grib_no_fail_on_wrong_length(flag):
    """
    @brief Do not fail if the message has the wrong length.

    @param flag True/False
    """
    raise NotImplementedError("API not implemented in CFFI porting.")


@require(flag=bool)
def grib_gts_header(flag):
    """
    @brief Set the GTS header on/off.

    @param flag True/False
    """
    context = lib.grib_context_get_default()
    if flag:
        lib.grib_gts_header_on(context)
    else:
        lib.grib_gts_header_off(context)


def grib_get_api_version(vformat=str):
    """
    @brief Get the API version.

    Returns the version of the API as a string in the format "major.minor.revision"
    or as an integer (10000*major + 100*minor + revision)
    """

    def div(v, d):
        return (v / d, v % d)

    if not lib:
        raise RuntimeError("Could not load the ecCodes library!")

    v = lib.grib_get_api_version()

    if vformat is str:
        v, revision = div(v, 100)
        v, minor = div(v, 100)
        major = v
        return "%d.%d.%d" % (major, minor, revision)
    else:
        return v


__version__ = grib_get_api_version()


def codes_get_version_info():
    """
    @brief Get version information.

    Returns a dictionary containing the versions of the ecCodes API and the Python bindings
    """
    vinfo = dict()
    vinfo["eccodes"] = grib_get_api_version()
    vinfo["bindings"] = bindings_version
    return vinfo


@require(order=int)
def codes_get_gaussian_latitudes(order):
    """
    @brief Return the Gaussian latitudes

    @param order    The Gaussian order/number (also called the truncation)
    @return         A list of latitudes with 2*order elements
    """
    num_elems = 2 * order
    outlats_p = ffi.new("double[]", num_elems)
    err = lib.grib_get_gaussian_latitudes(order, outlats_p)
    GRIB_CHECK(err)
    return outlats_p


@require(msgid=int)
def grib_get_message(msgid):
    """
    @brief Get the binary message.

    Returns the binary string message associated with the message identified by msgid.

    @see grib_new_from_message

    @param msgid      id of the message loaded in memory
    @return           binary string message associated with msgid
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    message_p = ffi.new("const void**")
    message_length_p = ffi.new("size_t*")
    err = lib.grib_get_message(h, message_p, message_length_p)
    GRIB_CHECK(err)
    # NOTE: ffi.string would stop on the first nul-character.
    fixed_length_buffer = ffi.buffer(
        ffi.cast("char*", message_p[0]), message_length_p[0]
    )
    # Convert to bytes
    return fixed_length_buffer[:]


@require(message=(bytes, str, memoryview))
def grib_new_from_message(message):
    """
    @brief Create a handle from a message in memory.

    Create a new message from the input binary string and return its id.

    @see grib_get_message

    @param         message binary string message
    @return        msgid of the newly created message
    @exception CodesInternalError
    """
    if isinstance(message, memoryview):
        message = ffi.from_buffer(message)

    if isinstance(message, str):
        message = message.encode(ENC)

    h = lib.grib_handle_new_from_message_copy(ffi.NULL, message, len(message))
    if h == ffi.NULL:
        raise errors.MessageInvalidError("new_from_message failed")
    return put_handle(h)


def codes_definition_path():
    """
    @brief Get the definition path
    """
    context = lib.grib_context_get_default()
    dpath = lib.codes_definition_path(context)
    return ffi.string(dpath).decode(ENC)


def codes_samples_path():
    """
    @brief Get the samples path
    """
    context = lib.grib_context_get_default()
    spath = lib.codes_samples_path(context)
    return ffi.string(spath).decode(ENC)


def grib_set_debug(dmode):
    """
    @brief Set the debug mode

    @param dmode  -1, 0 or 1
    """
    context = lib.grib_context_get_default()
    lib.grib_context_set_debug(context, dmode)


@require(val=int)
def grib_set_data_quality_checks(val):
    """
    @brief Enable/Disable GRIB data quality checks

    @param val  0, 1 or 2
                0 -> disable data quality checks
                1 -> failure results in an error
                2 -> failure results in a warning
    """
    assert val == 0 or val == 1 or val == 2
    context = lib.grib_context_get_default()
    lib.grib_context_set_data_quality_checks(context, val)


@require(defs_path=str)
def grib_set_definitions_path(defs_path):
    """
    @brief Set the definitions path

    @param defs_path   definitions path
    """
    context = lib.grib_context_get_default()
    lib.grib_context_set_definitions_path(context, defs_path.encode(ENC))


@require(samples_path=str)
def grib_set_samples_path(samples_path):
    """
    @brief Set the samples path

    @param samples_path   samples path
    """
    context = lib.grib_context_get_default()
    lib.grib_context_set_samples_path(context, samples_path.encode(ENC))


def grib_context_delete():
    """
    @brief Wipe all the cached data and definitions files in the context
    """
    lib.grib_context_delete(ffi.NULL)


def codes_bufr_multi_element_constant_arrays_on():
    """
    @brief BUFR: Turn on the mode where you get multiple elements
    in constant arrays

    @exception CodesInternalError
    """
    context = lib.grib_context_get_default()
    lib.codes_bufr_multi_element_constant_arrays_on(context)


def codes_bufr_multi_element_constant_arrays_off():
    """
    @brief BUFR: Turn off the mode where you get multiple elements
    in constant arrays i.e. you get a single element

    @exception CodesInternalError
    """
    context = lib.grib_context_get_default()
    lib.codes_bufr_multi_element_constant_arrays_off(context)


@require(msgid=int)
def codes_dump(msgid, output_fileobj=sys.stdout, mode="wmo", flags=0):
    """
    @brief Print all keys to an output file object, with the given dump mode and flags

    @param msgid          id of the message loaded in memory
    @param output_fileobj output file object e.g., sys.stdout
    @param mode           dump mode e.g., "wmo", "debug", "json"
    """
    h = get_handle(msgid)
    lib.grib_dump_content(h, output_fileobj, mode.encode(ENC), flags, ffi.NULL)


# Convert the C codes_bufr_header struct to a Python dictionary
def _convert_struct_to_dict(s):
    result = {}
    ident_found = False
    for a in dir(s):
        value = getattr(s, a)
        if not ident_found and a == "ident":
            value = ffi.string(value).decode(ENC)
            ident_found = True
        result[a] = value
    return result


def codes_bufr_extract_headers(filepath, is_strict=True):
    """
    @brief BUFR header extraction

    @param filepath       path of input BUFR file
    @param is_strict      fail as soon as any invalid BUFR message is encountered
    @return               a generator that yields items (each item is a dictionary)
    @exception CodesInternalError
    """
    context = lib.grib_context_get_default()
    headers_p = ffi.new("struct codes_bufr_header**")
    num_message_p = ffi.new("int*")

    err = lib.codes_bufr_extract_headers_malloc(
        context, filepath.encode(ENC), headers_p, num_message_p, is_strict
    )
    GRIB_CHECK(err)

    num_messages = num_message_p[0]
    headers = headers_p[0]

    # result = []
    # for i in range(num_messages):
    #    d = _convert_struct_to_dict(headers[i])
    #    result.append(d)
    # return result

    i = 0
    while i < num_messages:
        yield _convert_struct_to_dict(headers[i])
        i += 1


@require(msgid=int)
def codes_bufr_key_is_header(msgid, key):
    """
    @brief Check if the BUFR key is in the header or in the data section.

    If the data section has not been unpacked, then passing in a key from
    the data section will throw KeyValueNotFoundError.

    @param msgid      id of the BUFR message loaded in memory
    @param key        key name
    @return           1->header, 0->data section
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    err, value = err_last(lib.codes_bufr_key_is_header)(h, key.encode(ENC))
    GRIB_CHECK(err)
    return value


@require(msgid=int)
def codes_bufr_key_is_coordinate(msgid, key):
    """
    @brief Check if the BUFR key corresponds to a coordinate descriptor.

    If the data section has not been unpacked, then passing in a key from
    the data section will throw KeyValueNotFoundError.

    @param msgid      id of the BUFR message loaded in memory
    @param key        key name
    @return           1->coordinate, 0->not coordinate
    @exception CodesInternalError
    """
    h = get_handle(msgid)
    err, value = err_last(lib.codes_bufr_key_is_coordinate)(h, key.encode(ENC))
    GRIB_CHECK(err)
    return value


def codes_extract_offsets(filepath, product_kind, is_strict=True):
    """
    @brief Message offset extraction

    @param filepath      path of input file
    @param product_kind  one of CODES_PRODUCT_GRIB, CODES_PRODUCT_BUFR, CODES_PRODUCT_ANY or CODES_PRODUCT_GTS
    @param is_strict     if True, fail as soon as any invalid message is encountered
    @return              a generator that yields offsets (as integers)
    @exception CodesInternalError
    """
    context = lib.grib_context_get_default()
    offsets_p = ffi.new("long int**")
    num_message_p = ffi.new("int*")

    err = lib.codes_extract_offsets_malloc(
        context, filepath.encode(ENC), product_kind, offsets_p, num_message_p, is_strict
    )
    GRIB_CHECK(err)

    num_messages = num_message_p[0]
    offsets = offsets_p[0]

    i = 0
    while i < num_messages:
        yield offsets[i]
        i += 1


def codes_extract_offsets_sizes(filepath, product_kind, is_strict=True):
    """
    @brief Message offset and size extraction

    @param filepath      path of input file
    @param product_kind  one of CODES_PRODUCT_GRIB, CODES_PRODUCT_BUFR, CODES_PRODUCT_ANY or CODES_PRODUCT_GTS
    @param is_strict     if True, fail as soon as any invalid message is encountered
    @return              a generator that yields lists of pairs of offsets and sizes (as integers)
    @exception CodesInternalError
    """
    context = lib.grib_context_get_default()
    offsets_p = ffi.new("long int**")
    sizes_p = ffi.new("size_t**")
    num_message_p = ffi.new("int*")

    err = lib.codes_extract_offsets_sizes_malloc(
        context,
        filepath.encode(ENC),
        product_kind,
        offsets_p,
        sizes_p,
        num_message_p,
        is_strict,
    )
    GRIB_CHECK(err)

    num_messages = num_message_p[0]
    offsets = offsets_p[0]
    sizes = sizes_p[0]

    i = 0
    while i < num_messages:
        yield (offsets[i], sizes[i])
        i += 1


@require(select=int)
def codes_get_features(select=CODES_FEATURES_ALL):
    """
    @brief Get the list of library features.

    @param select   One of CODES_FEATURES_ALL, CODES_FEATURES_ENABLED or CODES_FEATURES_DISABLED
    @return         space-separated string of feature names
    @exception CodesInternalError
    """
    ssize = 1024
    result = ffi.new("char[]", ssize)
    size_p = ffi.new("size_t *", ssize)
    err = lib.codes_get_features(result, size_p, select)
    GRIB_CHECK(err)
    return ffi.string(result).decode(ENC)


# -------------------------------
# EXPERIMENTAL FEATURES
# -------------------------------
@require(msgid=int)
def grib_nearest_new(msgid):
    h = get_handle(msgid)
    err, nid = err_last(lib.grib_nearest_new)(h)
    GRIB_CHECK(err)
    return put_grib_nearest(nid)


def put_grib_nearest(nid):
    return int(ffi.cast("size_t", nid))


def get_grib_nearest(nid):
    return ffi.cast("grib_nearest*", nid)


@require(nid=int)
def grib_nearest_delete(nid):
    nh = get_grib_nearest(nid)
    lib.grib_nearest_delete(nh)


@require(nid=int, gribid=int)
def grib_nearest_find(nid, gribid, inlat, inlon, flags, is_lsm=False, npoints=4):
    # flags has to be one of:
    #  GRIB_NEAREST_SAME_GRID
    #  GRIB_NEAREST_SAME_DATA
    #  GRIB_NEAREST_SAME_POINT
    if npoints != 4:
        raise errors.FunctionNotImplementedError(
            "grib_nearest_find npoints argument: Only 4 points supported"
        )
    if is_lsm:
        raise errors.FunctionNotImplementedError(
            "grib_nearest_find is_lsm argument: Land sea mask not supported"
        )

    h = get_handle(gribid)
    outlats_p = ffi.new("double[]", npoints)
    outlons_p = ffi.new("double[]", npoints)
    values_p = ffi.new("double[]", npoints)
    distances_p = ffi.new("double[]", npoints)
    indexes_p = ffi.new("int[]", npoints)
    size = ffi.new("size_t *")
    nh = get_grib_nearest(nid)
    err = lib.grib_nearest_find(
        nh,
        h,
        inlat,
        inlon,
        flags,
        outlats_p,
        outlons_p,
        values_p,
        distances_p,
        indexes_p,
        size,
    )
    GRIB_CHECK(err)
    result = []
    for i in range(npoints):
        result.append(
            Bunch(
                lat=outlats_p[i],
                lon=outlons_p[i],
                value=values_p[i],
                distance=distances_p[i],
                index=indexes_p[i],
            )
        )

    return tuple(result)


def codes_get_library_path():
    return library_path
