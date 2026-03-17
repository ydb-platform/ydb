#------------------------------------------------------------------------------
# Copyright (c) 2022, 2024, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# utils.pyx
#
# Cython file defining utility methods (embedded in base_impl.pyx).
#------------------------------------------------------------------------------

cdef int _set_app_context_param(dict args, str name, object target) except -1:
    """
    Sets an application context parameter to the value provided in the
    dictionary, if a value is provided. This value is then set directly on the
    target.
    """
    in_val = args.get(name)
    if in_val is not None:
        message = (
            "appcontext should be a list of 3-tuples and each 3-tuple should "
            "contain three strings"
        )
        if not isinstance(in_val, list):
            raise TypeError(message)
        for entry in in_val:
            if not isinstance(entry, tuple) or len(entry) != 3:
                raise TypeError(message)
            for value in entry:
                if not isinstance(value, str):
                    raise TypeError(message)
        setattr(target, name, in_val)


cdef int _set_bool_param(dict args, str name, bint *out_val) except -1:
    """
    Sets a boolean parameter to the value provided in the dictionary. This can
    be a case-insenstive string matching on/off, yes/no or true/false (such as
    when parsed from a connect string). It can also be a directly passed
    argument which will be explicitly converted to a boolean value.
    """
    in_val = args.get(name)
    if in_val is not None:
        if isinstance(in_val, str):
            out_val[0] = (in_val.strip().lower() in ("on", "yes", "true"))
        else:
            out_val[0] = bool(in_val)


cdef int _set_duration_param(dict args, str name, double *out_val) except -1:
    """
    Sets a duration parameter to the value provided in the dictionary. This can
    be a string (such as when parsed from a connect string) containing a
    floating point value followed by an otional unit: ms (milliseconds), sec
    (seconds) or min (minutes). It can also be a directly passed argument which
    will be explicitly converted to a floating point value.
    """
    in_val = args.get(name)
    if in_val is not None:
        if isinstance(in_val, str):
            in_val = in_val.strip().lower()
            if in_val.endswith("sec"):
                out_val[0] = float(in_val[:-3].strip())
            elif in_val.endswith("ms"):
                out_val[0] = float(in_val[:-2].strip()) / 1000
            elif in_val.endswith("min"):
                out_val[0] = float(in_val[:-3].strip()) * 60
            else:
                out_val[0] = float(in_val.strip())
        else:
            out_val[0] = float(in_val)


cdef int _set_int_param(dict args, str name, int* out_val) except -1:
    """
    Sets an integer parameter to the value provided in the dictionary. This
    can be a string (such as when parsed from a connect string). It can also be
    a directly passed argument which will be explicitly converted to an integer
    value.
    """
    in_val = args.get(name)
    if in_val is not None:
        out_val[0] = int(in_val)


cdef int _set_uint_param(dict args, str name, uint32_t* out_val) except -1:
    """
    Sets an unsigned integer parameter to the value provided in the dictionary.
    This can be a string (such as when parsed from a connect string). It can
    also be a directly passed argument which will be explicitly converted to an
    integer value.
    """
    in_val = args.get(name)
    if in_val is not None:
        out_val[0] = int(in_val)


cdef int _set_obj_param(dict args, str name, object target) except -1:
    """
    Sets an object parameter to the value provided in the dictionary, if a
    value is provided. This value is then set directly on the target.
    """
    in_val = args.get(name)
    if in_val is not None:
        setattr(target, name, in_val)


cdef int _set_purity_param(dict args, str name, uint32_t* out_val) except -1:
    """
    Sets a purity parameter to the value provided in the dictionary. This
    must be one of "new" or "self" currently (or the equivalent constants, if
    specified directly). If it is not one of these values an error is raised.
    """
    cdef bint ok = True
    in_val = args.get(name)
    if in_val is not None:
        if isinstance(in_val, str):
            in_val = in_val.lower()
            if in_val == "new":
                out_val[0] = PURITY_NEW
            elif in_val == "self":
                out_val[0] = PURITY_SELF
            else:
                ok = False
        elif isinstance(in_val, int):
            if in_val == PURITY_NEW:
                out_val[0] = PURITY_NEW
            elif in_val == PURITY_SELF:
                out_val[0] = PURITY_SELF
            elif in_val == PURITY_DEFAULT:
                out_val[0] = PURITY_DEFAULT
            else:
                ok = False
        else:
            ok = False
        if not ok:
            errors._raise_err(errors.ERR_INVALID_POOL_PURITY,
                              purity=in_val)


cdef int _set_ssl_version_param(dict args, str name, object target) except -1:
    """
    Sets a SSL version parameter to the value specified. This must be one of
    the values "tlsv1.2" or "tlsv1.3". If it is not one of these values
    an error is raised. If a value is specified and meets the criteria it is
    set directly on the target (since strings are treated as Python objects).
    """
    in_val = args.get(name)
    if in_val is not None:
        if isinstance(in_val, str):
            in_val = in_val.lower()
            if in_val == "tlsv1.2":
                in_val = ssl.TLSVersion.TLSv1_2
            elif in_val == "tlsv1.3":
                in_val = ssl.TLSVersion.TLSv1_3
        if in_val is not ssl.TLSVersion.TLSv1_2 \
                and in_val is not ssl.TLSVersion.TLSv1_3:
            errors._raise_err(errors.ERR_INVALID_SSL_VERSION,
                              ssl_version=in_val)
        setattr(target, name, in_val)


cdef int _set_str_param(dict args, str name, object target, bint check_network_character_set = False) except -1:
    """
    Sets a string parameter to the value provided in the dictionary. If a value
    is specified it is set directly on the target (since strings are treated as
    Python objects). Note that if check_network_character_set is True, an
    exception is thrown if the sanitized value does not match the input value.
    """
    in_val = args.get(name)
    if in_val:
        in_val = str(in_val)
        if check_network_character_set:
            if sanitize(in_val) != in_val:
                errors._raise_err(errors.ERR_INVALID_NETWORK_NAME, name=name)
        setattr(target, name, in_val)


def init_base_impl(package):
    """
    Initializes globals after the package has been completely initialized. This
    is to avoid circular imports and eliminate the need for global lookups.
    """
    global \
        errors, \
        exceptions, \
        utils, \
        DRIVER_VERSION, \
        PY_TYPE_ASYNC_CURSOR, \
        PY_TYPE_ASYNC_LOB, \
        PY_TYPE_CONNECT_PARAMS, \
        PY_TYPE_CURSOR, \
        PY_TYPE_DB_OBJECT, \
        PY_TYPE_DB_OBJECT_TYPE, \
        PY_TYPE_FETCHINFO, \
        PY_TYPE_INTERVAL_YM, \
        PY_TYPE_JSON_ID, \
        PY_TYPE_LOB, \
        PY_TYPE_MESSAGE, \
        PY_TYPE_MESSAGE_QUERY, \
        PY_TYPE_MESSAGE_ROW, \
        PY_TYPE_MESSAGE_TABLE, \
        PY_TYPE_POOL_PARAMS, \
        PY_TYPE_VAR

    errors = package.errors
    exceptions = package.exceptions
    utils = package.utils
    DRIVER_VERSION = package.__version__
    PY_TYPE_ASYNC_CURSOR = package.AsyncCursor
    PY_TYPE_ASYNC_LOB = package.AsyncLOB
    PY_TYPE_CONNECT_PARAMS = package.ConnectParams
    PY_TYPE_CURSOR = package.Cursor
    PY_TYPE_DB_OBJECT = package.DbObject
    PY_TYPE_DB_OBJECT_TYPE = package.DbObjectType
    PY_TYPE_FETCHINFO = package.FetchInfo
    PY_TYPE_INTERVAL_YM = package.IntervalYM
    PY_TYPE_JSON_ID = package.JsonId
    PY_TYPE_LOB = package.LOB
    PY_TYPE_MESSAGE = package.Message
    PY_TYPE_MESSAGE_QUERY = package.MessageQuery
    PY_TYPE_MESSAGE_ROW = package.MessageRow
    PY_TYPE_MESSAGE_TABLE = package.MessageTable
    PY_TYPE_POOL_PARAMS = package.PoolParams
    PY_TYPE_VAR = package.Var


def sanitize(str value):
    """
    Replaces invalid characters in a string with characters guaranteed to be in
    the network character set.
    """
    cdef str sanitized_value = value

    # strip single or double quotes from the start and end
    if sanitized_value.startswith(("'", '"')):
        sanitized_value = sanitized_value[1:]
    if sanitized_value.endswith(("'", '"')):
        sanitized_value = sanitized_value[:-1]

    # replace invalid characters with '?'
    sanitized_value = "".join(
        char if char in VALID_NETWORK_NAME_CHARS else "?"
        for char in sanitized_value
    )

    # if the last character is a backslash, replace it with '?'
    if sanitized_value.endswith("\\"):
        sanitized_value = sanitized_value[:-1] + "?"

    return sanitized_value
