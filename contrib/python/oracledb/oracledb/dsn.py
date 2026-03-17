# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
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
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# dsn.py
#
# Contains makedsn(), a method available for backwards compatibility with
# cx_Oracle. Use of the ConnectParams class or the keyword arguments to
# connect() and create_pool() is recommended instead.
# -----------------------------------------------------------------------------

from . import errors


def _check_arg(name: str, value: str) -> None:
    """
    Checks the argument to ensure that it does not contain (, ) or = as these
    characters are not permitted within connect strings.
    """
    if "(" in value or ")" in value or "=" in value:
        errors._raise_err(errors.ERR_INVALID_MAKEDSN_ARG, name=name)


def makedsn(
    host: str,
    port: int,
    sid: str = None,
    service_name: str = None,
    region: str = None,
    sharding_key: str = None,
    super_sharding_key: str = None,
) -> str:
    """
    Return a string suitable for use as the dsn parameter for connect(). This
    string is identical to the strings that are defined in the tnsnames.ora
    file.
    """
    connect_data_parts = []
    _check_arg("host", host)
    if service_name is not None:
        _check_arg("service_name", service_name)
        connect_data_parts.append(f"(SERVICE_NAME={service_name})")
    elif sid is not None:
        _check_arg("sid", sid)
        connect_data_parts.append(f"(SID={sid})")
    if region is not None:
        _check_arg("region", region)
        connect_data_parts.append(f"(REGION={region})")
    if sharding_key is not None:
        _check_arg("sharding_key", sharding_key)
        connect_data_parts.append(f"(SHARDING_KEY={sharding_key})")
    if super_sharding_key is not None:
        _check_arg("super_sharding_key", super_sharding_key)
        connect_data_parts.append(f"(SUPER_SHARDING_KEY={super_sharding_key})")
    connect_data = "".join(connect_data_parts)
    return (
        f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})"
        f"(PORT={port}))(CONNECT_DATA={connect_data}))"
    )
