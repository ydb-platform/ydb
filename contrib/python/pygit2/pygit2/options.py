# Copyright 2010-2025 The pygit2 contributors
#
# This file is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2,
# as published by the Free Software Foundation.
#
# In addition to the permissions in the GNU General Public License,
# the authors give you unlimited permission to link the compiled
# version of this file into combinations with other programs,
# and to distribute those combinations without any restriction
# coming from the use of this file.  (The General Public License
# restrictions do apply in other respects; for example, they cover
# modification of the file, and distribution when not linked into
# a combined executable.)
#
# This file is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

"""
Libgit2 global options management using CFFI.
"""

from __future__ import annotations

# Import only for type checking to avoid circular imports
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from .errors import check_error
from .ffi import C, ffi
from .utils import to_bytes, to_str

if TYPE_CHECKING:
    from ._libgit2.ffi import NULL_TYPE, ArrayC, char, char_pointer
    from .enums import ConfigLevel, ObjectType, Option

# Export GIT_OPT constants for backward compatibility
GIT_OPT_GET_MWINDOW_SIZE: int = C.GIT_OPT_GET_MWINDOW_SIZE
GIT_OPT_SET_MWINDOW_SIZE: int = C.GIT_OPT_SET_MWINDOW_SIZE
GIT_OPT_GET_MWINDOW_MAPPED_LIMIT: int = C.GIT_OPT_GET_MWINDOW_MAPPED_LIMIT
GIT_OPT_SET_MWINDOW_MAPPED_LIMIT: int = C.GIT_OPT_SET_MWINDOW_MAPPED_LIMIT
GIT_OPT_GET_SEARCH_PATH: int = C.GIT_OPT_GET_SEARCH_PATH
GIT_OPT_SET_SEARCH_PATH: int = C.GIT_OPT_SET_SEARCH_PATH
GIT_OPT_SET_CACHE_OBJECT_LIMIT: int = C.GIT_OPT_SET_CACHE_OBJECT_LIMIT
GIT_OPT_SET_CACHE_MAX_SIZE: int = C.GIT_OPT_SET_CACHE_MAX_SIZE
GIT_OPT_ENABLE_CACHING: int = C.GIT_OPT_ENABLE_CACHING
GIT_OPT_GET_CACHED_MEMORY: int = C.GIT_OPT_GET_CACHED_MEMORY
GIT_OPT_GET_TEMPLATE_PATH: int = C.GIT_OPT_GET_TEMPLATE_PATH
GIT_OPT_SET_TEMPLATE_PATH: int = C.GIT_OPT_SET_TEMPLATE_PATH
GIT_OPT_SET_SSL_CERT_LOCATIONS: int = C.GIT_OPT_SET_SSL_CERT_LOCATIONS
GIT_OPT_SET_USER_AGENT: int = C.GIT_OPT_SET_USER_AGENT
GIT_OPT_ENABLE_STRICT_OBJECT_CREATION: int = C.GIT_OPT_ENABLE_STRICT_OBJECT_CREATION
GIT_OPT_ENABLE_STRICT_SYMBOLIC_REF_CREATION: int = (
    C.GIT_OPT_ENABLE_STRICT_SYMBOLIC_REF_CREATION
)
GIT_OPT_SET_SSL_CIPHERS: int = C.GIT_OPT_SET_SSL_CIPHERS
GIT_OPT_GET_USER_AGENT: int = C.GIT_OPT_GET_USER_AGENT
GIT_OPT_ENABLE_OFS_DELTA: int = C.GIT_OPT_ENABLE_OFS_DELTA
GIT_OPT_ENABLE_FSYNC_GITDIR: int = C.GIT_OPT_ENABLE_FSYNC_GITDIR
GIT_OPT_GET_WINDOWS_SHAREMODE: int = C.GIT_OPT_GET_WINDOWS_SHAREMODE
GIT_OPT_SET_WINDOWS_SHAREMODE: int = C.GIT_OPT_SET_WINDOWS_SHAREMODE
GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION: int = C.GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION
GIT_OPT_SET_ALLOCATOR: int = C.GIT_OPT_SET_ALLOCATOR
GIT_OPT_ENABLE_UNSAVED_INDEX_SAFETY: int = C.GIT_OPT_ENABLE_UNSAVED_INDEX_SAFETY
GIT_OPT_GET_PACK_MAX_OBJECTS: int = C.GIT_OPT_GET_PACK_MAX_OBJECTS
GIT_OPT_SET_PACK_MAX_OBJECTS: int = C.GIT_OPT_SET_PACK_MAX_OBJECTS
GIT_OPT_DISABLE_PACK_KEEP_FILE_CHECKS: int = C.GIT_OPT_DISABLE_PACK_KEEP_FILE_CHECKS
GIT_OPT_GET_MWINDOW_FILE_LIMIT: int = C.GIT_OPT_GET_MWINDOW_FILE_LIMIT
GIT_OPT_SET_MWINDOW_FILE_LIMIT: int = C.GIT_OPT_SET_MWINDOW_FILE_LIMIT
GIT_OPT_GET_OWNER_VALIDATION: int = C.GIT_OPT_GET_OWNER_VALIDATION
GIT_OPT_SET_OWNER_VALIDATION: int = C.GIT_OPT_SET_OWNER_VALIDATION
GIT_OPT_ENABLE_HTTP_EXPECT_CONTINUE: int = C.GIT_OPT_ENABLE_HTTP_EXPECT_CONTINUE
GIT_OPT_SET_ODB_PACKED_PRIORITY: int = C.GIT_OPT_SET_ODB_PACKED_PRIORITY
GIT_OPT_SET_ODB_LOOSE_PRIORITY: int = C.GIT_OPT_SET_ODB_LOOSE_PRIORITY
GIT_OPT_GET_EXTENSIONS: int = C.GIT_OPT_GET_EXTENSIONS
GIT_OPT_SET_EXTENSIONS: int = C.GIT_OPT_SET_EXTENSIONS
GIT_OPT_GET_HOMEDIR: int = C.GIT_OPT_GET_HOMEDIR
GIT_OPT_SET_HOMEDIR: int = C.GIT_OPT_SET_HOMEDIR
GIT_OPT_SET_SERVER_CONNECT_TIMEOUT: int = C.GIT_OPT_SET_SERVER_CONNECT_TIMEOUT
GIT_OPT_GET_SERVER_CONNECT_TIMEOUT: int = C.GIT_OPT_GET_SERVER_CONNECT_TIMEOUT
GIT_OPT_SET_SERVER_TIMEOUT: int = C.GIT_OPT_SET_SERVER_TIMEOUT
GIT_OPT_GET_SERVER_TIMEOUT: int = C.GIT_OPT_GET_SERVER_TIMEOUT
GIT_OPT_GET_USER_AGENT_PRODUCT: int = C.GIT_OPT_GET_USER_AGENT_PRODUCT
GIT_OPT_SET_USER_AGENT_PRODUCT: int = C.GIT_OPT_SET_USER_AGENT_PRODUCT
GIT_OPT_ADD_SSL_X509_CERT: int = C.GIT_OPT_ADD_SSL_X509_CERT


NOT_PASSED = object()


def check_args(option: Option, arg1: Any, arg2: Any, expected: int) -> None:
    if expected == 0 and (arg1 is not NOT_PASSED or arg2 is not NOT_PASSED):
        raise TypeError(f'option({option}) takes no additional arguments')

    if expected == 1 and (arg1 is NOT_PASSED or arg2 is not NOT_PASSED):
        raise TypeError(f'option({option}, x) requires 1 additional argument')

    if expected == 2 and (arg1 is NOT_PASSED or arg2 is NOT_PASSED):
        raise TypeError(f'option({option}, x, y) requires 2 additional arguments')


@overload
def option(
    option_type: Literal[
        Option.GET_MWINDOW_SIZE,
        Option.GET_MWINDOW_MAPPED_LIMIT,
        Option.GET_MWINDOW_FILE_LIMIT,
    ],
) -> int: ...


@overload
def option(
    option_type: Literal[
        Option.SET_MWINDOW_SIZE,
        Option.SET_MWINDOW_MAPPED_LIMIT,
        Option.SET_MWINDOW_FILE_LIMIT,
        Option.SET_CACHE_MAX_SIZE,
    ],
    arg1: int,  # value
) -> None: ...


@overload
def option(
    option_type: Literal[Option.GET_SEARCH_PATH],
    arg1: ConfigLevel,  # value
) -> str: ...


@overload
def option(
    option_type: Literal[Option.SET_SEARCH_PATH],
    arg1: ConfigLevel,  # type
    arg2: str,  # value
) -> None: ...


@overload
def option(
    option_type: Literal[Option.SET_CACHE_OBJECT_LIMIT],
    arg1: ObjectType,  # type
    arg2: int,  # limit
) -> None: ...


@overload
def option(option_type: Literal[Option.GET_CACHED_MEMORY]) -> tuple[int, int]: ...


@overload
def option(
    option_type: Literal[Option.SET_SSL_CERT_LOCATIONS],
    arg1: str | bytes | None,  # cert_file
    arg2: str | bytes | None,  # cert_dir
) -> None: ...


@overload
def option(
    option_type: Literal[
        Option.ENABLE_CACHING,
        Option.ENABLE_STRICT_OBJECT_CREATION,
        Option.ENABLE_STRICT_SYMBOLIC_REF_CREATION,
        Option.ENABLE_OFS_DELTA,
        Option.ENABLE_FSYNC_GITDIR,
        Option.ENABLE_STRICT_HASH_VERIFICATION,
        Option.ENABLE_UNSAVED_INDEX_SAFETY,
        Option.DISABLE_PACK_KEEP_FILE_CHECKS,
        Option.SET_OWNER_VALIDATION,
    ],
    arg1: bool,  # value
) -> None: ...


@overload
def option(option_type: Literal[Option.GET_OWNER_VALIDATION]) -> bool: ...


@overload
def option(
    option_type: Literal[
        Option.GET_TEMPLATE_PATH,
        Option.GET_USER_AGENT,
        Option.GET_HOMEDIR,
        Option.GET_USER_AGENT_PRODUCT,
    ],
) -> str | None: ...


@overload
def option(
    option_type: Literal[
        Option.SET_TEMPLATE_PATH,
        Option.SET_USER_AGENT,
        Option.SET_SSL_CIPHERS,
        Option.SET_HOMEDIR,
        Option.SET_USER_AGENT_PRODUCT,
    ],
    arg1: str | bytes,  # value
) -> None: ...


@overload
def option(
    option_type: Literal[
        Option.GET_WINDOWS_SHAREMODE,
        Option.GET_PACK_MAX_OBJECTS,
        Option.GET_SERVER_CONNECT_TIMEOUT,
        Option.GET_SERVER_TIMEOUT,
    ],
) -> int: ...


@overload
def option(
    option_type: Literal[
        Option.SET_WINDOWS_SHAREMODE,
        Option.SET_PACK_MAX_OBJECTS,
        Option.ENABLE_HTTP_EXPECT_CONTINUE,
        Option.SET_ODB_PACKED_PRIORITY,
        Option.SET_ODB_LOOSE_PRIORITY,
        Option.SET_SERVER_CONNECT_TIMEOUT,
        Option.SET_SERVER_TIMEOUT,
    ],
    arg1: int,  # value
) -> None: ...


@overload
def option(option_type: Literal[Option.GET_EXTENSIONS]) -> list[str]: ...


@overload
def option(
    option_type: Literal[Option.SET_EXTENSIONS],
    arg1: list[str],  # extensions
    arg2: int,  # length
) -> None: ...


@overload
def option(
    option_type: Literal[Option.ADD_SSL_X509_CERT],
    arg1: str | bytes,  # certificate
) -> None: ...


# Fallback overload for generic Option values (used in tests)
@overload
def option(option_type: Option, arg1: Any = ..., arg2: Any = ...) -> Any: ...


def option(option_type: Option, arg1: Any = NOT_PASSED, arg2: Any = NOT_PASSED) -> Any:
    """
    Get or set a libgit2 option.

    Parameters:

    GIT_OPT_GET_SEARCH_PATH, level
        Get the config search path for the given level.

    GIT_OPT_SET_SEARCH_PATH, level, path
        Set the config search path for the given level.

    GIT_OPT_GET_MWINDOW_SIZE
        Get the maximum mmap window size.

    GIT_OPT_SET_MWINDOW_SIZE, size
        Set the maximum mmap window size.

    GIT_OPT_GET_MWINDOW_FILE_LIMIT
        Get the maximum number of files that will be mapped at any time by the library.

    GIT_OPT_SET_MWINDOW_FILE_LIMIT, size
        Set the maximum number of files that can be mapped at any time by the library. The default (0) is unlimited.

    GIT_OPT_GET_OWNER_VALIDATION
        Gets the owner validation setting for repository directories.

    GIT_OPT_SET_OWNER_VALIDATION, enabled
        Set that repository directories should be owned by the current user.
        The default is to validate ownership.

    GIT_OPT_GET_TEMPLATE_PATH
        Get the default template path.

    GIT_OPT_SET_TEMPLATE_PATH, path
        Set the default template path.

    GIT_OPT_GET_USER_AGENT
        Get the user agent string.

    GIT_OPT_SET_USER_AGENT, user_agent
        Set the user agent string.

    GIT_OPT_GET_PACK_MAX_OBJECTS
        Get the maximum number of objects to include in a pack.

    GIT_OPT_SET_PACK_MAX_OBJECTS, count
        Set the maximum number of objects to include in a pack.
    """

    result: str | None | list[str]

    if option_type in (
        C.GIT_OPT_GET_MWINDOW_SIZE,
        C.GIT_OPT_GET_MWINDOW_MAPPED_LIMIT,
        C.GIT_OPT_GET_MWINDOW_FILE_LIMIT,
    ):
        check_args(option_type, arg1, arg2, 0)

        size_ptr = ffi.new('size_t *')
        err = C.git_libgit2_opts(option_type, size_ptr)
        check_error(err)
        return size_ptr[0]

    elif option_type in (
        C.GIT_OPT_SET_MWINDOW_SIZE,
        C.GIT_OPT_SET_MWINDOW_MAPPED_LIMIT,
        C.GIT_OPT_SET_MWINDOW_FILE_LIMIT,
    ):
        check_args(option_type, arg1, arg2, 1)

        if not isinstance(arg1, int):
            raise TypeError(f'option value must be an integer, not {type(arg1)}')
        size = arg1
        if size < 0:
            raise ValueError('size must be non-negative')

        err = C.git_libgit2_opts(option_type, ffi.cast('size_t', size))
        check_error(err)
        return None

    elif option_type == C.GIT_OPT_GET_SEARCH_PATH:
        check_args(option_type, arg1, arg2, 1)

        level = int(arg1)  # Convert enum to int
        buf = ffi.new('git_buf *')
        err = C.git_libgit2_opts(option_type, ffi.cast('int', level), buf)
        check_error(err)

        try:
            if buf.ptr != ffi.NULL:
                result = to_str(ffi.string(buf.ptr))
            else:
                result = None
        finally:
            C.git_buf_dispose(buf)

        return result

    elif option_type == C.GIT_OPT_SET_SEARCH_PATH:
        check_args(option_type, arg1, arg2, 2)

        level = int(arg1)  # Convert enum to int
        path = arg2

        path_cdata: ArrayC[char] | NULL_TYPE
        if path is None:
            path_cdata = ffi.NULL
        else:
            path_bytes = to_bytes(path)
            path_cdata = ffi.new('char[]', path_bytes)

        err = C.git_libgit2_opts(option_type, ffi.cast('int', level), path_cdata)
        check_error(err)
        return None

    elif option_type == C.GIT_OPT_SET_CACHE_OBJECT_LIMIT:
        check_args(option_type, arg1, arg2, 2)

        object_type = int(arg1)  # Convert enum to int
        if not isinstance(arg2, int):
            raise TypeError(
                f'option value must be an integer, not {type(arg2).__name__}'
            )
        size = arg2
        if size < 0:
            raise ValueError('size must be non-negative')

        err = C.git_libgit2_opts(
            option_type, ffi.cast('int', object_type), ffi.cast('size_t', size)
        )
        check_error(err)
        return None

    elif option_type == C.GIT_OPT_SET_CACHE_MAX_SIZE:
        check_args(option_type, arg1, arg2, 1)

        size = arg1
        if not isinstance(size, int):
            raise TypeError(
                f'option value must be an integer, not {type(size).__name__}'
            )

        err = C.git_libgit2_opts(option_type, ffi.cast('ssize_t', size))
        check_error(err)
        return None

    elif option_type == C.GIT_OPT_GET_CACHED_MEMORY:
        check_args(option_type, arg1, arg2, 0)

        current_ptr = ffi.new('ssize_t *')
        allowed_ptr = ffi.new('ssize_t *')
        err = C.git_libgit2_opts(option_type, current_ptr, allowed_ptr)
        check_error(err)
        return (current_ptr[0], allowed_ptr[0])

    elif option_type == C.GIT_OPT_SET_SSL_CERT_LOCATIONS:
        check_args(option_type, arg1, arg2, 2)

        cert_file = arg1
        cert_dir = arg2

        cert_file_cdata: ArrayC[char] | NULL_TYPE
        if cert_file is None:
            cert_file_cdata = ffi.NULL
        else:
            cert_file_bytes = to_bytes(cert_file)
            cert_file_cdata = ffi.new('char[]', cert_file_bytes)

        cert_dir_cdata: ArrayC[char] | NULL_TYPE
        if cert_dir is None:
            cert_dir_cdata = ffi.NULL
        else:
            cert_dir_bytes = to_bytes(cert_dir)
            cert_dir_cdata = ffi.new('char[]', cert_dir_bytes)

        err = C.git_libgit2_opts(option_type, cert_file_cdata, cert_dir_cdata)
        check_error(err)
        return None

    # Handle boolean/int enable/disable options
    elif option_type in (
        C.GIT_OPT_ENABLE_CACHING,
        C.GIT_OPT_ENABLE_STRICT_OBJECT_CREATION,
        C.GIT_OPT_ENABLE_STRICT_SYMBOLIC_REF_CREATION,
        C.GIT_OPT_ENABLE_OFS_DELTA,
        C.GIT_OPT_ENABLE_FSYNC_GITDIR,
        C.GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION,
        C.GIT_OPT_ENABLE_UNSAVED_INDEX_SAFETY,
        C.GIT_OPT_DISABLE_PACK_KEEP_FILE_CHECKS,
        C.GIT_OPT_SET_OWNER_VALIDATION,
    ):
        check_args(option_type, arg1, arg2, 1)

        enabled = arg1
        # Convert to int (0 or 1)
        value = 1 if enabled else 0

        err = C.git_libgit2_opts(option_type, ffi.cast('int', value))
        check_error(err)
        return None

    elif option_type == C.GIT_OPT_GET_OWNER_VALIDATION:
        check_args(option_type, arg1, arg2, 0)

        enabled_ptr = ffi.new('int *')
        err = C.git_libgit2_opts(option_type, enabled_ptr)
        check_error(err)
        return bool(enabled_ptr[0])

    elif option_type == C.GIT_OPT_GET_TEMPLATE_PATH:
        check_args(option_type, arg1, arg2, 0)

        buf = ffi.new('git_buf *')
        err = C.git_libgit2_opts(option_type, buf)
        check_error(err)

        try:
            if buf.ptr != ffi.NULL:
                result = to_str(ffi.string(buf.ptr))
            else:
                result = None
        finally:
            C.git_buf_dispose(buf)

        return result

    elif option_type == C.GIT_OPT_SET_TEMPLATE_PATH:
        check_args(option_type, arg1, arg2, 1)

        path = arg1
        template_path_cdata: ArrayC[char] | NULL_TYPE
        if path is None:
            template_path_cdata = ffi.NULL
        else:
            path_bytes = to_bytes(path)
            template_path_cdata = ffi.new('char[]', path_bytes)

        err = C.git_libgit2_opts(option_type, template_path_cdata)
        check_error(err)
        return None

    elif option_type == C.GIT_OPT_GET_USER_AGENT:
        check_args(option_type, arg1, arg2, 0)

        buf = ffi.new('git_buf *')
        err = C.git_libgit2_opts(option_type, buf)
        check_error(err)

        try:
            if buf.ptr != ffi.NULL:
                result = to_str(ffi.string(buf.ptr))
            else:
                result = None
        finally:
            C.git_buf_dispose(buf)

        return result

    elif option_type == C.GIT_OPT_SET_USER_AGENT:
        check_args(option_type, arg1, arg2, 1)

        agent = arg1
        agent_bytes = to_bytes(agent)
        agent_cdata = ffi.new('char[]', agent_bytes)

        err = C.git_libgit2_opts(option_type, agent_cdata)
        check_error(err)
        return None

    elif option_type == C.GIT_OPT_SET_SSL_CIPHERS:
        check_args(option_type, arg1, arg2, 1)

        ciphers = arg1
        ciphers_bytes = to_bytes(ciphers)
        ciphers_cdata = ffi.new('char[]', ciphers_bytes)

        err = C.git_libgit2_opts(option_type, ciphers_cdata)
        check_error(err)
        return None

    # Handle GET_WINDOWS_SHAREMODE
    elif option_type == C.GIT_OPT_GET_WINDOWS_SHAREMODE:
        check_args(option_type, arg1, arg2, 0)

        value_ptr = ffi.new('unsigned int *')
        err = C.git_libgit2_opts(option_type, value_ptr)
        check_error(err)
        return value_ptr[0]

    # Handle SET_WINDOWS_SHAREMODE
    elif option_type == C.GIT_OPT_SET_WINDOWS_SHAREMODE:
        check_args(option_type, arg1, arg2, 1)

        if not isinstance(arg1, int):
            raise TypeError(
                f'option value must be an integer, not {type(arg1).__name__}'
            )
        value = arg1
        if value < 0:
            raise ValueError('value must be non-negative')

        err = C.git_libgit2_opts(option_type, ffi.cast('unsigned int', value))
        check_error(err)
        return None

    # Handle GET_PACK_MAX_OBJECTS
    elif option_type == C.GIT_OPT_GET_PACK_MAX_OBJECTS:
        check_args(option_type, arg1, arg2, 0)

        size_ptr = ffi.new('size_t *')
        err = C.git_libgit2_opts(option_type, size_ptr)
        check_error(err)
        return size_ptr[0]

    # Handle SET_PACK_MAX_OBJECTS
    elif option_type == C.GIT_OPT_SET_PACK_MAX_OBJECTS:
        check_args(option_type, arg1, arg2, 1)

        if not isinstance(arg1, int):
            raise TypeError(
                f'option value must be an integer, not {type(arg1).__name__}'
            )
        size = arg1
        if size < 0:
            raise ValueError('size must be non-negative')

        err = C.git_libgit2_opts(option_type, ffi.cast('size_t', size))
        check_error(err)
        return None

    # Handle ENABLE_HTTP_EXPECT_CONTINUE
    elif option_type == C.GIT_OPT_ENABLE_HTTP_EXPECT_CONTINUE:
        check_args(option_type, arg1, arg2, 1)

        enabled = arg1
        # Convert to int (0 or 1)
        value = 1 if enabled else 0

        err = C.git_libgit2_opts(option_type, ffi.cast('int', value))
        check_error(err)
        return None

    # Handle SET_ODB_PACKED_PRIORITY
    elif option_type == C.GIT_OPT_SET_ODB_PACKED_PRIORITY:
        check_args(option_type, arg1, arg2, 1)

        if not isinstance(arg1, int):
            raise TypeError(
                f'option value must be an integer, not {type(arg1).__name__}'
            )
        priority = arg1

        err = C.git_libgit2_opts(option_type, ffi.cast('int', priority))
        check_error(err)
        return None

    # Handle SET_ODB_LOOSE_PRIORITY
    elif option_type == C.GIT_OPT_SET_ODB_LOOSE_PRIORITY:
        check_args(option_type, arg1, arg2, 1)

        if not isinstance(arg1, int):
            raise TypeError(
                f'option value must be an integer, not {type(arg1).__name__}'
            )
        priority = arg1

        err = C.git_libgit2_opts(option_type, ffi.cast('int', priority))
        check_error(err)
        return None

    # Handle GET_EXTENSIONS
    elif option_type == C.GIT_OPT_GET_EXTENSIONS:
        check_args(option_type, arg1, arg2, 0)

        # GET_EXTENSIONS expects a git_strarray pointer
        strarray = ffi.new('git_strarray *')
        err = C.git_libgit2_opts(option_type, strarray)
        check_error(err)

        result = []
        try:
            if strarray.strings != ffi.NULL:
                # Cast to the non-NULL type for type checking
                strings = cast('ArrayC[char_pointer]', strarray.strings)
                for i in range(strarray.count):
                    if strings[i] != ffi.NULL:
                        result.append(to_str(ffi.string(strings[i])))
        finally:
            # Must dispose of the strarray to free the memory
            C.git_strarray_dispose(strarray)

        return result

    # Handle SET_EXTENSIONS
    elif option_type == C.GIT_OPT_SET_EXTENSIONS:
        check_args(option_type, arg1, arg2, 2)

        extensions = arg1
        length = arg2

        if not isinstance(extensions, list):
            raise TypeError('extensions must be a list of strings')
        if not isinstance(length, int):
            raise TypeError('length must be an integer')

        # Create array of char pointers
        # libgit2 will make its own copies with git__strdup
        ext_array: ArrayC[char_pointer] = ffi.new('char *[]', len(extensions))
        ext_strings: list[ArrayC[char]] = []  # Keep references during the call

        for i, ext in enumerate(extensions):
            ext_bytes = to_bytes(ext)
            ext_string: ArrayC[char] = ffi.new('char[]', ext_bytes)
            ext_strings.append(ext_string)
            ext_array[i] = ffi.cast('char *', ext_string)

        err = C.git_libgit2_opts(option_type, ext_array, ffi.cast('size_t', length))
        check_error(err)
        return None

    # Handle GET_HOMEDIR
    elif option_type == C.GIT_OPT_GET_HOMEDIR:
        check_args(option_type, arg1, arg2, 0)

        buf = ffi.new('git_buf *')
        err = C.git_libgit2_opts(option_type, buf)
        check_error(err)

        try:
            if buf.ptr != ffi.NULL:
                result = to_str(ffi.string(buf.ptr))
            else:
                result = None
        finally:
            C.git_buf_dispose(buf)

        return result

    # Handle SET_HOMEDIR
    elif option_type == C.GIT_OPT_SET_HOMEDIR:
        check_args(option_type, arg1, arg2, 1)

        path = arg1
        homedir_cdata: ArrayC[char] | NULL_TYPE
        if path is None:
            homedir_cdata = ffi.NULL
        else:
            path_bytes = to_bytes(path)
            homedir_cdata = ffi.new('char[]', path_bytes)

        err = C.git_libgit2_opts(option_type, homedir_cdata)
        check_error(err)
        return None

    # Handle GET_SERVER_CONNECT_TIMEOUT
    elif option_type == C.GIT_OPT_GET_SERVER_CONNECT_TIMEOUT:
        check_args(option_type, arg1, arg2, 0)

        timeout_ptr = ffi.new('int *')
        err = C.git_libgit2_opts(option_type, timeout_ptr)
        check_error(err)
        return timeout_ptr[0]

    # Handle SET_SERVER_CONNECT_TIMEOUT
    elif option_type == C.GIT_OPT_SET_SERVER_CONNECT_TIMEOUT:
        check_args(option_type, arg1, arg2, 1)

        if not isinstance(arg1, int):
            raise TypeError(
                f'option value must be an integer, not {type(arg1).__name__}'
            )
        timeout = arg1

        err = C.git_libgit2_opts(option_type, ffi.cast('int', timeout))
        check_error(err)
        return None

    # Handle GET_SERVER_TIMEOUT
    elif option_type == C.GIT_OPT_GET_SERVER_TIMEOUT:
        check_args(option_type, arg1, arg2, 0)

        timeout_ptr = ffi.new('int *')
        err = C.git_libgit2_opts(option_type, timeout_ptr)
        check_error(err)
        return timeout_ptr[0]

    # Handle SET_SERVER_TIMEOUT
    elif option_type == C.GIT_OPT_SET_SERVER_TIMEOUT:
        check_args(option_type, arg1, arg2, 1)

        if not isinstance(arg1, int):
            raise TypeError(
                f'option value must be an integer, not {type(arg1).__name__}'
            )
        timeout = arg1

        err = C.git_libgit2_opts(option_type, ffi.cast('int', timeout))
        check_error(err)
        return None

    # Handle GET_USER_AGENT_PRODUCT
    elif option_type == C.GIT_OPT_GET_USER_AGENT_PRODUCT:
        check_args(option_type, arg1, arg2, 0)

        buf = ffi.new('git_buf *')
        err = C.git_libgit2_opts(option_type, buf)
        check_error(err)

        try:
            if buf.ptr != ffi.NULL:
                result = to_str(ffi.string(buf.ptr))
            else:
                result = None
        finally:
            C.git_buf_dispose(buf)

        return result

    # Handle SET_USER_AGENT_PRODUCT
    elif option_type == C.GIT_OPT_SET_USER_AGENT_PRODUCT:
        check_args(option_type, arg1, arg2, 1)

        product = arg1
        product_bytes = to_bytes(product)
        product_cdata = ffi.new('char[]', product_bytes)

        err = C.git_libgit2_opts(option_type, product_cdata)
        check_error(err)
        return None

    # Not implemented - ADD_SSL_X509_CERT requires directly binding with OpenSSL
    # as the API works accepts a X509* struct.  Use GIT_OPT_SET_SSL_CERT_LOCATIONS
    # instead.
    elif option_type == C.GIT_OPT_ADD_SSL_X509_CERT:
        raise NotImplementedError('Use GIT_OPT_SET_SSL_CERT_LOCATIONS instead')

    # Not implemented - SET_ALLOCATOR is not feasible from Python level
    # because it requires providing C function pointers for memory management
    # (malloc, free, etc.) that must handle raw memory at the C level,
    # which cannot be safely implemented in pure Python.
    elif option_type == C.GIT_OPT_SET_ALLOCATOR:
        raise NotImplementedError('Setting a custom allocator not possible from Python')

    else:
        raise ValueError(f'Invalid option {option_type}')
