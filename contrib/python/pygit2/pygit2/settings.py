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
Settings mapping.
"""

from ssl import DefaultVerifyPaths, get_default_verify_paths
from typing import overload

import pygit2.enums

from .enums import ConfigLevel, Option
from .errors import GitError
from .options import option


class SearchPathList:
    def __getitem__(self, key: ConfigLevel) -> str:
        return option(Option.GET_SEARCH_PATH, key)

    def __setitem__(self, key: ConfigLevel, value: str) -> None:
        option(Option.SET_SEARCH_PATH, key, value)


class Settings:
    """Library-wide settings interface."""

    __slots__ = '_default_tls_verify_paths', '_ssl_cert_dir', '_ssl_cert_file'

    _search_path = SearchPathList()
    _default_tls_verify_paths: DefaultVerifyPaths | None
    _ssl_cert_file: str | bytes | None
    _ssl_cert_dir: str | bytes | None

    def __init__(self) -> None:
        """Initialize global pygit2 and libgit2 settings."""
        self._initialize_tls_certificate_locations()

    def _initialize_tls_certificate_locations(self) -> None:
        """Set up initial TLS file and directory lookup locations."""
        self._default_tls_verify_paths = get_default_verify_paths()
        try:
            self.set_ssl_cert_locations(
                self._default_tls_verify_paths.cafile,
                self._default_tls_verify_paths.capath,
            )
        except GitError as git_err:
            valid_msg = "TLS backend doesn't support certificate locations"
            if str(git_err) != valid_msg:
                raise
            self._default_tls_verify_paths = None
            self._ssl_cert_file = None
            self._ssl_cert_dir = None

    @property
    def search_path(self) -> SearchPathList:
        """Configuration file search path.

        This behaves like an array whose indices correspond to ConfigLevel values.
        The local search path cannot be changed.
        """
        return self._search_path

    @property
    def mwindow_size(self) -> int:
        """Get or set the maximum mmap window size"""
        return option(Option.GET_MWINDOW_SIZE)

    @mwindow_size.setter
    def mwindow_size(self, value: int) -> None:
        option(Option.SET_MWINDOW_SIZE, value)

    @property
    def mwindow_mapped_limit(self) -> int:
        """
        Get or set the maximum memory that will be mapped in total by the
        library
        """
        return option(Option.GET_MWINDOW_MAPPED_LIMIT)

    @mwindow_mapped_limit.setter
    def mwindow_mapped_limit(self, value: int) -> None:
        option(Option.SET_MWINDOW_MAPPED_LIMIT, value)

    @property
    def mwindow_file_limit(self) -> int:
        """Get or set the maximum number of files to be mapped at any time"""
        return option(Option.GET_MWINDOW_FILE_LIMIT)

    @mwindow_file_limit.setter
    def mwindow_file_limit(self, value: int) -> None:
        option(Option.SET_MWINDOW_FILE_LIMIT, value)

    @property
    def cached_memory(self) -> tuple[int, int]:
        """
        Get the current bytes in cache and the maximum that would be
        allowed in the cache.
        """
        return option(Option.GET_CACHED_MEMORY)

    def enable_caching(self, value: bool = True) -> None:
        """
        Enable or disable caching completely.

        Because caches are repository-specific, disabling the cache
        cannot immediately clear all cached objects, but each cache will
        be cleared on the next attempt to update anything in it.
        """
        return option(Option.ENABLE_CACHING, value)

    def disable_pack_keep_file_checks(self, value: bool = True) -> None:
        """
        This will cause .keep file existence checks to be skipped when
        accessing packfiles, which can help performance with remote
        filesystems.
        """
        return option(Option.DISABLE_PACK_KEEP_FILE_CHECKS, value)

    def cache_max_size(self, value: int) -> None:
        """
        Set the maximum total data size that will be cached in memory
        across all repositories before libgit2 starts evicting objects
        from the cache.  This is a soft limit, in that the library might
        briefly exceed it, but will start aggressively evicting objects
        from cache when that happens.  The default cache size is 256MB.
        """
        return option(Option.SET_CACHE_MAX_SIZE, value)

    def cache_object_limit(
        self, object_type: pygit2.enums.ObjectType, value: int
    ) -> None:
        """
        Set the maximum data size for the given type of object to be
        considered eligible for caching in memory.  Setting to value to
        zero means that that type of object will not be cached.
        Defaults to 0 for enums.ObjectType.BLOB (i.e. won't cache blobs)
        and 4k for COMMIT, TREE, and TAG.
        """
        return option(Option.SET_CACHE_OBJECT_LIMIT, object_type, value)

    @property
    def ssl_cert_file(self) -> str | bytes | None:
        """TLS certificate file path."""
        return self._ssl_cert_file

    @ssl_cert_file.setter
    def ssl_cert_file(self, value: str | bytes) -> None:
        """Set the TLS cert file path."""
        self.set_ssl_cert_locations(value, self._ssl_cert_dir)

    @ssl_cert_file.deleter
    def ssl_cert_file(self) -> None:
        """Reset the TLS cert file path."""
        self.ssl_cert_file = self._default_tls_verify_paths.cafile  # type: ignore[union-attr]

    @property
    def ssl_cert_dir(self) -> str | bytes | None:
        """TLS certificates lookup directory path."""
        return self._ssl_cert_dir

    @ssl_cert_dir.setter
    def ssl_cert_dir(self, value: str | bytes) -> None:
        """Set the TLS certificate lookup folder."""
        self.set_ssl_cert_locations(self._ssl_cert_file, value)

    @ssl_cert_dir.deleter
    def ssl_cert_dir(self) -> None:
        """Reset the TLS certificate lookup folder."""
        self.ssl_cert_dir = self._default_tls_verify_paths.capath  # type: ignore[union-attr]

    @overload
    def set_ssl_cert_locations(
        self, cert_file: str | bytes | None, cert_dir: str | bytes
    ) -> None: ...
    @overload
    def set_ssl_cert_locations(
        self, cert_file: str | bytes, cert_dir: str | bytes | None
    ) -> None: ...
    def set_ssl_cert_locations(
        self, cert_file: str | bytes | None, cert_dir: str | bytes | None
    ) -> None:
        """
        Set the SSL certificate-authority locations.

        - `cert_file` is the location of a file containing several
          certificates concatenated together.
        - `cert_dir` is the location of a directory holding several
          certificates, one per file.

        Either parameter may be `NULL`, but not both.
        """
        if cert_file and cert_dir:
            option(Option.SET_SSL_CERT_LOCATIONS, cert_file, cert_dir)
        self._ssl_cert_file = cert_file
        self._ssl_cert_dir = cert_dir

    @property
    def template_path(self) -> str | None:
        """Get or set the default template path for new repositories"""
        return option(Option.GET_TEMPLATE_PATH)

    @template_path.setter
    def template_path(self, value: str | bytes) -> None:
        option(Option.SET_TEMPLATE_PATH, value)

    @property
    def user_agent(self) -> str | None:
        """Get or set the user agent string for network operations"""
        return option(Option.GET_USER_AGENT)

    @user_agent.setter
    def user_agent(self, value: str | bytes) -> None:
        option(Option.SET_USER_AGENT, value)

    @property
    def user_agent_product(self) -> str | None:
        """Get or set the user agent product name"""
        return option(Option.GET_USER_AGENT_PRODUCT)

    @user_agent_product.setter
    def user_agent_product(self, value: str | bytes) -> None:
        option(Option.SET_USER_AGENT_PRODUCT, value)

    def set_ssl_ciphers(self, ciphers: str | bytes) -> None:
        """Set the SSL ciphers to use for HTTPS connections"""
        option(Option.SET_SSL_CIPHERS, ciphers)

    def enable_strict_object_creation(self, value: bool = True) -> None:
        """Enable or disable strict object creation validation"""
        option(Option.ENABLE_STRICT_OBJECT_CREATION, value)

    def enable_strict_symbolic_ref_creation(self, value: bool = True) -> None:
        """Enable or disable strict symbolic reference creation validation"""
        option(Option.ENABLE_STRICT_SYMBOLIC_REF_CREATION, value)

    def enable_ofs_delta(self, value: bool = True) -> None:
        """Enable or disable offset delta encoding"""
        option(Option.ENABLE_OFS_DELTA, value)

    def enable_fsync_gitdir(self, value: bool = True) -> None:
        """Enable or disable fsync for git directory operations"""
        option(Option.ENABLE_FSYNC_GITDIR, value)

    def enable_strict_hash_verification(self, value: bool = True) -> None:
        """Enable or disable strict hash verification"""
        option(Option.ENABLE_STRICT_HASH_VERIFICATION, value)

    def enable_unsaved_index_safety(self, value: bool = True) -> None:
        """Enable or disable unsaved index safety checks"""
        option(Option.ENABLE_UNSAVED_INDEX_SAFETY, value)

    def enable_http_expect_continue(self, value: bool = True) -> None:
        """Enable or disable HTTP Expect/Continue for large pushes"""
        option(Option.ENABLE_HTTP_EXPECT_CONTINUE, value)

    @property
    def windows_sharemode(self) -> int:
        """Get or set the Windows share mode for opening files"""
        return option(Option.GET_WINDOWS_SHAREMODE)

    @windows_sharemode.setter
    def windows_sharemode(self, value: int) -> None:
        option(Option.SET_WINDOWS_SHAREMODE, value)

    @property
    def pack_max_objects(self) -> int:
        """Get or set the maximum number of objects in a pack"""
        return option(Option.GET_PACK_MAX_OBJECTS)

    @pack_max_objects.setter
    def pack_max_objects(self, value: int) -> None:
        option(Option.SET_PACK_MAX_OBJECTS, value)

    @property
    def owner_validation(self) -> bool:
        """Get or set repository directory ownership validation"""
        return option(Option.GET_OWNER_VALIDATION)

    @owner_validation.setter
    def owner_validation(self, value: bool) -> None:
        option(Option.SET_OWNER_VALIDATION, value)

    def set_odb_packed_priority(self, priority: int) -> None:
        """Set the priority for packed ODB backend (default 1)"""
        option(Option.SET_ODB_PACKED_PRIORITY, priority)

    def set_odb_loose_priority(self, priority: int) -> None:
        """Set the priority for loose ODB backend (default 2)"""
        option(Option.SET_ODB_LOOSE_PRIORITY, priority)

    @property
    def extensions(self) -> list[str]:
        """Get the list of enabled extensions"""
        return option(Option.GET_EXTENSIONS)

    def set_extensions(self, extensions: list[str]) -> None:
        """Set the list of enabled extensions"""
        option(Option.SET_EXTENSIONS, extensions, len(extensions))

    @property
    def homedir(self) -> str | None:
        """Get or set the home directory"""
        return option(Option.GET_HOMEDIR)

    @homedir.setter
    def homedir(self, value: str | bytes) -> None:
        option(Option.SET_HOMEDIR, value)

    @property
    def server_connect_timeout(self) -> int:
        """Get or set the server connection timeout in milliseconds"""
        return option(Option.GET_SERVER_CONNECT_TIMEOUT)

    @server_connect_timeout.setter
    def server_connect_timeout(self, value: int) -> None:
        option(Option.SET_SERVER_CONNECT_TIMEOUT, value)

    @property
    def server_timeout(self) -> int:
        """Get or set the server timeout in milliseconds"""
        return option(Option.GET_SERVER_TIMEOUT)

    @server_timeout.setter
    def server_timeout(self, value: int) -> None:
        option(Option.SET_SERVER_TIMEOUT, value)
