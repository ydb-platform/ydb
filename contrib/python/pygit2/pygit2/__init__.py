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

# ruff: noqa: F401 F403 F405

# Standard Library
import functools
import os
import typing

# High level API
from . import enums
from ._build import __version__

# Low level API
from ._pygit2 import (
    GIT_APPLY_LOCATION_BOTH,
    GIT_APPLY_LOCATION_INDEX,
    GIT_APPLY_LOCATION_WORKDIR,
    GIT_BLAME_FIRST_PARENT,
    GIT_BLAME_IGNORE_WHITESPACE,
    GIT_BLAME_NORMAL,
    GIT_BLAME_TRACK_COPIES_ANY_COMMIT_COPIES,
    GIT_BLAME_TRACK_COPIES_SAME_COMMIT_COPIES,
    GIT_BLAME_TRACK_COPIES_SAME_COMMIT_MOVES,
    GIT_BLAME_TRACK_COPIES_SAME_FILE,
    GIT_BLAME_USE_MAILMAP,
    GIT_BLOB_FILTER_ATTRIBUTES_FROM_COMMIT,
    GIT_BLOB_FILTER_ATTRIBUTES_FROM_HEAD,
    GIT_BLOB_FILTER_CHECK_FOR_BINARY,
    GIT_BLOB_FILTER_NO_SYSTEM_ATTRIBUTES,
    GIT_BRANCH_ALL,
    GIT_BRANCH_LOCAL,
    GIT_BRANCH_REMOTE,
    GIT_CHECKOUT_ALLOW_CONFLICTS,
    GIT_CHECKOUT_CONFLICT_STYLE_DIFF3,
    GIT_CHECKOUT_CONFLICT_STYLE_MERGE,
    GIT_CHECKOUT_CONFLICT_STYLE_ZDIFF3,
    GIT_CHECKOUT_DISABLE_PATHSPEC_MATCH,
    GIT_CHECKOUT_DONT_OVERWRITE_IGNORED,
    GIT_CHECKOUT_DONT_REMOVE_EXISTING,
    GIT_CHECKOUT_DONT_UPDATE_INDEX,
    GIT_CHECKOUT_DONT_WRITE_INDEX,
    GIT_CHECKOUT_DRY_RUN,
    GIT_CHECKOUT_FORCE,
    GIT_CHECKOUT_NO_REFRESH,
    GIT_CHECKOUT_NONE,
    GIT_CHECKOUT_RECREATE_MISSING,
    GIT_CHECKOUT_REMOVE_IGNORED,
    GIT_CHECKOUT_REMOVE_UNTRACKED,
    GIT_CHECKOUT_SAFE,
    GIT_CHECKOUT_SKIP_LOCKED_DIRECTORIES,
    GIT_CHECKOUT_SKIP_UNMERGED,
    GIT_CHECKOUT_UPDATE_ONLY,
    GIT_CHECKOUT_USE_OURS,
    GIT_CHECKOUT_USE_THEIRS,
    GIT_CONFIG_HIGHEST_LEVEL,
    GIT_CONFIG_LEVEL_APP,
    GIT_CONFIG_LEVEL_GLOBAL,
    GIT_CONFIG_LEVEL_LOCAL,
    GIT_CONFIG_LEVEL_PROGRAMDATA,
    GIT_CONFIG_LEVEL_SYSTEM,
    GIT_CONFIG_LEVEL_WORKTREE,
    GIT_CONFIG_LEVEL_XDG,
    GIT_DELTA_ADDED,
    GIT_DELTA_CONFLICTED,
    GIT_DELTA_COPIED,
    GIT_DELTA_DELETED,
    GIT_DELTA_IGNORED,
    GIT_DELTA_MODIFIED,
    GIT_DELTA_RENAMED,
    GIT_DELTA_TYPECHANGE,
    GIT_DELTA_UNMODIFIED,
    GIT_DELTA_UNREADABLE,
    GIT_DELTA_UNTRACKED,
    GIT_DESCRIBE_ALL,
    GIT_DESCRIBE_DEFAULT,
    GIT_DESCRIBE_TAGS,
    GIT_DIFF_BREAK_REWRITES,
    GIT_DIFF_BREAK_REWRITES_FOR_RENAMES_ONLY,
    GIT_DIFF_DISABLE_PATHSPEC_MATCH,
    GIT_DIFF_ENABLE_FAST_UNTRACKED_DIRS,
    GIT_DIFF_FIND_ALL,
    GIT_DIFF_FIND_AND_BREAK_REWRITES,
    GIT_DIFF_FIND_BY_CONFIG,
    GIT_DIFF_FIND_COPIES,
    GIT_DIFF_FIND_COPIES_FROM_UNMODIFIED,
    GIT_DIFF_FIND_DONT_IGNORE_WHITESPACE,
    GIT_DIFF_FIND_EXACT_MATCH_ONLY,
    GIT_DIFF_FIND_FOR_UNTRACKED,
    GIT_DIFF_FIND_IGNORE_LEADING_WHITESPACE,
    GIT_DIFF_FIND_IGNORE_WHITESPACE,
    GIT_DIFF_FIND_REMOVE_UNMODIFIED,
    GIT_DIFF_FIND_RENAMES,
    GIT_DIFF_FIND_RENAMES_FROM_REWRITES,
    GIT_DIFF_FIND_REWRITES,
    GIT_DIFF_FLAG_BINARY,
    GIT_DIFF_FLAG_EXISTS,
    GIT_DIFF_FLAG_NOT_BINARY,
    GIT_DIFF_FLAG_VALID_ID,
    GIT_DIFF_FLAG_VALID_SIZE,
    GIT_DIFF_FORCE_BINARY,
    GIT_DIFF_FORCE_TEXT,
    GIT_DIFF_IGNORE_BLANK_LINES,
    GIT_DIFF_IGNORE_CASE,
    GIT_DIFF_IGNORE_FILEMODE,
    GIT_DIFF_IGNORE_SUBMODULES,
    GIT_DIFF_IGNORE_WHITESPACE,
    GIT_DIFF_IGNORE_WHITESPACE_CHANGE,
    GIT_DIFF_IGNORE_WHITESPACE_EOL,
    GIT_DIFF_INCLUDE_CASECHANGE,
    GIT_DIFF_INCLUDE_IGNORED,
    GIT_DIFF_INCLUDE_TYPECHANGE,
    GIT_DIFF_INCLUDE_TYPECHANGE_TREES,
    GIT_DIFF_INCLUDE_UNMODIFIED,
    GIT_DIFF_INCLUDE_UNREADABLE,
    GIT_DIFF_INCLUDE_UNREADABLE_AS_UNTRACKED,
    GIT_DIFF_INCLUDE_UNTRACKED,
    GIT_DIFF_INDENT_HEURISTIC,
    GIT_DIFF_MINIMAL,
    GIT_DIFF_NORMAL,
    GIT_DIFF_PATIENCE,
    GIT_DIFF_RECURSE_IGNORED_DIRS,
    GIT_DIFF_RECURSE_UNTRACKED_DIRS,
    GIT_DIFF_REVERSE,
    GIT_DIFF_SHOW_BINARY,
    GIT_DIFF_SHOW_UNMODIFIED,
    GIT_DIFF_SHOW_UNTRACKED_CONTENT,
    GIT_DIFF_SKIP_BINARY_CHECK,
    GIT_DIFF_STATS_FULL,
    GIT_DIFF_STATS_INCLUDE_SUMMARY,
    GIT_DIFF_STATS_NONE,
    GIT_DIFF_STATS_NUMBER,
    GIT_DIFF_STATS_SHORT,
    GIT_DIFF_UPDATE_INDEX,
    GIT_FILEMODE_BLOB,
    GIT_FILEMODE_BLOB_EXECUTABLE,
    GIT_FILEMODE_COMMIT,
    GIT_FILEMODE_LINK,
    GIT_FILEMODE_TREE,
    GIT_FILEMODE_UNREADABLE,
    GIT_FILTER_ALLOW_UNSAFE,
    GIT_FILTER_ATTRIBUTES_FROM_COMMIT,
    GIT_FILTER_ATTRIBUTES_FROM_HEAD,
    GIT_FILTER_CLEAN,
    GIT_FILTER_DEFAULT,
    GIT_FILTER_DRIVER_PRIORITY,
    GIT_FILTER_NO_SYSTEM_ATTRIBUTES,
    GIT_FILTER_SMUDGE,
    GIT_FILTER_TO_ODB,
    GIT_FILTER_TO_WORKTREE,
    GIT_MERGE_ANALYSIS_FASTFORWARD,
    GIT_MERGE_ANALYSIS_NONE,
    GIT_MERGE_ANALYSIS_NORMAL,
    GIT_MERGE_ANALYSIS_UNBORN,
    GIT_MERGE_ANALYSIS_UP_TO_DATE,
    GIT_MERGE_PREFERENCE_FASTFORWARD_ONLY,
    GIT_MERGE_PREFERENCE_NO_FASTFORWARD,
    GIT_MERGE_PREFERENCE_NONE,
    GIT_OBJECT_ANY,
    GIT_OBJECT_BLOB,
    GIT_OBJECT_COMMIT,
    GIT_OBJECT_INVALID,
    GIT_OBJECT_OFS_DELTA,
    GIT_OBJECT_REF_DELTA,
    GIT_OBJECT_TAG,
    GIT_OBJECT_TREE,
    GIT_OID_HEX_ZERO,
    GIT_OID_HEXSZ,
    GIT_OID_MINPREFIXLEN,
    GIT_OID_RAWSZ,
    GIT_REFERENCES_ALL,
    GIT_REFERENCES_BRANCHES,
    GIT_REFERENCES_TAGS,
    GIT_RESET_HARD,
    GIT_RESET_MIXED,
    GIT_RESET_SOFT,
    GIT_REVSPEC_MERGE_BASE,
    GIT_REVSPEC_RANGE,
    GIT_REVSPEC_SINGLE,
    GIT_SORT_NONE,
    GIT_SORT_REVERSE,
    GIT_SORT_TIME,
    GIT_SORT_TOPOLOGICAL,
    GIT_STASH_APPLY_DEFAULT,
    GIT_STASH_APPLY_REINSTATE_INDEX,
    GIT_STASH_DEFAULT,
    GIT_STASH_INCLUDE_IGNORED,
    GIT_STASH_INCLUDE_UNTRACKED,
    GIT_STASH_KEEP_ALL,
    GIT_STASH_KEEP_INDEX,
    GIT_STATUS_CONFLICTED,
    GIT_STATUS_CURRENT,
    GIT_STATUS_IGNORED,
    GIT_STATUS_INDEX_DELETED,
    GIT_STATUS_INDEX_MODIFIED,
    GIT_STATUS_INDEX_NEW,
    GIT_STATUS_INDEX_RENAMED,
    GIT_STATUS_INDEX_TYPECHANGE,
    GIT_STATUS_WT_DELETED,
    GIT_STATUS_WT_MODIFIED,
    GIT_STATUS_WT_NEW,
    GIT_STATUS_WT_RENAMED,
    GIT_STATUS_WT_TYPECHANGE,
    GIT_STATUS_WT_UNREADABLE,
    GIT_SUBMODULE_IGNORE_ALL,
    GIT_SUBMODULE_IGNORE_DIRTY,
    GIT_SUBMODULE_IGNORE_NONE,
    GIT_SUBMODULE_IGNORE_UNSPECIFIED,
    GIT_SUBMODULE_IGNORE_UNTRACKED,
    GIT_SUBMODULE_STATUS_IN_CONFIG,
    GIT_SUBMODULE_STATUS_IN_HEAD,
    GIT_SUBMODULE_STATUS_IN_INDEX,
    GIT_SUBMODULE_STATUS_IN_WD,
    GIT_SUBMODULE_STATUS_INDEX_ADDED,
    GIT_SUBMODULE_STATUS_INDEX_DELETED,
    GIT_SUBMODULE_STATUS_INDEX_MODIFIED,
    GIT_SUBMODULE_STATUS_WD_ADDED,
    GIT_SUBMODULE_STATUS_WD_DELETED,
    GIT_SUBMODULE_STATUS_WD_INDEX_MODIFIED,
    GIT_SUBMODULE_STATUS_WD_MODIFIED,
    GIT_SUBMODULE_STATUS_WD_UNINITIALIZED,
    GIT_SUBMODULE_STATUS_WD_UNTRACKED,
    GIT_SUBMODULE_STATUS_WD_WD_MODIFIED,
    LIBGIT2_VER_MAJOR,
    LIBGIT2_VER_MINOR,
    LIBGIT2_VER_REVISION,
    LIBGIT2_VERSION,
    AlreadyExistsError,
    Blob,
    Branch,
    Commit,
    Diff,
    DiffDelta,
    DiffFile,
    DiffHunk,
    DiffLine,
    DiffStats,
    FilterSource,
    GitError,
    InvalidSpecError,
    Mailmap,
    Note,
    Object,
    Odb,
    OdbBackend,
    OdbBackendLoose,
    OdbBackendPack,
    Oid,
    Patch,
    Refdb,
    RefdbBackend,
    RefdbFsBackend,
    Reference,
    RefLogEntry,
    RevSpec,
    Signature,
    Stash,
    Tag,
    Tree,
    TreeBuilder,
    Walker,
    Worktree,
    _cache_enums,
    discover_repository,
    filter_register,
    filter_unregister,
    hash,
    hashfile,
    init_file_backend,
    reference_is_valid_name,
    tree_entry_cmp,
)
from .blame import Blame, BlameHunk
from .blob import BlobIO
from .callbacks import (
    CheckoutCallbacks,
    Payload,
    RemoteCallbacks,
    StashApplyCallbacks,
    get_credentials,
    git_clone_options,
    git_fetch_options,
    git_proxy_options,
)
from .config import Config
from .credentials import *
from .errors import Passthrough, check_error
from .ffi import C, ffi
from .filter import Filter
from .index import Index, IndexEntry
from .legacyenums import *
from .options import (
    GIT_OPT_ADD_SSL_X509_CERT,
    GIT_OPT_DISABLE_PACK_KEEP_FILE_CHECKS,
    GIT_OPT_ENABLE_CACHING,
    GIT_OPT_ENABLE_FSYNC_GITDIR,
    GIT_OPT_ENABLE_HTTP_EXPECT_CONTINUE,
    GIT_OPT_ENABLE_OFS_DELTA,
    GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION,
    GIT_OPT_ENABLE_STRICT_OBJECT_CREATION,
    GIT_OPT_ENABLE_STRICT_SYMBOLIC_REF_CREATION,
    GIT_OPT_ENABLE_UNSAVED_INDEX_SAFETY,
    GIT_OPT_GET_CACHED_MEMORY,
    GIT_OPT_GET_EXTENSIONS,
    GIT_OPT_GET_HOMEDIR,
    GIT_OPT_GET_MWINDOW_FILE_LIMIT,
    GIT_OPT_GET_MWINDOW_MAPPED_LIMIT,
    GIT_OPT_GET_MWINDOW_SIZE,
    GIT_OPT_GET_OWNER_VALIDATION,
    GIT_OPT_GET_PACK_MAX_OBJECTS,
    GIT_OPT_GET_SEARCH_PATH,
    GIT_OPT_GET_SERVER_CONNECT_TIMEOUT,
    GIT_OPT_GET_SERVER_TIMEOUT,
    GIT_OPT_GET_TEMPLATE_PATH,
    GIT_OPT_GET_USER_AGENT,
    GIT_OPT_GET_USER_AGENT_PRODUCT,
    GIT_OPT_GET_WINDOWS_SHAREMODE,
    GIT_OPT_SET_ALLOCATOR,
    GIT_OPT_SET_CACHE_MAX_SIZE,
    GIT_OPT_SET_CACHE_OBJECT_LIMIT,
    GIT_OPT_SET_EXTENSIONS,
    GIT_OPT_SET_HOMEDIR,
    GIT_OPT_SET_MWINDOW_FILE_LIMIT,
    GIT_OPT_SET_MWINDOW_MAPPED_LIMIT,
    GIT_OPT_SET_MWINDOW_SIZE,
    GIT_OPT_SET_ODB_LOOSE_PRIORITY,
    GIT_OPT_SET_ODB_PACKED_PRIORITY,
    GIT_OPT_SET_OWNER_VALIDATION,
    GIT_OPT_SET_PACK_MAX_OBJECTS,
    GIT_OPT_SET_SEARCH_PATH,
    GIT_OPT_SET_SERVER_CONNECT_TIMEOUT,
    GIT_OPT_SET_SERVER_TIMEOUT,
    GIT_OPT_SET_SSL_CERT_LOCATIONS,
    GIT_OPT_SET_SSL_CIPHERS,
    GIT_OPT_SET_TEMPLATE_PATH,
    GIT_OPT_SET_USER_AGENT,
    GIT_OPT_SET_USER_AGENT_PRODUCT,
    GIT_OPT_SET_WINDOWS_SHAREMODE,
    option,
)
from .packbuilder import PackBuilder
from .remotes import Remote
from .repository import Repository
from .settings import Settings
from .submodules import Submodule
from .transaction import ReferenceTransaction
from .utils import to_bytes, to_str

# Features
features = enums.Feature(C.git_libgit2_features())

# libgit version tuple
LIBGIT2_VER = (LIBGIT2_VER_MAJOR, LIBGIT2_VER_MINOR, LIBGIT2_VER_REVISION)

# Let _pygit2 cache references to Python enum types.
# This is separate from PyInit__pygit2() to avoid a circular import.
_cache_enums()


def init_repository(
    path: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None,
    bare: bool = False,
    flags: enums.RepositoryInitFlag = enums.RepositoryInitFlag.MKPATH,
    mode: int | enums.RepositoryInitMode = enums.RepositoryInitMode.SHARED_UMASK,
    workdir_path: typing.Optional[str] = None,
    description: typing.Optional[str] = None,
    template_path: typing.Optional[str] = None,
    initial_head: typing.Optional[str] = None,
    origin_url: typing.Optional[str] = None,
) -> Repository:
    """
    Creates a new Git repository in the given *path*.

    If *bare* is True the repository will be bare, i.e. it will not have a
    working copy.

    The *flags* may be a combination of enums.RepositoryInitFlag constants:

    - BARE (overridden by the *bare* parameter)
    - NO_REINIT
    - NO_DOTGIT_DIR
    - MKDIR
    - MKPATH (set by default)
    - EXTERNAL_TEMPLATE

    The *mode* parameter may be any of the predefined modes in
    enums.RepositoryInitMode (SHARED_UMASK being the default), or a custom int.

    The *workdir_path*, *description*, *template_path*, *initial_head* and
    *origin_url* are all strings.

    If a repository already exists at *path*, it may be opened successfully but
    you must not rely on that behavior and should use the Repository
    constructor directly instead.

    See libgit2's documentation on git_repository_init_ext for further details.
    """
    # Pre-process input parameters
    if path is None:
        raise TypeError('Expected string type for path, found None.')

    if bare:
        flags |= enums.RepositoryInitFlag.BARE

    # Options
    options = ffi.new('git_repository_init_options *')
    C.git_repository_init_options_init(options, C.GIT_REPOSITORY_INIT_OPTIONS_VERSION)
    options.flags = int(flags)
    options.mode = mode

    if workdir_path:
        workdir_path_ref = ffi.new('char []', to_bytes(workdir_path))
        options.workdir_path = workdir_path_ref

    if description:
        description_ref = ffi.new('char []', to_bytes(description))
        options.description = description_ref

    if template_path:
        template_path_ref = ffi.new('char []', to_bytes(template_path))
        options.template_path = template_path_ref

    if initial_head:
        initial_head_ref = ffi.new('char []', to_bytes(initial_head))
        options.initial_head = initial_head_ref

    if origin_url:
        origin_url_ref = ffi.new('char []', to_bytes(origin_url))
        options.origin_url = origin_url_ref

    # Call
    crepository = ffi.new('git_repository **')
    err = C.git_repository_init_ext(crepository, to_bytes(path), options)
    check_error(err)

    # Ok
    return Repository(to_str(path))


def clone_repository(
    url: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    bare: bool = False,
    repository: typing.Callable | None = None,
    remote: typing.Callable | None = None,
    checkout_branch: str | bytes | None = None,
    callbacks: RemoteCallbacks | None = None,
    depth: int = 0,
    proxy: None | bool | str = None,
) -> Repository:
    """
    Clones a new Git repository from *url* in the given *path*.

    Returns: a Repository class pointing to the newly cloned repository.

    Parameters:

    url : str or bytes or pathlike object
        URL of the repository to clone.
    path : str or bytes or pathlike object
        Local path to clone into.
    bare : bool
        Whether the local repository should be bare.
    remote : callable
        Callback for the remote to use.

        The remote callback has `(Repository, name, url) -> Remote` as a
        signature. The Remote it returns will be used instead of the default
        one.
    repository : callable
        Callback for the repository to use.

        The repository callback has `(path, bare) -> Repository` as a
        signature. The Repository it returns will be used instead of creating a
        new one.
    checkout_branch : str or bytes
        Branch to checkout after the clone. The default is to use the remote's
        default branch.
    callbacks : RemoteCallbacks
        Object which implements the callbacks as methods.

        The callbacks should be an object which inherits from
        `pyclass:RemoteCallbacks`.
    depth : int
        Number of commits to clone.

        If greater than 0, creates a shallow clone with a history truncated to
        the specified number of commits.
        The default is 0 (full commit history).
    proxy : None or True or str
        Proxy configuration. Can be one of:

        * `None` (the default) to disable proxy usage
        * `True` to enable automatic proxy detection
        * an url to a proxy (`http://proxy.example.org:3128/`)
    """

    if callbacks is None:
        callbacks = RemoteCallbacks()

    # Add repository and remote to the payload
    payload = callbacks
    payload.repository = repository
    payload.remote = remote

    with git_clone_options(payload):
        opts = payload.clone_options
        opts.bare = bare
        opts.fetch_opts.depth = depth

        if checkout_branch:
            checkout_branch_ref = ffi.new('char []', to_bytes(checkout_branch))
            opts.checkout_branch = checkout_branch_ref

        with git_fetch_options(payload, opts=opts.fetch_opts):
            with git_proxy_options(payload, opts.fetch_opts.proxy_opts, proxy):
                crepo = ffi.new('git_repository **')
                err = C.git_clone(crepo, to_bytes(url), to_bytes(path), opts)
                payload.check_error(err)

    # Ok
    return Repository._from_c(crepo[0], owned=True)


tree_entry_key = functools.cmp_to_key(tree_entry_cmp)

settings = Settings()

__all__ = (
    # Standard Library
    'functools',
    'os',
    'typing',
    # Standard Library symbols
    'TYPE_CHECKING',
    'annotations',
    # Low level API
    'GIT_OID_HEX_ZERO',
    'GIT_OID_HEXSZ',
    'GIT_OID_MINPREFIXLEN',
    'GIT_OID_RAWSZ',
    'LIBGIT2_VER_MAJOR',
    'LIBGIT2_VER_MINOR',
    'LIBGIT2_VER_REVISION',
    'LIBGIT2_VERSION',
    'Object',
    'Reference',
    'AlreadyExistsError',
    'Blob',
    'Branch',
    'Commit',
    'Diff',
    'DiffDelta',
    'DiffFile',
    'DiffHunk',
    'DiffLine',
    'DiffStats',
    'GitError',
    'InvalidSpecError',
    'Mailmap',
    'Note',
    'Odb',
    'OdbBackend',
    'OdbBackendLoose',
    'OdbBackendPack',
    'Oid',
    'Patch',
    'RefLogEntry',
    'Refdb',
    'RefdbBackend',
    'RefdbFsBackend',
    'RevSpec',
    'Signature',
    'Stash',
    'Tag',
    'Tree',
    'TreeBuilder',
    'Walker',
    'Worktree',
    'discover_repository',
    'hash',
    'hashfile',
    'init_file_backend',
    'option',
    'reference_is_valid_name',
    'tree_entry_cmp',
    # Low Level API (not present in .pyi)
    'FilterSource',
    'filter_register',
    'filter_unregister',
    'GIT_APPLY_LOCATION_BOTH',
    'GIT_APPLY_LOCATION_INDEX',
    'GIT_APPLY_LOCATION_WORKDIR',
    'GIT_BLAME_FIRST_PARENT',
    'GIT_BLAME_IGNORE_WHITESPACE',
    'GIT_BLAME_NORMAL',
    'GIT_BLAME_TRACK_COPIES_ANY_COMMIT_COPIES',
    'GIT_BLAME_TRACK_COPIES_SAME_COMMIT_COPIES',
    'GIT_BLAME_TRACK_COPIES_SAME_COMMIT_MOVES',
    'GIT_BLAME_TRACK_COPIES_SAME_FILE',
    'GIT_BLAME_USE_MAILMAP',
    'GIT_BLOB_FILTER_ATTRIBUTES_FROM_COMMIT',
    'GIT_BLOB_FILTER_ATTRIBUTES_FROM_HEAD',
    'GIT_BLOB_FILTER_CHECK_FOR_BINARY',
    'GIT_BLOB_FILTER_NO_SYSTEM_ATTRIBUTES',
    'GIT_BRANCH_ALL',
    'GIT_BRANCH_LOCAL',
    'GIT_BRANCH_REMOTE',
    'GIT_CHECKOUT_ALLOW_CONFLICTS',
    'GIT_CHECKOUT_CONFLICT_STYLE_DIFF3',
    'GIT_CHECKOUT_CONFLICT_STYLE_MERGE',
    'GIT_CHECKOUT_CONFLICT_STYLE_ZDIFF3',
    'GIT_CHECKOUT_DISABLE_PATHSPEC_MATCH',
    'GIT_CHECKOUT_DONT_OVERWRITE_IGNORED',
    'GIT_CHECKOUT_DONT_REMOVE_EXISTING',
    'GIT_CHECKOUT_DONT_UPDATE_INDEX',
    'GIT_CHECKOUT_DONT_WRITE_INDEX',
    'GIT_CHECKOUT_DRY_RUN',
    'GIT_CHECKOUT_FORCE',
    'GIT_CHECKOUT_NO_REFRESH',
    'GIT_CHECKOUT_NONE',
    'GIT_CHECKOUT_RECREATE_MISSING',
    'GIT_CHECKOUT_REMOVE_IGNORED',
    'GIT_CHECKOUT_REMOVE_UNTRACKED',
    'GIT_CHECKOUT_SAFE',
    'GIT_CHECKOUT_SKIP_LOCKED_DIRECTORIES',
    'GIT_CHECKOUT_SKIP_UNMERGED',
    'GIT_CHECKOUT_UPDATE_ONLY',
    'GIT_CHECKOUT_USE_OURS',
    'GIT_CHECKOUT_USE_THEIRS',
    'GIT_CONFIG_HIGHEST_LEVEL',
    'GIT_CONFIG_LEVEL_APP',
    'GIT_CONFIG_LEVEL_GLOBAL',
    'GIT_CONFIG_LEVEL_LOCAL',
    'GIT_CONFIG_LEVEL_PROGRAMDATA',
    'GIT_CONFIG_LEVEL_SYSTEM',
    'GIT_CONFIG_LEVEL_WORKTREE',
    'GIT_CONFIG_LEVEL_XDG',
    'GIT_DELTA_ADDED',
    'GIT_DELTA_CONFLICTED',
    'GIT_DELTA_COPIED',
    'GIT_DELTA_DELETED',
    'GIT_DELTA_IGNORED',
    'GIT_DELTA_MODIFIED',
    'GIT_DELTA_RENAMED',
    'GIT_DELTA_TYPECHANGE',
    'GIT_DELTA_UNMODIFIED',
    'GIT_DELTA_UNREADABLE',
    'GIT_DELTA_UNTRACKED',
    'GIT_DESCRIBE_ALL',
    'GIT_DESCRIBE_DEFAULT',
    'GIT_DESCRIBE_TAGS',
    'GIT_DIFF_BREAK_REWRITES_FOR_RENAMES_ONLY',
    'GIT_DIFF_BREAK_REWRITES',
    'GIT_DIFF_DISABLE_PATHSPEC_MATCH',
    'GIT_DIFF_ENABLE_FAST_UNTRACKED_DIRS',
    'GIT_DIFF_FIND_ALL',
    'GIT_DIFF_FIND_AND_BREAK_REWRITES',
    'GIT_DIFF_FIND_BY_CONFIG',
    'GIT_DIFF_FIND_COPIES_FROM_UNMODIFIED',
    'GIT_DIFF_FIND_COPIES',
    'GIT_DIFF_FIND_DONT_IGNORE_WHITESPACE',
    'GIT_DIFF_FIND_EXACT_MATCH_ONLY',
    'GIT_DIFF_FIND_FOR_UNTRACKED',
    'GIT_DIFF_FIND_IGNORE_LEADING_WHITESPACE',
    'GIT_DIFF_FIND_IGNORE_WHITESPACE',
    'GIT_DIFF_FIND_REMOVE_UNMODIFIED',
    'GIT_DIFF_FIND_RENAMES_FROM_REWRITES',
    'GIT_DIFF_FIND_RENAMES',
    'GIT_DIFF_FIND_REWRITES',
    'GIT_DIFF_FLAG_BINARY',
    'GIT_DIFF_FLAG_EXISTS',
    'GIT_DIFF_FLAG_NOT_BINARY',
    'GIT_DIFF_FLAG_VALID_ID',
    'GIT_DIFF_FLAG_VALID_SIZE',
    'GIT_DIFF_FORCE_BINARY',
    'GIT_DIFF_FORCE_TEXT',
    'GIT_DIFF_IGNORE_BLANK_LINES',
    'GIT_DIFF_IGNORE_CASE',
    'GIT_DIFF_IGNORE_FILEMODE',
    'GIT_DIFF_IGNORE_SUBMODULES',
    'GIT_DIFF_IGNORE_WHITESPACE_CHANGE',
    'GIT_DIFF_IGNORE_WHITESPACE_EOL',
    'GIT_DIFF_IGNORE_WHITESPACE',
    'GIT_DIFF_INCLUDE_CASECHANGE',
    'GIT_DIFF_INCLUDE_IGNORED',
    'GIT_DIFF_INCLUDE_TYPECHANGE_TREES',
    'GIT_DIFF_INCLUDE_TYPECHANGE',
    'GIT_DIFF_INCLUDE_UNMODIFIED',
    'GIT_DIFF_INCLUDE_UNREADABLE_AS_UNTRACKED',
    'GIT_DIFF_INCLUDE_UNREADABLE',
    'GIT_DIFF_INCLUDE_UNTRACKED',
    'GIT_DIFF_INDENT_HEURISTIC',
    'GIT_DIFF_MINIMAL',
    'GIT_DIFF_NORMAL',
    'GIT_DIFF_PATIENCE',
    'GIT_DIFF_RECURSE_IGNORED_DIRS',
    'GIT_DIFF_RECURSE_UNTRACKED_DIRS',
    'GIT_DIFF_REVERSE',
    'GIT_DIFF_SHOW_BINARY',
    'GIT_DIFF_SHOW_UNMODIFIED',
    'GIT_DIFF_SHOW_UNTRACKED_CONTENT',
    'GIT_DIFF_SKIP_BINARY_CHECK',
    'GIT_DIFF_STATS_FULL',
    'GIT_DIFF_STATS_INCLUDE_SUMMARY',
    'GIT_DIFF_STATS_NONE',
    'GIT_DIFF_STATS_NUMBER',
    'GIT_DIFF_STATS_SHORT',
    'GIT_DIFF_UPDATE_INDEX',
    'GIT_FILEMODE_BLOB_EXECUTABLE',
    'GIT_FILEMODE_BLOB',
    'GIT_FILEMODE_COMMIT',
    'GIT_FILEMODE_LINK',
    'GIT_FILEMODE_TREE',
    'GIT_FILEMODE_UNREADABLE',
    'GIT_FILTER_ALLOW_UNSAFE',
    'GIT_FILTER_ATTRIBUTES_FROM_COMMIT',
    'GIT_FILTER_ATTRIBUTES_FROM_HEAD',
    'GIT_FILTER_CLEAN',
    'GIT_FILTER_DEFAULT',
    'GIT_FILTER_DRIVER_PRIORITY',
    'GIT_FILTER_NO_SYSTEM_ATTRIBUTES',
    'GIT_FILTER_SMUDGE',
    'GIT_FILTER_TO_ODB',
    'GIT_FILTER_TO_WORKTREE',
    'GIT_MERGE_ANALYSIS_FASTFORWARD',
    'GIT_MERGE_ANALYSIS_NONE',
    'GIT_MERGE_ANALYSIS_NORMAL',
    'GIT_MERGE_ANALYSIS_UNBORN',
    'GIT_MERGE_ANALYSIS_UP_TO_DATE',
    'GIT_MERGE_PREFERENCE_FASTFORWARD_ONLY',
    'GIT_MERGE_PREFERENCE_NO_FASTFORWARD',
    'GIT_MERGE_PREFERENCE_NONE',
    'GIT_OBJECT_ANY',
    'GIT_OBJECT_BLOB',
    'GIT_OBJECT_COMMIT',
    'GIT_OBJECT_INVALID',
    'GIT_OBJECT_OFS_DELTA',
    'GIT_OBJECT_REF_DELTA',
    'GIT_OBJECT_TAG',
    'GIT_OBJECT_TREE',
    'GIT_OPT_ADD_SSL_X509_CERT',
    'GIT_OPT_DISABLE_PACK_KEEP_FILE_CHECKS',
    'GIT_OPT_ENABLE_CACHING',
    'GIT_OPT_ENABLE_FSYNC_GITDIR',
    'GIT_OPT_ENABLE_HTTP_EXPECT_CONTINUE',
    'GIT_OPT_ENABLE_OFS_DELTA',
    'GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION',
    'GIT_OPT_ENABLE_STRICT_OBJECT_CREATION',
    'GIT_OPT_ENABLE_STRICT_SYMBOLIC_REF_CREATION',
    'GIT_OPT_ENABLE_UNSAVED_INDEX_SAFETY',
    'GIT_OPT_GET_CACHED_MEMORY',
    'GIT_OPT_GET_EXTENSIONS',
    'GIT_OPT_GET_HOMEDIR',
    'GIT_OPT_GET_MWINDOW_FILE_LIMIT',
    'GIT_OPT_GET_MWINDOW_MAPPED_LIMIT',
    'GIT_OPT_GET_MWINDOW_SIZE',
    'GIT_OPT_GET_OWNER_VALIDATION',
    'GIT_OPT_GET_PACK_MAX_OBJECTS',
    'GIT_OPT_GET_SEARCH_PATH',
    'GIT_OPT_GET_SERVER_CONNECT_TIMEOUT',
    'GIT_OPT_GET_SERVER_TIMEOUT',
    'GIT_OPT_GET_TEMPLATE_PATH',
    'GIT_OPT_GET_USER_AGENT',
    'GIT_OPT_GET_USER_AGENT_PRODUCT',
    'GIT_OPT_GET_WINDOWS_SHAREMODE',
    'GIT_OPT_SET_ALLOCATOR',
    'GIT_OPT_SET_CACHE_MAX_SIZE',
    'GIT_OPT_SET_CACHE_OBJECT_LIMIT',
    'GIT_OPT_SET_EXTENSIONS',
    'GIT_OPT_SET_HOMEDIR',
    'GIT_OPT_SET_MWINDOW_FILE_LIMIT',
    'GIT_OPT_SET_MWINDOW_MAPPED_LIMIT',
    'GIT_OPT_SET_MWINDOW_SIZE',
    'GIT_OPT_SET_ODB_LOOSE_PRIORITY',
    'GIT_OPT_SET_ODB_PACKED_PRIORITY',
    'GIT_OPT_SET_OWNER_VALIDATION',
    'GIT_OPT_SET_PACK_MAX_OBJECTS',
    'GIT_OPT_SET_SEARCH_PATH',
    'GIT_OPT_SET_SERVER_CONNECT_TIMEOUT',
    'GIT_OPT_SET_SERVER_TIMEOUT',
    'GIT_OPT_SET_SSL_CERT_LOCATIONS',
    'GIT_OPT_SET_SSL_CIPHERS',
    'GIT_OPT_SET_TEMPLATE_PATH',
    'GIT_OPT_SET_USER_AGENT',
    'GIT_OPT_SET_USER_AGENT_PRODUCT',
    'GIT_OPT_SET_WINDOWS_SHAREMODE',
    'GIT_REFERENCES_ALL',
    'GIT_REFERENCES_BRANCHES',
    'GIT_REFERENCES_TAGS',
    'GIT_RESET_HARD',
    'GIT_RESET_MIXED',
    'GIT_RESET_SOFT',
    'GIT_REVSPEC_MERGE_BASE',
    'GIT_REVSPEC_RANGE',
    'GIT_REVSPEC_SINGLE',
    'GIT_SORT_NONE',
    'GIT_SORT_REVERSE',
    'GIT_SORT_TIME',
    'GIT_SORT_TOPOLOGICAL',
    'GIT_STASH_APPLY_DEFAULT',
    'GIT_STASH_APPLY_REINSTATE_INDEX',
    'GIT_STASH_DEFAULT',
    'GIT_STASH_INCLUDE_IGNORED',
    'GIT_STASH_INCLUDE_UNTRACKED',
    'GIT_STASH_KEEP_ALL',
    'GIT_STASH_KEEP_INDEX',
    'GIT_STATUS_CONFLICTED',
    'GIT_STATUS_CURRENT',
    'GIT_STATUS_IGNORED',
    'GIT_STATUS_INDEX_DELETED',
    'GIT_STATUS_INDEX_MODIFIED',
    'GIT_STATUS_INDEX_NEW',
    'GIT_STATUS_INDEX_RENAMED',
    'GIT_STATUS_INDEX_TYPECHANGE',
    'GIT_STATUS_WT_DELETED',
    'GIT_STATUS_WT_MODIFIED',
    'GIT_STATUS_WT_NEW',
    'GIT_STATUS_WT_RENAMED',
    'GIT_STATUS_WT_TYPECHANGE',
    'GIT_STATUS_WT_UNREADABLE',
    'GIT_SUBMODULE_IGNORE_ALL',
    'GIT_SUBMODULE_IGNORE_DIRTY',
    'GIT_SUBMODULE_IGNORE_NONE',
    'GIT_SUBMODULE_IGNORE_UNSPECIFIED',
    'GIT_SUBMODULE_IGNORE_UNTRACKED',
    'GIT_SUBMODULE_STATUS_IN_CONFIG',
    'GIT_SUBMODULE_STATUS_IN_HEAD',
    'GIT_SUBMODULE_STATUS_IN_INDEX',
    'GIT_SUBMODULE_STATUS_IN_WD',
    'GIT_SUBMODULE_STATUS_INDEX_ADDED',
    'GIT_SUBMODULE_STATUS_INDEX_DELETED',
    'GIT_SUBMODULE_STATUS_INDEX_MODIFIED',
    'GIT_SUBMODULE_STATUS_WD_ADDED',
    'GIT_SUBMODULE_STATUS_WD_DELETED',
    'GIT_SUBMODULE_STATUS_WD_INDEX_MODIFIED',
    'GIT_SUBMODULE_STATUS_WD_MODIFIED',
    'GIT_SUBMODULE_STATUS_WD_UNINITIALIZED',
    'GIT_SUBMODULE_STATUS_WD_UNTRACKED',
    'GIT_SUBMODULE_STATUS_WD_WD_MODIFIED',
    # High level API.
    'enums',
    'blame',
    'Blame',
    'BlameHunk',
    'blob',
    'BlobIO',
    'callbacks',
    'Payload',
    'RemoteCallbacks',
    'CheckoutCallbacks',
    'StashApplyCallbacks',
    'git_clone_options',
    'git_fetch_options',
    'git_proxy_options',
    'get_credentials',
    'config',
    'Config',
    'credentials',
    'CredentialType',
    'Username',
    'UserPass',
    'Keypair',
    'KeypairFromAgent',
    'KeypairFromMemory',
    'errors',
    'check_error',
    'Passthrough',
    'ffi',
    'C',
    'filter',
    'Filter',
    'index',
    'Index',
    'IndexEntry',
    'legacyenums',
    'GIT_FEATURE_THREADS',
    'GIT_FEATURE_HTTPS',
    'GIT_FEATURE_SSH',
    'GIT_FEATURE_NSEC',
    'GIT_REPOSITORY_INIT_BARE',
    'GIT_REPOSITORY_INIT_NO_REINIT',
    'GIT_REPOSITORY_INIT_NO_DOTGIT_DIR',
    'GIT_REPOSITORY_INIT_MKDIR',
    'GIT_REPOSITORY_INIT_MKPATH',
    'GIT_REPOSITORY_INIT_EXTERNAL_TEMPLATE',
    'GIT_REPOSITORY_INIT_RELATIVE_GITLINK',
    'GIT_REPOSITORY_INIT_SHARED_UMASK',
    'GIT_REPOSITORY_INIT_SHARED_GROUP',
    'GIT_REPOSITORY_INIT_SHARED_ALL',
    'GIT_REPOSITORY_OPEN_NO_SEARCH',
    'GIT_REPOSITORY_OPEN_CROSS_FS',
    'GIT_REPOSITORY_OPEN_BARE',
    'GIT_REPOSITORY_OPEN_NO_DOTGIT',
    'GIT_REPOSITORY_OPEN_FROM_ENV',
    'GIT_REPOSITORY_STATE_NONE',
    'GIT_REPOSITORY_STATE_MERGE',
    'GIT_REPOSITORY_STATE_REVERT',
    'GIT_REPOSITORY_STATE_REVERT_SEQUENCE',
    'GIT_REPOSITORY_STATE_CHERRYPICK',
    'GIT_REPOSITORY_STATE_CHERRYPICK_SEQUENCE',
    'GIT_REPOSITORY_STATE_BISECT',
    'GIT_REPOSITORY_STATE_REBASE',
    'GIT_REPOSITORY_STATE_REBASE_INTERACTIVE',
    'GIT_REPOSITORY_STATE_REBASE_MERGE',
    'GIT_REPOSITORY_STATE_APPLY_MAILBOX',
    'GIT_REPOSITORY_STATE_APPLY_MAILBOX_OR_REBASE',
    'GIT_ATTR_CHECK_FILE_THEN_INDEX',
    'GIT_ATTR_CHECK_INDEX_THEN_FILE',
    'GIT_ATTR_CHECK_INDEX_ONLY',
    'GIT_ATTR_CHECK_NO_SYSTEM',
    'GIT_ATTR_CHECK_INCLUDE_HEAD',
    'GIT_ATTR_CHECK_INCLUDE_COMMIT',
    'GIT_FETCH_PRUNE_UNSPECIFIED',
    'GIT_FETCH_PRUNE',
    'GIT_FETCH_NO_PRUNE',
    'GIT_CHECKOUT_NOTIFY_NONE',
    'GIT_CHECKOUT_NOTIFY_CONFLICT',
    'GIT_CHECKOUT_NOTIFY_DIRTY',
    'GIT_CHECKOUT_NOTIFY_UPDATED',
    'GIT_CHECKOUT_NOTIFY_UNTRACKED',
    'GIT_CHECKOUT_NOTIFY_IGNORED',
    'GIT_CHECKOUT_NOTIFY_ALL',
    'GIT_STASH_APPLY_PROGRESS_NONE',
    'GIT_STASH_APPLY_PROGRESS_LOADING_STASH',
    'GIT_STASH_APPLY_PROGRESS_ANALYZE_INDEX',
    'GIT_STASH_APPLY_PROGRESS_ANALYZE_MODIFIED',
    'GIT_STASH_APPLY_PROGRESS_ANALYZE_UNTRACKED',
    'GIT_STASH_APPLY_PROGRESS_CHECKOUT_UNTRACKED',
    'GIT_STASH_APPLY_PROGRESS_CHECKOUT_MODIFIED',
    'GIT_STASH_APPLY_PROGRESS_DONE',
    'GIT_CREDENTIAL_USERPASS_PLAINTEXT',
    'GIT_CREDENTIAL_SSH_KEY',
    'GIT_CREDENTIAL_SSH_CUSTOM',
    'GIT_CREDENTIAL_DEFAULT',
    'GIT_CREDENTIAL_SSH_INTERACTIVE',
    'GIT_CREDENTIAL_USERNAME',
    'GIT_CREDENTIAL_SSH_MEMORY',
    'packbuilder',
    'PackBuilder',
    'refspec',
    'remotes',
    'Remote',
    'repository',
    'Repository',
    'branches',
    'references',
    'settings',
    'Settings',
    'submodules',
    'Submodule',
    'transaction',
    'ReferenceTransaction',
    'utils',
    'to_bytes',
    'to_str',
    # __init__ module defined symbols
    'features',
    'LIBGIT2_VER',
    'init_repository',
    'clone_repository',
    'tree_entry_key',
)
