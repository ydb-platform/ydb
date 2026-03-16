from collections.abc import Iterator, Sequence
from io import DEFAULT_BUFFER_SIZE, IOBase
from pathlib import Path
from queue import Queue
from threading import Event
from typing import (  # noqa: UP035
    Generic,
    Literal,
    Optional,
    Type,
    TypedDict,
    TypeVar,
    overload,
)

from . import Index
from ._libgit2.ffi import (
    GitCommitC,
    GitObjectC,
    GitProxyOptionsC,
    GitSignatureC,
    _Pointer,
)
from .enums import (
    ApplyLocation,
    BlobFilter,
    BranchType,
    DeltaStatus,
    DiffFind,
    DiffFlag,
    DiffOption,
    DiffStatsFormat,
    FileMode,
    MergeAnalysis,
    MergePreference,
    ObjectType,
    ReferenceFilter,
    ReferenceType,
    ResetMode,
    SortMode,
)
from .filter import Filter

GIT_OBJ_BLOB = Literal[3]
GIT_OBJ_COMMIT = Literal[1]
GIT_OBJ_TAG = Literal[4]
GIT_OBJ_TREE = Literal[2]

LIBGIT2_VER_MAJOR: int
LIBGIT2_VER_MINOR: int
LIBGIT2_VER_REVISION: int
LIBGIT2_VERSION: str
GIT_OID_RAWSZ: int
GIT_OID_HEXSZ: int
GIT_OID_HEX_ZERO: str
GIT_OID_MINPREFIXLEN: int
GIT_OBJECT_ANY: int
GIT_OBJECT_INVALID: int
GIT_OBJECT_COMMIT: int
GIT_OBJECT_TREE: int
GIT_OBJECT_BLOB: int
GIT_OBJECT_TAG: int
GIT_OBJECT_OFS_DELTA: int
GIT_OBJECT_REF_DELTA: int
GIT_FILEMODE_UNREADABLE: int
GIT_FILEMODE_TREE: int
GIT_FILEMODE_BLOB: int
GIT_FILEMODE_BLOB_EXECUTABLE: int
GIT_FILEMODE_LINK: int
GIT_FILEMODE_COMMIT: int
GIT_SORT_NONE: int
GIT_SORT_TOPOLOGICAL: int
GIT_SORT_TIME: int
GIT_SORT_REVERSE: int
GIT_RESET_SOFT: int
GIT_RESET_MIXED: int
GIT_RESET_HARD: int
GIT_REFERENCES_ALL: int
GIT_REFERENCES_BRANCHES: int
GIT_REFERENCES_TAGS: int
GIT_REVSPEC_SINGLE: int
GIT_REVSPEC_RANGE: int
GIT_REVSPEC_MERGE_BASE: int
GIT_BRANCH_LOCAL: int
GIT_BRANCH_REMOTE: int
GIT_BRANCH_ALL: int
GIT_STATUS_CURRENT: int
GIT_STATUS_INDEX_NEW: int
GIT_STATUS_INDEX_MODIFIED: int
GIT_STATUS_INDEX_DELETED: int
GIT_STATUS_INDEX_RENAMED: int
GIT_STATUS_INDEX_TYPECHANGE: int
GIT_STATUS_WT_NEW: int
GIT_STATUS_WT_MODIFIED: int
GIT_STATUS_WT_DELETED: int
GIT_STATUS_WT_TYPECHANGE: int
GIT_STATUS_WT_RENAMED: int
GIT_STATUS_WT_UNREADABLE: int
GIT_STATUS_IGNORED: int
GIT_STATUS_CONFLICTED: int
GIT_CHECKOUT_NONE: int
GIT_CHECKOUT_SAFE: int
GIT_CHECKOUT_FORCE: int
GIT_CHECKOUT_RECREATE_MISSING: int
GIT_CHECKOUT_ALLOW_CONFLICTS: int
GIT_CHECKOUT_REMOVE_UNTRACKED: int
GIT_CHECKOUT_REMOVE_IGNORED: int
GIT_CHECKOUT_UPDATE_ONLY: int
GIT_CHECKOUT_DONT_UPDATE_INDEX: int
GIT_CHECKOUT_NO_REFRESH: int
GIT_CHECKOUT_SKIP_UNMERGED: int
GIT_CHECKOUT_USE_OURS: int
GIT_CHECKOUT_USE_THEIRS: int
GIT_CHECKOUT_DISABLE_PATHSPEC_MATCH: int
GIT_CHECKOUT_SKIP_LOCKED_DIRECTORIES: int
GIT_CHECKOUT_DONT_OVERWRITE_IGNORED: int
GIT_CHECKOUT_CONFLICT_STYLE_MERGE: int
GIT_CHECKOUT_CONFLICT_STYLE_DIFF3: int
GIT_CHECKOUT_DONT_REMOVE_EXISTING: int
GIT_CHECKOUT_DONT_WRITE_INDEX: int
GIT_CHECKOUT_DRY_RUN: int
GIT_CHECKOUT_CONFLICT_STYLE_ZDIFF3: int
GIT_DIFF_NORMAL: int
GIT_DIFF_REVERSE: int
GIT_DIFF_INCLUDE_IGNORED: int
GIT_DIFF_RECURSE_IGNORED_DIRS: int
GIT_DIFF_INCLUDE_UNTRACKED: int
GIT_DIFF_RECURSE_UNTRACKED_DIRS: int
GIT_DIFF_INCLUDE_UNMODIFIED: int
GIT_DIFF_INCLUDE_TYPECHANGE: int
GIT_DIFF_INCLUDE_TYPECHANGE_TREES: int
GIT_DIFF_IGNORE_FILEMODE: int
GIT_DIFF_IGNORE_SUBMODULES: int
GIT_DIFF_IGNORE_CASE: int
GIT_DIFF_INCLUDE_CASECHANGE: int
GIT_DIFF_DISABLE_PATHSPEC_MATCH: int
GIT_DIFF_SKIP_BINARY_CHECK: int
GIT_DIFF_ENABLE_FAST_UNTRACKED_DIRS: int
GIT_DIFF_UPDATE_INDEX: int
GIT_DIFF_INCLUDE_UNREADABLE: int
GIT_DIFF_INCLUDE_UNREADABLE_AS_UNTRACKED: int
GIT_DIFF_INDENT_HEURISTIC: int
GIT_DIFF_IGNORE_BLANK_LINES: int
GIT_DIFF_FORCE_TEXT: int
GIT_DIFF_FORCE_BINARY: int
GIT_DIFF_IGNORE_WHITESPACE: int
GIT_DIFF_IGNORE_WHITESPACE_CHANGE: int
GIT_DIFF_IGNORE_WHITESPACE_EOL: int
GIT_DIFF_SHOW_UNTRACKED_CONTENT: int
GIT_DIFF_SHOW_UNMODIFIED: int
GIT_DIFF_PATIENCE: int
GIT_DIFF_MINIMAL: int
GIT_DIFF_SHOW_BINARY: int
GIT_DIFF_STATS_NONE: int
GIT_DIFF_STATS_FULL: int
GIT_DIFF_STATS_SHORT: int
GIT_DIFF_STATS_NUMBER: int
GIT_DIFF_STATS_INCLUDE_SUMMARY: int
GIT_DIFF_FIND_BY_CONFIG: int
GIT_DIFF_FIND_RENAMES: int
GIT_DIFF_FIND_RENAMES_FROM_REWRITES: int
GIT_DIFF_FIND_COPIES: int
GIT_DIFF_FIND_COPIES_FROM_UNMODIFIED: int
GIT_DIFF_FIND_REWRITES: int
GIT_DIFF_BREAK_REWRITES: int
GIT_DIFF_FIND_AND_BREAK_REWRITES: int
GIT_DIFF_FIND_FOR_UNTRACKED: int
GIT_DIFF_FIND_ALL: int
GIT_DIFF_FIND_IGNORE_LEADING_WHITESPACE: int
GIT_DIFF_FIND_IGNORE_WHITESPACE: int
GIT_DIFF_FIND_DONT_IGNORE_WHITESPACE: int
GIT_DIFF_FIND_EXACT_MATCH_ONLY: int
GIT_DIFF_BREAK_REWRITES_FOR_RENAMES_ONLY: int
GIT_DIFF_FIND_REMOVE_UNMODIFIED: int
GIT_DIFF_FLAG_BINARY: int
GIT_DIFF_FLAG_NOT_BINARY: int
GIT_DIFF_FLAG_VALID_ID: int
GIT_DIFF_FLAG_EXISTS: int
GIT_DIFF_FLAG_VALID_SIZE: int
GIT_DELTA_UNMODIFIED: int
GIT_DELTA_ADDED: int
GIT_DELTA_DELETED: int
GIT_DELTA_MODIFIED: int
GIT_DELTA_RENAMED: int
GIT_DELTA_COPIED: int
GIT_DELTA_IGNORED: int
GIT_DELTA_UNTRACKED: int
GIT_DELTA_TYPECHANGE: int
GIT_DELTA_UNREADABLE: int
GIT_DELTA_CONFLICTED: int
GIT_CONFIG_LEVEL_PROGRAMDATA: int
GIT_CONFIG_LEVEL_SYSTEM: int
GIT_CONFIG_LEVEL_XDG: int
GIT_CONFIG_LEVEL_GLOBAL: int
GIT_CONFIG_LEVEL_LOCAL: int
GIT_CONFIG_LEVEL_WORKTREE: int
GIT_CONFIG_LEVEL_APP: int
GIT_CONFIG_HIGHEST_LEVEL: int
GIT_BLAME_NORMAL: int
GIT_BLAME_TRACK_COPIES_SAME_FILE: int
GIT_BLAME_TRACK_COPIES_SAME_COMMIT_MOVES: int
GIT_BLAME_TRACK_COPIES_SAME_COMMIT_COPIES: int
GIT_BLAME_TRACK_COPIES_ANY_COMMIT_COPIES: int
GIT_BLAME_FIRST_PARENT: int
GIT_BLAME_USE_MAILMAP: int
GIT_BLAME_IGNORE_WHITESPACE: int
GIT_MERGE_ANALYSIS_NONE: int
GIT_MERGE_ANALYSIS_NORMAL: int
GIT_MERGE_ANALYSIS_UP_TO_DATE: int
GIT_MERGE_ANALYSIS_FASTFORWARD: int
GIT_MERGE_ANALYSIS_UNBORN: int
GIT_MERGE_PREFERENCE_NONE: int
GIT_MERGE_PREFERENCE_NO_FASTFORWARD: int
GIT_MERGE_PREFERENCE_FASTFORWARD_ONLY: int
GIT_DESCRIBE_DEFAULT: int
GIT_DESCRIBE_TAGS: int
GIT_DESCRIBE_ALL: int
GIT_STASH_DEFAULT: int
GIT_STASH_KEEP_INDEX: int
GIT_STASH_INCLUDE_UNTRACKED: int
GIT_STASH_INCLUDE_IGNORED: int
GIT_STASH_KEEP_ALL: int
GIT_STASH_APPLY_DEFAULT: int
GIT_STASH_APPLY_REINSTATE_INDEX: int
GIT_APPLY_LOCATION_WORKDIR: int
GIT_APPLY_LOCATION_INDEX: int
GIT_APPLY_LOCATION_BOTH: int
GIT_SUBMODULE_IGNORE_UNSPECIFIED: int
GIT_SUBMODULE_IGNORE_NONE: int
GIT_SUBMODULE_IGNORE_UNTRACKED: int
GIT_SUBMODULE_IGNORE_DIRTY: int
GIT_SUBMODULE_IGNORE_ALL: int
GIT_SUBMODULE_STATUS_IN_HEAD: int
GIT_SUBMODULE_STATUS_IN_INDEX: int
GIT_SUBMODULE_STATUS_IN_CONFIG: int
GIT_SUBMODULE_STATUS_IN_WD: int
GIT_SUBMODULE_STATUS_INDEX_ADDED: int
GIT_SUBMODULE_STATUS_INDEX_DELETED: int
GIT_SUBMODULE_STATUS_INDEX_MODIFIED: int
GIT_SUBMODULE_STATUS_WD_UNINITIALIZED: int
GIT_SUBMODULE_STATUS_WD_ADDED: int
GIT_SUBMODULE_STATUS_WD_DELETED: int
GIT_SUBMODULE_STATUS_WD_MODIFIED: int
GIT_SUBMODULE_STATUS_WD_INDEX_MODIFIED: int
GIT_SUBMODULE_STATUS_WD_WD_MODIFIED: int
GIT_SUBMODULE_STATUS_WD_UNTRACKED: int
GIT_BLOB_FILTER_CHECK_FOR_BINARY: int
GIT_BLOB_FILTER_NO_SYSTEM_ATTRIBUTES: int
GIT_BLOB_FILTER_ATTRIBUTES_FROM_HEAD: int
GIT_BLOB_FILTER_ATTRIBUTES_FROM_COMMIT: int
GIT_FILTER_DRIVER_PRIORITY: int
GIT_FILTER_TO_WORKTREE: int
GIT_FILTER_SMUDGE: int
GIT_FILTER_TO_ODB: int
GIT_FILTER_CLEAN: int
GIT_FILTER_DEFAULT: int
GIT_FILTER_ALLOW_UNSAFE: int
GIT_FILTER_NO_SYSTEM_ATTRIBUTES: int
GIT_FILTER_ATTRIBUTES_FROM_HEAD: int
GIT_FILTER_ATTRIBUTES_FROM_COMMIT: int

T = TypeVar('T')

class _ObjectBase(Generic[T]):
    _pointer: _Pointer[T]
    filemode: FileMode
    id: Oid
    name: str | None
    raw_name: bytes | None
    short_id: str
    type: 'Literal[GIT_OBJ_COMMIT] | Literal[GIT_OBJ_TREE] | Literal[GIT_OBJ_TAG] | Literal[GIT_OBJ_BLOB]'
    type_str: "Literal['commit'] | Literal['tree'] | Literal['tag'] | Literal['blob']"
    author: Signature
    committer: Signature
    tree: Tree
    @overload
    def peel(
        self, target_type: 'Literal[GIT_OBJ_COMMIT, ObjectType.COMMIT] | Type[Commit]'
    ) -> 'Commit': ...
    @overload
    def peel(
        self, target_type: 'Literal[GIT_OBJ_TREE, ObjectType.TREE] | Type[Tree]'
    ) -> 'Tree': ...
    @overload
    def peel(
        self, target_type: 'Literal[GIT_OBJ_TAG, ObjectType.TAG] | Type[Tag]'
    ) -> 'Tag': ...
    @overload
    def peel(
        self, target_type: 'Literal[GIT_OBJ_BLOB, ObjectType.BLOB] | Type[Blob]'
    ) -> 'Blob': ...
    @overload
    def peel(self, target_type: 'None') -> 'Commit|Tree|Tag|Blob': ...
    def read_raw(self) -> bytes: ...
    def __eq__(self, other) -> bool: ...
    def __ge__(self, other) -> bool: ...
    def __gt__(self, other) -> bool: ...
    def __hash__(self) -> int: ...
    def __le__(self, other) -> bool: ...
    def __lt__(self, other) -> bool: ...
    def __ne__(self, other) -> bool: ...

class Object(_ObjectBase[GitObjectC]):
    pass

class Reference:
    name: str
    raw_name: bytes
    raw_shorthand: bytes
    raw_target: Oid | bytes
    shorthand: str
    target: Oid | str
    type: ReferenceType
    def __init__(self, *args) -> None: ...
    def delete(self) -> None: ...
    def log(self) -> Iterator[RefLogEntry]: ...
    @overload
    def peel(self, type: 'Literal[GIT_OBJ_COMMIT] | Type[Commit]') -> 'Commit': ...
    @overload
    def peel(self, type: 'Literal[GIT_OBJ_TREE] | Type[Tree]') -> 'Tree': ...
    @overload
    def peel(self, type: 'Literal[GIT_OBJ_TAG] | Type[Tag]') -> 'Tag': ...
    @overload
    def peel(self, type: 'Literal[GIT_OBJ_BLOB] | Type[Blob]') -> 'Blob': ...
    @overload
    def peel(self, type: 'None' = None) -> 'Commit|Tree|Tag|Blob': ...
    def rename(self, new_name: str) -> None: ...
    def resolve(self) -> Reference: ...
    def set_target(self, target: _OidArg, message: str = ...) -> None: ...
    def __eq__(self, other) -> bool: ...
    def __ge__(self, other) -> bool: ...
    def __gt__(self, other) -> bool: ...
    def __le__(self, other) -> bool: ...
    def __lt__(self, other) -> bool: ...
    def __ne__(self, other) -> bool: ...

class AlreadyExistsError(ValueError): ...

class Blob(Object):
    data: bytes
    is_binary: bool
    size: int
    def diff(
        self,
        blob: Blob = ...,
        flag: int = ...,
        old_as_path: str = ...,
        new_as_path: str = ...,
    ) -> Patch: ...
    def diff_to_buffer(
        self,
        buffer: Optional[bytes | str] = None,
        flag: DiffOption = DiffOption.NORMAL,
        old_as_path: str = ...,
        buffer_as_path: str = ...,
    ) -> Patch: ...
    def _write_to_queue(
        self,
        queue: Queue[bytes],
        ready: Event,
        done: Event,
        chunk_size: int = DEFAULT_BUFFER_SIZE,
        as_path: Optional[str] = None,
        flags: BlobFilter = BlobFilter.CHECK_FOR_BINARY,
        commit_id: Optional[Oid] = None,
    ) -> None: ...
    def __buffer__(self, flags: int) -> memoryview: ...
    def __release_buffer__(self, buffer: memoryview) -> None: ...

class Branch(Reference):
    branch_name: str
    raw_branch_name: bytes
    remote_name: str
    upstream: Branch
    upstream_name: str
    def delete(self) -> None: ...
    def is_checked_out(self) -> bool: ...
    def is_head(self) -> bool: ...
    def rename(self, name: str, force: bool = False) -> 'Branch': ...  # type: ignore[override]

class FetchOptions:
    # incomplete
    depth: int
    proxy_opts: GitProxyOptionsC

class CloneOptions:
    # incomplete
    version: int
    checkout_opts: object
    fetch_opts: FetchOptions
    bare: int
    local: object
    checkout_branch: object
    repository_cb: object
    repository_cb_payload: object
    remote_cb: object
    remote_cb_payload: object

class Commit(_ObjectBase[GitCommitC]):
    _pointer: _Pointer[GitCommitC]
    author: Signature
    commit_time: int
    commit_time_offset: int
    committer: Signature
    gpg_signature: tuple[bytes, bytes]
    message: str
    message_encoding: str
    message_trailers: dict[str, str]
    parent_ids: list[Oid]
    parents: list[Commit]
    raw_message: bytes
    tree: Tree
    tree_id: Oid

class Diff:
    deltas: Iterator[DiffDelta]
    patch: str | None
    patchid: Oid
    stats: DiffStats
    text: str
    def find_similar(
        self,
        flags: DiffFind = DiffFind.FIND_BY_CONFIG,
        rename_threshold: int = 50,
        copy_threshold: int = 50,
        rename_from_rewrite_threshold: int = 50,
        break_rewrite_threshold: int = 60,
        rename_limit: int = 1000,
    ) -> None: ...
    def merge(self, diff: Diff) -> None: ...
    @staticmethod
    def from_c(diff, repo) -> Diff: ...
    @staticmethod
    def parse_diff(git_diff: str | bytes) -> Diff: ...
    def __getitem__(self, index: int) -> Patch | None: ...  # Diff_getitem
    def __iter__(self) -> Iterator[Patch | None]: ...  # -> DiffIter
    def __len__(self) -> int: ...

class DiffDelta:
    flags: DiffFlag
    is_binary: bool
    nfiles: int
    new_file: DiffFile
    old_file: DiffFile
    similarity: int
    status: DeltaStatus
    def status_char(self) -> str: ...

class DiffFile:
    flags: DiffFlag
    id: Oid
    mode: FileMode
    path: str
    raw_path: bytes
    size: int
    @staticmethod
    def from_c(bytes) -> DiffFile: ...

class DiffHunk:
    header: str
    lines: list[DiffLine]
    new_lines: int
    new_start: int
    old_lines: int
    old_start: int

class DiffLine:
    content: str
    content_offset: int
    new_lineno: int
    num_lines: int
    old_lineno: int
    origin: str
    raw_content: bytes

class DiffStats:
    deletions: int
    files_changed: int
    insertions: int
    def format(self, format: DiffStatsFormat, width: int) -> str: ...

class FilterSource:
    # probably incomplete
    repo: object
    pass

class GitError(Exception): ...
class InvalidSpecError(ValueError): ...

class Mailmap:
    def __init__(self, *args) -> None: ...
    def add_entry(
        self,
        real_name: str | None = ...,
        real_email: str | None = ...,
        replace_name: str | None = ...,
        replace_email: str = ...,
    ) -> None: ...
    @staticmethod
    def from_buffer(buffer: str | bytes) -> Mailmap: ...
    @staticmethod
    def from_repository(repository: Repository) -> Mailmap: ...
    def resolve(self, name: str, email: str) -> tuple[str, str]: ...
    def resolve_signature(self, sig: Signature) -> Signature: ...

class Note:
    annotated_id: Oid
    id: Oid
    message: str
    data: bytes
    def remove(
        self, author: Signature, committer: Signature, ref: str = 'refs/notes/commits'
    ) -> None: ...

class Odb:
    backends: Iterator[OdbBackend]
    def __init__(self, *args, **kwargs) -> None: ...
    def add_backend(self, backend: OdbBackend, priority: int) -> None: ...
    def add_disk_alternate(self, path: str | Path) -> None: ...
    def exists(self, oid: _OidArg) -> bool: ...
    def read(self, oid: _OidArg) -> tuple[int, bytes]: ...
    def write(self, type: int, data: bytes | str) -> Oid: ...
    def __contains__(self, other: _OidArg) -> bool: ...
    def __iter__(self) -> Iterator[Oid]: ...  # Odb_as_iter

class OdbBackend:
    def __init__(self, *args, **kwargs) -> None: ...
    def exists(self, oid: _OidArg) -> bool: ...
    def exists_prefix(self, partial_id: _OidArg) -> Oid: ...
    def read(self, oid: _OidArg) -> tuple[int, bytes]: ...
    def read_header(self, oid: _OidArg) -> tuple[int, int]: ...
    def read_prefix(self, oid: _OidArg) -> tuple[int, bytes, Oid]: ...
    def refresh(self) -> None: ...
    def __iter__(self) -> Iterator[Oid]: ...  # OdbBackend_as_iter

class OdbBackendLoose(OdbBackend):
    def __init__(self, *args, **kwargs) -> None: ...

class OdbBackendPack(OdbBackend):
    def __init__(self, *args, **kwargs) -> None: ...

class Oid:
    raw: bytes
    def __init__(self, raw: bytes = ..., hex: str = ...) -> None: ...
    def __eq__(self, other) -> bool: ...
    def __ge__(self, other) -> bool: ...
    def __gt__(self, other) -> bool: ...
    def __hash__(self) -> int: ...
    def __le__(self, other) -> bool: ...
    def __lt__(self, other) -> bool: ...
    def __ne__(self, other) -> bool: ...
    def __bool__(self) -> bool: ...

class Patch:
    data: bytes
    delta: DiffDelta
    hunks: list[DiffHunk]
    line_stats: tuple[int, int, int]  # context, additions, deletions
    text: str | None

    @staticmethod
    def create_from(
        old: Blob | bytes | None,
        new: Blob | bytes | None,
        old_as_path: str = ...,
        new_as_path: str = ...,
        flag: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Patch: ...

class RefLogEntry:
    committer: Signature
    message: str
    oid_new: Oid
    oid_old: Oid
    def __init__(self, *args, **kwargs) -> None: ...

class Refdb:
    def __init__(self, *args, **kwargs) -> None: ...
    def compress(self) -> None: ...
    @staticmethod
    def new(repo: Repository) -> Refdb: ...
    @staticmethod
    def open(repo: Repository) -> Refdb: ...
    def set_backend(self, backend: RefdbBackend) -> None: ...

class RefdbBackend:
    def __init__(self, *args, **kwargs) -> None: ...
    def compress(self) -> None: ...
    def delete(
        self, ref_name: str, old_id: _OidArg, old_target: str | None
    ) -> None: ...
    def ensure_log(self, ref_name: str) -> bool: ...
    def exists(self, refname: str) -> bool: ...
    def has_log(self, ref_name: str) -> bool: ...
    def lookup(self, refname: str) -> Reference: ...
    def rename(
        self, old_name: str, new_name: str, force: bool, who: Signature, message: str
    ) -> Reference: ...
    def write(
        self,
        ref: Reference,
        force: bool,
        who: Signature,
        message: str,
        old: None | _OidArg,
        old_target: None | str,
    ) -> None: ...
    def __iter__(self) -> Iterator[Reference]: ...

class RefdbFsBackend(RefdbBackend):
    def __init__(self, *args, **kwargs) -> None: ...

_Proxy = None | Literal[True] | str

class _StrArray:
    # incomplete
    count: int

class PushOptions:
    version: int
    pb_parallelism: int
    callbacks: object  # TODO
    proxy_opts: GitProxyOptionsC
    follow_redirects: object  # TODO
    custom_headers: _StrArray
    remote_push_options: _StrArray

class _LsRemotesDict(TypedDict):
    local: bool
    loid: Oid | None
    name: str | None
    symref_target: str | None
    oid: Oid

class Repository:
    def TreeBuilder(self, src: Tree | _OidArg = ...) -> TreeBuilder: ...
    def _disown(self, *args, **kwargs) -> None: ...
    def add_worktree(
        self, name: str, path: str | Path, ref: Reference = ...
    ) -> Worktree: ...
    def applies(
        self,
        diff: Diff,
        location: ApplyLocation = ApplyLocation.INDEX,
        raise_error: bool = False,
    ) -> bool: ...
    def apply(
        self, diff: Diff, location: ApplyLocation = ApplyLocation.WORKDIR
    ) -> None: ...
    def cherrypick(self, id: _OidArg) -> None: ...
    def compress_references(self) -> None: ...
    def create_blob(self, data: str | bytes) -> Oid: ...
    def create_blob_fromdisk(self, path: str) -> Oid: ...
    def create_blob_fromiobase(self, iobase: IOBase) -> Oid: ...
    def create_blob_fromworkdir(self, path: str | Path) -> Oid: ...
    def create_branch(self, name: str, commit: Commit, force=False) -> Branch: ...
    def create_commit(
        self,
        reference_name: Optional[str],
        author: Signature,
        committer: Signature,
        message: str | bytes,
        tree: _OidArg,
        parents: Sequence[_OidArg],
        encoding: str = ...,
    ) -> Oid: ...
    def create_commit_string(
        self,
        author: Signature,
        committer: Signature,
        message: str | bytes,
        tree: _OidArg,
        parents: list[_OidArg],
        encoding: str = ...,
    ) -> Oid: ...
    def create_commit_with_signature(
        self, content: str, signature: str, signature_field: Optional[str] = None
    ) -> Oid: ...
    def create_note(
        self,
        message: str,
        author: Signature,
        committer: Signature,
        annotated_id: str,
        ref: str = 'refs/notes/commits',
        force: bool = False,
    ) -> Oid: ...
    def create_reference_direct(
        self, name: str, target: _OidArg, force: bool, message: Optional[str] = None
    ) -> Reference: ...
    def create_reference_symbolic(
        self, name: str, target: str, force: bool, message: Optional[str] = None
    ) -> Reference: ...
    def create_tag(
        self, name: str, oid: _OidArg, type: ObjectType, tagger: Signature, message: str
    ) -> Oid: ...
    def descendant_of(self, oid1: _OidArg, oid2: _OidArg) -> bool: ...
    def expand_id(self, hex: str) -> Oid: ...
    def free(self) -> None: ...
    def git_object_lookup_prefix(self, oid: _OidArg) -> Object: ...
    def list_worktrees(self) -> list[str]: ...
    def listall_branches(self, flag: BranchType = BranchType.LOCAL) -> list[str]: ...
    def listall_mergeheads(self) -> list[Oid]: ...
    def listall_stashes(self) -> list[Stash]: ...
    def listall_submodules(self) -> list[str]: ...
    def lookup_branch(
        self, branch_name: str | bytes, branch_type: BranchType = BranchType.LOCAL
    ) -> Branch: ...
    def lookup_note(
        self, annotated_id: str, ref: str = 'refs/notes/commits'
    ) -> Note: ...
    def lookup_reference(self, name: str) -> Reference: ...
    def lookup_reference_dwim(self, name: str) -> Reference: ...
    def lookup_worktree(self, name: str) -> Worktree: ...
    def merge_analysis(
        self, their_head: _OidArg, our_ref: str = 'HEAD'
    ) -> tuple[MergeAnalysis, MergePreference]: ...
    def merge_base(self, oid1: _OidArg, oid2: _OidArg) -> Oid: ...
    def merge_base_many(self, oids: list[_OidArg]) -> Oid: ...
    def merge_base_octopus(self, oids: list[_OidArg]) -> Oid: ...
    def notes(self) -> Iterator[Note]: ...
    def path_is_ignored(self, path: str) -> bool: ...
    def raw_listall_branches(
        self, flag: BranchType = BranchType.LOCAL
    ) -> list[bytes]: ...
    def raw_listall_references(self) -> list[bytes]: ...
    def references_iterator_init(self) -> Iterator[Reference]: ...
    def references_iterator_next(
        self,
        iter: Iterator[T],
        references_return_type: ReferenceFilter = ReferenceFilter.ALL,
    ) -> Reference: ...
    def reset(self, oid: _OidArg, reset_type: ResetMode) -> None: ...
    def revparse(self, revspec: str) -> RevSpec: ...
    def revparse_ext(self, revision: str) -> tuple[Object, Reference]: ...
    def revparse_single(self, revision: str) -> Object: ...
    def set_odb(self, odb: Odb) -> None: ...
    def set_refdb(self, refdb: Refdb) -> None: ...
    def status(
        self, untracked_files: str = 'all', ignored: bool = False
    ) -> dict[str, int]: ...
    def status_file(self, path: str) -> int: ...
    def walk(
        self, oid: _OidArg | None, sort_mode: SortMode = SortMode.NONE
    ) -> Walker: ...

class RevSpec:
    flags: int
    from_object: Object
    to_object: Object

class Signature:
    _encoding: str | None
    _pointer: _Pointer[GitSignatureC]
    email: str
    name: str
    offset: int
    raw_email: bytes
    raw_name: bytes
    time: int
    def __init__(
        self,
        name: str | bytes,
        email: str,
        time: int = -1,
        offset: int = 0,
        encoding: Optional[str] = None,
    ) -> None: ...
    def __eq__(self, other) -> bool: ...
    def __ge__(self, other) -> bool: ...
    def __gt__(self, other) -> bool: ...
    def __le__(self, other) -> bool: ...
    def __lt__(self, other) -> bool: ...
    def __ne__(self, other) -> bool: ...

class Stash:
    commit_id: Oid
    message: str
    raw_message: bytes
    def __eq__(self, other) -> bool: ...
    def __ge__(self, other) -> bool: ...
    def __gt__(self, other) -> bool: ...
    def __le__(self, other) -> bool: ...
    def __lt__(self, other) -> bool: ...
    def __ne__(self, other) -> bool: ...

class Tag(Object):
    message: str
    name: str
    raw_message: bytes
    raw_name: bytes
    tagger: Signature
    target: Oid
    def get_object(self) -> Object: ...

class Tree(Object):
    def diff_to_index(
        self,
        index: Index,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Diff: ...
    def diff_to_tree(
        self,
        tree: Tree = ...,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 3,
        swap: bool = False,
    ) -> Diff: ...
    def diff_to_workdir(
        self,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Diff: ...
    def __contains__(self, other: str) -> bool: ...  # Tree_contains
    def __getitem__(self, index: str | int) -> Tree | Blob: ...  # Tree_subscript
    def __iter__(self) -> Iterator[Object]: ...
    def __len__(self) -> int: ...  # Tree_len
    def __rtruediv__(self, other: str) -> Tree | Blob: ...
    def __truediv__(self, other: str) -> Tree | Blob: ...  # Tree_divide

class TreeBuilder:
    def clear(self) -> None: ...
    def get(self, name: str) -> Object: ...
    def insert(self, name: str, oid: _OidArg, attr: int) -> None: ...
    def remove(self, name: str) -> None: ...
    def write(self) -> Oid: ...
    def __len__(self) -> int: ...

class Walker:
    def hide(self, oid: _OidArg) -> None: ...
    def push(self, oid: _OidArg) -> None: ...
    def reset(self) -> None: ...
    def simplify_first_parent(self) -> None: ...
    def sort(self, mode: SortMode) -> None: ...
    def __iter__(self) -> Iterator[Commit]: ...  # Walker: ...
    def __next__(self) -> Commit: ...

class Worktree:
    is_prunable: bool
    name: str
    path: str
    def prune(self, force=False) -> None: ...

def discover_repository(
    path: str | Path, across_fs: bool = False, ceiling_dirs: str = ...
) -> str | None: ...
def hash(data: bytes | str) -> Oid: ...
def hashfile(path: str) -> Oid: ...
def init_file_backend(path: str, flags: int = 0) -> object: ...
def reference_is_valid_name(refname: str) -> bool: ...
def tree_entry_cmp(a: Object, b: Object) -> int: ...
def _cache_enums() -> None: ...
def filter_register(name: str, filter: type[Filter]) -> None: ...
def filter_unregister(name: str) -> None: ...

_OidArg = str | Oid
