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

from typing import Any, Generic, Literal, NewType, SupportsIndex, TypeVar, overload

T = TypeVar('T')

NULL_TYPE = NewType('NULL_TYPE', object)
NULL: NULL_TYPE = ...

char = NewType('char', object)
char_pointer = NewType('char_pointer', object)

class size_t:
    def __getitem__(self, item: Literal[0]) -> int: ...

class int_c:
    def __getitem__(self, item: Literal[0]) -> int: ...

class int64_t:
    def __getitem__(self, item: Literal[0]) -> int: ...

class ssize_t:
    def __getitem__(self, item: Literal[0]) -> int: ...

class _Pointer(Generic[T]):
    def __setitem__(self, item: Literal[0], a: T) -> None: ...
    @overload
    def __getitem__(self, item: Literal[0]) -> T: ...
    @overload
    def __getitem__(self, item: slice[None, None, None]) -> bytes: ...

class _MultiPointer(Generic[T]):
    def __getitem__(self, item: int) -> T: ...

class ArrayC(Generic[T]):
    # incomplete!
    # def _len(self, ?) -> ?: ...
    def __getitem__(self, index: int) -> T: ...
    def __setitem__(self, index: int, value: T) -> None: ...

class GitTimeC:
    # incomplete
    time: int
    offset: int

class GitSignatureC:
    name: char_pointer
    email: char_pointer
    when: GitTimeC

class GitHunkC:
    # incomplete
    boundary: char
    final_start_line_number: int
    final_signature: GitSignatureC
    orig_signature: GitSignatureC
    orig_start_line_number: int
    orig_path: char_pointer
    lines_in_hunk: int

class GitRepositoryC:
    # incomplete
    # TODO: this has to be unified with pygit2._pygit2(pyi).Repository
    # def _from_c(cls, ptr: 'GitRepositoryC', owned: bool) -> 'Repository': ...
    pass

class GitFetchOptionsC:
    # TODO: FetchOptions exist in _pygit2.pyi
    # incomplete
    depth: int

class GitSubmoduleC:
    pass

class GitSubmoduleUpdateOptionsC:
    fetch_opts: GitFetchOptionsC

class GitRemoteHeadC:
    local: int
    oid: GitOidC
    loid: GitOidC
    name: char_pointer
    symref_target: char_pointer

class UnsignedIntC:
    def __getitem__(self, item: Literal[0]) -> int: ...

class GitOidC:
    id: _Pointer[bytes]

class GitBlameOptionsC:
    flags: int
    min_match_characters: int
    newest_commit: object
    oldest_commit: object
    min_line: int
    max_line: int

class GitBlameC:
    # incomplete
    pass

class GitMergeOptionsC:
    file_favor: int
    flags: int
    file_flags: int

class GitAnnotatedCommitC:
    pass

class GitAttrOptionsC:
    # incomplete
    version: int
    flags: int

class GitBufC:
    ptr: char_pointer

class GitCheckoutOptionsC:
    # incomplete
    checkout_strategy: int

class GitCommitC:
    pass

class GitConfigC:
    # incomplete
    pass

class GitConfigIteratorC:
    # incomplete
    pass

class GitConfigEntryC:
    # incomplete
    name: char_pointer
    value: char_pointer
    level: int

class GitDescribeFormatOptionsC:
    version: int
    abbreviated_size: int
    always_use_long_format: int
    dirty_suffix: ArrayC[char]

class GitDescribeOptionsC:
    version: int
    max_candidates_tags: int
    describe_strategy: int
    pattern: ArrayC[char]
    only_follow_first_parent: int
    show_commit_oid_as_fallback: int

class GitDescribeResultC:
    pass

class GitIndexC:
    pass

class GitIndexEntryC:
    # incomplete?
    mode: int
    path: ArrayC[char]

class GitMergeFileResultC:
    pass

class GitObjectC:
    pass

class GitStashSaveOptionsC:
    version: int
    flags: int
    stasher: GitSignatureC
    message: ArrayC[char]
    paths: GitStrrayC

class GitStrrayC:
    # incomplete?
    strings: NULL_TYPE | ArrayC[char_pointer]
    count: int

class GitTreeC:
    pass

class GitRepositoryInitOptionsC:
    version: int
    flags: int
    mode: int
    workdir_path: ArrayC[char]
    description: ArrayC[char]
    template_path: ArrayC[char]
    initial_head: ArrayC[char]
    origin_url: ArrayC[char]

class GitCloneOptionsC:
    pass

class GitPackbuilderC:
    pass

class GitProxyTC:
    pass

class GitProxyOptionsC:
    version: int
    type: GitProxyTC
    url: char_pointer
    # credentials
    # certificate_check
    # payload

class GitRemoteC:
    pass

class GitReferenceC:
    pass

class GitTransactionC:
    pass

def string(a: char_pointer) -> bytes: ...
@overload
def new(a: Literal['git_repository **']) -> _Pointer[GitRepositoryC]: ...
@overload
def new(a: Literal['git_remote **']) -> _Pointer[GitRemoteC]: ...
@overload
def new(a: Literal['git_transaction **']) -> _Pointer[GitTransactionC]: ...
@overload
def new(a: Literal['git_repository_init_options *']) -> GitRepositoryInitOptionsC: ...
@overload
def new(a: Literal['git_submodule_update_options *']) -> GitSubmoduleUpdateOptionsC: ...
@overload
def new(a: Literal['git_submodule **']) -> _Pointer[GitSubmoduleC]: ...
@overload
def new(a: Literal['unsigned int *']) -> UnsignedIntC: ...
@overload
def new(a: Literal['git_proxy_options *']) -> GitProxyOptionsC: ...
@overload
def new(a: Literal['git_oid *']) -> GitOidC: ...
@overload
def new(a: Literal['git_blame **']) -> _Pointer[GitBlameC]: ...
@overload
def new(a: Literal['git_clone_options *']) -> GitCloneOptionsC: ...
@overload
def new(a: Literal['git_merge_options *']) -> GitMergeOptionsC: ...
@overload
def new(a: Literal['git_blame_options *']) -> GitBlameOptionsC: ...
@overload
def new(a: Literal['git_annotated_commit **']) -> _Pointer[GitAnnotatedCommitC]: ...
@overload
def new(a: Literal['git_attr_options *']) -> GitAttrOptionsC: ...
@overload
def new(a: Literal['git_buf *']) -> GitBufC: ...
@overload
def new(a: Literal['char *'], b: bytes) -> char_pointer: ...
@overload
def new(a: Literal['char *[]'], b: list[char_pointer]) -> ArrayC[char_pointer]: ...
@overload
def new(a: Literal['git_checkout_options *']) -> GitCheckoutOptionsC: ...
@overload
def new(a: Literal['git_commit **']) -> _Pointer[GitCommitC]: ...
@overload
def new(a: Literal['git_config *']) -> GitConfigC: ...
@overload
def new(a: Literal['git_config **']) -> _Pointer[GitConfigC]: ...
@overload
def new(a: Literal['git_config_iterator **']) -> _Pointer[GitConfigIteratorC]: ...
@overload
def new(a: Literal['git_config_entry **']) -> _Pointer[GitConfigEntryC]: ...
@overload
def new(a: Literal['git_describe_format_options *']) -> GitDescribeFormatOptionsC: ...
@overload
def new(a: Literal['git_describe_options *']) -> GitDescribeOptionsC: ...
@overload
def new(a: Literal['git_describe_result *']) -> GitDescribeResultC: ...
@overload
def new(a: Literal['git_describe_result **']) -> _Pointer[GitDescribeResultC]: ...
@overload
def new(a: Literal['struct git_reference **']) -> _Pointer[GitReferenceC]: ...
@overload
def new(a: Literal['git_index **']) -> _Pointer[GitIndexC]: ...
@overload
def new(a: Literal['git_index_entry *']) -> GitIndexEntryC: ...
@overload
def new(a: Literal['git_merge_file_result *']) -> GitMergeFileResultC: ...
@overload
def new(a: Literal['git_object *']) -> GitObjectC: ...
@overload
def new(a: Literal['git_object **']) -> _Pointer[GitObjectC]: ...
@overload
def new(a: Literal['git_packbuilder **']) -> _Pointer[GitPackbuilderC]: ...
@overload
def new(a: Literal['git_signature *']) -> GitSignatureC: ...
@overload
def new(a: Literal['git_signature **']) -> _Pointer[GitSignatureC]: ...
@overload
def new(a: Literal['int *']) -> int_c: ...
@overload
def new(a: Literal['int64_t *']) -> int64_t: ...
@overload
def new(
    a: Literal['git_remote_head ***'],
) -> _Pointer[_MultiPointer[GitRemoteHeadC]]: ...
@overload
def new(a: Literal['size_t *', 'size_t*']) -> size_t: ...
@overload
def new(a: Literal['ssize_t *', 'ssize_t*']) -> ssize_t: ...
@overload
def new(a: Literal['git_stash_save_options *']) -> GitStashSaveOptionsC: ...
@overload
def new(a: Literal['git_strarray *']) -> GitStrrayC: ...
@overload
def new(a: Literal['git_tree **']) -> _Pointer[GitTreeC]: ...
@overload
def new(a: Literal['git_buf *'], b: tuple[NULL_TYPE, Literal[0]]) -> GitBufC: ...
@overload
def new(a: Literal['char **']) -> _Pointer[char_pointer]: ...
@overload
def new(a: Literal['void **'], b: bytes) -> _Pointer[bytes]: ...
@overload
def new(a: Literal['char[]', 'char []'], b: bytes | NULL_TYPE) -> ArrayC[char]: ...
@overload
def new(
    a: Literal['char *[]'], b: int
) -> ArrayC[char_pointer]: ...  # For ext_array in SET_EXTENSIONS
@overload
def new(
    a: Literal['char *[]'], b: list[Any]
) -> ArrayC[char_pointer]: ...  # For string arrays
def addressof(a: object, attribute: str) -> _Pointer[object]: ...

class buffer(bytes):
    def __init__(self, a: object) -> None: ...
    def __setitem__(self, item: slice[None, None, None], value: bytes) -> None: ...
    @overload
    def __getitem__(self, item: SupportsIndex) -> int: ...
    @overload
    def __getitem__(self, item: slice[Any, Any, Any]) -> bytes: ...

@overload
def cast(a: Literal['int'], b: object) -> int: ...
@overload
def cast(a: Literal['unsigned int'], b: object) -> int: ...
@overload
def cast(a: Literal['size_t'], b: object) -> int: ...
@overload
def cast(a: Literal['ssize_t'], b: object) -> int: ...
@overload
def cast(a: Literal['char *'], b: object) -> char_pointer: ...
