# https://mypy.readthedocs.io/en/stable/common_issues.html#using-classes-that-are-generic-in-stubs-but-not-at-runtime
from __future__ import annotations

import binascii
import collections
import concurrent.futures
import contextlib
import hashlib
import io
import itertools
import math
import multiprocessing as mp
import os
import re
import shutil
import stat as stat_module
import tempfile
import time
import urllib.parse
from functools import partial
from types import TracebackType
from typing import (
    Any,
    BinaryIO,
    Callable,
    Iterator,
    Literal,
    NamedTuple,
    Sequence,
    TextIO,
    cast,
    overload,
)

import filelock
import urllib3

from blobfile import _azure as azure
from blobfile import _common as common
from blobfile import _gcp as gcp
from blobfile._common import (
    CHUNK_SIZE,
    DEFAULT_CONNECTION_POOL_MAX_SIZE,
    DEFAULT_MAX_CONNECTION_POOL_COUNT,
    Config,
    DirEntry,
    Error,
    RemoteOrLocalPath,
    Request,
    RestartableStreamingWriteFailure,
    Stat,
    get_log_threshold_for_error,
    path_to_str,
)

# https://cloud.google.com/storage/docs/naming
# https://www.w3.org/TR/xml/#charsets
INVALID_CHARS = set().union(range(0x0, 0x9)).union(range(0xB, 0xE)).union(range(0xE, 0x20))

DEFAULT_AZURE_WRITE_CHUNK_SIZE = 8 * 2**20
DEFAULT_GOOGLE_WRITE_CHUNK_SIZE = 8 * 2**20
DEFAULT_RETRY_LOG_THRESHOLD = 0
DEFAULT_RETRY_COMMON_LOG_THRESHOLD = 2
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_READ_TIMEOUT = 30
DEFAULT_BUFFER_SIZE = 8 * 2**20
DEFAULT_USE_BLIND_WRITES = os.getenv("BLOBFILE_USE_BLIND_WRITES", "0") == "1"


def _execute_fn_and_ignore_result(fn: Callable[..., object], *args: Any):
    fn(*args)


class Context:
    def __init__(self, conf: Config):
        self._conf = conf

    def copy(
        self,
        src: RemoteOrLocalPath,
        dst: RemoteOrLocalPath,
        overwrite: bool = False,
        parallel: bool = False,
        parallel_executor: concurrent.futures.Executor | None = None,
        return_md5: bool = False,
        dst_version: str | None = None,
    ) -> str | None:
        src = path_to_str(src)
        dst = path_to_str(dst)
        # it would be best to check isdir() for remote paths, but that would
        # involve 2 extra network requests, so just do this test instead
        if _guess_isdir(src):
            raise IsADirectoryError(f"Is a directory: '{src}'")
        if _guess_isdir(dst):
            raise IsADirectoryError(f"Is a directory: '{dst}'")

        if not overwrite:
            if self.exists(dst):
                raise FileExistsError(
                    f"Destination '{dst}' already exists and overwrite is disabled"
                )

        if dst_version is not None:
            assert _is_azure_path(
                dst
            ), f"Destination version was specified, but destination path {dst} does not support a version check"

        if parallel:
            copy_fn = None
            if (_is_azure_path(src) or _is_gcp_path(src)) and _is_local_path(dst):
                copy_fn = _parallel_download

            if _is_local_path(src) and _is_azure_path(dst):
                copy_fn = partial(azure.parallel_upload, dst_version=dst_version)

            if _is_local_path(src) and _is_gcp_path(dst):
                copy_fn = gcp.parallel_upload

            if _is_azure_path(src) and _is_azure_path(dst):
                src_account, _, _ = azure.split_path(src)
                dst_account, _, _ = azure.split_path(dst)
                if src_account != dst_account:
                    # normal remote copy is pretty fast and doesn't benefit from parallelization when used within
                    # a storage account
                    copy_fn = partial(azure.parallel_remote_copy, dst_version=dst_version)

            if copy_fn is not None:
                if parallel_executor is None:
                    with concurrent.futures.ProcessPoolExecutor(
                        mp_context=mp.get_context(self._conf.multiprocessing_start_method)
                    ) as executor:
                        return copy_fn(self._conf, executor, src, dst, return_md5=return_md5)
                else:
                    return copy_fn(self._conf, parallel_executor, src, dst, return_md5=return_md5)

        # special case cloud to cloud copy, don't download the file
        if _is_gcp_path(src) and _is_gcp_path(dst):
            return gcp.remote_copy(self._conf, src=src, dst=dst, return_md5=return_md5)

        if _is_azure_path(src) and _is_azure_path(dst):
            # support could be added here for this but it's currently missing
            assert (
                dst_version is None
            ), f"Destination version was specified, but destination path {dst} does not support a version check"
            return azure.remote_copy(self._conf, src=src, dst=dst, return_md5=return_md5)

        for attempt, backoff in enumerate(common.exponential_sleep_generator()):
            try:
                with (
                    self.BlobFile(src, "rb", streaming=True) as src_f,
                    self.BlobFile(
                        dst, "wb", streaming=True, version=dst_version, partial_writes_on_exc=False
                    ) as dst_f,
                ):
                    m = hashlib.md5()
                    while True:
                        block = src_f.read(CHUNK_SIZE)
                        if block == b"":
                            break
                        if return_md5:
                            m.update(block)
                        dst_f.write(block)
                    if return_md5:
                        return m.hexdigest()
                    else:
                        return None
            except RestartableStreamingWriteFailure as err:
                # currently this is the only type of failure we retry, since we can re-read the source
                # stream from the beginning
                # if this failure occurs, the upload must be restarted from the beginning
                # https://cloud.google.com/storage/docs/resumable-uploads#practices
                # https://github.com/googleapis/gcs-resumable-upload/issues/15#issuecomment-249324122
                if self._conf.retry_limit is not None and attempt >= self._conf.retry_limit:
                    raise

                if attempt >= get_log_threshold_for_error(self._conf, str(err)):
                    self._conf.log_callback(
                        f"error {err} when executing a streaming write to {dst} attempt {attempt}, sleeping for {backoff:.1f} seconds before retrying"
                    )
                time.sleep(backoff)

    def exists(self, path: RemoteOrLocalPath) -> bool:
        path = path_to_str(path)
        if _is_local_path(path):
            return os.path.exists(path)
        elif _is_gcp_path(path):
            st = gcp.maybe_stat(self._conf, path)
            if st is not None:
                return True
            return self.isdir(path)
        elif _is_azure_path(path):
            st = azure.maybe_stat(self._conf, path)
            if st is not None:
                return True
            return self.isdir(path)
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def basename(self, path: RemoteOrLocalPath) -> str:
        path = path_to_str(path)
        if _is_gcp_path(path):
            _, obj = gcp.split_path(path)
            return obj.split("/")[-1]
        elif _is_azure_path(path):
            _, _, obj = azure.split_path(path)
            return obj.split("/")[-1]
        else:
            return os.path.basename(path)

    def glob(self, pattern: str, parallel: bool = False) -> Iterator[str]:
        if _is_local_path(pattern):
            # scanglob currently does an os.stat for each matched file
            # until scanglob can be implemented directly on scandir
            # this code is here to avoid that
            if "?" in pattern or "[" in pattern or "]" in pattern:
                raise Error("Advanced glob queries are not supported")
            yield from _local_glob(pattern)
        else:
            for entry in self.scanglob(pattern=pattern, parallel=parallel):
                yield entry.path

    def scanglob(
        self, pattern: str, parallel: bool = False, shard_prefix_length: int = 0
    ) -> Iterator[DirEntry]:
        if "?" in pattern or "[" in pattern or "]" in pattern:
            raise Error("Advanced glob queries are not supported")

        if _is_local_path(pattern):
            for filepath in _local_glob(pattern):
                # doing a stat call for each file isn't the most efficient
                # iglob uses os.scandir internally, but doesn't expose the information from that, so we'd
                # need to re-implement local glob
                # we could make the behavior with remote glob more consistent though if we did that
                s = os.stat(filepath)
                is_dir = stat_module.S_ISDIR(s.st_mode)
                yield DirEntry(
                    path=filepath,
                    name=self.basename(filepath),
                    is_dir=is_dir,
                    is_file=not is_dir,
                    stat=(
                        None
                        if is_dir
                        else Stat(
                            size=s.st_size,
                            mtime=s.st_mtime,
                            ctime=s.st_ctime,
                            md5=None,
                            version=None,
                        )
                    ),
                )
        elif _is_gcp_path(pattern) or _is_azure_path(pattern):
            if "*" not in pattern:
                entry = _get_entry(self._conf, pattern)
                if entry is not None:
                    yield entry
                return

            if _is_gcp_path(pattern):
                bucket, blob_prefix = gcp.split_path(pattern)
                if "*" in bucket:
                    raise Error("Wildcards cannot be used in bucket name")
                root = gcp.combine_path(bucket, "")
            else:
                account, container, blob_prefix = azure.split_path(pattern)
                if "*" in account or "*" in container:
                    raise Error("Wildcards cannot be used in account or container")
                root = azure.combine_path(self._conf, account, container, "")

            if shard_prefix_length == 0:
                initial_tasks = [_GlobTask("", _split_path(blob_prefix))]
            else:
                assert (
                    parallel
                ), "You probably want to use parallel=True if you are setting shard_prefix_length > 0"
                initial_tasks = []
                valid_chars = [
                    i for i in range(256) if i not in INVALID_CHARS.union([ord("/"), ord("*")])
                ]
                pattern_prefix, pattern_suffix = blob_prefix.split("*", maxsplit=1)
                for repeat in range(1, shard_prefix_length + 1):
                    for chars in itertools.product(valid_chars, repeat=repeat):
                        prefix = ""
                        for c in chars:
                            prefix += chr(c)
                        # we need to check for exact matches for shorter prefix lengths
                        # if we only searched for prefixes of length `shard_prefix_length`
                        # we would skip shorter names, for instance "a" would be skipped if we
                        # we had `shard_prefix_length=2`
                        # instead we check for an exact match for everything shorter than
                        # our `shard_prefix_length`
                        exact = repeat != shard_prefix_length
                        if exact:
                            pat = pattern_prefix + prefix
                        else:
                            pat = pattern_prefix + prefix + "*" + pattern_suffix
                        initial_tasks.append(_GlobTask("", _split_path(pat)))

            if parallel:
                mp_ctx = mp.get_context(self._conf.multiprocessing_start_method)
                tasks = mp_ctx.Queue()
                for t in initial_tasks:
                    tasks.put(t)
                tasks_enqueued = len(initial_tasks)
                results = mp_ctx.Queue()

                tasks_done = 0
                with mp_ctx.Pool(
                    initializer=_glob_worker, initargs=(self._conf, root, tasks, results)
                ):
                    while tasks_done < tasks_enqueued:
                        r = results.get()
                        if isinstance(r, _GlobEntry):
                            yield r.entry
                        elif isinstance(r, _GlobTask):
                            tasks.put(r)
                            tasks_enqueued += 1
                        elif isinstance(r, _GlobTaskComplete):
                            tasks_done += 1
                        else:
                            raise Error("Invalid result")
            else:
                dq: collections.deque[_GlobTask] = collections.deque()
                for t in initial_tasks:
                    dq.append(t)
                while len(dq) > 0:
                    t = dq.popleft()
                    for r in _process_glob_task(conf=self._conf, root=root, t=t):
                        if isinstance(r, _GlobEntry):
                            yield r.entry
                        else:
                            dq.append(r)
        else:
            raise Error(f"Unrecognized path '{pattern}'")

    def isdir(self, path: RemoteOrLocalPath) -> bool:
        path = path_to_str(path)
        if _is_local_path(path):
            return os.path.isdir(path)
        elif _is_gcp_path(path):
            return gcp.isdir(self._conf, path)
        elif _is_azure_path(path):
            return azure.isdir(self._conf, path)
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def listdir(self, path: RemoteOrLocalPath, shard_prefix_length: int = 0) -> Iterator[str]:
        path = path_to_str(path)
        for entry in self.scandir(path, shard_prefix_length=shard_prefix_length):
            yield entry.name

    def scandir(self, path: RemoteOrLocalPath, shard_prefix_length: int = 0) -> Iterator[DirEntry]:
        path = path_to_str(path)
        if (_is_gcp_path(path) or _is_azure_path(path)) and not path.endswith("/"):
            path += "/"
        if not self.exists(path):
            raise FileNotFoundError(f"The system cannot find the path specified: '{path}'")
        if not self.isdir(path):
            raise NotADirectoryError(f"The directory name is invalid: '{path}'")
        if _is_local_path(path):
            for de in os.scandir(path):
                if de.is_dir():
                    yield DirEntry(
                        name=de.name,
                        path=os.path.abspath(de.path),
                        is_dir=True,
                        is_file=False,
                        stat=None,
                    )
                else:
                    s = de.stat()
                    yield DirEntry(
                        name=de.name,
                        path=os.path.abspath(de.path),
                        is_dir=False,
                        is_file=True,
                        stat=Stat(
                            size=s.st_size,
                            mtime=s.st_mtime,
                            ctime=s.st_ctime,
                            md5=None,
                            version=None,
                        ),
                    )
        elif _is_gcp_path(path) or _is_azure_path(path):
            if shard_prefix_length == 0:
                yield from _list_blobs_in_dir(conf=self._conf, prefix=path, exclude_prefix=True)
            else:
                mp_ctx = mp.get_context(self._conf.multiprocessing_start_method)
                prefixes = mp_ctx.Queue()
                items = mp_ctx.Queue()
                tasks_enqueued = 0

                valid_chars = [i for i in range(256) if i not in INVALID_CHARS and i != ord("/")]
                for repeat in range(1, shard_prefix_length + 1):
                    for chars in itertools.product(valid_chars, repeat=repeat):
                        prefix = ""
                        for c in chars:
                            prefix += chr(c)
                        # we need to check for exact matches for shorter prefix lengths
                        # if we only searched for prefixes of length `shard_prefix_length`
                        # we would skip shorter names, for instance "a" would be skipped if we
                        # we had `shard_prefix_length=2`
                        # instead we check for an exact match for everything shorter than
                        # our `shard_prefix_length`
                        exact = repeat != shard_prefix_length
                        prefixes.put((path, prefix, exact))
                        tasks_enqueued += 1

                tasks_done = 0
                with mp_ctx.Pool(
                    initializer=_sharded_listdir_worker, initargs=(self._conf, prefixes, items)
                ):
                    while tasks_done < tasks_enqueued:
                        entry = items.get()
                        if entry is None:
                            tasks_done += 1
                            continue
                        yield entry
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def makedirs(self, path: RemoteOrLocalPath) -> None:
        path = path_to_str(path)
        if _is_local_path(path):
            os.makedirs(path, exist_ok=True)
        elif _is_gcp_path(path):
            gcp.mkdirfile(self._conf, path)
        elif _is_azure_path(path):
            azure.mkdirfile(self._conf, path)
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def remove(self, path: RemoteOrLocalPath) -> None:
        path = path_to_str(path)
        if _is_local_path(path):
            os.remove(path)
        elif _is_gcp_path(path):
            if path.endswith("/"):
                raise IsADirectoryError(f"Is a directory: '{path}'")
            ok = gcp.remove(self._conf, path)
            if not ok:
                raise FileNotFoundError(f"The system cannot find the path specified: '{path}'")
        elif _is_azure_path(path):
            if path.endswith("/"):
                raise IsADirectoryError(f"Is a directory: '{path}'")
            ok = azure.remove(self._conf, path)
            if not ok:
                raise FileNotFoundError(f"The system cannot find the path specified: '{path}'")
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def rmdir(self, path: RemoteOrLocalPath) -> None:
        path = path_to_str(path)
        if _is_local_path(path):
            os.rmdir(path)
            return

        # directories in blob storage are different from normal directories
        # a directory exists if there are any blobs that have that directory as a prefix
        # when the last blob with that prefix is deleted, the directory no longer exists
        # except in the case when there is a blob with a name ending in a slash
        # representing an empty directory

        # to make this more usable it is not an error to delete a directory that does
        # not exist, but is still an error to delete a non-empty one
        if not path.endswith("/"):
            path += "/"

        if _is_gcp_path(path):
            _, blob = gcp.split_path(path)
        elif _is_azure_path(path):
            _, _, blob = azure.split_path(path)
        else:
            raise Error(f"Unrecognized path: '{path}'")

        if blob == "":
            raise Error(f"Cannot delete bucket: '{path}'")
        it = self.listdir(path)
        try:
            next(it)
        except FileNotFoundError:
            # this directory does not exist
            return
        except StopIteration:
            # this directory exists and is empty
            pass
        else:
            # this directory exists but is not empty
            raise OSError(f"The directory is not empty: '{path}'")

        if _is_gcp_path(path):
            bucket, blob = gcp.split_path(path)
            req = Request(
                url=gcp.build_url("/storage/v1/b/{bucket}/o/{object}", bucket=bucket, object=blob),
                method="DELETE",
                success_codes=(204,),
            )
            gcp.execute_api_request(self._conf, req)
        elif _is_azure_path(path):
            account, container, blob = azure.split_path(path)
            req = Request(
                url=azure.build_url(account, "/{container}/{blob}", container=container, blob=blob),
                method="DELETE",
                success_codes=(202,),
            )
            azure.execute_api_request(self._conf, req)
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def stat(self, path: RemoteOrLocalPath) -> Stat:
        path = path_to_str(path)
        if _is_local_path(path):
            s = os.stat(path)
            return Stat(size=s.st_size, mtime=s.st_mtime, ctime=s.st_ctime, md5=None, version=None)
        elif _is_gcp_path(path):
            st = gcp.maybe_stat(self._conf, path)
            if st is None:
                raise FileNotFoundError(f"No such file: '{path}'")
            return st
        elif _is_azure_path(path):
            st = azure.maybe_stat(self._conf, path)
            if st is None:
                raise FileNotFoundError(f"No such file: '{path}'")
            return st
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def set_mtime(self, path: RemoteOrLocalPath, mtime: float, version: str | None = None) -> bool:
        path = path_to_str(path)
        if _is_local_path(path):
            assert version is None
            os.utime(path, times=(mtime, mtime))
            return True
        elif _is_gcp_path(path):
            return gcp.set_mtime(self._conf, path=path, mtime=mtime, version=version)
        elif _is_azure_path(path):
            return azure.set_mtime(self._conf, path=path, mtime=mtime, version=version)
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def rmtree(
        self,
        path: RemoteOrLocalPath,
        parallel: bool = False,
        parallel_executor: concurrent.futures.Executor | None = None,
    ) -> None:
        path = path_to_str(path)
        if not self.isdir(path):
            raise NotADirectoryError(f"The directory name is invalid: '{path}'")

        if _is_local_path(path):
            shutil.rmtree(path)
        elif _is_gcp_path(path) or _is_azure_path(path):
            if not path.endswith("/"):
                path += "/"

            if _is_gcp_path(path):

                def request_generator():
                    bucket, blob = gcp.split_path(path)
                    for entry in gcp.list_blobs(self._conf, path):
                        entry_slash_path = _get_slash_path(entry)
                        entry_bucket, entry_blob = gcp.split_path(entry_slash_path)
                        assert entry_bucket == bucket and entry_blob.startswith(blob)
                        req = Request(
                            url=gcp.build_url(
                                "/storage/v1/b/{bucket}/o/{object}",
                                bucket=bucket,
                                object=entry_blob,
                            ),
                            method="DELETE",
                            # 404 is allowed in case a failed request successfully deleted the file
                            # before erroring out
                            success_codes=(204, 404),
                        )
                        yield req

                fn = gcp.execute_api_request

            elif _is_azure_path(path):

                def request_generator():
                    account, container, blob = azure.split_path(path)
                    for entry in azure.list_blobs(self._conf, path):
                        entry_slash_path = _get_slash_path(entry)
                        entry_account, entry_container, entry_blob = azure.split_path(
                            entry_slash_path
                        )
                        assert (
                            entry_account == account
                            and entry_container == container
                            and entry_blob.startswith(blob)
                        )
                        req = Request(
                            url=azure.build_url(
                                account, "/{container}/{blob}", container=container, blob=entry_blob
                            ),
                            method="DELETE",
                            # 404 is allowed in case a failed request successfully deleted the file
                            # before erroring out
                            success_codes=(202, 404),
                        )
                        yield req

                fn = azure.execute_api_request
            else:
                raise Error(f"Unrecognized path: '{path}'")

            if parallel:
                if parallel_executor is None:
                    executor = concurrent.futures.ProcessPoolExecutor(
                        mp_context=mp.get_context(self._conf.multiprocessing_start_method)
                    )
                    context = executor
                else:
                    executor = parallel_executor
                    context = contextlib.nullcontext()

                futures = []
                with context:
                    for req in request_generator():
                        f = executor.submit(_execute_fn_and_ignore_result, fn, self._conf, req)
                        futures.append(f)
                for f in futures:
                    f.result()
            else:
                for req in request_generator():
                    fn(self._conf, req)
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def walk(
        self,
        top: RemoteOrLocalPath,
        topdown: bool = True,
        onerror: Callable[[OSError], None] | None = None,
    ) -> Iterator[tuple[str, Sequence[str], Sequence[str]]]:
        top = path_to_str(top)
        if not self.isdir(top):
            return

        if _is_local_path(top):
            top = os.path.normpath(top)
            for root, dirnames, filenames in os.walk(top=top, topdown=topdown, onerror=onerror):
                assert isinstance(root, str)
                if root.endswith(os.sep):
                    root = root[:-1]
                yield (root, sorted(dirnames), sorted(filenames))
        elif _is_gcp_path(top) or _is_azure_path(top):
            top = _normalize_path(self._conf, top)
            if not top.endswith("/"):
                top += "/"
            if topdown:
                dq: collections.deque[str] = collections.deque()
                dq.append(top)
                while len(dq) > 0:
                    cur = dq.popleft()
                    assert cur.endswith("/")
                    if _is_gcp_path(top):
                        it = gcp.list_blobs(self._conf, cur, delimiter="/")
                    elif _is_azure_path(top):
                        it = azure.list_blobs(self._conf, cur, delimiter="/")
                    else:
                        raise Error(f"Unrecognized path: '{top}'")
                    dirnames = []
                    filenames = []
                    for entry in it:
                        entry_path = _get_slash_path(entry)
                        if entry_path == cur:
                            continue
                        if entry.is_dir:
                            dirnames.append(entry.name)
                        else:
                            filenames.append(entry.name)
                    yield (_strip_slash(cur), dirnames, filenames)
                    dq.extend(self.join(cur, dirname) + "/" for dirname in dirnames)
            else:
                if _is_gcp_path(top):
                    it = gcp.list_blobs(self._conf, top)
                elif _is_azure_path(top):
                    it = azure.list_blobs(self._conf, top)
                else:
                    raise Error(f"Unrecognized path: '{top}'")

                cur = []
                dirnames_stack = [[]]
                filenames_stack = [[]]
                for entry in it:
                    entry_slash_path = _get_slash_path(entry)
                    if entry_slash_path == top:
                        continue
                    relpath = entry_slash_path[len(top) :]
                    parts = relpath.split("/")
                    dirpath = parts[:-1]
                    if dirpath != cur:
                        # pop directories from the current path until we match the prefix of this new path
                        while cur != dirpath[: len(cur)]:
                            yield (top + "/".join(cur), dirnames_stack.pop(), filenames_stack.pop())
                            cur.pop()
                        # push directories from the new path until the current path matches it
                        while cur != dirpath:
                            dirname = dirpath[len(cur)]
                            cur.append(dirname)
                            filenames_stack.append([])
                            # add this to child dir to the list of dirs for the parent
                            dirnames_stack[-1].append(dirname)
                            dirnames_stack.append([])
                    if entry.is_file:
                        filenames_stack[-1].append(entry.name)
                while len(cur) > 0:
                    yield (top + "/".join(cur), dirnames_stack.pop(), filenames_stack.pop())
                    cur.pop()
                yield (_strip_slash(top), dirnames_stack.pop(), filenames_stack.pop())
                assert len(dirnames_stack) == 0 and len(filenames_stack) == 0
        else:
            raise Error(f"Unrecognized path: '{top}'")

    def dirname(self, path: RemoteOrLocalPath) -> str:
        path = path_to_str(path)
        if _is_gcp_path(path):
            return gcp.dirname(self._conf, path)
        elif _is_azure_path(path):
            return azure.dirname(self._conf, path)
        else:
            return os.path.dirname(path)

    def join(self, a: RemoteOrLocalPath, *args: str) -> str:
        a = path_to_str(a)
        out = a
        for b in args:
            out = _join2(self._conf, out, b)
        return out

    def get_url(self, path: RemoteOrLocalPath) -> tuple[str, float | None]:
        path = path_to_str(path)
        if _is_gcp_path(path):
            return gcp.get_url(self._conf, path)
        elif _is_azure_path(path):
            return azure.get_url(self._conf, path)
        elif _is_local_path(path):
            return f"file://{path}", None
        else:
            raise Error(f"Unrecognized path: '{path}'")

    def md5(self, path: RemoteOrLocalPath) -> str:
        path = path_to_str(path)
        if _is_gcp_path(path):
            st = gcp.maybe_stat(self._conf, path)
            if st is None:
                raise FileNotFoundError(f"No such file: '{path}'")

            h = st.md5
            if h is not None:
                return h

            # this is probably a composite object, calculate the md5 and store it on the file if the file has not changed
            with self.BlobFile(path, "rb") as f:
                result = common.block_md5(f).hex()

            assert st.version is not None
            gcp.maybe_update_md5(self._conf, path, st.version, result)
            return result
        elif _is_azure_path(path):
            st = azure.maybe_stat(self._conf, path)
            if st is None:
                raise FileNotFoundError(f"No such file: '{path}'")
            # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties
            h = st.md5
            if h is None:
                # md5 is missing, calculate it and store it on file if the file has not changed
                with self.BlobFile(path, "rb") as f:
                    h = common.block_md5(f).hex()
                assert st.version is not None
                azure.maybe_update_md5(self._conf, path, st.version, h)
            return h
        else:
            with self.BlobFile(path, "rb") as f:
                return common.block_md5(f).hex()

    def last_version_seen(self, file: TextIO | BinaryIO) -> str | None:
        actual: object
        actual = file
        if isinstance(actual, io.TextIOWrapper):
            actual = actual.buffer
        if isinstance(actual, (io.BufferedReader, io.BufferedWriter)):
            actual = actual.raw
        if not isinstance(actual, (azure.StreamingReadFile, azure.StreamingWriteFile)):
            raise ValueError("File was not an Azure BlobFile opened in streaming mode")
        return actual._version  # pyright: ignore[reportPrivateUsage]

    def read_text(self, path: RemoteOrLocalPath) -> str:
        with self.BlobFile(path, "r") as f:
            return f.read()

    def read_bytes(self, path: RemoteOrLocalPath) -> bytes:
        with self.BlobFile(path, "rb") as f:
            return f.read()

    def write_text(self, path: RemoteOrLocalPath, text: str) -> None:
        with self.BlobFile(path, "w", partial_writes_on_exc=False) as f:
            f.write(text)

    def write_bytes(self, path: RemoteOrLocalPath, data: bytes) -> None:
        with self.BlobFile(path, "wb", partial_writes_on_exc=False) as f:
            f.write(data)

    @overload
    def BlobFile(
        self,
        path: RemoteOrLocalPath,
        mode: Literal["rb", "wb", "ab"],
        streaming: bool | None = ...,
        buffer_size: int | None = ...,
        cache_dir: str | None = ...,
        file_size: int | None = None,
        version: str | None = None,
        partial_writes_on_exc: bool = True,
    ) -> BinaryIO: ...

    @overload
    def BlobFile(
        self,
        path: RemoteOrLocalPath,
        mode: Literal["r", "w", "a"] = ...,
        streaming: bool | None = ...,
        buffer_size: int | None = ...,
        cache_dir: str | None = ...,
        file_size: int | None = None,
        version: str | None = None,
        partial_writes_on_exc: bool = True,
    ) -> TextIO: ...

    def BlobFile(
        self,
        path: RemoteOrLocalPath,
        mode: Literal["r", "rb", "w", "wb", "a", "ab"] = "r",
        streaming: bool | None = None,
        buffer_size: int | None = None,
        cache_dir: str | None = None,
        file_size: int | None = None,
        version: str | None = None,
        partial_writes_on_exc: bool = True,
    ):
        """
        Open a local or remote file for reading or writing

        Args:
            path: local or remote path
            mode: one of "r", "rb", "w", "wb", "a", "ab" indicating the mode to open the file in
            streaming: the default for `streaming` is `True` when `mode` is in `"r", "rb"` and `False` when `mode` is in `"w", "wb", "a", "ab"`.
                * `streaming=True`:
                    * Reading is done without downloading the entire remote file.
                    * Writing is done to the remote file directly, but only in chunks of a few MB in size. `flush()` will not cause an early write.
                    * Appending is not implemented.
                * `streaming=False`:
                    * Reading is done by downloading the remote file to a local file during the constructor.
                    * Writing is done by uploading the file on `close()` or during destruction.
                    * Appending is done by downloading the file during construction and uploading on `close()` or during destruction.
            buffer_size: number of bytes to buffer, this can potentially make reading more efficient.
            cache_dir: a directory in which to cache files for reading, only valid if `streaming=False` and `mode` is in `"r", "rb"`.   You are reponsible for cleaning up the cache directory.
            file_size: size of the file being opened, can be specified directly to avoid checking the file size when opening the file.  While this will avoid a network request, it also means that you may get an error when first reading a file that does not exist rather than when opening it.  Only valid for modes "r" and "rb".  This valid will be ignored for local files.
            version: a version number of the file being opened, used to prevent overwriting a file that has changed since it was opened.  Only valid for modes "w", "wb", "a", "ab"
            partial_writes_on_exc: whether to write partially-written data to the file if an exception occurs. Only valid for writing modes, and otherwise ignored. For backwards compatibility, the default is True.

        Returns:
            A file-like object
        """
        path = path_to_str(path)
        if _guess_isdir(path):
            raise IsADirectoryError(f"Is a directory: '{path}'")

        if streaming is None:
            streaming = mode in ("r", "rb")

        if file_size is not None:
            assert mode in ("r", "rb"), "Can only specify file_size when reading"

        if version:
            assert not _is_local_path(path), "Cannot specify version when writing to local file"
            # we check for gcp later to raise a NotImplementedError instead

        if _is_local_path(path) and "w" in mode:
            # local filesystems require that intermediate directories exist, but this is not required by the
            # remote filesystems
            # for consistency, automatically create local intermediate directories
            if self.dirname(path) != "":
                self.makedirs(self.dirname(path))

        if buffer_size is None:
            buffer_size = self._conf.default_buffer_size

        if streaming:
            if mode not in ("w", "wb", "r", "rb"):
                raise Error(f"Invalid mode for streaming file: '{mode}'")
            if cache_dir is not None:
                raise Error("Cannot specify cache_dir for streaming files")
            if _is_local_path(path):
                # Note: io.FileIO eagerly creates the file if it doesn't exist. This behavior is different from the
                # cloud file implementations below, where the file is only created once the chunks are finalized and
                # committed.
                f = io.FileIO(path, mode=mode)
                if "r" in mode:
                    f = io.BufferedReader(f, buffer_size=buffer_size)
                else:
                    # We do not need to use _BufferedWriterForProxyFile, which is only needed for the case that the
                    # underlying file is remote.
                    f = io.BufferedWriter(f, buffer_size=buffer_size)
            elif _is_gcp_path(path):
                if version:
                    raise NotImplementedError("Cannot specify version for GCP files")
                if mode in ("w", "wb"):
                    f = gcp.StreamingWriteFile(
                        self._conf, path, partial_writes_on_exc=partial_writes_on_exc
                    )
                elif mode in ("r", "rb"):
                    f = gcp.StreamingReadFile(self._conf, path, size=file_size)
                    f = io.BufferedReader(f, buffer_size=buffer_size)
                else:
                    raise Error(f"Unsupported mode: '{mode}'")
            elif _is_azure_path(path):
                if mode in ("w", "wb"):
                    f = azure.StreamingWriteFile(
                        self._conf, path, version, partial_writes_on_exc=partial_writes_on_exc
                    )
                elif mode in ("r", "rb"):
                    f = azure.StreamingReadFile(self._conf, path, size=file_size, version=version)
                    f = io.BufferedReader(f, buffer_size=buffer_size)
                else:
                    raise Error(f"Unsupported mode: '{mode}'")
            else:
                raise Error(f"Unrecognized path: '{path}'")

            # this should be a protocol so we don't have to cast
            # but the standard library does not seem to have a file-like protocol
            binary_f = cast(BinaryIO, f)
            if "b" in mode:
                return binary_f
            else:
                text_f = io.TextIOWrapper(binary_f, encoding="utf8")
                # TextIOWrapper bypasses buffering on purpose: https://bugs.python.org/issue13393
                # Example: https://gist.github.com/christopher-hesse/b4aab4f6f9bcba597d079f3363dfab2c
                #
                # This happens when TextIOWrapper calls f.read1(CHUNK_SIZE)
                # https://github.com/python/cpython/blob/3d17c045b4c3d09b72bbd95ed78af1ae6f0d98d2/Modules/_io/textio.c#L1854
                # and BufferedReader only reads the requested size, not the buffer_size
                # https://github.com/python/cpython/blob/8666356280084f0426c28a981341f72eaaacd006/Modules/_io/bufferedio.c#L945
                #
                # The workaround appears to be to set the _CHUNK_SIZE property or monkey patch binary_f.read1 to call binary_f.read
                if hasattr(text_f, "_CHUNK_SIZE"):
                    setattr(text_f, "_CHUNK_SIZE", buffer_size)
                return cast(TextIO, text_f)
        else:
            remote_path = None
            tmp_dir = None
            if mode not in ("w", "wb", "r", "rb", "a", "ab"):
                raise Error(f"Invalid mode: '{mode}'")

            if cache_dir is not None and mode not in ("r", "rb"):
                raise Error("cache_dir only supported in read mode")

            local_filename = self.basename(path)
            if local_filename == "":
                local_filename = "local.tmp"
            if _is_gcp_path(path) or _is_azure_path(path):
                remote_path = path
                if mode in ("a", "ab"):
                    tmp_dir = tempfile.mkdtemp()
                    local_path = self.join(tmp_dir, local_filename)
                    if self.exists(remote_path):
                        self.copy(remote_path, local_path)
                elif mode in ("r", "rb"):
                    if cache_dir is None:
                        tmp_dir = tempfile.mkdtemp()
                        local_path = self.join(tmp_dir, local_filename)
                        self.copy(remote_path, local_path)
                    else:
                        if not _is_local_path(cache_dir):
                            raise Error(f"cache_dir must be a local path: '{cache_dir}'")
                        self.makedirs(cache_dir)
                        path_md5 = hashlib.md5(path.encode("utf8")).hexdigest()
                        lock_path = self.join(cache_dir, f"{path_md5}.lock")
                        tmp_path = self.join(cache_dir, f"{path_md5}.tmp")
                        with filelock.FileLock(lock_path):
                            remote_version = ""
                            # get some sort of consistent remote hash so we can check for a local file
                            if _is_gcp_path(path):
                                st = gcp.maybe_stat(self._conf, path)
                                if st is None:
                                    raise FileNotFoundError(f"No such file: '{path}'")
                                assert st.version is not None
                                remote_version = st.version
                                remote_hash = st.md5
                            elif _is_azure_path(path):
                                # in the azure case the remote md5 may not exist
                                # this duplicates some of md5() because we want more control
                                st = azure.maybe_stat(self._conf, path)
                                if st is None:
                                    raise FileNotFoundError(f"No such file: '{path}'")
                                assert st.version is not None
                                remote_version = st.version
                                remote_hash = st.md5
                            else:
                                raise Error(f"Unrecognized path: '{path}'")

                            perform_copy = False
                            if remote_hash is None:
                                # there is no remote md5, copy the file
                                # and attempt to update the md5
                                perform_copy = True
                            else:
                                expected_local_path = self.join(
                                    cache_dir, remote_hash, local_filename
                                )
                                perform_copy = not self.exists(expected_local_path)

                            if perform_copy:
                                local_hexdigest = self.copy(
                                    remote_path, tmp_path, overwrite=True, return_md5=True
                                )
                                assert local_hexdigest is not None, "failed to return md5"
                                # the file we downloaded may not match the remote file because
                                # the remote file changed while we were downloading it
                                # in this case make sure we don't cache it under the wrong md5
                                local_path = self.join(cache_dir, local_hexdigest, local_filename)
                                os.makedirs(self.dirname(local_path), exist_ok=True)
                                if os.path.exists(local_path):
                                    # the file is already here, nevermind
                                    os.remove(tmp_path)
                                else:
                                    os.replace(tmp_path, local_path)

                                if remote_hash is None:
                                    if _is_azure_path(path):
                                        azure.maybe_update_md5(
                                            self._conf, path, remote_version, local_hexdigest
                                        )
                                    elif _is_gcp_path(path):
                                        gcp.maybe_update_md5(
                                            self._conf, path, remote_version, local_hexdigest
                                        )
                            else:
                                assert remote_hash is not None
                                local_path = self.join(cache_dir, remote_hash, local_filename)
                else:
                    tmp_dir = tempfile.mkdtemp()
                    local_path = self.join(tmp_dir, local_filename)
            elif _is_local_path(path):
                local_path = path
            else:
                raise Error(f"Unrecognized path: '{path}'")

            f = _ProxyFile(
                ctx=self,
                local_path=local_path,
                mode=mode,
                tmp_dir=tmp_dir,
                remote_path=remote_path,
                version=version,
                partial_writes_on_exc=partial_writes_on_exc,
            )
            if "r" in mode:
                f = io.BufferedReader(f, buffer_size=buffer_size)
            else:
                f = _BufferedWriterForProxyFile(f, buffer_size=buffer_size)
            binary_f = cast(BinaryIO, f)
            if "b" in mode:
                return binary_f
            else:
                text_f = io.TextIOWrapper(binary_f, encoding="utf8")
                return cast(TextIO, text_f)


def default_log_fn(msg: str) -> None:
    print(f"blobfile: {msg}")


def _is_gcp_path(path: str) -> bool:
    url = urllib.parse.urlparse(path)
    return url.scheme == "gs"


def _is_azure_path(path: str) -> bool:
    url = urllib.parse.urlparse(path)
    return (
        url.scheme == "https" and url.netloc.endswith(".blob.core.windows.net")
    ) or url.scheme == "az"


def _is_local_path(path: str) -> bool:
    url = urllib.parse.urlparse(path)
    return not url.scheme and not url.netloc


def _download_chunk(
    conf: Config, src: str, dst: str, start: int, size: int, src_file_size: int
) -> None:
    ctx = Context(conf)
    with ctx.BlobFile(src, "rb", file_size=src_file_size) as src_f:
        src_f.seek(start)
        # open output file such that we can write directly to the correct range
        with open(dst, "rb+") as dst_f:
            dst_f.seek(start)
            bytes_read = 0
            while True:
                n = min(CHUNK_SIZE, size - bytes_read)
                assert n >= 0
                block = src_f.read(n)
                if block == b"":
                    if bytes_read != size:
                        raise Error(
                            f"read wrong number of bytes from file `{src}`, expected {size} but read {bytes_read}"
                        )
                    break
                dst_f.write(block)
                bytes_read += len(block)


def _parallel_download(
    conf: Config, executor: concurrent.futures.Executor, src: str, dst: str, return_md5: bool
) -> str | None:
    ctx = Context(conf=conf)

    s = ctx.stat(src)

    # pre-allocate output file
    if os.path.dirname(dst) != "":
        os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "wb") as f:
        if s.size > 0:
            f.seek(s.size - 1)
            f.write(b"\0")

    max_workers = getattr(executor, "_max_workers", os.cpu_count() or 1)
    part_size = max(math.ceil(s.size / max_workers), common.PARALLEL_COPY_MINIMUM_PART_SIZE)
    start = 0
    futures = []
    while start < s.size:
        future = executor.submit(
            _download_chunk, conf, src, dst, start, min(part_size, s.size - start), s.size
        )
        futures.append(future)
        start += part_size
    for future in futures:
        future.result()

    if return_md5:
        with ctx.BlobFile(dst, "rb") as f:
            return binascii.hexlify(common.block_md5(f)).decode("utf8")


def _string_overlap(s1: str, s2: str) -> int:
    length = min(len(s1), len(s2))
    for i in range(length):
        if s1[i] != s2[i]:
            return i
    return length


def _split_path(path: str) -> list[str]:
    # a/b/c => a/, b/, c
    # a/b/ => a/, b/
    # /a/b/c => /, a/, b/, c
    parts = []
    part = ""
    for c in path:
        part += c
        if c == "/":
            parts.append(part)
            part = ""
    if part != "":
        parts.append(part)
    return parts


def _expand_implicit_dirs(root: str, it: Iterator[DirEntry]) -> Iterator[DirEntry]:
    # blob storage does not always have definitions for each intermediate dir
    # if we have a listing like
    #  gs://test/a/b
    #  gs://test/a/b/c/d
    # then we emit an entry "gs://test/a/b/c" for the implicit dir "c"
    # requires that iterator return objects in sorted order
    if _is_gcp_path(root):
        entry_from_dirpath = gcp.entry_from_dirpath
    elif _is_azure_path(root):
        entry_from_dirpath = azure.entry_from_dirpath
    else:
        raise Error(f"Unrecognized path '{root}'")

    previous_path = root
    for entry in it:
        # find the overlap between the previous_path and the current
        entry_slash_path = _get_slash_path(entry)
        offset = _string_overlap(previous_path, entry_slash_path)
        relpath = entry_slash_path[offset:]
        cur = entry_slash_path[:offset]
        for part in _split_path(relpath)[:-1]:
            cur += part
            yield entry_from_dirpath(cur)
        yield entry
        # assert entry_slash_path.encode("utf_16_be") >= previous_path.encode("utf_16_be")
        previous_path = entry_slash_path


def _compile_pattern(s: str, sep: str = "/"):
    tokens = [t for t in re.split("([*]+)", s) if t != ""]
    regexp = ""
    for tok in tokens:
        if tok == "*":
            regexp += f"[^{sep}]*"
        elif tok == "**":
            regexp += ".*"
        else:
            regexp += re.escape(tok)
    return re.compile(regexp + f"{sep}?$")


def _glob_full(conf: Config, pattern: str) -> Iterator[DirEntry]:
    prefix, _, _ = pattern.partition("*")

    re_pattern = _compile_pattern(pattern)

    for entry in _expand_implicit_dirs(root=prefix, it=_list_blobs(conf=conf, path=prefix)):
        entry_slash_path = _get_slash_path(entry)
        if bool(re_pattern.match(entry_slash_path)):
            if entry_slash_path == prefix and entry.is_dir:
                # we matched the parent directory
                continue
            yield entry


class _GlobTask(NamedTuple):
    cur: str
    rem: Sequence[str]


class _GlobEntry(NamedTuple):
    entry: DirEntry


class _GlobTaskComplete(NamedTuple):
    pass


def _process_glob_task(conf: Config, root: str, t: _GlobTask) -> Iterator[_GlobTask | _GlobEntry]:
    cur = t.cur + t.rem[0]
    rem = t.rem[1:]
    if "**" in cur:
        for entry in _glob_full(conf, root + cur + "".join(rem)):
            yield _GlobEntry(entry)
    elif "*" in cur:
        re_pattern = _compile_pattern(root + cur)
        prefix, _, _ = cur.partition("*")
        path = root + prefix
        for entry in _list_blobs(conf=conf, path=path, delimiter="/"):
            entry_slash_path = _get_slash_path(entry)
            # in the case of dirname/* we should not return the path dirname/
            if entry_slash_path == path and entry.is_dir:
                # we matched the parent directory
                continue
            if bool(re_pattern.match(entry_slash_path)):
                if len(rem) == 0:
                    yield _GlobEntry(entry)
                else:
                    assert entry_slash_path.startswith(root)
                    yield _GlobTask(entry_slash_path[len(root) :], rem)
    else:
        if len(rem) == 0:
            path = root + cur
            entry = _get_entry(conf, path)
            if entry is not None:
                yield _GlobEntry(entry)
        else:
            yield _GlobTask(cur, rem)


def _glob_worker(
    conf: Config,
    root: str,
    tasks: mp.Queue[_GlobTask],
    results: mp.Queue[_GlobEntry | _GlobTask | _GlobTaskComplete],
) -> None:
    while True:
        t = tasks.get()
        for r in _process_glob_task(conf, root=root, t=t):
            results.put(r)
        results.put(_GlobTaskComplete())


def _local_glob(pattern: str) -> Iterator[str]:
    normalized_pattern = os.path.normpath(pattern)
    if pattern.endswith(("/", "\\")):
        # normpath will remove a trailing separator
        # but these affect the output of the glob
        normalized_pattern += os.sep

    if "*" in normalized_pattern:
        prefix = normalized_pattern.split("*")[0]
        if prefix.endswith(os.sep):
            base_dir = os.path.abspath(prefix)
            if not base_dir.endswith(os.sep):
                # if base_dir is the root directory it will already have a separator
                base_dir += os.sep
            pattern_suffix = normalized_pattern[len(prefix) :]
        else:
            dirpath = os.path.dirname(prefix)
            base_dir = os.path.abspath(dirpath)
            # prefix   dirpath  pattern_suffix
            #   /a/b   /a       /b
            #   a/c    a        /c
            #   b      ""       ""
            #   ""     ""       ""
            if len(dirpath) == 0:
                pattern_suffix = os.sep + normalized_pattern
            else:
                pattern_suffix = normalized_pattern[len(dirpath) :]
        full_pattern = base_dir + pattern_suffix
        regexp = _compile_pattern(full_pattern, sep=os.sep)
        for root, dirnames, filenames in os.walk(base_dir):
            paths = [os.path.join(root, dirname + os.sep) for dirname in dirnames]
            paths += [os.path.join(root, filename) for filename in filenames]
            for path in paths:
                if re.match(regexp, path):
                    if path.endswith(os.sep):
                        path = path[:-1]
                    yield path
    else:
        path = os.path.abspath(pattern)
        if os.path.exists(path):
            if path.endswith(os.sep):
                path = path[:-1]
            yield path


def _strip_slash(path: str) -> str:
    if path.endswith("/"):
        return path[:-1]
    else:
        return path


def _guess_isdir(path: str) -> bool:
    """
    Guess if a path is a directory without performing network requests
    """
    if _is_local_path(path) and os.path.isdir(path):
        return True
    elif (_is_gcp_path(path) or _is_azure_path(path)) and path.endswith("/"):
        return True
    return False


def _list_blobs(conf: Config, path: str, delimiter: str | None = None) -> Iterator[DirEntry]:
    params = {}
    if delimiter is not None:
        params["delimiter"] = delimiter

    if _is_gcp_path(path):
        yield from gcp.list_blobs(conf, path, delimiter=delimiter)
    elif _is_azure_path(path):
        yield from azure.list_blobs(conf, path, delimiter=delimiter)
    else:
        raise Error(f"Unrecognized path: '{path}'")


def _get_slash_path(entry: DirEntry) -> str:
    return entry.path + "/" if entry.is_dir else entry.path


def _normalize_path(conf: Config, path: str) -> str:
    # convert paths to the canonical format
    if _is_azure_path(path):
        return azure.combine_path(conf, *azure.split_path(path))
    return path


def _list_blobs_in_dir(conf: Config, prefix: str, exclude_prefix: bool) -> Iterator[DirEntry]:
    # the prefix check doesn't work without normalization
    normalized_prefix = _normalize_path(conf, prefix)
    for entry in _list_blobs(conf=conf, path=normalized_prefix, delimiter="/"):
        if exclude_prefix and _get_slash_path(entry) == normalized_prefix:
            continue
        yield entry


def _get_entry(conf: Config, path: str) -> DirEntry | None:
    ctx = Context(conf)
    if _is_gcp_path(path):
        st = gcp.maybe_stat(conf, path)
        if st is not None:
            if path.endswith("/"):
                return gcp.entry_from_dirpath(path)
            else:
                return gcp.entry_from_path_stat(path, st)
        if ctx.isdir(path):
            return gcp.entry_from_dirpath(path)
    elif _is_azure_path(path):
        st = azure.maybe_stat(conf, path)
        if st is not None:
            if path.endswith("/"):
                return azure.entry_from_dirpath(path)
            else:
                return azure.entry_from_path_stat(path, st)
        if ctx.isdir(path):
            return azure.entry_from_dirpath(path)
    else:
        raise Error(f"Unrecognized path: '{path}'")

    return None


def _sharded_listdir_worker(
    conf: Config, prefixes: mp.Queue[tuple[str, str, bool]], items: mp.Queue[DirEntry | None]
) -> None:
    while True:
        base, prefix, exact = prefixes.get(True)
        if exact:
            path = base + prefix
            entry = _get_entry(conf, path)
            if entry is not None:
                items.put(entry)
        else:
            it = _list_blobs_in_dir(conf, base + prefix, exclude_prefix=False)
            for entry in it:
                items.put(entry)
        items.put(None)  # indicate that we have finished this path


def _join2(conf: Config, a: str, b: str) -> str:
    if _is_local_path(a):
        return os.path.join(a, b)
    elif _is_gcp_path(a):
        return gcp.join_paths(conf, a, b)
    elif _is_azure_path(a):
        return azure.join_paths(conf, a, b)
    else:
        raise Error(f"Unrecognized path: '{a}'")


class _BufferedWriterForProxyFile(io.BufferedWriter):
    """
    This class is needed preventing partial writes from being written in the non-streaming case.
    Suppose a user does the following:

    with bf.BlobFile("path", "wb", streaming=False, partial_writes_on_exc=False) as f:
        f.write(b"meow")
        f.write(b"woof")

    and an exception occurs after the first write. Since partial_writes_on_exc is False, we do not
    want either of the writes to occur. The object returned by bf.BlobFile used to be an
    io.BufferedWriter wrapping a _ProxyFile. When an exception occurred, the __exit__ method of the
    io.BufferedWriter would be invoked, that would call the close() method of the _ProxyFile, and
    then the _ProxyFile would copy the local (proxy) file to the remote file. But at that point, the
    close method would have no idea that it's being invoked from exceptional context, and will copy
    the file.

    To fix this, we wrap the io.BufferedWriter with this class, which forwards whether an exception
    occurred to the underlying raw object, i.e. the _ProxyFile. This class is designed to solve
    *only* this problem, and should never be used for any other purpose.
    """

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        assert isinstance(self.raw, _ProxyFile)
        self.raw.had_exception = exc_val is not None

        return super().__exit__(exc_type, exc_val, exc_tb)


class _ProxyFile(io.FileIO):
    def __init__(
        self,
        ctx: Context,
        local_path: str,
        mode: Literal["r", "rb", "w", "wb", "a", "ab"],
        tmp_dir: str | None,
        remote_path: str | None,
        version: str | None,
        partial_writes_on_exc: bool = True,
    ) -> None:
        super().__init__(local_path, mode=mode)
        self._ctx = ctx
        self._mode = mode
        self._tmp_dir = tmp_dir
        self._local_path = local_path
        self._remote_path = remote_path
        self._closed = False
        self._version = version

        self._partial_writes_on_exc = partial_writes_on_exc
        self.had_exception: bool = False

    def close(self) -> None:
        if not hasattr(self, "_closed") or self._closed:
            return

        super().close()
        try:
            mode_should_write = self._mode in ("w", "wb", "a", "ab")
            should_do_write = not self.had_exception or self._partial_writes_on_exc

            if self._remote_path is not None and mode_should_write and should_do_write:
                self._ctx.copy(
                    self._local_path, self._remote_path, overwrite=True, dst_version=self._version
                )
        finally:
            # if the copy fails, still cleanup our local temp file so it is not leaked
            if self._tmp_dir is not None:
                os.remove(self._local_path)
                os.rmdir(self._tmp_dir)
        self._closed = True


def create_context(
    *,
    log_callback: Callable[[str], None] = default_log_fn,
    connection_pool_max_size: int = DEFAULT_CONNECTION_POOL_MAX_SIZE,
    max_connection_pool_count: int = DEFAULT_MAX_CONNECTION_POOL_COUNT,
    # https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs#about-block-blobs
    # the chunk size determines the maximum size of an individual blob
    azure_write_chunk_size: int = DEFAULT_AZURE_WRITE_CHUNK_SIZE,
    google_write_chunk_size: int = DEFAULT_GOOGLE_WRITE_CHUNK_SIZE,
    retry_log_threshold: int = DEFAULT_RETRY_LOG_THRESHOLD,
    retry_common_log_threshold: int = DEFAULT_RETRY_COMMON_LOG_THRESHOLD,
    retry_limit: int | None = None,
    connect_timeout: int | None = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: int | None = DEFAULT_READ_TIMEOUT,
    output_az_paths: bool = True,
    use_azure_storage_account_key_fallback: bool = False,
    get_http_pool: Callable[[], urllib3.PoolManager] | None = None,
    use_streaming_read: bool = False,
    use_blind_writes: bool = DEFAULT_USE_BLIND_WRITES,
    default_buffer_size: int = DEFAULT_BUFFER_SIZE,
    get_deadline: Callable[[], float] | None = None,
    save_access_token_to_disk: bool = True,
    multiprocessing_start_method: str = "spawn",
):
    """
    Same argument as configure(), but returns a Context object that has all the blobfile methods on it.
    """
    conf = Config(
        log_callback=log_callback,
        connection_pool_max_size=connection_pool_max_size,
        max_connection_pool_count=max_connection_pool_count,
        azure_write_chunk_size=azure_write_chunk_size,
        retry_log_threshold=retry_log_threshold,
        retry_common_log_threshold=retry_common_log_threshold,
        retry_limit=retry_limit,
        google_write_chunk_size=google_write_chunk_size,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        output_az_paths=output_az_paths,
        use_azure_storage_account_key_fallback=use_azure_storage_account_key_fallback,
        get_http_pool=get_http_pool,
        use_streaming_read=use_streaming_read,
        default_buffer_size=default_buffer_size,
        use_blind_writes=use_blind_writes,
        get_deadline=get_deadline,
        save_access_token_to_disk=save_access_token_to_disk,
        multiprocessing_start_method=multiprocessing_start_method,
    )
    return Context(conf=conf)
