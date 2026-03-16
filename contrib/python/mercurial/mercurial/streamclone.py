# streamclone.py - producing and consuming streaming repository data
#
# Copyright 2015 Gregory Szorc <gregory.szorc@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import collections
import contextlib
import errno
import os
import struct
import threading

from typing import (
    Callable,
    Iterable,
    Iterator,
    Optional,
)

from .i18n import _
from .interfaces import repository
from . import (
    bookmarks,
    bundle2 as bundle2mod,
    cacheutil,
    error,
    narrowspec,
    phases,
    pycompat,
    requirements as requirementsmod,
    scmutil,
    store,
    transaction,
    util,
)
from .revlogutils import (
    nodemap,
)


# Number arbitrarily picked, feel free to change them (but the LOW one)
#
# update the configuration documentation if you touch this.
DEFAULT_NUM_WRITER = {
    scmutil.RESOURCE_LOW: 1,
    scmutil.RESOURCE_MEDIUM: 4,
    scmutil.RESOURCE_HIGH: 8,
}


# Number arbitrarily picked, feel free to adjust them. Do update the
# documentation if you do so
DEFAULT_MEMORY_TARGET = {
    scmutil.RESOURCE_LOW: 50 * (2**20),  # 100 MB
    scmutil.RESOURCE_MEDIUM: 500 * 2**20,  # 500 MB
    scmutil.RESOURCE_HIGH: 2 * 2**30,  #   2 GB
}


def new_stream_clone_requirements(
    default_requirements: Iterable[bytes],
    streamed_requirements: Iterable[bytes],
) -> set[bytes]:
    """determine the final set of requirement for a new stream clone

    this method combine the "default" requirements that a new repository would
    use with the constaint we get from the stream clone content. We keep local
    configuration choice when possible.
    """
    requirements = set(default_requirements)
    requirements &= requirementsmod.STREAM_IGNORABLE_REQUIREMENTS | {
        requirementsmod.NARROW_REQUIREMENT
    }
    requirements.update(streamed_requirements)
    return requirements


def streamed_requirements(repo) -> set[bytes]:
    """the set of requirement the new clone will have to support

    This is used for advertising the stream options and to generate the actual
    stream content."""
    requiredformats = (
        repo.requirements - requirementsmod.STREAM_IGNORABLE_REQUIREMENTS
    )
    return requiredformats


def canperformstreamclone(pullop, bundle2: bool = False):
    """Whether it is possible to perform a streaming clone as part of pull.

    ``bundle2`` will cause the function to consider stream clone through
    bundle2 and only through bundle2.

    Returns a tuple of (supported, requirements). ``supported`` is True if
    streaming clone is supported and False otherwise. ``requirements`` is
    a set of repo requirements from the remote, or ``None`` if stream clone
    isn't supported.
    """
    repo = pullop.repo
    remote = pullop.remote

    # should we consider streaming clone at all ?
    streamrequested = pullop.streamclonerequested
    # If we don't have a preference, let the server decide for us. This
    # likely only comes into play in LANs.
    if streamrequested is None:
        # The server can advertise whether to prefer streaming clone.
        streamrequested = remote.capable(b'stream-preferred')
    if not streamrequested:
        return False, None

    # Streaming clone only works on an empty destination repository
    if len(repo):
        return False, None

    # Streaming clone only works if all data is being requested.
    if pullop.heads:
        return False, None

    bundle2supported = False
    if pullop.canusebundle2:
        local_caps = bundle2mod.getrepocaps(repo, role=b'client')
        local_supported = set(local_caps.get(b'stream', []))
        remote_supported = set(pullop.remotebundle2caps.get(b'stream', []))
        bundle2supported = bool(local_supported & remote_supported)
        # else
        # Server doesn't support bundle2 stream clone or doesn't support
        # the versions we support. Fall back and possibly allow legacy.

    # Ensures legacy code path uses available bundle2.
    if bundle2supported and not bundle2:
        return False, None
    # Ensures bundle2 doesn't try to do a stream clone if it isn't supported.
    elif bundle2 and not bundle2supported:
        return False, None

    # In order for stream clone to work, the client has to support all the
    # requirements advertised by the server.
    #
    # The server advertises its requirements via the "stream" and "streamreqs"
    # capability. "stream" (a value-less capability) is advertised if and only
    # if the only requirement is "revlogv1." Else, the "streamreqs" capability
    # is advertised and contains a comma-delimited list of requirements.
    requirements = set()
    if remote.capable(b'stream'):
        requirements.add(requirementsmod.REVLOGV1_REQUIREMENT)
    else:
        streamreqs = remote.capable(b'streamreqs')
        # This is weird and shouldn't happen with modern servers.
        if not streamreqs:
            pullop.repo.ui.warn(
                _(
                    b'warning: stream clone requested but server has them '
                    b'disabled\n'
                )
            )
            return False, None

        streamreqs = set(streamreqs.split(b','))
        # Server requires something we don't support. Bail.
        missingreqs = streamreqs - repo.supported
        if missingreqs:
            pullop.repo.ui.warn(
                _(
                    b'warning: stream clone requested but client is missing '
                    b'requirements: %s\n'
                )
                % b', '.join(sorted(missingreqs))
            )
            pullop.repo.ui.warn(
                _(
                    b'(see https://www.mercurial-scm.org/wiki/MissingRequirement '
                    b'for more information)\n'
                )
            )
            return False, None
        requirements = streamreqs

    return True, requirements


def maybeperformlegacystreamclone(pullop) -> None:
    """Possibly perform a legacy stream clone operation.

    Legacy stream clones are performed as part of pull but before all other
    operations.

    A legacy stream clone will not be performed if a bundle2 stream clone is
    supported.
    """
    from . import localrepo

    supported, requirements = canperformstreamclone(pullop)

    if not supported:
        return

    repo = pullop.repo
    remote = pullop.remote

    # Save remote branchmap. We will use it later to speed up branchcache
    # creation.
    rbranchmap = None
    if remote.capable(b'branchmap'):
        with remote.commandexecutor() as e:
            rbranchmap = e.callcommand(b'branchmap', {}).result()

    repo.ui.status(_(b'streaming all changes\n'))

    with remote.commandexecutor() as e:
        fp = e.callcommand(b'stream_out', {}).result()

    # TODO strictly speaking, this code should all be inside the context
    # manager because the context manager is supposed to ensure all wire state
    # is flushed when exiting. But the legacy peers don't do this, so it
    # doesn't matter.
    l = fp.readline()
    try:
        resp = int(l)
    except ValueError:
        raise error.ResponseError(
            _(b'unexpected response from remote server:'), l
        )
    if resp == 1:
        raise error.Abort(_(b'operation forbidden by server'))
    elif resp == 2:
        raise error.Abort(_(b'locking the remote repository failed'))
    elif resp != 0:
        raise error.Abort(_(b'the server sent an unknown error code'))

    l = fp.readline()
    try:
        filecount, bytecount = map(int, l.split(b' ', 1))
    except (ValueError, TypeError):
        raise error.ResponseError(
            _(b'unexpected response from remote server:'), l
        )

    with repo.lock():
        consumev1(repo, fp, filecount, bytecount)
        repo.requirements = new_stream_clone_requirements(
            repo.requirements,
            requirements,
        )
        repo.svfs.options = localrepo.resolvestorevfsoptions(
            repo.ui, repo.requirements, repo.features
        )
        scmutil.writereporequirements(repo)
        nodemap.post_stream_cleanup(repo)

        if rbranchmap:
            repo._branchcaches.replace(repo, rbranchmap)

        repo.invalidate()


def allowservergeneration(repo) -> bool:
    """Whether streaming clones are allowed from the server."""
    if repository.REPO_FEATURE_STREAM_CLONE not in repo.features:
        return False

    if not repo.ui.configbool(b'server', b'uncompressed', untrusted=True):
        return False

    # The way stream clone works makes it impossible to hide secret changesets.
    # So don't allow this by default.
    secret = phases.hassecret(repo)
    if secret:
        return repo.ui.configbool(b'server', b'uncompressedallowsecret')

    return True


# This is it's own function so extensions can override it.
def _walkstreamfiles(
    repo, matcher=None, phase: bool = False, obsolescence: bool = False
):
    return repo.store.walk(matcher, phase=phase, obsolescence=obsolescence)


def _report_transferred(
    repo, start_time: float, file_count: int, byte_count: int
):
    """common utility to report time it took to apply the stream bundle"""
    elapsed = util.timer() - start_time
    if elapsed <= 0:
        elapsed = 0.001
    m = _(b'stream-cloned %d files / %s in %.1f seconds (%s/sec)\n')
    m %= (
        file_count,
        util.bytecount(byte_count),
        elapsed,
        util.bytecount(byte_count / elapsed),
    )
    repo.ui.status(m)


def generatev1(repo) -> tuple[int, int, Iterator[bytes]]:
    """Emit content for version 1 of a streaming clone.

    This returns a 3-tuple of (file count, byte size, data iterator).

    The data iterator consists of N entries for each file being transferred.
    Each file entry starts as a line with the file name and integer size
    delimited by a null byte.

    The raw file data follows. Following the raw file data is the next file
    entry, or EOF.

    When used on the wire protocol, an additional line indicating protocol
    success will be prepended to the stream. This function is not responsible
    for adding it.

    This function will obtain a repository lock to ensure a consistent view of
    the store is captured. It therefore may raise LockError.
    """
    entries = []
    total_bytes = 0
    # Get consistent snapshot of repo, lock during scan.
    with repo.lock():
        repo.ui.debug(b'scanning\n')
        _test_sync_point_walk_1_2(repo)
        for entry in _walkstreamfiles(repo):
            for f in entry.files():
                file_size = f.file_size(repo.store.vfs)
                if file_size:
                    entries.append((f.unencoded_path, file_size))
                    total_bytes += file_size
        _test_sync_point_walk_3(repo)
    _test_sync_point_walk_4(repo)

    repo.ui.debug(
        b'%d files, %d bytes to transfer\n' % (len(entries), total_bytes)
    )

    svfs = repo.svfs
    debugflag = repo.ui.debugflag

    def emitrevlogdata() -> Iterator[bytes]:
        for name, size in entries:
            if debugflag:
                repo.ui.debug(b'sending %s (%d bytes)\n' % (name, size))
            # partially encode name over the wire for backwards compat
            yield b'%s\0%d\n' % (store.encodedir(name), size)
            # auditing at this stage is both pointless (paths are already
            # trusted by the local repo) and expensive
            with svfs(name, b'rb', auditpath=False) as fp:
                if size <= 65536:
                    yield fp.read(size)
                else:
                    yield from util.filechunkiter(fp, limit=size)

    return len(entries), total_bytes, emitrevlogdata()


def generatev1wireproto(repo) -> Iterator[bytes]:
    """Emit content for version 1 of streaming clone suitable for the wire.

    This is the data output from ``generatev1()`` with 2 header lines. The
    first line indicates overall success. The 2nd contains the file count and
    byte size of payload.

    The success line contains "0" for success, "1" for stream generation not
    allowed, and "2" for error locking the repository (possibly indicating
    a permissions error for the server process).
    """
    if not allowservergeneration(repo):
        yield b'1\n'
        return

    try:
        filecount, bytecount, it = generatev1(repo)
    except error.LockError:
        yield b'2\n'
        return

    # Indicates successful response.
    yield b'0\n'
    yield b'%d %d\n' % (filecount, bytecount)
    yield from it


def generatebundlev1(
    repo, compression: bytes = b'UN'
) -> tuple[set[bytes], Iterator[bytes]]:
    """Emit content for version 1 of a stream clone bundle.

    The first 4 bytes of the output ("HGS1") denote this as stream clone
    bundle version 1.

    The next 2 bytes indicate the compression type. Only "UN" is currently
    supported.

    The next 16 bytes are two 64-bit big endian unsigned integers indicating
    file count and byte count, respectively.

    The next 2 bytes is a 16-bit big endian unsigned short declaring the length
    of the requirements string, including a trailing \0. The following N bytes
    are the requirements string, which is ASCII containing a comma-delimited
    list of repo requirements that are needed to support the data.

    The remaining content is the output of ``generatev1()`` (which may be
    compressed in the future).

    Returns a tuple of (requirements, data generator).
    """
    if compression != b'UN':
        raise ValueError('we do not support the compression argument yet')

    requirements = streamed_requirements(repo)
    requires = b','.join(sorted(requirements))

    def gen() -> Iterator[bytes]:
        yield b'HGS1'
        yield compression

        filecount, bytecount, it = generatev1(repo)
        repo.ui.status(
            _(b'writing %d bytes for %d files\n') % (bytecount, filecount)
        )

        yield struct.pack(b'>QQ', filecount, bytecount)
        yield struct.pack(b'>H', len(requires) + 1)
        yield requires + b'\0'

        # This is where we'll add compression in the future.
        assert compression == b'UN'

        progress = repo.ui.makeprogress(
            _(b'bundle'), total=bytecount, unit=_(b'bytes')
        )
        progress.update(0)

        for chunk in it:
            progress.increment(step=len(chunk))
            yield chunk

        progress.complete()

    return requirements, gen()


def consumev1(repo, fp, filecount: int, bytecount: int) -> None:
    """Apply the contents from version 1 of a streaming clone file handle.

    This takes the output from "stream_out" and applies it to the specified
    repository.

    Like "stream_out," the status line added by the wire protocol is not
    handled by this function.
    """
    with repo.lock():
        repo.ui.status(
            _(b'%d files to transfer, %s of data\n')
            % (filecount, util.bytecount(bytecount))
        )
        progress = repo.ui.makeprogress(
            _(b'clone'), total=bytecount, unit=_(b'bytes')
        )
        progress.update(0)
        start = util.timer()

        # TODO: get rid of (potential) inconsistency
        #
        # If transaction is started and any @filecache property is
        # changed at this point, it causes inconsistency between
        # in-memory cached property and streamclone-ed file on the
        # disk. Nested transaction prevents transaction scope "clone"
        # below from writing in-memory changes out at the end of it,
        # even though in-memory changes are discarded at the end of it
        # regardless of transaction nesting.
        #
        # But transaction nesting can't be simply prohibited, because
        # nesting occurs also in ordinary case (e.g. enabling
        # clonebundles).

        total_file_count = 0
        with repo.transaction(b'clone'):
            with repo.svfs.backgroundclosing(repo.ui, expectedcount=filecount):
                for i in range(filecount):
                    # XXX doesn't support '\n' or '\r' in filenames
                    if hasattr(fp, 'readline'):
                        l = fp.readline()
                    else:
                        # inline clonebundles use a chunkbuffer, so no readline
                        # --> this should be small anyway, the first line
                        # only contains the size of the bundle
                        l_buf = []
                        while not (l_buf and l_buf[-1] == b'\n'):
                            l_buf.append(fp.read(1))
                        l = b''.join(l_buf)
                    try:
                        name, size = l.split(b'\0', 1)
                        size = int(size)
                    except (ValueError, TypeError):
                        raise error.ResponseError(
                            _(b'unexpected response from remote server:'), l
                        )
                    if repo.ui.debugflag:
                        repo.ui.debug(
                            b'adding %s (%s)\n' % (name, util.bytecount(size))
                        )
                    # for backwards compat, name was partially encoded
                    path = store.decodedir(name)
                    with repo.svfs(path, b'w', backgroundclose=True) as ofp:
                        total_file_count += 1
                        for chunk in util.filechunkiter(fp, limit=size):
                            progress.increment(step=len(chunk))
                            ofp.write(chunk)

            # force @filecache properties to be reloaded from
            # streamclone-ed file at next access
            repo.invalidate(clearfilecache=True)

        progress.complete()
        _report_transferred(repo, start, total_file_count, bytecount)


def readbundle1header(fp) -> tuple[int, int, set[bytes]]:
    compression = fp.read(2)
    if compression != b'UN':
        raise error.Abort(
            _(
                b'only uncompressed stream clone bundles are '
                b'supported; got %s'
            )
            % compression
        )

    filecount, bytecount = struct.unpack(b'>QQ', fp.read(16))
    requireslen = struct.unpack(b'>H', fp.read(2))[0]
    requires = fp.read(requireslen)

    if not requires.endswith(b'\0'):
        raise error.Abort(
            _(
                b'malformed stream clone bundle: '
                b'requirements not properly encoded'
            )
        )

    requirements = set(requires.rstrip(b'\0').split(b','))

    return filecount, bytecount, requirements


def applybundlev1(repo, fp) -> None:
    """Apply the content from a stream clone bundle version 1.

    We assume the 4 byte header has been read and validated and the file handle
    is at the 2 byte compression identifier.
    """
    if len(repo):
        raise error.Abort(
            _(b'cannot apply stream clone bundle on non-empty repo')
        )

    filecount, bytecount, requirements = readbundle1header(fp)
    missingreqs = requirements - repo.supported
    if missingreqs:
        raise error.Abort(
            _(b'unable to apply stream clone: unsupported format: %s')
            % b', '.join(sorted(missingreqs))
        )

    consumev1(repo, fp, filecount, bytecount)
    nodemap.post_stream_cleanup(repo)


class streamcloneapplier:
    """Class to manage applying streaming clone bundles.

    We need to wrap ``applybundlev1()`` in a dedicated type to enable bundle
    readers to perform bundle type-specific functionality.
    """

    def __init__(self, fh) -> None:
        self._fh = fh

    def apply(self, repo) -> None:
        return applybundlev1(repo, self._fh)


# type of file to stream
_fileappend = 0  # append only file
_filefull = 1  # full snapshot file

# Source of the file
_srcstore = b's'  # store (svfs)
_srccache = b'c'  # cache (cache)


# This is it's own function so extensions can override it.
def _walkstreamfullstorefiles(repo) -> list[bytes]:
    """list snapshot file from the store"""
    fnames = []
    if not repo.publishing():
        fnames.append(b'phaseroots')
    return fnames


def _filterfull(entry, copy, vfsmap):
    """actually copy the snapshot files"""
    src, name, ftype, data = entry
    if ftype != _filefull:
        return entry
    return (src, name, ftype, copy(vfsmap[src].join(name)))


class VolatileManager:
    """Manage temporary backups of volatile files during stream clone.

    This class will keep open file handles for the volatile files, writing the
    smaller ones on disk if the number of open file handles grow too much.

    This should be used as a Python context, the file handles and copies will
    be discarded when exiting the context.

    The preservation can be done by calling the object on the real path
    (encoded full path).

    Valid filehandles for any file should be retrieved by calling `open(path)`.
    """

    # arbitrarily picked as "it seemed fine" and much higher than the current
    # usage.  The Windows value of 2 is actually 1 file open at a time, due to
    # the `flush_count = self.MAX_OPEN // 2` and `self.MAX_OPEN - 1` threshold
    # for flushing to disk in __call__().
    MAX_OPEN = 2 if pycompat.iswindows else 100

    def __init__(self) -> None:
        self._counter = 0
        self._volatile_fps = None
        self._copies = None
        self._dst_dir = None

    def __enter__(self):
        if self._counter == 0:
            assert self._volatile_fps is None
            self._volatile_fps = {}
        self._counter += 1
        return self

    def __exit__(self, *args, **kwars):
        """discard all backups"""
        self._counter -= 1
        if self._counter == 0:
            for _size, fp in self._volatile_fps.values():
                fp.close()
            self._volatile_fps = None
            if self._copies is not None:
                for tmp in self._copies.values():
                    util.tryunlink(tmp)
                util.tryrmdir(self._dst_dir)
            self._copies = None
            self._dst_dir = None
            assert self._volatile_fps is None
            assert self._copies is None
            assert self._dst_dir is None

    def _init_tmp_copies(self) -> None:
        """prepare a temporary directory to save volatile files

        This will be used as backup if we have too many files open"""
        assert 0 < self._counter
        assert self._copies is None
        assert self._dst_dir is None
        self._copies = {}
        self._dst_dir = pycompat.mkdtemp(prefix=b'hg-clone-')

    def _flush_some_on_disk(self) -> None:
        """move some of the open files to tempory files on disk"""
        if self._copies is None:
            self._init_tmp_copies()
        flush_count = self.MAX_OPEN // 2
        for src, (size, fp) in sorted(self._volatile_fps.items())[:flush_count]:
            prefix = os.path.basename(src)
            fd, dst = pycompat.mkstemp(prefix=prefix, dir=self._dst_dir)
            self._copies[src] = dst
            os.close(fd)
            # we no longer hardlink, but on the other hand we rarely do this,
            # and we do it for the smallest file only and not at all in the
            # common case.
            with open(dst, 'wb') as bck:
                fp.seek(0)
                bck.write(fp.read())
            del self._volatile_fps[src]
            fp.close()

    def _keep_one(self, src: bytes) -> int:
        """preserve an open file handle for a given path"""
        # store the file quickly to ensure we close it if any error happens
        _, fp = self._volatile_fps[src] = (None, open(src, 'rb'))
        fp.seek(0, os.SEEK_END)
        size = fp.tell()
        self._volatile_fps[src] = (size, fp)
        return size

    def __call__(self, src: bytes) -> None:
        """preserve the volatile file at src"""
        assert 0 < self._counter
        if len(self._volatile_fps) >= (self.MAX_OPEN - 1):
            self._flush_some_on_disk()
        self._keep_one(src)

    def try_keep(self, src: bytes) -> int | None:
        """record a volatile file and returns it size

        return None if the file does not exists.

        Used for cache file that are not lock protected.
        """
        assert 0 < self._counter
        if len(self._volatile_fps) >= (self.MAX_OPEN - 1):
            self._flush_some_on_disk()
        try:
            return self._keep_one(src)
        except OSError as err:
            if err.errno not in (errno.ENOENT, errno.EPERM):
                raise
            return None

    @contextlib.contextmanager
    def open(self, src):
        assert 0 < self._counter
        entry = self._volatile_fps.get(src)
        if entry is not None:
            _size, fp = entry
            fp.seek(0)
            yield fp
        else:
            if self._copies is None:
                actual_path = src
            else:
                actual_path = self._copies.get(src, src)
            with open(actual_path, 'rb') as fp:
                yield fp


def _makemap(repo):
    """make a (src -> vfs) map for the repo"""
    vfsmap = {
        _srcstore: repo.svfs,
        _srccache: repo.cachevfs,
    }
    # we keep repo.vfs out of the on purpose, ther are too many danger there
    # (eg: .hg/hgrc)
    assert repo.vfs not in vfsmap.values()

    return vfsmap


def _emit2(repo, entries):
    """actually emit the stream bundle"""
    vfsmap = _makemap(repo)
    # we keep repo.vfs out of the on purpose, ther are too many danger there
    # (eg: .hg/hgrc),
    #
    # this assert is duplicated (from _makemap) as author might think this is
    # fine, while this is really not fine.
    if repo.vfs in vfsmap.values():
        raise error.ProgrammingError(
            'repo.vfs must not be added to vfsmap for security reasons'
        )

    # translate the vfs one
    entries = [(vfs_key, vfsmap[vfs_key], e) for (vfs_key, e) in entries]
    _test_sync_point_walk_1_2(repo)

    max_linkrev = len(repo)
    file_count = totalfilesize = 0
    with VolatileManager() as volatiles:
        # make sure we preserve volatile files
        with util.nogc():
            # record the expected size of every file
            for k, vfs, e in entries:
                e.preserve_volatiles(vfs, volatiles)
                for f in e.files():
                    file_count += 1
                    totalfilesize += f.file_size(vfs)

        progress = repo.ui.makeprogress(
            _(b'bundle'), total=totalfilesize, unit=_(b'bytes')
        )
        progress.update(0)
        with progress:
            # the first yield release the lock on the repository
            yield file_count, totalfilesize
            totalbytecount = 0

            for src, vfs, e in entries:
                entry_streams = e.get_streams(
                    repo=repo,
                    vfs=vfs,
                    volatiles=volatiles,
                    max_changeset=max_linkrev,
                    preserve_file_count=True,
                )
                for name, stream, size in entry_streams:
                    yield src
                    yield util.uvarintencode(len(name))
                    yield util.uvarintencode(size)
                    yield name
                    bytecount = 0
                    for chunk in stream:
                        bytecount += len(chunk)
                        totalbytecount += len(chunk)
                        progress.update(totalbytecount)
                        yield chunk
                    if bytecount != size:
                        # Would most likely be caused by a race due to `hg
                        # strip` or a revlog split
                        msg = _(
                            b'clone could only read %d bytes from %s, but '
                            b'expected %d bytes'
                        )
                        raise error.Abort(msg % (bytecount, name, size))


def _emit3(repo, entries) -> Iterator[bytes | None]:
    """actually emit the stream bundle (v3)"""
    vfsmap = _makemap(repo)
    # we keep repo.vfs out of the map on purpose, ther are too many dangers
    # there (eg: .hg/hgrc),
    #
    # this assert is duplicated (from _makemap) as authors might think this is
    # fine, while this is really not fine.
    if repo.vfs in vfsmap.values():
        raise error.ProgrammingError(
            'repo.vfs must not be added to vfsmap for security reasons'
        )

    # translate the vfs once
    # we only turn this into a list for the `_test_sync`, this is not ideal
    base_entries = list(entries)
    _test_sync_point_walk_1_2(repo)
    entries = []
    with VolatileManager() as volatiles:
        # make sure we preserve volatile files
        for vfs_key, e in base_entries:
            vfs = vfsmap[vfs_key]
            any_files = True
            if e.maybe_volatile:
                any_files = False
                e.preserve_volatiles(vfs, volatiles)
                for f in e.files():
                    if f.is_volatile:
                        # record the expected size under lock
                        f.file_size(vfs)
                    any_files = True
            if any_files:
                entries.append((vfs_key, vfsmap[vfs_key], e))

        total_entry_count = len(entries)

        max_linkrev = len(repo)
        progress = repo.ui.makeprogress(
            _(b'bundle'),
            total=total_entry_count,
            unit=_(b'entry'),
        )
        progress.update(0)
        # the first yield release the lock on the repository
        yield None
        with progress:
            yield util.uvarintencode(total_entry_count)

            for src, vfs, e in entries:
                entry_streams = e.get_streams(
                    repo=repo,
                    vfs=vfs,
                    volatiles=volatiles,
                    max_changeset=max_linkrev,
                )
                yield util.uvarintencode(len(entry_streams))
                for name, stream, size in entry_streams:
                    yield src
                    yield util.uvarintencode(len(name))
                    yield util.uvarintencode(size)
                    yield name
                    yield from stream
                progress.increment()


def _test_sync_point_walk_1_2(repo):
    """a function for synchronisation during tests

    Triggered after gather entry, but before starting to process/preserve them
    under lock.

    (on v1 is triggered before the actual walk start)
    """


def _test_sync_point_walk_3(repo):
    """a function for synchronisation during tests

    Triggered right before releasing the lock, but after computing what need
    needed to compute under lock.
    """


def _test_sync_point_walk_4(repo):
    """a function for synchronisation during tests

    Triggered right after releasing the lock.
    """


# not really a StoreEntry, but close enough
class CacheEntry(store.SimpleStoreEntry):
    """Represent an entry for Cache files

    It has special logic to preserve cache file early and accept optional
    presence.


    (Yes... this is not really a StoreEntry, but close enough. We could have a
    BaseEntry base class, bbut the store one would be identical)
    """

    def __init__(self, entry_path) -> None:
        super().__init__(
            entry_path,
            # we will directly deal with that in `setup_cache_file`
            is_volatile=True,
        )

    def preserve_volatiles(self, vfs, volatiles) -> None:
        self._file_size = volatiles.try_keep(vfs.join(self._entry_path))
        if self._file_size is None:
            self._files = []
        else:
            assert self._is_volatile
            self._files = [
                CacheFile(
                    unencoded_path=self._entry_path,
                    file_size=self._file_size,
                    is_volatile=self._is_volatile,
                )
            ]

    def files(self) -> list[store.StoreFile]:
        if self._files is None:
            self._files = [
                CacheFile(
                    unencoded_path=self._entry_path,
                    is_volatile=self._is_volatile,
                )
            ]
        return super().files()


class CacheFile(store.StoreFile):
    # inform the "copy/hardlink" version that this file might be missing
    # without consequences.
    optional: bool = True


def _entries_walk(repo, includes, excludes, includeobsmarkers: bool):
    """emit a seris of files information useful to clone a repo

    return (vfs-key, entry) iterator

    Where `entry` is StoreEntry. (used even for cache entries)
    """
    assert repo._currentlock(repo._lockref) is not None

    matcher = None
    if includes or excludes:
        matcher = narrowspec.match(repo.root, includes, excludes)

    phase = not repo.publishing()
    # Python is getting crazy at all the small container we creates, disabling
    # the gc while we do so helps performance a lot.
    with util.nogc():
        entries = _walkstreamfiles(
            repo,
            matcher,
            phase=phase,
            obsolescence=includeobsmarkers,
        )
        for entry in entries:
            yield (_srcstore, entry)

        for name in cacheutil.cachetocopy(repo):
            if repo.cachevfs.exists(name):
                # not really a StoreEntry, but close enough
                yield (_srccache, CacheEntry(entry_path=name))


def generatev2(repo, includes, excludes, includeobsmarkers: bool):
    """Emit content for version 2 of a streaming clone.

    the data stream consists the following entries:
    1) A char representing the file destination (eg: store or cache)
    2) A varint containing the length of the filename
    3) A varint containing the length of file data
    4) N bytes containing the filename (the internal, store-agnostic form)
    5) N bytes containing the file data

    Returns a 3-tuple of (file count, file size, data iterator).
    """

    with repo.lock():
        repo.ui.debug(b'scanning\n')

        entries = _entries_walk(
            repo,
            includes=includes,
            excludes=excludes,
            includeobsmarkers=includeobsmarkers,
        )
        chunks = _emit2(repo, entries)
        first = next(chunks)
        file_count, total_file_size = first
        _test_sync_point_walk_3(repo)
    _test_sync_point_walk_4(repo)

    return file_count, total_file_size, chunks


def generatev3(
    repo, includes, excludes, includeobsmarkers: bool
) -> Iterator[bytes | None]:
    """Emit content for version 3 of a streaming clone.

    the data stream consists the following:
    1) A varint E containing the number of entries (can be 0), then E entries follow
    2) For each entry:
    2.1) The number of files in this entry (can be 0, but typically 1 or 2)
    2.2) For each file:
    2.2.1) A char representing the file destination (eg: store or cache)
    2.2.2) A varint N containing the length of the filename
    2.2.3) A varint M containing the length of file data
    2.2.4) N bytes containing the filename (the internal, store-agnostic form)
    2.2.5) M bytes containing the file data

    Returns the data iterator.

    XXX This format is experimental and subject to change. Here is a
    XXX non-exhaustive list of things this format could do or change:

    - making it easier to write files in parallel
    - holding the lock for a shorter time
    - improving progress information
    - ways to adjust the number of expected entries/files ?
    """

    # Python is getting crazy at all the small container we creates while
    # considering the files to preserve, disabling the gc while we do so helps
    # performance a lot.
    with repo.lock(), util.nogc():
        repo.ui.debug(b'scanning\n')

        entries = _entries_walk(
            repo,
            includes=includes,
            excludes=excludes,
            includeobsmarkers=includeobsmarkers,
        )
        chunks = _emit3(repo, list(entries))
        first = next(chunks)
        assert first is None
        _test_sync_point_walk_3(repo)
    _test_sync_point_walk_4(repo)

    return chunks


@contextlib.contextmanager
def nested(*ctxs):
    this = ctxs[0]
    rest = ctxs[1:]
    with this:
        if rest:
            with nested(*rest):
                yield
        else:
            yield


class V2Report:
    """a small class to track the data we saw within the stream"""

    def __init__(self):
        self.byte_count = 0


def consumev2(repo, fp, filecount: int, filesize: int) -> None:
    """Apply the contents from a version 2 streaming clone.

    Data is read from an object that only needs to provide a ``read(size)``
    method.
    """
    with repo.lock():
        repo.ui.status(
            _(b'%d files to transfer, %s of data\n')
            % (filecount, util.bytecount(filesize))
        )
        progress = repo.ui.makeprogress(
            _(b'clone'),
            total=filesize,
            unit=_(b'bytes'),
        )
        start = util.timer()
        report = V2Report()

        vfsmap = _makemap(repo)
        # we keep repo.vfs out of the on purpose, ther are too many danger
        # there (eg: .hg/hgrc),
        #
        # this assert is duplicated (from _makemap) as author might think this
        # is fine, while this is really not fine.
        if repo.vfs in vfsmap.values():
            raise error.ProgrammingError(
                'repo.vfs must not be added to vfsmap for security reasons'
            )

        cpu_profile = scmutil.get_resource_profile(repo.ui, b'cpu')
        mem_profile = scmutil.get_resource_profile(repo.ui, b'memory')
        threaded = repo.ui.configbool(
            b"worker", b"parallel-stream-bundle-processing"
        )
        num_writer = repo.ui.configint(
            b"worker",
            b"parallel-stream-bundle-processing.num-writer",
        )
        if num_writer <= 0:
            num_writer = DEFAULT_NUM_WRITER[cpu_profile]
        memory_target = repo.ui.configbytes(
            b"worker",
            b"parallel-stream-bundle-processing.memory-target",
        )
        if memory_target < 0:
            memory_target = None
        elif memory_target == 0:
            memory_target = DEFAULT_MEMORY_TARGET[mem_profile]
        with repo.transaction(b'clone'):
            ctxs = (vfs.backgroundclosing(repo.ui) for vfs in vfsmap.values())
            with nested(*ctxs):
                workers = []
                info_queue = None
                data_queue = None
                mark_used = None
                try:
                    if not threaded:
                        fc = _FileChunker
                        raw_data = fp
                    else:
                        fc = _ThreadSafeFileChunker
                        data_queue = _DataQueue(memory_target=memory_target)
                        if memory_target is not None:
                            mark_used = data_queue.mark_used
                        raw_data = util.chunkbuffer(data_queue)

                        w = threading.Thread(
                            target=data_queue.fill_from,
                            args=(fp,),
                        )
                        workers.append(w)
                        w.start()
                    files = _v2_parse_files(
                        repo,
                        raw_data,
                        vfsmap,
                        filecount,
                        progress,
                        report,
                        file_chunker=fc,
                        mark_used=mark_used,
                    )
                    if not threaded:
                        _write_files(files)
                    else:
                        info_queue = _FileInfoQueue(files)

                        for __ in range(num_writer):
                            w = threading.Thread(
                                target=_write_files,
                                args=(info_queue,),
                            )
                            workers.append(w)
                            w.start()
                        info_queue.fill()
                except:  # re-raises
                    if data_queue is not None:
                        data_queue.abort()
                    raise
                finally:
                    # shut down all the workers
                    if info_queue is not None:
                        # this is strictly speaking one too many worker for
                        # this queu, but closing too many is not a problem.
                        info_queue.close(len(workers))
                    for w in workers:
                        w.join()

            # force @filecache properties to be reloaded from
            # streamclone-ed file at next access
            repo.invalidate(clearfilecache=True)

        progress.complete()
        # acknowledge the end of the bundle2 part, this help aligning
        # sequential and parallel behavior.
        remains = fp.read(1)
        assert not remains
        _report_transferred(repo, start, filecount, report.byte_count)


# iterator of chunk of bytes that constitute a file content.
FileChunksT = Iterator[bytes]
# Contains the information necessary to write stream file on disk
FileInfoT = tuple[
    bytes,  # real fs path
    Optional[int],  # permission to give to chmod
    FileChunksT,  # content
]


class _Queue:
    """a reimplementation of queue.Queue which doesn't use thread.Condition"""

    def __init__(self):
        self._queue = collections.deque()

        # the "_lock" protect manipulation of the "_queue" deque
        # the "_wait" is used to have the "get" thread waits for the
        # "put" thread when the queue is empty.
        #
        # This is similar to the "threading.Condition", but without the absurd
        # slowness of the stdlib implementation.
        #
        # the "_wait" is always released while holding the "_lock".
        self._lock = threading.Lock()
        self._wait = threading.Lock()

    def put(self, item):
        with self._lock:
            self._queue.append(item)
            # if anyone is waiting on item, unblock it.
            if self._wait.locked():
                self._wait.release()

    def get(self):
        with self._lock:
            while len(self._queue) == 0:
                # "arm"  the waiting lock
                self._wait.acquire(blocking=False)
                # release the lock to let other touch the queue
                # (especially the put call we wait on)
                self._lock.release()
                # wait for for a `put` call to release the lock
                self._wait.acquire()
                # grab the lock to look at a possible available value
                self._lock.acquire()
                # disarm the lock if necessary.
                #
                # If the queue only constains one item, keep the _wait lock
                # armed, as there is no need to wake another waiter anyway.
                if self._wait.locked() and len(self._queue) > 1:
                    self._wait.release()
            return self._queue.popleft()


class _DataQueue:
    """A queue passing data from the bundle stream to other thread

    It has a "memory_target" optional parameter to avoid buffering too much
    information. The implementation is not exact and the memory target might be
    exceed for a time in some situation.
    """

    def __init__(self, memory_target=None):
        self._q = _Queue()
        self._abort = False
        self._memory_target = memory_target
        if self._memory_target is not None and self._memory_target <= 0:
            raise error.ProgrammingError("memory target should be > 0")

        # the "_lock" protect manipulation of the _current_used" variable
        # the "_wait" is used to have the "reading" thread waits for the
        # "using" thread when the buffer is full.
        #
        # This is similar to the "threading.Condition", but without the absurd
        # slowness of the stdlib implementation.
        #
        # the "_wait" is always released while holding the "_lock".
        self._lock = threading.Lock()
        self._wait = threading.Lock()
        # only the stream reader touch this, it is find to touch without the lock
        self._current_read = 0
        # do not touch this without the lock
        self._current_used = 0

    def _has_free_space(self):
        """True if more data can be read without further exceeding memory target

        Must be called under the lock.
        """
        if self._memory_target is None:
            # Ideally we should not even get into the locking business in that
            # case, but we keep the implementation simple for now.
            return True
        return (self._current_read - self._current_used) < self._memory_target

    def mark_used(self, offset):
        """Notify we have used the buffer up to "offset"

        This is meant to be used from another thread than the one filler the queue.
        """
        if self._memory_target is not None:
            with self._lock:
                if offset > self._current_used:
                    self._current_used = offset
                # If the reader is waiting for room, unblock it.
                if self._wait.locked() and self._has_free_space():
                    self._wait.release()

    def fill_from(self, data):
        """fill the data queue from a bundle2 part object

        This is meant to be called by the data reading thread
        """
        q = self._q
        try:
            for item in data:
                self._current_read += len(item)
                q.put(item)
                if self._abort:
                    break
                if self._memory_target is not None:
                    with self._lock:
                        while not self._has_free_space():
                            # make sure the _wait lock is locked
                            # this is done under lock, so there case be no race with the release logic
                            self._wait.acquire(blocking=False)
                            self._lock.release()
                            # acquiring the lock will block until some other thread release it.
                            self._wait.acquire()
                            # lets dive into the locked section again
                            self._lock.acquire()
                            # make sure we release the lock we just grabed if
                            # needed.
                            if self._wait.locked():
                                self._wait.release()
        finally:
            q.put(None)

    def __iter__(self):
        """Iterate over the bundle chunkgs

        This is meant to be called by the data parsing thread."""
        q = self._q
        while (i := q.get()) is not None:
            yield i
            if self._abort:
                break

    def abort(self):
        """stop the data-reading thread and interrupt the comsuming iteration

        This is meant to be called on errors.
        """
        self._abort = True
        self._q.put(None)
        if self._memory_target is not None:
            with self._lock:
                # make sure we unstuck the reader thread.
                if self._wait.locked():
                    self._wait.release()


class _FileInfoQueue:
    """A thread-safe queue to passer parsed file information to the writers"""

    def __init__(self, info: Iterable[FileInfoT]):
        self._info = info
        self._q = _Queue()

    def fill(self):
        """iterate over the parsed information to file the queue

        This is meant to be call from the thread parsing the stream information.
        """
        q = self._q
        for i in self._info:
            q.put(i)

    def close(self, number_worker):
        """signal all the workers that we no longer have any file info coming

        Called from the thread parsing the stream information (and/or the main
        thread if different).
        """
        for __ in range(number_worker):
            self._q.put(None)

    def __iter__(self):
        """iterate over the available file info

        This is meant to be called from the writer threads.
        """
        q = self._q
        while (i := q.get()) is not None:
            yield i


class _FileChunker:
    """yield the chunk that constitute a file

    This class exists as the counterpart of the threaded version and
    would not be very useful on its own.
    """

    def __init__(
        self,
        fp: bundle2mod.unbundlepart,
        data_len: int,
        progress: scmutil.progress,
        report: V2Report,
        mark_used: Callable[[int], None] | None = None,
    ):
        self.report = report
        self.progress = progress
        self._chunks = util.filechunkiter(fp, limit=data_len)

    def fill(self) -> None:
        """Do nothing in non-threading context"""

    def __iter__(self) -> FileChunksT:
        for chunk in self._chunks:
            self.report.byte_count += len(chunk)
            self.progress.increment(step=len(chunk))
            yield chunk


class _ThreadSafeFileChunker(_FileChunker):
    """yield the chunk that constitute a file

    Make sure you  call the "fill" function in the main thread to read the
    right data at the right time.
    """

    def __init__(
        self,
        fp: bundle2mod.unbundlepart,
        data_len: int,
        progress: scmutil.progress,
        report: V2Report,
        mark_used: Callable[[int], None] | None = None,
    ):
        super().__init__(fp, data_len, progress, report)
        self._fp = fp
        self._queue = _Queue()
        self._mark_used = mark_used

    def fill(self) -> None:
        """fill the file chunker queue with data read from the stream

        This is meant to be called from the thread parsing information (and
        consuming the stream data).
        """
        try:
            for chunk in super().__iter__():
                offset = self._fp.tell()
                self._queue.put((chunk, offset))
        finally:
            self._queue.put(None)

    def __iter__(self) -> FileChunksT:
        """Iterate over all the file chunk

        This is meant to be called from the writer threads.
        """
        while (info := self._queue.get()) is not None:
            chunk, offset = info
            if self._mark_used is not None:
                self._mark_used(offset)
            yield chunk


def _trivial_file(
    chunk: bytes,
    mark_used: Callable[[int], None] | None,
    offset: int,
) -> FileChunksT:
    """used for single chunk file,"""
    if mark_used is not None:
        mark_used(offset)
    yield chunk


def _v2_parse_files(
    repo,
    fp: bundle2mod.unbundlepart,
    vfs_map,
    file_count: int,
    progress: scmutil.progress,
    report: V2Report,
    file_chunker: type[_FileChunker] = _FileChunker,
    mark_used: Callable[[int], None] | None = None,
) -> Iterator[FileInfoT]:
    """do the "stream-parsing" part of stream v2

    The parsed information are yield result for consumption by the "writer"
    """
    known_dirs = set()  # set of directory that we know to exists
    progress.update(0)
    for i in range(file_count):
        src = util.readexactly(fp, 1)
        namelen = util.uvarintdecodestream(fp)
        datalen = util.uvarintdecodestream(fp)

        name = util.readexactly(fp, namelen)

        if repo.ui.debugflag:
            repo.ui.debug(
                b'adding [%s] %s (%s)\n' % (src, name, util.bytecount(datalen))
            )
        vfs = vfs_map[src]
        path, mode = vfs.prepare_streamed_file(name, known_dirs)
        if datalen <= util.DEFAULT_FILE_CHUNK:
            c = fp.read(datalen)
            offset = fp.tell()
            report.byte_count += len(c)
            progress.increment(step=len(c))
            chunks = _trivial_file(c, mark_used, offset)
            yield (path, mode, iter(chunks))
        else:
            chunks = file_chunker(
                fp,
                datalen,
                progress,
                report,
                mark_used=mark_used,
            )
            yield (path, mode, iter(chunks))
            # make sure we read all the chunk before moving to the next file
            chunks.fill()


def _write_files(info: Iterable[FileInfoT]):
    """write files from parsed data"""
    io_flags = os.O_WRONLY | os.O_CREAT
    if pycompat.iswindows:
        io_flags |= os.O_BINARY
    for path, mode, data in info:
        if mode is None:
            fd = os.open(path, io_flags)
        else:
            fd = os.open(path, io_flags, mode=mode)
        try:
            for chunk in data:
                written = os.write(fd, chunk)
                # write missing pieces if the write was interrupted
                while written < len(chunk):
                    written = os.write(fd, chunk[written:])
        finally:
            os.close(fd)


def consumev3(repo, fp) -> None:
    """Apply the contents from a version 3 streaming clone.

    Data is read from an object that only needs to provide a ``read(size)``
    method.
    """
    with repo.lock():
        start = util.timer()

        entrycount = util.uvarintdecodestream(fp)
        repo.ui.status(_(b'%d entries to transfer\n') % (entrycount))

        progress = repo.ui.makeprogress(
            _(b'clone'),
            total=entrycount,
            unit=_(b'entries'),
        )
        progress.update(0)
        bytes_transferred = 0

        vfsmap = _makemap(repo)
        # we keep repo.vfs out of the on purpose, there are too many dangers
        # there (eg: .hg/hgrc),
        #
        # this assert is duplicated (from _makemap) as authors might think this
        # is fine, while this is really not fine.
        if repo.vfs in vfsmap.values():
            raise error.ProgrammingError(
                'repo.vfs must not be added to vfsmap for security reasons'
            )
        total_file_count = 0
        with repo.transaction(b'clone'):
            ctxs = (vfs.backgroundclosing(repo.ui) for vfs in vfsmap.values())
            with nested(*ctxs):
                for i in range(entrycount):
                    filecount = util.uvarintdecodestream(fp)
                    if filecount == 0:
                        if repo.ui.debugflag:
                            repo.ui.debug(b'entry with no files [%d]\n' % (i))
                    total_file_count += filecount
                    for i in range(filecount):
                        src = util.readexactly(fp, 1)
                        vfs = vfsmap[src]
                        namelen = util.uvarintdecodestream(fp)
                        datalen = util.uvarintdecodestream(fp)

                        name = util.readexactly(fp, namelen)

                        if repo.ui.debugflag:
                            msg = b'adding [%s] %s (%s)\n'
                            msg %= (src, name, util.bytecount(datalen))
                            repo.ui.debug(msg)
                        bytes_transferred += datalen

                        with vfs(name, b'w') as ofp:
                            for chunk in util.filechunkiter(fp, limit=datalen):
                                ofp.write(chunk)
                    progress.increment(step=1)

            # force @filecache properties to be reloaded from
            # streamclone-ed file at next access
            repo.invalidate(clearfilecache=True)

        progress.complete()
        _report_transferred(repo, start, total_file_count, bytes_transferred)


def applybundlev2(
    repo, fp, filecount: int, filesize: int, requirements: Iterable[bytes]
) -> None:
    from . import localrepo

    missingreqs = [r for r in requirements if r not in repo.supported]
    if missingreqs:
        raise error.Abort(
            _(b'unable to apply stream clone: unsupported format: %s')
            % b', '.join(sorted(missingreqs))
        )

    with util.nogc():
        consumev2(repo, fp, filecount, filesize)

    repo.requirements = new_stream_clone_requirements(
        repo.requirements,
        requirements,
    )
    repo.svfs.options = localrepo.resolvestorevfsoptions(
        repo.ui, repo.requirements, repo.features
    )
    scmutil.writereporequirements(repo)
    nodemap.post_stream_cleanup(repo)


def applybundlev3(repo, fp, requirements: Iterable[bytes]) -> None:
    from . import localrepo

    missingreqs = [r for r in requirements if r not in repo.supported]
    if missingreqs:
        msg = _(b'unable to apply stream clone: unsupported format: %s')
        msg %= b', '.join(sorted(missingreqs))
        raise error.Abort(msg)

    consumev3(repo, fp)

    repo.requirements = new_stream_clone_requirements(
        repo.requirements,
        requirements,
    )
    repo.svfs.options = localrepo.resolvestorevfsoptions(
        repo.ui, repo.requirements, repo.features
    )
    scmutil.writereporequirements(repo)
    nodemap.post_stream_cleanup(repo)


def _copy_files(src_vfs_map, dst_vfs_map, entries, progress) -> bool:
    hardlink = [True]

    def copy_used():
        hardlink[0] = False
        progress.topic = _(b'copying')

    for k, path, optional in entries:
        src_vfs = src_vfs_map[k]
        dst_vfs = dst_vfs_map[k]
        src_path = src_vfs.join(path)
        dst_path = dst_vfs.join(path)
        # We cannot use dirname and makedirs of dst_vfs here because the store
        # encoding confuses them. See issue 6581 for details.
        dirname = os.path.dirname(dst_path)
        if not os.path.exists(dirname):
            util.makedirs(dirname)
        dst_vfs.register_file(path)
        # XXX we could use the #nb_bytes argument.
        try:
            util.copyfile(
                src_path,
                dst_path,
                hardlink=hardlink[0],
                no_hardlink_cb=copy_used,
                check_fs_hardlink=False,
            )
        except FileNotFoundError:
            if not optional:
                raise
        progress.increment()
    return hardlink[0]


def local_copy(src_repo, dest_repo) -> None:
    """copy all content from one local repository to another

    This is useful for local clone"""
    src_store_requirements = {
        r
        for r in src_repo.requirements
        if r not in requirementsmod.WORKING_DIR_REQUIREMENTS
    }
    dest_store_requirements = {
        r
        for r in dest_repo.requirements
        if r not in requirementsmod.WORKING_DIR_REQUIREMENTS
    }
    assert src_store_requirements == dest_store_requirements

    with dest_repo.lock():
        with src_repo.lock():
            # bookmark is not integrated to the streaming as it might use the
            # `repo.vfs` and they are too many sentitive data accessible
            # through `repo.vfs` to expose it to streaming clone.
            src_book_vfs = bookmarks.bookmarksvfs(src_repo)
            srcbookmarks = src_book_vfs.join(b'bookmarks')
            bm_count = 0
            if os.path.exists(srcbookmarks):
                bm_count = 1

            entries = _entries_walk(
                src_repo,
                includes=None,
                excludes=None,
                includeobsmarkers=True,
            )
            entries = list(entries)
            src_vfs_map = _makemap(src_repo)
            dest_vfs_map = _makemap(dest_repo)
            total_files = sum(len(e[1].files()) for e in entries) + bm_count
            progress = src_repo.ui.makeprogress(
                topic=_(b'linking'),
                total=total_files,
                unit=_(b'files'),
            )
            # copy  files
            #
            # We could copy the full file while the source repository is locked
            # and the other one without the lock. However, in the linking case,
            # this would also requires checks that nobody is appending any data
            # to the files while we do the clone, so this is not done yet. We
            # could do this blindly when copying files.
            files = [
                (vfs_key, f.unencoded_path, f.optional)
                for vfs_key, e in entries
                for f in e.files()
            ]
            hardlink = _copy_files(src_vfs_map, dest_vfs_map, files, progress)

            # copy bookmarks over
            if bm_count:
                dst_book_vfs = bookmarks.bookmarksvfs(dest_repo)
                dstbookmarks = dst_book_vfs.join(b'bookmarks')
                util.copyfile(srcbookmarks, dstbookmarks)
        progress.complete()
        if hardlink:
            msg = b'linked %d files\n'
        else:
            msg = b'copied %d files\n'
        src_repo.ui.debug(msg % total_files)

        with dest_repo.transaction(b"localclone") as tr:
            dest_repo.store.write(tr)

        # clean up transaction file as they do not make sense
        transaction.cleanup_undo_files(dest_repo.ui.warn, dest_repo.vfs_map)
