# statichttprepo.py - simple http repository class for mercurial
#
# This provides read-only repo access to repositories exported via static http
#
# Copyright 2005-2007 Olivia Mackall <olivia@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import errno
import typing

from .i18n import _
from .node import sha1nodeconstants
from . import (
    branchmap,
    changelog,
    error,
    localrepo,
    manifest,
    namespaces,
    pathutil,
    pycompat,
    requirements as requirementsmod,
    url,
    util,
    vfs as vfsmod,
)
from .utils import (
    urlutil,
)

if typing.TYPE_CHECKING:
    from typing import (
        Callable,
    )

urlerr = util.urlerr
urlreq = util.urlreq


class httprangereader:
    def __init__(self, url, opener):
        # we assume opener has HTTPRangeHandler
        self.url = url
        self.pos = 0
        self.opener = opener
        self.name = url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def seek(self, pos):
        self.pos = pos

    def read(self, n: int = -1):
        req = urlreq.request(pycompat.strurl(self.url))
        end = ''

        if n == 0:
            return b''
        elif n > 0:
            end = "%d" % (self.pos + n - 1)
        if self.pos or end:
            req.add_header('Range', 'bytes=%d-%s' % (self.pos, end))

        try:
            f = self.opener.open(req)
            data = f.read()
            code = f.code
        except urlerr.httperror as inst:
            num = errno.ENOENT if inst.code == 404 else None
            # Explicitly convert the exception to str as Py3 will try
            # convert it to local encoding and with as the HTTPResponse
            # instance doesn't support encode.
            raise OSError(num, str(inst))
        except urlerr.urlerror as inst:
            raise OSError(None, inst.reason)

        if code == 200:
            # HTTPRangeHandler does nothing if remote does not support
            # Range headers and returns the full entity. Let's slice it.
            if n > 0 and (self.pos + n) < len(data):
                data = data[self.pos : self.pos + n]
            elif self.pos < len(data):
                data = data[self.pos :]
            else:
                data = b''
        elif 0 < n < len(data):
            data = data[:n]
        self.pos += len(data)
        return data

    def readlines(self):
        return self.read().splitlines(True)

    def __iter__(self):
        return iter(self.readlines())

    def close(self):
        pass


# _RangeError and _HTTPRangeHandler were originally in byterange.py,
# which was itself extracted from urlgrabber. See the last version of
# byterange.py from history if you need more information.
class _RangeError(IOError):
    """Error raised when an unsatisfiable range is requested."""


class _HTTPRangeHandler(urlreq.basehandler):
    """Handler that enables HTTP Range headers.

    This was extremely simple. The Range header is a HTTP feature to
    begin with so all this class does is tell urllib2 that the
    "206 Partial Content" response from the HTTP server is what we
    expected.
    """

    def http_error_206(self, req, fp, code, msg, hdrs):
        # 206 Partial Content Response
        r = urlreq.addinfourl(fp, hdrs, req.get_full_url())
        r.code = code
        r.msg = msg
        return r

    def http_error_416(self, req, fp, code, msg, hdrs):
        # HTTP's Range Not Satisfiable error
        raise _RangeError('Requested Range Not Satisfiable')


def build_opener(ui, authinfo):
    # urllib cannot handle URLs with embedded user or passwd
    urlopener = url.opener(ui, authinfo)
    urlopener.add_handler(_HTTPRangeHandler())

    class statichttpvfs(vfsmod.abstractvfs):
        def __init__(self, base):
            self.base = base
            self.options = {}

        def __call__(self, path, mode=b'r', *args, **kw):
            if mode not in (b'r', b'rb'):
                raise OSError('Permission denied')
            f = b"/".join((self.base, urlreq.quote(path)))
            return httprangereader(f, urlopener)

        def _auditpath(self, path: bytes, mode: bytes) -> None:
            raise NotImplementedError

        def join(self, path, *insidef):
            if path:
                return pathutil.join(self.base, path, *insidef)
            else:
                return self.base

    return statichttpvfs


class statichttppeer(localrepo.localpeer):
    def local(self):
        return None

    def canpush(self):
        return False


class statichttprepository(
    localrepo.localrepository, localrepo.revlogfilestorage
):
    supported = localrepo.localrepository._basesupported

    manifestlog: manifest.manifestlog
    sjoin: Callable[[bytes], bytes]

    def __init__(self, ui, path):
        self._url = path
        self.ui = ui

        self.root = path
        u = urlutil.url(path.rstrip(b'/') + b"/.hg")
        self.path, authinfo = u.authinfo()

        vfsclass = build_opener(ui, authinfo)
        self.vfs = vfsclass(self.path)
        self.cachevfs = vfsclass(self.vfs.join(b'cache'))
        self._phasedefaults = []

        self.names = namespaces.namespaces()
        self.filtername = None
        self._extrafilterid = None
        self._wanted_sidedata = set()
        self.features = set()

        try:
            requirements = set(self.vfs.read(b'requires').splitlines())
        except FileNotFoundError:
            requirements = set()

            # check if it is a non-empty old-style repository
            try:
                with self.vfs(b"00changelog.i") as fp:
                    fp.read(1)
            except FileNotFoundError:
                # we do not care about empty old-style repositories here
                msg = _(b"'%s' does not appear to be an hg repository") % path
                raise error.RepoError(msg)
        if requirementsmod.SHARESAFE_REQUIREMENT in requirements:
            storevfs = vfsclass(self.vfs.join(b'store'))
            requirements |= set(storevfs.read(b'requires').splitlines())

        supportedrequirements = localrepo.gathersupportedrequirements(ui)
        localrepo.ensurerequirementsrecognized(
            requirements, supportedrequirements
        )
        localrepo.ensurerequirementscompatible(ui, requirements)
        self.nodeconstants = sha1nodeconstants
        self.nullid = self.nodeconstants.nullid

        # setup store
        self.store = localrepo.makestore(requirements, self.path, vfsclass)
        self.spath = self.store.path
        self.svfs = self.store.opener
        # We can't use Rust because the Rust code cannot cope with the
        # `httprangereader` (yet?)
        self.svfs.rust_compatible = False
        self.sjoin = self.store.join
        self._filecache = {}
        self.requirements = requirements

        rootmanifest = manifest.manifestrevlog(self.nodeconstants, self.svfs)
        self.manifestlog = manifest.manifestlog(
            self.svfs, self, rootmanifest, self.narrowmatch()
        )
        self.changelog = changelog.changelog(self.svfs)
        self._tags = None
        self.nodetagscache = None
        self._branchcaches = branchmap.BranchMapCache()
        self._revbranchcache = None
        self.encodepats = None
        self.decodepats = None
        self._transref = None
        self._dirstate = None

    def _restrictcapabilities(self, caps):
        caps = super()._restrictcapabilities(caps)
        return caps.difference([b"pushkey"])

    def url(self):
        return self._url

    def local(self):
        return False

    def peer(self, path=None, remotehidden=False):
        return statichttppeer(self, path=path, remotehidden=remotehidden)

    def wlock(self, wait=True, steal_from=None):
        raise error.LockUnavailable(
            0,
            pycompat.sysstr(_(b'lock not available')),
            b'lock',
            _(b'cannot lock static-http repository'),
        )

    def lock(self, wait=True, steal_from=None):
        raise error.LockUnavailable(
            0,
            pycompat.sysstr(_(b'lock not available')),
            b'lock',
            _(b'cannot lock static-http repository'),
        )

    def _writecaches(self):
        pass  # statichttprepository are read only


def make_peer(
    ui, path, create, intents=None, createopts=None, remotehidden=False
):
    if create:
        raise error.Abort(_(b'cannot create new static-http repository'))
    url = path.loc[7:]
    return statichttprepository(ui, url).peer(
        path=path, remotehidden=remotehidden
    )
