# bundlerepo.py - repository class for viewing uncompressed bundles
#
# Copyright 2006, 2007 Benoit Boissinot <bboissin@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

"""Repository class for viewing uncompressed bundles.

This provides a read-only repository interface to bundles as if they
were part of the actual repository.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import typing

from .i18n import _
from .node import (
    hex,
    nullrev,
)

from . import (
    bundle2,
    changegroup,
    changelog,
    cmdutil,
    discovery,
    encoding,
    error,
    exchange,
    filelog,
    localrepo,
    manifest,
    mdiff,
    pathutil,
    phases,
    pycompat,
    revlog,
    revlogutils,
    util,
    vfs as vfsmod,
)
from .utils import (
    urlutil,
)

from .revlogutils import (
    constants as revlog_constants,
)


class bundlerevlog(revlog.revlog):
    def __init__(
        self, opener: typing.Any, target, radix, cgunpacker, linkmapper
    ):
        # TODO: figure out real type of opener
        #
        # How it works:
        # To retrieve a revision, we need to know the offset of the revision in
        # the bundle (an unbundle object). We store this offset in the index
        # (start). The base of the delta is stored in the base field.
        #
        # To differentiate a rev in the bundle from a rev in the revlog, we
        # check revision against repotiprev.
        opener = vfsmod.readonlyvfs(opener)
        revlog.revlog.__init__(
            self,
            opener,
            target=target,
            radix=radix,
            writable=False,
        )
        self.bundle = cgunpacker
        n = len(self)
        self.repotiprev = n - 1
        self.bundlerevs = set()  # used by 'bundle()' revset expression
        for deltadata in cgunpacker.deltaiter():
            size = len(deltadata.delta)
            start = cgunpacker.tell() - size

            if self.index.has_node(deltadata.node):
                # this can happen if two branches make the same change
                self.bundlerevs.add(self.index.rev(deltadata.node))
                continue
            if deltadata.link_node == deltadata.node:
                linkrev = nullrev
            else:
                linkrev = linkmapper(deltadata.link_node)

            for p in (deltadata.p1, deltadata.p2):
                if not self.index.has_node(p):
                    raise error.LookupError(
                        p, self.display_id, _(b"unknown parent")
                    )

            if not self.index.has_node(deltadata.delta_base):
                raise error.LookupError(
                    deltadata.delta_base,
                    self.display_id,
                    _(b'unknown delta base'),
                )

            baserev = self.rev(deltadata.delta_base)
            e = revlogutils.entry(
                flags=deltadata.flags,
                data_offset=start,
                data_compressed_length=size,
                data_delta_base=baserev,
                link_rev=linkrev,
                parent_rev_1=self.rev(deltadata.p1),
                parent_rev_2=self.rev(deltadata.p2),
                node_id=deltadata.node,
            )
            self.index.append(e)
            self.bundlerevs.add(n)
            n += 1

    @contextlib.contextmanager
    def reading(self):
        if self.repotiprev < 0:
            yield
        else:
            with super().reading() as x:
                yield x

    def _chunk(self, rev):
        # Warning: in case of bundle, the diff is against what we stored as
        # delta base, not against rev - 1
        # XXX: could use some caching
        if rev <= self.repotiprev:
            return super()._inner._chunk(rev)
        self.bundle.seek(self.start(rev))
        return self.bundle.read(self.length(rev))

    def revdiff(self, rev1, rev2):
        """return or calculate a delta between two revisions"""
        if rev1 > self.repotiprev and rev2 > self.repotiprev:
            # hot path for bundle
            revb = self.index[rev2][3]
            if revb == rev1:
                return self._chunk(rev2)
        elif rev1 <= self.repotiprev and rev2 <= self.repotiprev:
            return revlog.revlog.revdiff(self, rev1, rev2)

        return mdiff.textdiff(self.rawdata(rev1), self.rawdata(rev2))

    def _rawtext(self, node, rev):
        if rev is None:
            rev = self.rev(node)
        validated = False
        rawtext = None
        chain = []
        iterrev = rev
        # reconstruct the revision if it is from a changegroup
        while iterrev > self.repotiprev:
            if (
                self._inner._revisioncache
                and self._inner._revisioncache[1] == iterrev
            ):
                rawtext = self._inner._revisioncache[2]
                break
            chain.append(iterrev)
            iterrev = self.index[iterrev][3]
        if iterrev == nullrev:
            rawtext = b''
        elif rawtext is None:
            r = super()._rawtext(
                self.node(iterrev),
                iterrev,
            )
            __, rawtext, validated = r
        if chain:
            validated = False
        while chain:
            delta = self._chunk(chain.pop())
            rawtext = mdiff.patches(rawtext, [delta])
        return rev, rawtext, validated

    def addrevision(self, *args, **kwargs):
        raise NotImplementedError

    def addgroup(self, *args, **kwargs):
        raise NotImplementedError

    def strip(self, *args, **kwargs):
        raise NotImplementedError

    def checksize(self):
        raise NotImplementedError


class bundlechangelog(bundlerevlog, changelog.changelog):
    def __init__(self, opener, cgunpacker):
        changelog.changelog.__init__(self, opener)
        linkmapper = lambda x: x
        bundlerevlog.__init__(
            self,
            opener,
            (revlog_constants.KIND_CHANGELOG, None),
            self.radix,
            cgunpacker,
            linkmapper,
        )


class bundlemanifest(bundlerevlog, manifest.manifestrevlog):
    def __init__(
        self,
        nodeconstants,
        opener,
        cgunpacker,
        linkmapper,
        dirlogstarts=None,
        dir=b'',
    ):
        # XXX manifestrevlog is not actually a revlog , so mixing it with
        # bundlerevlog is not a good idea.
        manifest.manifestrevlog.__init__(self, nodeconstants, opener, tree=dir)
        bundlerevlog.__init__(
            self,
            opener,
            (revlog_constants.KIND_MANIFESTLOG, dir),
            self._revlog.radix,
            cgunpacker,
            linkmapper,
        )
        if dirlogstarts is None:
            dirlogstarts = {}
            if self.bundle.version == b"03":
                dirlogstarts = _getfilestarts(self.bundle)
        self._dirlogstarts = dirlogstarts
        self._linkmapper = linkmapper

    def dirlog(self, d):
        if d in self._dirlogstarts:
            self.bundle.seek(self._dirlogstarts[d])
            return bundlemanifest(
                self.nodeconstants,
                self.opener,
                self.bundle,
                self._linkmapper,
                self._dirlogstarts,
                dir=d,
            )
        return super().dirlog(d)


class bundlefilelog(filelog.filelog):
    def __init__(self, opener, path, cgunpacker, linkmapper):
        filelog.filelog.__init__(self, opener, path, writable=False)
        self._revlog = bundlerevlog(
            opener,
            # XXX should use the unencoded path
            target=(revlog_constants.KIND_FILELOG, path),
            radix=self._revlog.radix,
            cgunpacker=cgunpacker,
            linkmapper=linkmapper,
        )


class bundlepeer(localrepo.localpeer):
    def canpush(self):
        return False


class bundlephasecache(phases.phasecache):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if hasattr(self, 'opener'):
            self.opener = vfsmod.readonlyvfs(self.opener)

    def write(self, repo):
        raise NotImplementedError

    def _write(self, repo, fp):
        raise NotImplementedError

    def _updateroots(self, repo, phase, newroots, tr, invalidate=True):
        self._phaseroots[phase] = newroots
        if invalidate:
            self.invalidate()
        self.dirty = True


def _getfilestarts(cgunpacker):
    filespos = {}
    for chunkdata in iter(cgunpacker.filelogheader, {}):
        fname = chunkdata[b'filename']
        filespos[fname] = cgunpacker.tell()
        for chunk in iter(lambda: cgunpacker.deltachunk(None), None):
            pass
    return filespos


_bundle_repo_baseclass = object

if typing.TYPE_CHECKING:
    _bundle_repo_baseclass = localrepo.localrepository


class bundlerepository(_bundle_repo_baseclass):
    """A repository instance that is a union of a local repo and a bundle.

    Instances represent a read-only repository composed of a local repository
    with the contents of a bundle file applied. The repository instance is
    conceptually similar to the state of a repository after an
    ``hg unbundle`` operation. However, the contents of the bundle are never
    applied to the actual base repository.

    Instances constructed directly are not usable as repository objects.
    Use instance() or makebundlerepository() to create instances.
    """

    def __init__(self, bundlepath, url, tempparent):
        self._tempparent = tempparent
        self._url = url

        self.ui.setconfig(b'phases', b'publish', False, b'bundlerepo')

        # dict with the mapping 'filename' -> position in the changegroup.
        self._cgfilespos = {}
        self._bundlefile = None
        self._cgunpacker = None
        self.tempfile = None
        f = util.posixfile(bundlepath, b"rb")
        bundle = exchange.readbundle(self.ui, f, bundlepath)

        if isinstance(bundle, bundle2.unbundle20):
            self._bundlefile = bundle

            cgpart = None
            for part in bundle.iterparts(seekable=True):
                if part.type == b'phase-heads':
                    self._handle_bundle2_phase_part(bundle, part)
                elif part.type == b'changegroup':
                    if cgpart:
                        raise NotImplementedError(
                            b"can't process multiple changegroups"
                        )
                    cgpart = part
                    self._handle_bundle2_cg_part(bundle, part)

            if not cgpart:
                raise error.Abort(_(b"No changegroups found"))

            # This is required to placate a later consumer, which expects
            # the payload offset to be at the beginning of the changegroup.
            # We need to do this after the iterparts() generator advances
            # because iterparts() will seek to end of payload after the
            # generator returns control to iterparts().
            cgpart.seek(0, os.SEEK_SET)

        elif isinstance(bundle, changegroup.cg1unpacker):
            self._handle_bundle1(bundle, bundlepath)
        else:
            raise error.Abort(
                _(b'bundle type %r cannot be read') % type(bundle)
            )

    def _handle_bundle1(self, bundle, bundlepath):
        if bundle.compressed():
            f = self._writetempbundle(bundle.read, b'.hg10un', header=b'HG10UN')
            bundle = exchange.readbundle(self.ui, f, bundlepath, self.vfs)

        self._bundlefile = bundle
        self._cgunpacker = bundle

        self.firstnewrev = self.changelog.repotiprev + 1
        phases.retractboundary(
            self,
            None,
            phases.draft,
            [ctx.node() for ctx in self[self.firstnewrev :]],
        )

    def _handle_bundle2_cg_part(self, bundle, part):
        assert part.type == b'changegroup'
        cgstream = part
        targetphase = part.params.get(b'targetphase')
        try:
            targetphase = int(targetphase)
        except TypeError:
            pass
        if targetphase is None:
            targetphase = phases.draft
        if targetphase not in phases.allphases:
            m = _(b'unsupported targetphase: %d')
            m %= targetphase
            raise error.Abort(m)
        version = part.params.get(b'version', b'01')
        legalcgvers = changegroup.supportedincomingversions(self)
        if version not in legalcgvers:
            msg = _(b'Unsupported changegroup version: %s')
            raise error.Abort(msg % version)
        if bundle.compressed():
            cgstream = self._writetempbundle(part.read, b'.cg%sun' % version)

        self._cgunpacker = changegroup.getunbundler(version, cgstream, b'UN')

        self.firstnewrev = self.changelog.repotiprev + 1
        phases.retractboundary(
            self,
            None,
            targetphase,
            [ctx.node() for ctx in self[self.firstnewrev :]],
        )

    def _handle_bundle2_phase_part(self, bundle, part):
        assert part.type == b'phase-heads'

        unfi = self.unfiltered()
        headsbyphase = phases.binarydecode(part)
        phases.updatephases(unfi, lambda: None, headsbyphase)

    def _writetempbundle(self, readfn, suffix, header=b''):
        """Write a temporary file to disk"""
        fdtemp, temp = self.vfs.mkstemp(prefix=b"hg-bundle-", suffix=suffix)
        self.tempfile = temp

        with os.fdopen(fdtemp, 'wb') as fptemp:
            fptemp.write(header)
            while True:
                chunk = readfn(2**18)
                if not chunk:
                    break
                fptemp.write(chunk)

        return self.vfs.open(self.tempfile, mode=b"rb")

    @localrepo.unfilteredpropertycache
    def _phasecache(self):
        return bundlephasecache(self, self._phasedefaults)

    @localrepo.unfilteredpropertycache
    def changelog(self):
        # consume the header if it exists
        self._cgunpacker.changelogheader()
        c = bundlechangelog(self.svfs, self._cgunpacker)
        self.manstart = self._cgunpacker.tell()
        return c

    def _refreshchangelog(self):
        # changelog for bundle repo are not filecache, this method is not
        # applicable.
        pass

    @localrepo.unfilteredpropertycache
    def manifestlog(self):
        self._cgunpacker.seek(self.manstart)
        # consume the header if it exists
        self._cgunpacker.manifestheader()
        linkmapper = self.unfiltered().changelog.rev
        rootstore = bundlemanifest(
            self.nodeconstants, self.svfs, self._cgunpacker, linkmapper
        )
        self.filestart = self._cgunpacker.tell()

        return manifest.manifestlog(
            self.svfs, self, rootstore, self.narrowmatch()
        )

    def _consumemanifest(self):
        """Consumes the manifest portion of the bundle, setting filestart so the
        file portion can be read."""
        self._cgunpacker.seek(self.manstart)
        self._cgunpacker.manifestheader()
        for delta in self._cgunpacker.deltaiter():
            pass
        self.filestart = self._cgunpacker.tell()

    @localrepo.unfilteredpropertycache
    def manstart(self):
        self.changelog
        return self.manstart

    @localrepo.unfilteredpropertycache
    def filestart(self):
        self.manifestlog

        # If filestart was not set by self.manifestlog, that means the
        # manifestlog implementation did not consume the manifests from the
        # changegroup (ex: it might be consuming trees from a separate bundle2
        # part instead). So we need to manually consume it.
        if 'filestart' not in self.__dict__:
            self._consumemanifest()

        return self.filestart

    def url(self):
        return self._url

    def file(self, f, writable=False):
        if not self._cgfilespos:
            self._cgunpacker.seek(self.filestart)
            self._cgfilespos = _getfilestarts(self._cgunpacker)

        if f in self._cgfilespos:
            self._cgunpacker.seek(self._cgfilespos[f])
            linkmapper = self.unfiltered().changelog.rev
            return bundlefilelog(self.svfs, f, self._cgunpacker, linkmapper)
        else:
            return super().file(f, writable=writable)

    def close(self):
        """Close assigned bundle file immediately."""
        self._bundlefile.close()
        if self.tempfile is not None:
            self.vfs.unlink(self.tempfile)
        if self._tempparent:
            shutil.rmtree(self._tempparent, True)

    def cancopy(self):
        return False

    def peer(self, path=None, remotehidden=False):
        return bundlepeer(self, path=path, remotehidden=remotehidden)

    def getcwd(self):
        return encoding.getcwd()  # always outside the repo

    # Check if parents exist in localrepo before setting
    def setparents(self, p1, p2=None):
        if p2 is None:
            p2 = self.nullid
        p1rev = self.changelog.rev(p1)
        p2rev = self.changelog.rev(p2)
        msg = _(b"setting parent to node %s that only exists in the bundle\n")
        if self.changelog.repotiprev < p1rev:
            self.ui.warn(msg % hex(p1))
        if self.changelog.repotiprev < p2rev:
            self.ui.warn(msg % hex(p2))
        return super().setparents(p1, p2)


def instance(ui, path, create, intents=None, createopts=None):
    if create:
        raise error.Abort(_(b'cannot create new bundle repository'))
    # internal config: bundle.mainreporoot
    parentpath = ui.config(b"bundle", b"mainreporoot")
    if not parentpath:
        # try to find the correct path to the working directory repo
        parentpath = cmdutil.findrepo(encoding.getcwd())
        if parentpath is None:
            parentpath = b''
    if parentpath:
        # Try to make the full path relative so we get a nice, short URL.
        # In particular, we don't want temp dir names in test outputs.
        cwd = encoding.getcwd()
        if parentpath == cwd:
            parentpath = b''
        else:
            cwd = pathutil.normasprefix(cwd)
            if parentpath.startswith(cwd):
                parentpath = parentpath[len(cwd) :]
    u = urlutil.url(path)
    path = u.localpath()
    if u.scheme == b'bundle':
        s = path.split(b"+", 1)
        if len(s) == 1:
            repopath, bundlename = parentpath, s[0]
        else:
            repopath, bundlename = s
    else:
        repopath, bundlename = parentpath, path

    return makebundlerepository(ui, repopath, bundlename)


def makebundlerepository(ui, repopath, bundlepath):
    """Make a bundle repository object based on repo and bundle paths."""
    if repopath:
        url = b'bundle:%s+%s' % (util.expandpath(repopath), bundlepath)
    else:
        url = b'bundle:%s' % bundlepath

    # Because we can't make any guarantees about the type of the base
    # repository, we can't have a static class representing the bundle
    # repository. We also can't make any guarantees about how to even
    # call the base repository's constructor!
    #
    # So, our strategy is to go through ``localrepo.instance()`` to construct
    # a repo instance. Then, we dynamically create a new type derived from
    # both it and our ``bundlerepository`` class which overrides some
    # functionality. We then change the type of the constructed repository
    # to this new type and initialize the bundle-specific bits of it.

    try:
        repo = localrepo.instance(ui, repopath, create=False)
        tempparent = None
    except error.RequirementError:
        raise  # no fallback if the backing repo is unsupported
    except error.RepoError:
        tempparent = pycompat.mkdtemp()
        try:
            repo = localrepo.instance(ui, tempparent, create=True)
        except Exception:
            shutil.rmtree(tempparent)
            raise

    class derivedbundlerepository(bundlerepository, repo.__class__):
        pass

    repo.__class__ = derivedbundlerepository
    bundlerepository.__init__(repo, bundlepath, url, tempparent)

    return repo


class bundletransactionmanager:
    def transaction(self):
        return None

    def close(self):
        raise NotImplementedError

    def release(self):
        raise NotImplementedError


class getremotechanges_state_tracker:
    def __init__(self, peer, incoming, common, rheads):
        # bundle file to be deleted
        self.bundle = None
        # bundle repo to be closed
        self.bundlerepo = None
        # remote peer connection to be closed
        self.peer = peer
        # if peer is remote, `localrepo` will be equal to
        # `bundlerepo` when bundle is created.
        self.localrepo = peer.local()

        # `incoming` operation parameters:
        # (these get mutated by _create_bundle)
        self.incoming = incoming
        self.common = common
        self.rheads = rheads

    def cleanup(self):
        try:
            if self.bundlerepo:
                self.bundlerepo.close()
        finally:
            try:
                if self.bundle:
                    os.unlink(self.bundle)
            finally:
                self.peer.close()


def getremotechanges(
    ui, repo, peer, onlyheads=None, bundlename=None, force=False
):
    """obtains a bundle of changes incoming from peer

    "onlyheads" restricts the returned changes to those reachable from the
      specified heads.
    "bundlename", if given, stores the bundle to this file path permanently;
      otherwise it's stored to a temp file and gets deleted again when you call
      the returned "cleanupfn".
    "force" indicates whether to proceed on unrelated repos.

    Returns a tuple (local, csets, cleanupfn):

    "local" is a local repo from which to obtain the actual incoming
      changesets; it is a bundlerepo for the obtained bundle when the
      original "peer" is remote.
    "csets" lists the incoming changeset node ids.
    "cleanupfn" must be called without arguments when you're done processing
      the changes; it closes both the original "peer" and the one returned
      here.
    """
    tmp = discovery.findcommonincoming(repo, peer, heads=onlyheads, force=force)
    common, incoming, rheads = tmp
    if not incoming:
        try:
            if bundlename:
                os.unlink(bundlename)
        except OSError:
            pass
        return repo, [], peer.close

    commonset = set(common)
    rheads = [x for x in rheads if x not in commonset]

    state = getremotechanges_state_tracker(peer, incoming, common, rheads)

    try:
        csets = _getremotechanges_slowpath(
            state, ui, repo, bundlename=bundlename, onlyheads=onlyheads
        )
        return (state.localrepo, csets, state.cleanup)
    except:  # re-raises
        state.cleanup()
        raise


def _create_bundle(state, ui, repo, bundlename, onlyheads):
    # create a bundle (uncompressed if peer repo is not local)

    # developer config: devel.legacy.exchange
    legexc = ui.configlist(b'devel', b'legacy.exchange')
    forcebundle1 = b'bundle2' not in legexc and b'bundle1' in legexc
    canbundle2 = (
        not forcebundle1
        and state.peer.capable(b'getbundle')
        and state.peer.capable(b'bundle2')
    )
    if canbundle2:
        with state.peer.commandexecutor() as e:
            b2 = e.callcommand(
                b'getbundle',
                {
                    b'source': b'incoming',
                    b'common': state.common,
                    b'heads': state.rheads,
                    b'bundlecaps': exchange.caps20to10(repo, role=b'client'),
                    b'cg': True,
                },
            ).result()

            fname = state.bundle = changegroup.writechunks(
                ui, b2._forwardchunks(), bundlename
            )
    else:
        if state.peer.capable(b'getbundle'):
            with state.peer.commandexecutor() as e:
                cg = e.callcommand(
                    b'getbundle',
                    {
                        b'source': b'incoming',
                        b'common': state.common,
                        b'heads': state.rheads,
                    },
                ).result()
        elif onlyheads is None and not state.peer.capable(b'changegroupsubset'):
            # compat with older servers when pulling all remote heads

            with state.peer.commandexecutor() as e:
                cg = e.callcommand(
                    b'changegroup',
                    {
                        b'nodes': state.incoming,
                        b'source': b'incoming',
                    },
                ).result()

            state.rheads = None
        else:
            with state.peer.commandexecutor() as e:
                cg = e.callcommand(
                    b'changegroupsubset',
                    {
                        b'bases': state.incoming,
                        b'heads': state.rheads,
                        b'source': b'incoming',
                    },
                ).result()

        if state.localrepo:
            bundletype = b"HG10BZ"
        else:
            bundletype = b"HG10UN"
        fname = state.bundle = bundle2.writebundle(
            ui, cg, bundlename, bundletype
        )
    # keep written bundle?
    if bundlename:
        state.bundle = None

    return fname


def _getremotechanges_slowpath(
    state, ui, repo, bundlename=None, onlyheads=None
):
    if bundlename or not state.localrepo:
        fname = _create_bundle(
            state,
            ui,
            repo,
            bundlename=bundlename,
            onlyheads=onlyheads,
        )
        if not state.localrepo:
            # use the created uncompressed bundlerepo
            state.localrepo = state.bundlerepo = makebundlerepository(
                repo.baseui, repo.root, fname
            )

            # this repo contains local and peer now, so filter out local again
            state.common = repo.heads()

    if state.localrepo:
        # Part of common may be remotely filtered
        # So use an unfiltered version
        # The discovery process probably need cleanup to avoid that
        state.localrepo = state.localrepo.unfiltered()

    csets = state.localrepo.changelog.findmissing(state.common, state.rheads)

    if state.bundlerepo:
        bundlerepo = state.bundlerepo
        reponodes = [ctx.node() for ctx in bundlerepo[bundlerepo.firstnewrev :]]

        with state.peer.commandexecutor() as e:
            remotephases = e.callcommand(
                b'listkeys',
                {
                    b'namespace': b'phases',
                },
            ).result()

        pullop = exchange.pulloperation(
            bundlerepo, state.peer, path=None, heads=reponodes
        )
        pullop.trmanager = bundletransactionmanager()
        exchange._pullapplyphases(pullop, remotephases)

    return csets
