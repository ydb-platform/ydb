# repository.py - Interfaces and base classes for repositories and peers.
#
# Copyright 2017 Gregory Szorc <gregory.szorc@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import abc
import typing

from typing import (
    Any,
    Callable,
    Collection,
    Iterable,
    Iterator,
    Mapping,
    Protocol,
)

from ..i18n import _
from .. import error

if typing.TYPE_CHECKING:
    # We need to fully qualify the set primitive when typing the imanifestdict
    # class, so its set() method doesn't hide the primitive.
    import builtins

    from collections.abc import (
        ByteString,  # TODO: change to Buffer for 3.14
    )

    from ._basetypes import (
        HgPathT,
        RevsetAliasesT,
        UiT as Ui,
        VfsT as Vfs,
    )

    from . import (
        dirstate as intdirstate,
        matcher,
        misc,
        status as istatus,
        transaction as inttxn,
    )

    # TODO: make a protocol class for this
    NodeConstants = Any


# Local repository feature string.

# Revlogs are being used for file storage.
REPO_FEATURE_REVLOG_FILE_STORAGE = b'revlogfilestorage'
# The storage part of the repository is shared from an external source.
REPO_FEATURE_SHARED_STORAGE = b'sharedstore'
# LFS supported for backing file storage.
REPO_FEATURE_LFS = b'lfs'
# Repository supports being stream cloned.
REPO_FEATURE_STREAM_CLONE = b'streamclone'
# Repository supports (at least) some sidedata to be stored
REPO_FEATURE_SIDE_DATA = b'side-data'
# Files storage may lack data for all ancestors.
REPO_FEATURE_SHALLOW_FILE_STORAGE = b'shallowfilestorage'

REVISION_FLAG_CENSORED = 1 << 15
REVISION_FLAG_ELLIPSIS = 1 << 14
REVISION_FLAG_EXTSTORED = 1 << 13
REVISION_FLAG_HASCOPIESINFO = 1 << 12  # used only by changelog
FILEREVISION_FLAG_HASMETA = 1 << 11  # used only by filelog
# XXX the two above could be combined in one
REVISION_FLAG_DELTA_IS_SNAPSHOT = 1 << 10

REVISION_FLAGS_KNOWN = (
    REVISION_FLAG_CENSORED
    | REVISION_FLAG_ELLIPSIS
    | REVISION_FLAG_EXTSTORED
    | REVISION_FLAG_HASCOPIESINFO
    | FILEREVISION_FLAG_HASMETA
    | REVISION_FLAG_DELTA_IS_SNAPSHOT
)

CG_DELTAMODE_STD = b'default'
CG_DELTAMODE_PREV = b'previous'
CG_DELTAMODE_FULL = b'fulltext'
CG_DELTAMODE_P1 = b'p1'


## Cache related constants:
#
# Used to control which cache should be warmed in a repo.updatecaches(…) call.

# Warm branchmaps of all known repoview's filter-level
CACHE_BRANCHMAP_ALL = b"branchmap-all"
# Warm branchmaps of repoview's filter-level used by server
CACHE_BRANCHMAP_SERVED = b"branchmap-served"
# Warm internal changelog cache (eg: persistent nodemap)
CACHE_CHANGELOG_CACHE = b"changelog-cache"
# check of a branchmap can use the "pure topo" mode
CACHE_BRANCHMAP_DETECT_PURE_TOPO = b"branchmap-detect-pure-topo"
# Warm full manifest cache
CACHE_FULL_MANIFEST = b"full-manifest"
# Warm file-node-tags cache
CACHE_FILE_NODE_TAGS = b"file-node-tags"
# Warm internal manifestlog cache (eg: persistent nodemap)
CACHE_MANIFESTLOG_CACHE = b"manifestlog-cache"
# Warn rev branch cache
CACHE_REV_BRANCH = b"rev-branch-cache"
# Warm tags' cache for default repoview'
CACHE_TAGS_DEFAULT = b"tags-default"
# Warm tags' cache for  repoview's filter-level used by server
CACHE_TAGS_SERVED = b"tags-served"

# the cache to warm by default after a simple transaction
# (this is a mutable set to let extension update it)
CACHES_DEFAULT = {
    CACHE_BRANCHMAP_SERVED,
}

# the caches to warm when warming all of them
# (this is a mutable set to let extension update it)
CACHES_ALL = {
    CACHE_BRANCHMAP_SERVED,
    CACHE_BRANCHMAP_ALL,
    CACHE_BRANCHMAP_DETECT_PURE_TOPO,
    CACHE_REV_BRANCH,
    CACHE_CHANGELOG_CACHE,
    CACHE_FILE_NODE_TAGS,
    CACHE_FULL_MANIFEST,
    CACHE_MANIFESTLOG_CACHE,
    CACHE_TAGS_DEFAULT,
    CACHE_TAGS_SERVED,
}

# the cache to warm by default on simple call
# (this is a mutable set to let extension update it)
CACHES_POST_CLONE = CACHES_ALL.copy()
CACHES_POST_CLONE.discard(CACHE_FILE_NODE_TAGS)
CACHES_POST_CLONE.discard(CACHE_REV_BRANCH)


class _ipeerconnection(Protocol):
    """Represents a "connection" to a repository.

    This is the base interface for representing a connection to a repository.
    It holds basic properties and methods applicable to all peer types.

    This is not a complete interface definition and should not be used
    outside of this module.
    """

    ui: Ui
    """ui.ui instance"""

    path: misc.IPath | None
    """a urlutil.path instance or None"""

    @abc.abstractmethod
    def url(self):
        """Returns a URL string representing this peer.

        Currently, implementations expose the raw URL used to construct the
        instance. It may contain credentials as part of the URL. The
        expectations of the value aren't well-defined and this could lead to
        data leakage.

        TODO audit/clean consumers and more clearly define the contents of this
        value.
        """

    @abc.abstractmethod
    def local(self):
        """Returns a local repository instance.

        If the peer represents a local repository, returns an object that
        can be used to interface with it. Otherwise returns ``None``.
        """

    @abc.abstractmethod
    def canpush(self):
        """Returns a boolean indicating if this peer can be pushed to."""

    @abc.abstractmethod
    def close(self):
        """Close the connection to this peer.

        This is called when the peer will no longer be used. Resources
        associated with the peer should be cleaned up.
        """


class ipeercapabilities(Protocol):
    """Peer sub-interface related to capabilities."""

    @abc.abstractmethod
    def capable(self, name: bytes) -> bool | bytes:
        """Determine support for a named capability.

        Returns ``False`` if capability not supported.

        Returns ``True`` if boolean capability is supported. Returns a string
        if capability support is non-boolean.

        Capability strings may or may not map to wire protocol capabilities.
        """

    @abc.abstractmethod
    def capabilities(self) -> set[bytes]:
        """Obtain capabilities of the peer.

        Returns a set of string capabilities.
        """

    @abc.abstractmethod
    def requirecap(self, name: bytes, purpose: bytes) -> None:
        """Require a capability to be present.

        Raises a ``CapabilityError`` if the capability isn't present.
        """


class ipeercommands(Protocol):
    """Client-side interface for communicating over the wire protocol.

    This interface is used as a gateway to the Mercurial wire protocol.
    methods commonly call wire protocol commands of the same name.
    """

    @abc.abstractmethod
    def branchmap(self):
        """Obtain heads in named branches.

        Returns a dict mapping branch name to an iterable of nodes that are
        heads on that branch.
        """

    @abc.abstractmethod
    def get_cached_bundle_inline(self, path):
        """Retrieve a clonebundle across the wire.

        Returns a chunkbuffer
        """

    @abc.abstractmethod
    def clonebundles(self):
        """Obtains the clone bundles manifest for the repo.

        Returns the manifest as unparsed bytes.
        """

    @abc.abstractmethod
    def debugwireargs(self, one, two, three=None, four=None, five=None):
        """Used to facilitate debugging of arguments passed over the wire."""

    @abc.abstractmethod
    def getbundle(self, source, **kwargs):
        """Obtain remote repository data as a bundle.

        This command is how the bulk of repository data is transferred from
        the peer to the local repository

        Returns a generator of bundle data.
        """

    @abc.abstractmethod
    def heads(self):
        """Determine all known head revisions in the peer.

        Returns an iterable of binary nodes.
        """

    @abc.abstractmethod
    def known(self, nodes):
        """Determine whether multiple nodes are known.

        Accepts an iterable of nodes whose presence to check for.

        Returns an iterable of booleans indicating of the corresponding node
        at that index is known to the peer.
        """

    @abc.abstractmethod
    def listkeys(self, namespace):
        """Obtain all keys in a pushkey namespace.

        Returns an iterable of key names.
        """

    @abc.abstractmethod
    def lookup(self, key):
        """Resolve a value to a known revision.

        Returns a binary node of the resolved revision on success.
        """

    @abc.abstractmethod
    def pushkey(self, namespace, key, old, new):
        """Set a value using the ``pushkey`` protocol.

        Arguments correspond to the pushkey namespace and key to operate on and
        the old and new values for that key.

        Returns a string with the peer result. The value inside varies by the
        namespace.
        """

    @abc.abstractmethod
    def stream_out(self):
        """Obtain streaming clone data.

        Successful result should be a generator of data chunks.
        """

    @abc.abstractmethod
    def unbundle(self, bundle, heads, url):
        """Transfer repository data to the peer.

        This is how the bulk of data during a push is transferred.

        Returns the integer number of heads added to the peer.
        """


class ipeerlegacycommands(Protocol):
    """Interface for implementing support for legacy wire protocol commands.

    Wire protocol commands transition to legacy status when they are no longer
    used by modern clients. To facilitate identifying which commands are
    legacy, the interfaces are split.
    """

    @abc.abstractmethod
    def between(self, pairs):
        """Obtain nodes between pairs of nodes.

        ``pairs`` is an iterable of node pairs.

        Returns an iterable of iterables of nodes corresponding to each
        requested pair.
        """

    @abc.abstractmethod
    def branches(self, nodes):
        """Obtain ancestor changesets of specific nodes back to a branch point.

        For each requested node, the peer finds the first ancestor node that is
        a DAG root or is a merge.

        Returns an iterable of iterables with the resolved values for each node.
        """

    @abc.abstractmethod
    def changegroup(self, nodes, source):
        """Obtain a changegroup with data for descendants of specified nodes."""

    @abc.abstractmethod
    def changegroupsubset(self, bases, heads, source):
        pass


class ipeercommandexecutor(Protocol):
    """Represents a mechanism to execute remote commands.

    This is the primary interface for requesting that wire protocol commands
    be executed. Instances of this interface are active in a context manager
    and have a well-defined lifetime. When the context manager exits, all
    outstanding requests are waited on.
    """

    @abc.abstractmethod
    def callcommand(self, name, args):
        """Request that a named command be executed.

        Receives the command name and a dictionary of command arguments.

        Returns a ``concurrent.futures.Future`` that will resolve to the
        result of that command request. That exact value is left up to
        the implementation and possibly varies by command.

        Not all commands can coexist with other commands in an executor
        instance: it depends on the underlying wire protocol transport being
        used and the command itself.

        Implementations MAY call ``sendcommands()`` automatically if the
        requested command can not coexist with other commands in this executor.

        Implementations MAY call ``sendcommands()`` automatically when the
        future's ``result()`` is called. So, consumers using multiple
        commands with an executor MUST ensure that ``result()`` is not called
        until all command requests have been issued.
        """

    @abc.abstractmethod
    def sendcommands(self):
        """Trigger submission of queued command requests.

        Not all transports submit commands as soon as they are requested to
        run. When called, this method forces queued command requests to be
        issued. It will no-op if all commands have already been sent.

        When called, no more new commands may be issued with this executor.
        """

    @abc.abstractmethod
    def close(self):
        """Signal that this command request is finished.

        When called, no more new commands may be issued. All outstanding
        commands that have previously been issued are waited on before
        returning. This not only includes waiting for the futures to resolve,
        but also waiting for all response data to arrive. In other words,
        calling this waits for all on-wire state for issued command requests
        to finish.

        When used as a context manager, this method is called when exiting the
        context manager.

        This method may call ``sendcommands()`` if there are buffered commands.
        """


class ipeerrequests(Protocol):
    """Interface for executing commands on a peer."""

    limitedarguments: bool
    """True if the peer cannot receive large argument value for commands."""

    @abc.abstractmethod
    def commandexecutor(self):
        """A context manager that resolves to an ipeercommandexecutor.

        The object this resolves to can be used to issue command requests
        to the peer.

        Callers should call its ``callcommand`` method to issue command
        requests.

        A new executor should be obtained for each distinct set of commands
        (possibly just a single command) that the consumer wants to execute
        as part of a single operation or round trip. This is because some
        peers are half-duplex and/or don't support persistent connections.
        e.g. in the case of HTTP peers, commands sent to an executor represent
        a single HTTP request. While some peers may support multiple command
        sends over the wire per executor, consumers need to code to the least
        capable peer. So it should be assumed that command executors buffer
        called commands until they are told to send them and that each
        command executor could result in a new connection or wire-level request
        being issued.
        """


# TODO: make this a Protocol class when 3.11 is the minimum supported version?
class peer(_ipeerconnection, ipeercapabilities, ipeerrequests):
    """Unified interface for peer repositories.

    All peer instances must conform to this interface.
    """

    limitedarguments: bool = False
    path: misc.IPath | None
    ui: Ui

    def __init__(
        self,
        ui: Ui,
        path: misc.IPath | None = None,
        remotehidden: bool = False,
    ) -> None:
        self.ui = ui
        self.path = path

    def capable(self, name: bytes) -> bool | bytes:
        caps = self.capabilities()
        if name in caps:
            return True

        name = b'%s=' % name
        for cap in caps:
            if cap.startswith(name):
                return cap[len(name) :]

        return False

    def requirecap(self, name: bytes, purpose: bytes) -> None:
        if self.capable(name):
            return

        raise error.CapabilityError(
            _(
                b'cannot %s; remote repository does not support the '
                b'\'%s\' capability'
            )
            % (purpose, name)
        )


class iverifyproblem(Protocol):
    """Represents a problem with the integrity of the repository.

    Instances of this interface are emitted to describe an integrity issue
    with a repository (e.g. corrupt storage, missing data, etc).

    Instances are essentially messages associated with severity.
    """

    warning: bytes | None
    """Message indicating a non-fatal problem."""

    error: bytes | None
    """Message indicating a fatal problem."""

    node: bytes | None
    """Revision encountering the problem.

    ``None`` means the problem doesn't apply to a single revision.
    """


class irevisiondelta(Protocol):
    """Represents a delta between one revision and another.

    Instances convey enough information to allow a revision to be exchanged
    with another repository.

    Instances represent the fulltext revision data or a delta against
    another revision. Therefore the ``revision`` and ``delta`` attributes
    are mutually exclusive.

    Typically used for changegroup generation.
    """

    node: bytes
    """20 byte node of this revision."""

    p1node: bytes
    """20 byte node of 1st parent of this revision."""

    p2node: bytes
    """20 byte node of 2nd parent of this revision."""

    # TODO: is this really optional? revlog.revlogrevisiondelta defaults to None
    linknode: bytes | None
    """20 byte node of the changelog revision this node is linked to."""

    flags: int
    """2 bytes of integer flags that apply to this revision.

    This is a bitwise composition of the ``REVISION_FLAG_*`` constants.
    """

    basenode: bytes
    """20 byte node of the revision this data is a delta against.

    ``nullid`` indicates that the revision is a full revision and not
    a delta.
    """

    baserevisionsize: int | None
    """Size of base revision this delta is against.

    May be ``None`` if ``basenode`` is ``nullid``.
    """

    # TODO: is this really optional? (Seems possible in
    #  storageutil.emitrevisions()).
    revision: bytes | None
    """Raw fulltext of revision data for this node."""

    delta: bytes | None
    """Delta between ``basenode`` and ``node``.

    Stored in the bdiff delta format.
    """

    sidedata: bytes | None
    """Raw sidedata bytes for the given revision."""

    protocol_flags: int
    """Single byte of integer flags that can influence the protocol.

    This is a bitwise composition of the ``storageutil.CG_FLAG*`` constants.
    """

    snapshot_level: int | None
    """If we are sending a delta from a snapshot, this is set to the snapshot
    level. If the delta sent is not from a snapshot, set it to -1.

    Set to None if no information is available about snapshot level.
    """


class ifilerevisionssequence(Protocol):
    """Contains index data for all revisions of a file.

    Types implementing this behave like lists of tuples. The index
    in the list corresponds to the revision number. The values contain
    index metadata.

    The *null* revision (revision number -1) is always the last item
    in the index.
    """

    @abc.abstractmethod
    def __len__(self):
        """The total number of revisions."""

    @abc.abstractmethod
    def __getitem__(self, rev):
        """Returns the object having a specific revision number.

        Returns an 8-tuple with the following fields:

        offset+flags
           Contains the offset and flags for the revision. 64-bit unsigned
           integer where first 6 bytes are the offset and the next 2 bytes
           are flags. The offset can be 0 if it is not used by the store.
        compressed size
            Size of the revision data in the store. It can be 0 if it isn't
            needed by the store.
        uncompressed size
            Fulltext size. It can be 0 if it isn't needed by the store.
        base revision
            Revision number of revision the delta for storage is encoded
            against. -1 indicates not encoded against a base revision.
        link revision
            Revision number of changelog revision this entry is related to.
        p1 revision
            Revision number of 1st parent. -1 if no 1st parent.
        p2 revision
            Revision number of 2nd parent. -1 if no 1st parent.
        node
            Binary node value for this revision number.

        Negative values should index off the end of the sequence. ``-1``
        should return the null revision. ``-2`` should return the most
        recent revision.
        """

    @abc.abstractmethod
    def __contains__(self, rev):
        """Whether a revision number exists."""

    @abc.abstractmethod
    def insert(self, i, entry):
        """Add an item to the index at specific revision."""


class ifileindex(Protocol):
    """Storage interface for index data of a single file.

    File storage data is divided into index metadata and data storage.
    This interface defines the index portion of the interface.

    The index logically consists of:

    * A mapping between revision numbers and nodes.
    * DAG data (storing and querying the relationship between nodes).
    * Metadata to facilitate storage.
    """

    nullid: bytes
    """node for the null revision for use as delta base."""

    @abc.abstractmethod
    def __len__(self) -> int:
        """Obtain the number of revisions stored for this file."""

    @abc.abstractmethod
    def __iter__(self) -> Iterator[int]:
        """Iterate over revision numbers for this file."""

    @abc.abstractmethod
    def hasnode(self, node):
        """Returns a bool indicating if a node is known to this store.

        Implementations must only return True for full, binary node values:
        hex nodes, revision numbers, and partial node matches must be
        rejected.

        The null node is never present.
        """

    @abc.abstractmethod
    def revs(self, start=0, stop=None):
        """Iterate over revision numbers for this file, with control."""

    @abc.abstractmethod
    def parents(self, node):
        """Returns a 2-tuple of parent nodes for a revision.

        Values will be ``nullid`` if the parent is empty.
        """

    @abc.abstractmethod
    def parentrevs(self, rev):
        """Like parents() but operates on revision numbers."""

    @abc.abstractmethod
    def rev(self, node):
        """Obtain the revision number given a node.

        Raises ``error.LookupError`` if the node is not known.
        """

    @abc.abstractmethod
    def node(self, rev):
        """Obtain the node value given a revision number.

        Raises ``IndexError`` if the node is not known.
        """

    @abc.abstractmethod
    def lookup(self, node):
        """Attempt to resolve a value to a node.

        Value can be a binary node, hex node, revision number, or a string
        that can be converted to an integer.

        Raises ``error.LookupError`` if a node could not be resolved.
        """

    @abc.abstractmethod
    def linkrev(self, rev):
        """Obtain the changeset revision number a revision is linked to."""

    @abc.abstractmethod
    def iscensored(self, rev):
        """Return whether a revision's content has been censored."""

    @abc.abstractmethod
    def commonancestorsheads(self, node1, node2):
        """Obtain an iterable of nodes containing heads of common ancestors.

        See ``ancestor.commonancestorsheads()``.
        """

    @abc.abstractmethod
    def descendants(self, revs):
        """Obtain descendant revision numbers for a set of revision numbers.

        If ``nullrev`` is in the set, this is equivalent to ``revs()``.
        """

    @abc.abstractmethod
    def heads(self, start=None, stop=None):
        """Obtain a list of nodes that are DAG heads, with control.

        The set of revisions examined can be limited by specifying
        ``start`` and ``stop``. ``start`` is a node. ``stop`` is an
        iterable of nodes. DAG traversal starts at earlier revision
        ``start`` and iterates forward until any node in ``stop`` is
        encountered.
        """

    @abc.abstractmethod
    def children(self, node):
        """Obtain nodes that are children of a node.

        Returns a list of nodes.
        """


class ifiledata(Protocol):
    """Storage interface for data storage of a specific file.

    This complements ``ifileindex`` and provides an interface for accessing
    data for a tracked file.
    """

    @abc.abstractmethod
    def size(self, rev):
        """Obtain the fulltext size of file data.

        Any metadata is excluded from size measurements.
        """

    @abc.abstractmethod
    def revision(self, node):
        """Obtain fulltext data for a node.

        By default, any storage transformations are applied before the data
        is returned. If ``raw`` is True, non-raw storage transformations
        are not applied.

        The fulltext data may contain a header containing metadata. Most
        consumers should use ``read()`` to obtain the actual file data.
        """

    @abc.abstractmethod
    def rawdata(self, node):
        """Obtain raw data for a node."""

    @abc.abstractmethod
    def read(self, node):
        """Resolve file fulltext data.

        This is similar to ``revision()`` except any metadata in the data
        headers is stripped.
        """

    @abc.abstractmethod
    def renamed(self, node):
        """Obtain copy metadata for a node.

        Returns ``False`` if no copy metadata is stored or a 2-tuple of
        (path, node) from which this revision was copied.
        """

    @abc.abstractmethod
    def cmp(self, node, fulltext):
        """Compare fulltext to another revision.

        Returns True if the fulltext is different from what is stored.

        This takes copy metadata into account.

        TODO better document the copy metadata and censoring logic.
        """

    @abc.abstractmethod
    def emitrevisions(
        self,
        nodes,
        nodesorder=None,
        revisiondata=False,
        assumehaveparentrevisions=False,
        deltamode=CG_DELTAMODE_STD,
    ):
        """Produce ``irevisiondelta`` for revisions.

        Given an iterable of nodes, emits objects conforming to the
        ``irevisiondelta`` interface that describe revisions in storage.

        This method is a generator.

        The input nodes may be unordered. Implementations must ensure that a
        node's parents are emitted before the node itself. Transitively, this
        means that a node may only be emitted once all its ancestors in
        ``nodes`` have also been emitted.

        By default, emits "index" data (the ``node``, ``p1node``, and
        ``p2node`` attributes). If ``revisiondata`` is set, revision data
        will also be present on the emitted objects.

        With default argument values, implementations can choose to emit
        either fulltext revision data or a delta. When emitting deltas,
        implementations must consider whether the delta's base revision
        fulltext is available to the receiver.

        The base revision fulltext is guaranteed to be available if any of
        the following are met:

        * Its fulltext revision was emitted by this method call.
        * A delta for that revision was emitted by this method call.
        * ``assumehaveparentrevisions`` is True and the base revision is a
          parent of the node.

        ``nodesorder`` can be used to control the order that revisions are
        emitted. By default, revisions can be reordered as long as they are
        in DAG topological order (see above). If the value is ``nodes``,
        the iteration order from ``nodes`` should be used. If the value is
        ``storage``, then the native order from the backing storage layer
        is used. (Not all storage layers will have strong ordering and behavior
        of this mode is storage-dependent.) ``nodes`` ordering can force
        revisions to be emitted before their ancestors, so consumers should
        use it with care.

        The ``linknode`` attribute on the returned ``irevisiondelta`` may not
        be set and it is the caller's responsibility to resolve it, if needed.

        If ``deltamode`` is CG_DELTAMODE_PREV and revision data is requested,
        all revision data should be emitted as deltas against the revision
        emitted just prior. The initial revision should be a delta against its
        1st parent.
        """


class ifilemutation(Protocol):
    """Storage interface for mutation events of a tracked file."""

    @abc.abstractmethod
    def add(self, filedata, meta, transaction, linkrev, p1, p2):
        """Add a new revision to the store.

        Takes file data, dictionary of metadata, a transaction, linkrev,
        and parent nodes.

        Returns the node that was added.

        May no-op if a revision matching the supplied data is already stored.
        """

    @abc.abstractmethod
    def addrevision(
        self,
        revisiondata,
        transaction,
        linkrev,
        p1,
        p2,
        node=None,
        flags=0,
        cachedelta=None,
    ):
        """Add a new revision to the store and return its number.

        This is similar to ``add()`` except it operates at a lower level.

        The data passed in already contains a metadata header, if any.

        ``node`` and ``flags`` can be used to define the expected node and
        the flags to use with storage. ``flags`` is a bitwise value composed
        of the various ``REVISION_FLAG_*`` constants.

        ``add()`` is usually called when adding files from e.g. the working
        directory. ``addrevision()`` is often called by ``add()`` and for
        scenarios where revision data has already been computed, such as when
        applying raw data from a peer repo.
        """

    @abc.abstractmethod
    def addgroup(
        self,
        deltas,
        linkmapper,
        transaction,
        addrevisioncb=None,
        duplicaterevisioncb=None,
        maybemissingparents=False,
    ):
        """Process a series of deltas for storage.

        ``deltas`` is an iterable of 7-tuples of
        (node, p1, p2, linknode, deltabase, delta, flags) defining revisions
        to add.

        The ``delta`` field contains ``mpatch`` data to apply to a base
        revision, identified by ``deltabase``. The base node can be
        ``nullid``, in which case the header from the delta can be ignored
        and the delta used as the fulltext.

        ``alwayscache`` instructs the lower layers to cache the content of the
        newly added revision, even if it needs to be explicitly computed.
        This used to be the default when ``addrevisioncb`` was provided up to
        Mercurial 5.8.

        ``addrevisioncb`` should be called for each new rev as it is committed.
        ``duplicaterevisioncb`` should be called for all revs with a
        pre-existing node.

        ``maybemissingparents`` is a bool indicating whether the incoming
        data may reference parents/ancestor revisions that aren't present.
        This flag is set when receiving data into a "shallow" store that
        doesn't hold all history.

        Returns a list of nodes that were processed. A node will be in the list
        even if it existed in the store previously.
        """

    @abc.abstractmethod
    def censorrevision(self, tr, node, tombstone=b''):
        """Remove the content of a single revision.

        The specified ``node`` will have its content purged from storage.
        Future attempts to access the revision data for this node will
        result in failure.

        A ``tombstone`` message can optionally be stored. This message may be
        displayed to users when they attempt to access the missing revision
        data.

        Storage backends may have stored deltas against the previous content
        in this revision. As part of censoring a revision, these storage
        backends are expected to rewrite any internally stored deltas such
        that they no longer reference the deleted content.
        """

    @abc.abstractmethod
    def getstrippoint(self, minlink):
        """Find the minimum revision that must be stripped to strip a linkrev.

        Returns a 2-tuple containing the minimum revision number and a set
        of all revisions numbers that would be broken by this strip.

        TODO this is highly revlog centric and should be abstracted into
        a higher-level deletion API. ``repair.strip()`` relies on this.
        """

    @abc.abstractmethod
    def strip(self, minlink, transaction):
        """Remove storage of items starting at a linkrev.

        This uses ``getstrippoint()`` to determine the first node to remove.
        Then it effectively truncates storage for all revisions after that.

        TODO this is highly revlog centric and should be abstracted into a
        higher-level deletion API.
        """


class ifilestorage(ifileindex, ifiledata, ifilemutation, Protocol):
    """Complete storage interface for a single tracked file."""

    @abc.abstractmethod
    def files(self):
        """Obtain paths that are backing storage for this file.

        TODO this is used heavily by verify code and there should probably
        be a better API for that.
        """

    @abc.abstractmethod
    def storageinfo(
        self,
        exclusivefiles=False,
        sharedfiles=False,
        revisionscount=False,
        trackedsize=False,
        storedsize=False,
    ):
        """Obtain information about storage for this file's data.

        Returns a dict describing storage for this tracked path. The keys
        in the dict map to arguments of the same. The arguments are bools
        indicating whether to calculate and obtain that data.

        exclusivefiles
           Iterable of (vfs, path) describing files that are exclusively
           used to back storage for this tracked path.

        sharedfiles
           Iterable of (vfs, path) describing files that are used to back
           storage for this tracked path. Those files may also provide storage
           for other stored entities.

        revisionscount
           Number of revisions available for retrieval.

        trackedsize
           Total size in bytes of all tracked revisions. This is a sum of the
           length of the fulltext of all revisions.

        storedsize
           Total size in bytes used to store data for all tracked revisions.
           This is commonly less than ``trackedsize`` due to internal usage
           of deltas rather than fulltext revisions.

        Not all storage backends may support all queries are have a reasonable
        value to use. In that case, the value should be set to ``None`` and
        callers are expected to handle this special value.
        """

    @abc.abstractmethod
    def verifyintegrity(self, state) -> Iterable[iverifyproblem]:
        """Verifies the integrity of file storage.

        ``state`` is a dict holding state of the verifier process. It can be
        used to communicate data between invocations of multiple storage
        primitives.

        If individual revisions cannot have their revision content resolved,
        the method is expected to set the ``skipread`` key to a set of nodes
        that encountered problems.  If set, the method can also add the node(s)
        to ``safe_renamed`` in order to indicate nodes that may perform the
        rename checks with currently accessible data.

        The method yields objects conforming to the ``iverifyproblem``
        interface.
        """


class idirs(Protocol):
    """Interface representing a collection of directories from paths.

    This interface is essentially a derived data structure representing
    directories from a collection of paths.
    """

    @abc.abstractmethod
    def addpath(self, path):
        """Add a path to the collection.

        All directories in the path will be added to the collection.
        """

    @abc.abstractmethod
    def delpath(self, path):
        """Remove a path from the collection.

        If the removal was the last path in a particular directory, the
        directory is removed from the collection.
        """

    @abc.abstractmethod
    def __iter__(self):
        """Iterate over the directories in this collection of paths."""

    @abc.abstractmethod
    def __contains__(self, path):
        """Whether a specific directory is in this collection."""


class imanifestdict(Protocol):
    """Interface representing a manifest data structure.

    A manifest is effectively a dict mapping paths to entries. Each entry
    consists of a binary node and extra flags affecting that entry.
    """

    @abc.abstractmethod
    def __getitem__(self, key: bytes) -> bytes:
        """Returns the binary node value for a path in the manifest.

        Raises ``KeyError`` if the path does not exist in the manifest.

        Equivalent to ``self.find(path)[0]``.
        """

    @abc.abstractmethod
    def find(self, path: bytes) -> tuple[bytes, bytes]:
        """Returns the entry for a path in the manifest.

        Returns a 2-tuple of (node, flags).

        Raises ``KeyError`` if the path does not exist in the manifest.
        """

    @abc.abstractmethod
    def __len__(self) -> int:
        """Return the number of entries in the manifest."""

    @abc.abstractmethod
    def __nonzero__(self) -> bool:
        """Returns True if the manifest has entries, False otherwise."""

    __bool__ = __nonzero__

    @abc.abstractmethod
    def set(self, path: bytes, node: bytes, flags: bytes) -> None:
        """Define the node value and flags for a path in the manifest.

        Equivalent to __setitem__ followed by setflag, but can be more efficient.
        """

    @abc.abstractmethod
    def __setitem__(self, path: bytes, node: bytes) -> None:
        """Define the node value for a path in the manifest.

        If the path is already in the manifest, its flags will be copied to
        the new entry.
        """

    @abc.abstractmethod
    def __contains__(self, path: bytes) -> bool:
        """Whether a path exists in the manifest."""

    @abc.abstractmethod
    def __delitem__(self, path: bytes) -> None:
        """Remove a path from the manifest.

        Raises ``KeyError`` if the path is not in the manifest.
        """

    @abc.abstractmethod
    def __iter__(self) -> Iterator[bytes]:
        """Iterate over paths in the manifest."""

    @abc.abstractmethod
    def iterkeys(self) -> Iterator[bytes]:
        """Iterate over paths in the manifest."""

    @abc.abstractmethod
    def keys(self) -> list[bytes]:
        """Obtain a list of paths in the manifest."""

    @abc.abstractmethod
    def filesnotin(self, other, match=None) -> builtins.set[bytes]:
        """Obtain the set of paths in this manifest but not in another.

        ``match`` is an optional matcher function to be applied to both
        manifests.

        Returns a set of paths.
        """

    @abc.abstractmethod
    def dirs(self) -> misc.IDirs:
        """Returns an object implementing the ``idirs`` interface."""

    @abc.abstractmethod
    def hasdir(self, dir: bytes) -> bool:
        """Returns a bool indicating if a directory is in this manifest."""

    @abc.abstractmethod
    def walk(self, match: matcher.IMatcher) -> Iterator[bytes]:
        """Generator of paths in manifest satisfying a matcher.

        If the matcher has explicit files listed and they don't exist in
        the manifest, ``match.bad()`` is called for each missing file.
        """

    @abc.abstractmethod
    def diff(
        self,
        other: Any,  # TODO: 'manifestdict' or (better) equivalent interface
        match: matcher.IMatcher | None = None,
        clean: bool = False,
    ) -> dict[
        bytes,
        tuple[tuple[bytes | None, bytes], tuple[bytes | None, bytes]] | None,
    ]:
        """Find differences between this manifest and another.

        This manifest is compared to ``other``.

        If ``match`` is provided, the two manifests are filtered against this
        matcher and only entries satisfying the matcher are compared.

        If ``clean`` is True, unchanged files are included in the returned
        object.

        Returns a dict with paths as keys and values of 2-tuples of 2-tuples of
        the form ``((node1, flag1), (node2, flag2))`` where ``(node1, flag1)``
        represents the node and flags for this manifest and ``(node2, flag2)``
        are the same for the other manifest.
        """

    @abc.abstractmethod
    def setflag(self, path: bytes, flag: bytes) -> None:
        """Set the flag value for a given path.

        Raises ``KeyError`` if the path is not already in the manifest.
        """

    @abc.abstractmethod
    def get(self, path: bytes, default=None) -> bytes | None:
        """Obtain the node value for a path or a default value if missing."""

    @abc.abstractmethod
    def flags(self, path: bytes) -> bytes:
        """Return the flags value for a path (default: empty bytestring)."""

    @abc.abstractmethod
    def copy(self) -> imanifestdict:
        """Return a copy of this manifest."""

    @abc.abstractmethod
    def items(self) -> Iterator[tuple[bytes, bytes]]:
        """Returns an iterable of (path, node) for items in this manifest."""

    @abc.abstractmethod
    def iteritems(self) -> Iterator[tuple[bytes, bytes]]:
        """Identical to items()."""

    @abc.abstractmethod
    def iterentries(self) -> Iterator[tuple[bytes, bytes, bytes]]:
        """Returns an iterable of (path, node, flags) for this manifest.

        Similar to ``iteritems()`` except items are a 3-tuple and include
        flags.
        """

    @abc.abstractmethod
    def text(self) -> ByteString:
        """Obtain the raw data representation for this manifest.

        Result is used to create a manifest revision.
        """

    @abc.abstractmethod
    def fastdelta(
        self, base: ByteString, changes: Iterable[tuple[bytes, bool]]
    ) -> tuple[ByteString, ByteString]:
        """Obtain a delta between this manifest and another given changes.

        ``base`` in the raw data representation for another manifest.

        ``changes`` is an iterable of ``(path, to_delete)``.

        Returns a 2-tuple containing ``bytearray(self.text())`` and the
        delta between ``base`` and this manifest.

        If this manifest implementation can't support ``fastdelta()``,
        raise ``mercurial.manifest.FastdeltaUnavailable``.
        """


class imanifestrevisionbase(Protocol):
    """Base interface representing a single revision of a manifest.

    Should not be used as a primary interface: should always be inherited
    as part of a larger interface.
    """

    @abc.abstractmethod
    def copy(self):
        """Obtain a copy of this manifest instance.

        Returns an object conforming to the ``imanifestrevisionwritable``
        interface. The instance will be associated with the same
        ``imanifestlog`` collection as this instance.
        """

    @abc.abstractmethod
    def read(self):
        """Obtain the parsed manifest data structure.

        The returned object conforms to the ``imanifestdict`` interface.
        """


class imanifestrevisionstored(imanifestrevisionbase, Protocol):
    """Interface representing a manifest revision committed to storage."""

    @abc.abstractmethod
    def node(self) -> bytes:
        """The binary node for this manifest."""

    parents: list[bytes]
    """List of binary nodes that are parents for this manifest revision."""

    @abc.abstractmethod
    def readdelta(self, shallow: bool = False):
        """Obtain the manifest data structure representing changes from parent.

        This manifest is compared to its 1st parent. A new manifest
        representing those differences is constructed.

        If `shallow` is True, this will read the delta for this directory,
        without recursively reading subdirectory manifests. Instead, any
        subdirectory entry will be reported as it appears in the manifest, i.e.
        the subdirectory will be reported among files and distinguished only by
        its 't' flag. This only apply if the underlying manifest support it.

        The returned object conforms to the ``imanifestdict`` interface.
        """

    @abc.abstractmethod
    def read_any_fast_delta(
        self,
        valid_bases: Collection[int] | None = None,
        *,
        shallow: bool = False,
    ):
        """read some manifest information as fast if possible

        This might return a "delta", a manifest object containing only file
        changed compared to another revisions. The `valid_bases` argument
        control the set of revision that might be used as a base.

        If no delta can be retrieved quickly, a full read of the manifest will
        be performed instead.

        The function return a tuple with two elements. The first one is the
        delta base used (or None if we did a full read), the second one is the
        manifest information.

        If `shallow` is True, this will read the delta for this directory,
        without recursively reading subdirectory manifests. Instead, any
        subdirectory entry will be reported as it appears in the manifest, i.e.
        the subdirectory will be reported among files and distinguished only by
        its 't' flag. This only apply if the underlying manifest support it.

        The returned object conforms to the ``imanifestdict`` interface.
        """

    @abc.abstractmethod
    def read_delta_parents(self, *, shallow: bool = False, exact: bool = True):
        """return a diff from this revision against both parents.

        If `exact` is False, this might return a superset of the diff, containing
        files that are actually present as is in one of the parents.

        If `shallow` is True, this will read the delta for this directory,
        without recursively reading subdirectory manifests. Instead, any
        subdirectory entry will be reported as it appears in the manifest, i.e.
        the subdirectory will be reported among files and distinguished only by
        its 't' flag. This only apply if the underlying manifest support it.

        The returned object conforms to the ``imanifestdict`` interface."""

    @abc.abstractmethod
    def read_delta_new_entries(self, *, shallow: bool = False):
        """Return a manifest containing just the entries that might be new to
        the repository.

        This is often equivalent to a diff against both parents, but without
        garantee. For performance reason, It might contains more files in some cases.

        If `shallow` is True, this will read the delta for this directory,
        without recursively reading subdirectory manifests. Instead, any
        subdirectory entry will be reported as it appears in the manifest, i.e.
        the subdirectory will be reported among files and distinguished only by
        its 't' flag. This only apply if the underlying manifest support it.

        The returned object conforms to the ``imanifestdict`` interface."""

    @abc.abstractmethod
    def readfast(self, shallow: bool = False):
        """Calls either ``read()`` or ``readdelta()``.

        The faster of the two options is called.
        """

    @abc.abstractmethod
    def find(self, key: bytes) -> tuple[bytes, bytes]:
        """Calls ``self.read().find(key)``.

        Returns a 2-tuple of ``(node, flags)`` or raises ``KeyError``.
        """


class imanifestrevisionwritable(imanifestrevisionbase, Protocol):
    """Interface representing a manifest revision that can be committed."""

    @abc.abstractmethod
    def write(
        self, transaction, linkrev, p1node, p2node, added, removed, match=None
    ):
        """Add this revision to storage.

        Takes a transaction object, the changeset revision number it will
        be associated with, its parent nodes, and lists of added and
        removed paths.

        If match is provided, storage can choose not to inspect or write out
        items that do not match. Storage is still required to be able to provide
        the full manifest in the future for any directories written (these
        manifests should not be "narrowed on disk").

        Returns the binary node of the created revision.
        """


class imanifeststorage(Protocol):
    """Storage interface for manifest data."""

    nodeconstants: NodeConstants
    """nodeconstants used by the current repository."""

    tree: bytes
    """The path to the directory this manifest tracks.

    The empty bytestring represents the root manifest.
    """

    index: ifilerevisionssequence
    """An ``ifilerevisionssequence`` instance."""

    opener: Vfs
    """VFS opener to use to access underlying files used for storage.

    TODO this is revlog specific and should not be exposed.
    """

    # TODO: finish type hints
    fulltextcache: dict
    """Dict with cache of fulltexts.

    TODO this doesn't feel appropriate for the storage interface.
    """

    @abc.abstractmethod
    def __len__(self):
        """Obtain the number of revisions stored for this manifest."""

    @abc.abstractmethod
    def __iter__(self):
        """Iterate over revision numbers for this manifest."""

    @abc.abstractmethod
    def rev(self, node):
        """Obtain the revision number given a binary node.

        Raises ``error.LookupError`` if the node is not known.
        """

    @abc.abstractmethod
    def node(self, rev):
        """Obtain the node value given a revision number.

        Raises ``error.LookupError`` if the revision is not known.
        """

    @abc.abstractmethod
    def lookup(self, value):
        """Attempt to resolve a value to a node.

        Value can be a binary node, hex node, revision number, or a bytes
        that can be converted to an integer.

        Raises ``error.LookupError`` if a ndoe could not be resolved.
        """

    @abc.abstractmethod
    def parents(self, node):
        """Returns a 2-tuple of parent nodes for a node.

        Values will be ``nullid`` if the parent is empty.
        """

    @abc.abstractmethod
    def parentrevs(self, rev):
        """Like parents() but operates on revision numbers."""

    @abc.abstractmethod
    def linkrev(self, rev):
        """Obtain the changeset revision number a revision is linked to."""

    @abc.abstractmethod
    def revision(self, node):
        """Obtain fulltext data for a node."""

    @abc.abstractmethod
    def rawdata(self, node):
        """Obtain raw data for a node."""

    @abc.abstractmethod
    def revdiff(self, rev1, rev2):
        """Obtain a delta between two revision numbers.

        The returned data is the result of ``bdiff.bdiff()`` on the raw
        revision data.
        """

    @abc.abstractmethod
    def cmp(self, node, fulltext):
        """Compare fulltext to another revision.

        Returns True if the fulltext is different from what is stored.
        """

    @abc.abstractmethod
    def emitrevisions(
        self,
        nodes,
        nodesorder=None,
        revisiondata=False,
        assumehaveparentrevisions=False,
    ):
        """Produce ``irevisiondelta`` describing revisions.

        See the documentation for ``ifiledata`` for more.
        """

    @abc.abstractmethod
    def addgroup(
        self,
        deltas,
        linkmapper,
        transaction,
        addrevisioncb=None,
        duplicaterevisioncb=None,
    ):
        """Process a series of deltas for storage.

        See the documentation in ``ifilemutation`` for more.
        """

    @abc.abstractmethod
    def rawsize(self, rev):
        """Obtain the size of tracked data.

        Is equivalent to ``len(m.rawdata(node))``.

        TODO this method is only used by upgrade code and may be removed.
        """

    @abc.abstractmethod
    def getstrippoint(self, minlink):
        """Find minimum revision that must be stripped to strip a linkrev.

        See the documentation in ``ifilemutation`` for more.
        """

    @abc.abstractmethod
    def strip(self, minlink, transaction):
        """Remove storage of items starting at a linkrev.

        See the documentation in ``ifilemutation`` for more.
        """

    @abc.abstractmethod
    def checksize(self):
        """Obtain the expected sizes of backing files.

        TODO this is used by verify and it should not be part of the interface.
        """

    @abc.abstractmethod
    def files(self):
        """Obtain paths that are backing storage for this manifest.

        TODO this is used by verify and there should probably be a better API
        for this functionality.
        """

    @abc.abstractmethod
    def deltaparent(self, rev):
        """Obtain the revision that a revision is delta'd against.

        TODO delta encoding is an implementation detail of storage and should
        not be exposed to the storage interface.
        """

    @abc.abstractmethod
    def clone(self, tr, dest, **kwargs):
        """Clone this instance to another."""

    @abc.abstractmethod
    def clearcaches(self, clear_persisted_data=False):
        """Clear any caches associated with this instance."""

    @abc.abstractmethod
    def dirlog(self, d):
        """Obtain a manifest storage instance for a tree."""

    @abc.abstractmethod
    def add(
        self,
        m,
        transaction,
        link,
        p1,
        p2,
        added,
        removed,
        readtree=None,
        match=None,
    ):
        """Add a revision to storage.

        ``m`` is an object conforming to ``imanifestdict``.

        ``link`` is the linkrev revision number.

        ``p1`` and ``p2`` are the parent revision numbers.

        ``added`` and ``removed`` are iterables of added and removed paths,
        respectively.

        ``readtree`` is a function that can be used to read the child tree(s)
        when recursively writing the full tree structure when using
        treemanifets.

        ``match`` is a matcher that can be used to hint to storage that not all
        paths must be inspected; this is an optimization and can be safely
        ignored. Note that the storage must still be able to reproduce a full
        manifest including files that did not match.
        """

    @abc.abstractmethod
    def storageinfo(
        self,
        exclusivefiles=False,
        sharedfiles=False,
        revisionscount=False,
        trackedsize=False,
        storedsize=False,
    ):
        """Obtain information about storage for this manifest's data.

        See ``ifilestorage.storageinfo()`` for a description of this method.
        This one behaves the same way, except for manifest data.
        """

    @abc.abstractmethod
    def get_revlog(self):
        """return an actual revlog instance if any

        This exist because a lot of code leverage the fact the underlying
        storage is a revlog for optimization, so giving simple way to access
        the revlog instance helps such code.
        """


class imanifestlog(Protocol):
    """Interface representing a collection of manifest snapshots.

    Represents the root manifest in a repository.

    Also serves as a means to access nested tree manifests and to cache
    tree manifests.
    """

    nodeconstants: NodeConstants
    """nodeconstants used by the current repository."""

    narrowed: bool
    """True, is the manifest is narrowed by a matcher"""

    @abc.abstractmethod
    def __getitem__(self, node):
        """Obtain a manifest instance for a given binary node.

        Equivalent to calling ``self.get('', node)``.

        The returned object conforms to the ``imanifestrevisionstored``
        interface.
        """

    @abc.abstractmethod
    def get(self, tree, node, verify=True):
        """Retrieve the manifest instance for a given directory and binary node.

        ``node`` always refers to the node of the root manifest (which will be
        the only manifest if flat manifests are being used).

        If ``tree`` is the empty string, the root manifest is returned.
        Otherwise the manifest for the specified directory will be returned
        (requires tree manifests).

        If ``verify`` is True, ``LookupError`` is raised if the node is not
        known.

        The returned object conforms to the ``imanifestrevisionstored``
        interface.
        """

    @abc.abstractmethod
    def getstorage(self, tree):
        """Retrieve an interface to storage for a particular tree.

        If ``tree`` is the empty bytestring, storage for the root manifest will
        be returned. Otherwise storage for a tree manifest is returned.

        TODO formalize interface for returned object.
        """

    @abc.abstractmethod
    def clearcaches(self, clear_persisted_data: bool = False) -> None:
        """Clear caches associated with this collection."""

    @abc.abstractmethod
    def rev(self, node):
        """Obtain the revision number for a binary node.

        Raises ``error.LookupError`` if the node is not known.
        """

    @abc.abstractmethod
    def update_caches(self, transaction):
        """update whatever cache are relevant for the used storage."""


class ilocalrepositoryfilestorage(Protocol):
    """Local repository sub-interface providing access to tracked file storage.

    This interface defines how a repository accesses storage for a single
    tracked file path.
    """

    @abc.abstractmethod
    def file(self, f: HgPathT, writable: bool = False) -> ifilestorage:
        """Obtain a filelog for a tracked path.

        The returned type conforms to the ``ifilestorage`` interface.
        """


class ilocalrepositorymain(Protocol):
    """Main interface for local repositories.

    This currently captures the reality of things - not how things should be.
    """

    nodeconstants: NodeConstants
    """Constant nodes matching the hash function used by the repository."""

    nullid: bytes
    """null revision for the hash function used by the repository."""

    supported: set[bytes]
    """Set of requirements that this repo is capable of opening."""

    requirements: set[bytes]
    """Set of requirements this repo uses."""

    features: set[bytes]
    """Set of "features" this repository supports.

    A "feature" is a loosely-defined term. It can refer to a feature
    in the classical sense or can describe an implementation detail
    of the repository. For example, a ``readonly`` feature may denote
    the repository as read-only. Or a ``revlogfilestore`` feature may
    denote that the repository is using revlogs for file storage.

    The intent of features is to provide a machine-queryable mechanism
    for repo consumers to test for various repository characteristics.

    Features are similar to ``requirements``. The main difference is that
    requirements are stored on-disk and represent requirements to open the
    repository. Features are more run-time capabilities of the repository
    and more granular capabilities (which may be derived from requirements).
    """

    filtername: bytes
    """Name of the repoview that is active on this repo."""

    vfs_map: Mapping[bytes, Vfs]
    """a bytes-key → vfs mapping used by transaction and others"""

    wvfs: Vfs
    """VFS used to access the working directory."""

    vfs: Vfs
    """VFS rooted at the .hg directory.

    Used to access repository data not in the store.
    """

    svfs: Vfs
    """VFS rooted at the store.

    Used to access repository data in the store. Typically .hg/store.
    But can point elsewhere if the store is shared.
    """

    root: bytes
    """Path to the root of the working directory."""

    path: bytes
    """Path to the .hg directory."""

    origroot: bytes
    """The filesystem path that was used to construct the repo."""

    auditor: Any
    """A pathauditor for the working directory.

    This checks if a path refers to a nested repository.

    Operates on the filesystem.
    """

    nofsauditor: Any  # TODO: add type hints
    """A pathauditor for the working directory.

    This is like ``auditor`` except it doesn't do filesystem checks.
    """

    baseui: Ui
    """Original ui instance passed into constructor."""

    ui: Ui
    """Main ui instance for this instance."""

    sharedpath: bytes
    """Path to the .hg directory of the repo this repo was shared from."""

    store: Any  # TODO: add type hints
    """A store instance."""

    spath: bytes
    """Path to the store."""

    sjoin: Callable[[bytes], bytes]
    """Alias to self.store.join."""

    cachevfs: Vfs
    """A VFS used to access the cache directory.

    Typically .hg/cache.
    """

    wcachevfs: Vfs
    """A VFS used to access the cache directory dedicated to working copy

    Typically .hg/wcache.
    """

    filteredrevcache: Any  # TODO: add type hints
    """Holds sets of revisions to be filtered."""

    names: Any  # TODO: add type hints
    """A ``namespaces`` instance."""

    filecopiesmode: Any  # TODO: add type hints
    """The way files copies should be dealt with in this repo."""

    @abc.abstractmethod
    def close(self):
        """Close the handle on this repository."""

    @abc.abstractmethod
    def peer(self, path=None):
        """Obtain an object conforming to the ``peer`` interface."""

    @abc.abstractmethod
    def unfiltered(self):
        """Obtain an unfiltered/raw view of this repo."""

    @abc.abstractmethod
    def filtered(self, name, visibilityexceptions=None):
        """Obtain a named view of this repository."""

    obsstore: Any  # TODO: add type hints
    """A store of obsolescence data."""

    changelog: Any  # TODO: add type hints
    """A handle on the changelog revlog."""

    manifestlog: imanifestlog
    """An instance conforming to the ``imanifestlog`` interface.

    Provides access to manifests for the repository.
    """

    dirstate: intdirstate.idirstate
    """Working directory state."""

    narrowpats: Any  # TODO: add type hints
    """Matcher patterns for this repository's narrowspec."""

    @abc.abstractmethod
    def narrowmatch(self, match=None, includeexact=False):
        """Obtain a matcher for the narrowspec."""

    @abc.abstractmethod
    def setnarrowpats(self, newincludes, newexcludes):
        """Define the narrowspec for this repository."""

    @abc.abstractmethod
    def __getitem__(self, changeid):
        """Try to resolve a changectx."""

    @abc.abstractmethod
    def __contains__(self, changeid):
        """Whether a changeset exists."""

    @abc.abstractmethod
    def __nonzero__(self):
        """Always returns True."""
        return True

    __bool__ = __nonzero__

    @abc.abstractmethod
    def __len__(self):
        """Returns the number of changesets in the repo."""

    @abc.abstractmethod
    def __iter__(self):
        """Iterate over revisions in the changelog."""

    @abc.abstractmethod
    def revs(self, expr, *args):
        """Evaluate a revset.

        Emits revisions.
        """

    @abc.abstractmethod
    def set(self, expr, *args):
        """Evaluate a revset.

        Emits changectx instances.
        """

    @abc.abstractmethod
    def anyrevs(
        self,
        specs: list[bytes],
        user: bool = False,
        localalias: RevsetAliasesT | None = None,
    ):
        """Find revisions matching one of the given revsets."""

    @abc.abstractmethod
    def url(self):
        """Returns a string representing the location of this repo."""

    @abc.abstractmethod
    def hook(self, name, throw=False, **args):
        """Call a hook."""

    @abc.abstractmethod
    def tags(self):
        """Return a mapping of tag to node."""

    @abc.abstractmethod
    def tagtype(self, tagname):
        """Return the type of a given tag."""

    @abc.abstractmethod
    def tagslist(self):
        """Return a list of tags ordered by revision."""

    @abc.abstractmethod
    def nodetags(self, node):
        """Return the tags associated with a node."""

    @abc.abstractmethod
    def nodebookmarks(self, node):
        """Return the list of bookmarks pointing to the specified node."""

    @abc.abstractmethod
    def branchmap(self):
        """Return a mapping of branch to heads in that branch."""

    @abc.abstractmethod
    def revbranchcache(self):
        pass

    @abc.abstractmethod
    def register_changeset(self, rev, changelogrevision):
        """Extension point for caches for new nodes.

        Multiple consumers are expected to need parts of the changelogrevision,
        so it is provided as optimization to avoid duplicate lookups. A simple
        cache would be fragile when other revisions are accessed, too."""
        pass

    @abc.abstractmethod
    def branchtip(self, branchtip, ignoremissing=False):
        """Return the tip node for a given branch."""

    @abc.abstractmethod
    def lookup(self, key):
        """Resolve the node for a revision."""

    @abc.abstractmethod
    def lookupbranch(self, key):
        """Look up the branch name of the given revision or branch name."""

    @abc.abstractmethod
    def known(self, nodes):
        """Determine whether a series of nodes is known.

        Returns a list of bools.
        """

    @abc.abstractmethod
    def local(self):
        """Whether the repository is local."""
        return True

    @abc.abstractmethod
    def publishing(self):
        """Whether the repository is a publishing repository."""

    @abc.abstractmethod
    def cancopy(self):
        pass

    @abc.abstractmethod
    def shared(self):
        """The type of shared repository or None."""

    @abc.abstractmethod
    def wjoin(self, f: bytes, *insidef: bytes) -> bytes:
        """Calls self.vfs.reljoin(self.root, f, *insidef)"""

    @abc.abstractmethod
    def setparents(self, p1, p2):
        """Set the parent nodes of the working directory."""

    @abc.abstractmethod
    def filectx(self, path, changeid=None, fileid=None):
        """Obtain a filectx for the given file revision."""

    @abc.abstractmethod
    def getcwd(self):
        """Obtain the current working directory from the dirstate."""

    @abc.abstractmethod
    def pathto(self, f: bytes, cwd: bytes | None = None) -> bytes:
        """Obtain the relative path to a file."""

    @abc.abstractmethod
    def adddatafilter(self, name, fltr):
        pass

    @abc.abstractmethod
    def wread(self, filename: bytes) -> bytes:
        """Read a file from wvfs, using data filters."""

    @abc.abstractmethod
    def wwrite(
        self,
        filename: bytes,
        data: bytes,
        flags: bytes,
        backgroundclose: bool = False,
        **kwargs,
    ) -> int:
        """Write data to a file in the wvfs, using data filters."""

    @abc.abstractmethod
    def wwritedata(self, filename: bytes, data: bytes) -> bytes:
        """Resolve data for writing to the wvfs, using data filters."""

    @abc.abstractmethod
    def currenttransaction(self) -> inttxn.ITransaction | None:
        """Obtain the current transaction instance or None."""

    @abc.abstractmethod
    def transaction(self, desc: bytes, report=None) -> inttxn.ITransaction:
        """Open a new transaction to write to the repository."""

    @abc.abstractmethod
    def undofiles(self):
        """Returns a list of (vfs, path) for files to undo transactions."""

    @abc.abstractmethod
    def recover(self):
        """Roll back an interrupted transaction."""

    @abc.abstractmethod
    def rollback(self, dryrun=False, force=False):
        """Undo the last transaction.

        DANGEROUS.
        """

    @abc.abstractmethod
    def updatecaches(self, tr=None, full=False, caches=None):
        """Warm repo caches."""

    @abc.abstractmethod
    def invalidatecaches(self):
        """Invalidate cached data due to the repository mutating."""

    @abc.abstractmethod
    def invalidatevolatilesets(self):
        pass

    @abc.abstractmethod
    def invalidatedirstate(self):
        """Invalidate the dirstate."""

    @abc.abstractmethod
    def invalidate(self, clearfilecache=False):
        pass

    @abc.abstractmethod
    def invalidateall(self):
        pass

    @abc.abstractmethod
    def lock(self, wait=True, steal_from=None):
        """Lock the repository store and return a lock instance.

        If another lock object is specified through the "steal_from" argument,
        the new lock will reuse the on-disk lock of that "stolen" lock instead
        of creating its own. The "stolen" lock is no longer usable for any
        purpose and won't execute its release callback.

        That steal_from argument is used during local clone when reloading a
        repository. If we could remove the need for this during copy clone, we
        could remove this function.
        """

    @abc.abstractmethod
    def currentlock(self):
        """Return the lock if it's held or None."""

    @abc.abstractmethod
    def wlock(self, wait=True, steal_from=None):
        """Lock the non-store parts of the repository.

        If another lock object is specified through the "steal_from" argument,
        the new lock will reuse the on-disk lock of that "stolen" lock instead
        of creating its own. The "stolen" lock is no longer usable for any
        purpose and won't execute its release callback.

        That steal_from argument is used during local clone when reloading a
        repository. If we could remove the need for this during copy clone, we
        could remove this function.
        """

    @abc.abstractmethod
    def currentwlock(self):
        """Return the wlock if it's held or None."""

    @abc.abstractmethod
    def checkcommitpatterns(
        self,
        wctx,
        match: matcher.IMatcher,
        status: istatus.Status,
        fail: Callable[[bytes], bytes],
    ) -> None:
        pass

    @abc.abstractmethod
    def commit(
        self,
        text=b'',
        user=None,
        date=None,
        match=None,
        force=False,
        editor=False,
        extra=None,
    ):
        """Add a new revision to the repository."""

    @abc.abstractmethod
    def commitctx(self, ctx, error=False, origctx=None):
        """Commit a commitctx instance to the repository."""

    @abc.abstractmethod
    def destroying(self):
        """Inform the repository that nodes are about to be destroyed."""

    @abc.abstractmethod
    def destroyed(self):
        """Inform the repository that nodes have been destroyed."""

    @abc.abstractmethod
    def status(
        self,
        node1=b'.',
        node2=None,
        match: matcher.IMatcher | None = None,
        ignored: bool = False,
        clean: bool = False,
        unknown: bool = False,
        listsubrepos: bool = False,
    ) -> istatus.Status:
        """Convenience method to call repo[x].status()."""

    @abc.abstractmethod
    def addpostdsstatus(self, ps):
        pass

    @abc.abstractmethod
    def postdsstatus(self):
        pass

    @abc.abstractmethod
    def clearpostdsstatus(self):
        pass

    @abc.abstractmethod
    def heads(self, start=None):
        """Obtain list of nodes that are DAG heads."""

    @abc.abstractmethod
    def branchheads(self, branch=None, start=None, closed=False):
        pass

    @abc.abstractmethod
    def branches(self, nodes):
        pass

    @abc.abstractmethod
    def between(self, pairs):
        pass

    @abc.abstractmethod
    def checkpush(self, pushop):
        pass

    prepushoutgoinghooks: misc.IHooks
    """util.hooks instance."""

    @abc.abstractmethod
    def pushkey(self, namespace, key, old, new):
        pass

    @abc.abstractmethod
    def listkeys(self, namespace):
        pass

    @abc.abstractmethod
    def debugwireargs(self, one, two, three=None, four=None, five=None):
        pass

    @abc.abstractmethod
    def savecommitmessage(self, text):
        pass

    @abc.abstractmethod
    def register_sidedata_computer(
        self, kind, category, keys, computer, flags, replace=False
    ):
        pass

    @abc.abstractmethod
    def register_wanted_sidedata(self, category):
        pass


class completelocalrepository(
    ilocalrepositorymain,
    ilocalrepositoryfilestorage,
    Protocol,
):
    """Complete interface for a local repository."""
