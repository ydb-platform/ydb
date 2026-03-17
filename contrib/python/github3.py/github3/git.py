"""
This module contains all the classes relating to Git Data.

See also: http://developer.github.com/v3/git/
"""
import base64
from json import dumps

from . import models
from .decorators import requires_auth


class Blob(models.GitHubCore):
    """This object provides an interface to the API representation of a blob.

    See also: http://developer.github.com/v3/git/blobs/

    .. versionchanged:: 1.0.0

       - The :attr:`content` is no longer forcibly coerced to bytes.

    This object has the following atributes

    .. attribute:: content

        The raw content of the blob. This may be base64 encoded text. Use
        :meth:`decode_content` to receive the non-encoded text.

    .. attribute:: encoding

        The encoding that GitHub reports for this blob's content.

    .. attribute:: size

        The size of this blob's content in bytes.

    .. attribute:: sha

        The SHA1 of this blob's content.
    """

    def _update_attributes(self, blob):
        self._api = blob["url"]
        self.content = blob["content"]
        self.encoding = blob["encoding"]
        self.size = blob["size"]
        self.sha = blob["sha"]

    def _repr(self):
        return f"<Blob [{self.sha:.10}]>"

    def decode_content(self):
        """Return the unencoded content of this blob.

        If the content is base64 encoded, this will properly decode it.
        Otherwise, it will return the content as returned by the API.

        :returns:
            Decoded content as text
        :rtype:
            unicode
        """
        if self.encoding == "base64" and self.content:
            return base64.b64decode(self.content.encode("utf-8")).decode(
                "utf-8"
            )
        return self.content


class _Commit(models.GitHubCore):
    class_name = "_Commit"

    def _update_attributes(self, commit):
        self._api = commit["url"]
        self.author = commit["author"]
        if self.author.get("name"):
            self._author_name = self.author["name"]
        self.committer = commit["committer"]
        if self.committer:
            self._commit_name = self.committer.get("name")
        self.message = commit["message"]
        self.tree = CommitTree(commit["tree"], self)

    def _repr(self):
        return f"<{self.class_name} [{self.sha}]>"


class Commit(_Commit):
    """This represents a commit as returned by the git API.

    This is distinct from :class:`~github3.repos.commit.RepoCommit`.
    Primarily this object represents the commit data stored by git and
    it has no relationship to the repository on GitHub.

    See also: http://developer.github.com/v3/git/commits/

    This object has all of the attributes of a
    :class:`~github3.git.ShortCommit` as well as the following attributes:

    .. attribute:: parents

        The list of commits that are the parents of this commit. This may be
        empty if this is the initial commit, or it may have several if it is
        the result of an octopus merge. Each parent is represented as a
        dictionary with the API URL and SHA1.

    .. attribute:: sha

        The unique SHA1 which identifies this commit.

    .. attribute:: verification

        The GPG verification data about this commit. See
        https://developer.github.com/v3/git/commits/#commit-signature-verification
        for more information.
    """

    class_name = "Commit"

    def _update_attributes(self, commit):
        super()._update_attributes(commit)
        self.parents = commit["parents"]
        self.sha = commit["sha"]
        if not self.sha:
            i = self._api.rfind("/")
            self.sha = self._api[i + 1 :]
        self._uniq = self.sha
        self.verification = commit["verification"]


class ShortCommit(_Commit):
    """This represents a commit as returned by the git API.

    This is distinct from :class:`~github3.repos.commit.RepoCommit`.
    Primarily this object represents the commit data stored by git. This
    shorter representation of a Commit is most often found on a
    :class:`~github3.repos.commit.RepoCommit` to represent the git data
    associated with it.

    See also: http://developer.github.com/v3/git/commits/

    This object has the following attributes:

    .. attribute:: author

        This is a dictionary with at least the name and email of the author
        of this commit as well as the date it was authored.

    .. attribute:: committer

        This is a dictionary with at least the name and email of the committer
        of this commit as well as the date it was committed.

    .. attribute:: message

        The commit message that describes the changes as written by the author
        and committer.

    .. attribute:: tree

        The git tree object this commit points to.
    """

    class_name = "ShortCommit"
    _refresh_to = Commit


class Reference(models.GitHubCore):
    """Object representing a git reference associated with a repository.

    This represents a reference (or ref) created on a repository via git.

    See also: http://developer.github.com/v3/git/refs/

    This object has the following attributes:

    .. attribute:: object

        A :class:`~github3.git.GitObject` that this reference points to.

    .. attribute:: ref

        The string path to the reference, e.g., ``'refs/heads/sc/feature-a'``.
    """

    def _update_attributes(self, ref):
        self._api = ref["url"]
        self.object = GitObject(ref["object"], self)
        self.ref = ref["ref"]

    def _repr(self):
        return f"<Reference [{self.ref}]>"

    @requires_auth
    def delete(self):
        """Delete this reference.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_auth
    def update(self, sha, force=False):
        """Update this reference.

        :param str sha:
            (required), sha of the reference
        :param bool force:
            (optional), force the update or not
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        data = {"sha": sha, "force": force}
        json = self._json(self._patch(self._api, data=dumps(data)), 200)
        if json:
            self._update_attributes(json)
            return True
        return False


class GitObject(models.GitHubCore):
    """This object represents an arbitrary 'object' in git.

    This object is intended to be versatile and is usually found on one of the
    following:

    - :class:`~github3.git.Reference`
    - :class:`~github3.git.Tag`

    This object has the following attributes:

    .. attribute:: sha

        The SHA1 of the object this is representing.

    .. attribute:: type

        The name of the type of object this is representing.
    """

    def _update_attributes(self, obj):
        self._api = obj["url"]
        self.sha = obj["sha"]
        self.type = obj["type"]

    def _repr(self):
        return f"<Git Object [{self.sha}]>"


class Tag(models.GitHubCore):
    """This represents an annotated tag.

    Tags are a special kind of git reference and annotated tags have more
    information than lightweight tags.

    See also: http://developer.github.com/v3/git/tags/

    This object has the following attributes:

    .. attribute:: message

        This is the message that was written to accompany the creation of the
        annotated tag.

    .. attribute:: object

        A :class:`~github3.git.GitObject` that represents the underlying git
        object.

    .. attribute:: sha

        The SHA1 of this tag in the git repository.

    .. attribute:: tag

        The "lightweight" tag (or reference) that backs this annotated tag.

    .. attribute:: tagger

        The person who created this tag.
    """

    def _update_attributes(self, tag):
        self._api = tag["url"]
        self.message = tag["message"]
        self.object = GitObject(tag["object"], self)
        self.sha = tag["sha"]
        self.tag = tag["tag"]
        self.tagger = tag["tagger"]

    def _repr(self):
        return f"<Tag [{self.tag}]>"


class CommitTree(models.GitHubCore):
    """This object represents the abbreviated tree data in a commit.

    The API returns different representations of different objects. When
    representing a :class:`~github3.git.ShortCommit` or
    :class:`~github3.git.Commit`, the API returns an abbreviated
    representation of a git tree.

    This object has the following attributes:

    .. attribute:: sha

        The SHA1 of this tree in the git repository.
    """

    def _update_attributes(self, tree):
        self._api = tree["url"]
        self.sha = tree["sha"]

    def _repr(self):
        return f"<CommitTree [{self.sha}]>"

    def to_tree(self):
        """Retrieve a full Tree object for this CommitTree.

        :returns:
            The full git data about this tree
        :rtype:
            :class:`~github3.git.Tree`
        """
        json = self._json(self._get(self._api), 200)
        return self._instance_or_null(Tree, json)

    refresh = to_tree


class Tree(models.GitHubCore):
    """This represents a tree object from a git repository.

    Trees tend to represent directories and subdirectories.

    See also: http://developer.github.com/v3/git/trees/

    This object has the following attributes:

    .. attribute:: sha

        The SHA1 of this tree in the git repository.

    .. attribute:: tree

        A list that represents the nodes in the tree. If this list has members
        it will have instances of :class:`~github3.git.Hash`.
    """

    def _update_attributes(self, tree):
        self._api = tree["url"]
        self.sha = tree["sha"]
        self.tree = tree["tree"]
        if self.tree:
            self.tree = [Hash(t, self) for t in self.tree]

    def _repr(self):
        return f"<Tree [{self.sha}]>"

    def __eq__(self, other):
        return self.as_dict() == other.as_dict()

    def __ne__(self, other):
        return self.as_dict() != other.as_dict()

    def recurse(self):
        """Recurse into this tree.

        :returns:
            A new tree
        :rtype:
            :class:`~github3.git.Tree`
        """
        json = self._json(
            self._get(self._api, params={"recursive": "1"}), 200
        )
        return self._instance_or_null(Tree, json)


class Hash(models.GitHubCore):
    """This is used to represent the elements of a tree.

    This provides the path to the object and the type of object it is. For
    a brief explanation of what these types are and represent, this
    StackOverflow question answers some of that:
    https://stackoverflow.com/a/18605496/1953283

    See also: http://developer.github.com/v3/git/trees/#create-a-tree

    This object has the following attributes:

    .. attribute:: mode

        The mode of the file, directory, or link.

    .. attribute:: path

        The path to the file, directory, or link.

    .. attribute:: sha

        The SHA1 for this hash.

    .. attribute:: size

        This attribute is only not ``None`` if the :attr:`type` is not a tree.

    .. attribute:: type

        The type of git object this is representing, e.g., tree, blob, etc.
    """

    def _update_attributes(self, info):
        self._api = info.get("url")
        self.mode = info["mode"]
        self.path = info["path"]
        self.sha = info["sha"]
        self.size = None
        self.type = info["type"]

        if self.type != "tree":
            self.size = info.get("size")

    def _repr(self):
        return f"<Hash [{self.sha}]>"
