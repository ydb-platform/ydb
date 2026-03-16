"""This module contains the Gist, ShortGist, and GistFork objects."""
from json import dumps

from . import comment
from . import file as gistfile
from . import history
from .. import models
from .. import users
from ..decorators import requires_auth


class _Gist(models.GitHubCore):
    """This object holds all the information returned by Github about a gist.

    With it you can comment on or fork the gist (assuming you are
    authenticated), edit or delete the gist (assuming you own it).  You can
    also "star" or "unstar" the gist (again assuming you have authenticated).

    Two gist instances can be checked like so::

        g1 == g2
        g1 != g2

    And is equivalent to::

        g1.id == g2.id
        g1.id != g2.id

    See also: http://developer.github.com/v3/gists/

    """

    class_name = "_Gist"
    _file_class = gistfile.ShortGistFile

    def _update_attributes(self, gist):
        self.comments_count = gist["comments"]
        self.comments_url = gist["comments_url"]
        self.created_at = self._strptime(gist["created_at"])
        self.description = gist["description"]
        self.files = {
            filename: self._file_class(gfile, self)
            for filename, gfile in gist["files"].items()
        }
        self.git_pull_url = gist["git_pull_url"]
        self.git_push_url = gist["git_push_url"]
        self.html_url = gist["html_url"]
        self.id = gist["id"]
        self.owner = gist.get("owner")
        if self.owner is not None:
            self.owner = users.ShortUser(self.owner, self)
        self.public = gist["public"]
        self.updated_at = self._strptime(gist["updated_at"])
        self.url = self._api = gist["url"]

    def __str__(self):
        return self.id

    def _repr(self):
        return "<{s.class_name} [{s.id}]>".format(s=self)

    @requires_auth
    def create_comment(self, body):
        """Create a comment on this gist.

        :param str body:
            (required), body of the comment
        :returns:
            Created comment or None
        :rtype:
            :class:`~github3.gists.comment.GistComment`
        """
        json = None
        if body:
            url = self._build_url("comments", base_url=self._api)
            json = self._json(self._post(url, data={"body": body}), 201)
        return self._instance_or_null(comment.GistComment, json)

    @requires_auth
    def delete(self):
        """Delete this gist.

        :returns:
            Whether the deletion was successful or not
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_auth
    def edit(self, description="", files={}):
        """Edit this gist.

        :param str description:
            (optional), description of the gist
        :param dict files:
            (optional), files that make up this gist; the key(s) should be the
            file name(s) and the values should be another (optional) dictionary
            with (optional) keys: 'content' and 'filename' where the former is
            the content of the file and the latter is the new name of the file.
        :returns:
            Whether the edit was successful or not
        :rtype:
            bool
        """
        data = {}
        json = None
        if description:
            data["description"] = description
        if files:
            data["files"] = files
        if data:
            json = self._json(self._patch(self._api, data=dumps(data)), 200)
        if json:
            self._update_attributes(json)
            return True
        return False

    @requires_auth
    def fork(self):
        """Fork this gist.

        :returns:
            New gist if successfully forked, ``None`` otherwise
        :rtype:
            :class:`~github3.gists.gist.ShortGist`
        """
        url = self._build_url("forks", base_url=self._api)
        json = self._json(self._post(url), 201)
        return self._instance_or_null(ShortGist, json)

    @requires_auth
    def is_starred(self):
        """Check to see if this gist is starred by the authenticated user.

        :returns:
            True if it is starred, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("star", base_url=self._api)
        return self._boolean(self._get(url), 204, 404)

    def comments(self, number=-1, etag=None):
        """Iterate over comments on this gist.

        :param int number:
            (optional), number of comments to iterate over.
            Default: -1 will iterate over all comments on the gist
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of comments
        :rtype:
            :class:`~github3.gists.comment.GistComment`
        """
        url = self._build_url("comments", base_url=self._api)
        return self._iter(int(number), url, comment.GistComment, etag=etag)

    def commits(self, number=-1, etag=None):
        """Iterate over the commits on this gist.

        These commits will be requested from the API and should be the same as
        what is in ``Gist.history``.

        .. versionadded:: 0.6

        .. versionchanged:: 0.9

            Added param ``etag``.

        :param int number:
            (optional), number of commits to iterate over.
            Default: -1 will iterate over all commits associated with this
            gist.
        :param str etag:
            (optional), ETag from a previous request to this endpoint.
        :returns:
            generator of the gist's history
        :rtype:
            :class:`~github3.gists.history.GistHistory`
        """
        url = self._build_url("commits", base_url=self._api)
        return self._iter(int(number), url, history.GistHistory)

    def forks(self, number=-1, etag=None):
        """Iterator of forks of this gist.

        .. versionchanged:: 0.9

            Added params ``number`` and ``etag``.

        :param int number:
            (optional), number of forks to iterate over.
            Default: -1 will iterate over all forks of this gist.
        :param str etag:
            (optional), ETag from a previous request to this endpoint.
        :returns:
            generator of gists
        :rtype:
            :class:`~github3.gists.gist.ShortGist`
        """
        url = self._build_url("forks", base_url=self._api)
        return self._iter(int(number), url, ShortGist, etag=etag)

    @requires_auth
    def star(self):
        """Star this gist.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("star", base_url=self._api)
        return self._boolean(self._put(url), 204, 404)

    @requires_auth
    def unstar(self):
        """Un-star this gist.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("star", base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)


class GistFork(models.GitHubCore):
    """This object represents a forked Gist.

    This has a subset of attributes of a
    :class:`~github3.gists.gist.ShortGist`:

    .. attribute:: created_at

        The date and time when the gist was created.

    .. attribute:: id

        The unique identifier of the gist.

    .. attribute:: owner

        The user who forked the gist.

    .. attribute:: updated_at

        The date and time of the most recent modification of the fork.

    .. attribute:: url

        The API URL for the fork.
    """

    def _update_attributes(self, fork):
        self.created_at = self._strptime(fork["created_at"])
        self.id = fork["id"]
        self.owner = users.ShortUser(fork["user"], self)
        self.updated_at = self._strptime(fork["updated_at"])
        self.url = self._api = fork["url"]

    def _repr(self):
        return f"<GistFork [{self.id}]>"

    def to_gist(self):
        """Retrieve the full Gist representation of this fork.

        :returns:
            The Gist if retrieving it was successful or ``None``
        :rtype:
            :class:`~github3.gists.gist.Gist`
        """
        json = self._json(self._get(self.url), 200)
        return self._instance_or_null(Gist, json)

    refresh = to_gist


class Gist(_Gist):
    """This object constitutes the full representation of a Gist.

    GitHub's API returns different amounts of information about gists
    based upon how that information is retrieved. This object exists to
    represent the full amount of information returned for a specific
    gist. For example, you would receive this class when calling
    :meth:`~github3.github.GitHub.gist`. To provide a clear distinction
    between the types of gists, github3.py uses different classes with
    different sets of attributes.

    This object has all the same attributes as
    :class:`~github3.gists.gist.ShortGist` as well as:

    .. attribute:: commits_url

        The URL to retrieve gist commits from the GitHub API.

    .. attribute:: original_forks

        A list of :class:`~github3.gists.gist.GistFork` objects representing
        each fork of this gist. To retrieve the most recent list of forks, use
        the :meth:`forks` method.

    .. attribute:: forks_url

        The URL to retrieve the current listing of forks of this gist.

    .. attribute:: history

        A list of :class:`~github3.gists.history.GistHistory` objects
        representing each change made to this gist.

    .. attribute:: truncated

        This is a boolean attribute that indicates whether the content of this
        Gist has been truncated or not.
    """

    class_name = "Gist"
    _file_class = gistfile.GistFile

    def _update_attributes(self, gist):
        super()._update_attributes(gist)
        self.commits_url = gist["commits_url"]
        self.original_forks = [GistFork(fork, self) for fork in gist["forks"]]
        self.forks_url = gist["forks_url"]
        self.history = [history.GistHistory(h, self) for h in gist["history"]]
        self.truncated = gist["truncated"]


class ShortGist(_Gist):
    """Short representation of a gist.

    GitHub's API returns different amounts of information about gists
    based upon how that information is retrieved. This object exists to
    represent the full amount of information returned for a specific
    gist. For example, you would receive this class when calling
    :meth:`~github3.github.GitHub.all_gists`. To provide a clear distinction
    between the types of gists, github3.py uses different classes with
    different sets of attributes.

    This object only has the following attributes:

    .. attribute:: url

        The GitHub API URL for this repository, e.g.,
        ``https://api.github.com/gists/6faaaeb956dec3f51a9bd630a3490291``.

    .. attribute:: comments_count

        Number of comments on this gist

    .. attribute:: description

        Description of the gist as written by the creator

    .. attribute:: html_url

        The URL of this gist on GitHub, e.g.,
        ``https://gist.github.com/sigmavirus24/6faaaeb956dec3f51a9bd630a3490291``

    .. attribute:: id

        The unique identifier for this gist.

    .. attribute:: public

        This is a boolean attribute  describing if the gist is public or
        private

    .. attribute:: git_pull_url

        The git URL to pull this gist, e.g.,
        ``git://gist.github.com/sigmavirus24/6faaaeb956dec3f51a9bd630a3490291.git``

    .. attribute:: git_push_url

        The git URL to push to gist, e.g.,
        ``git@gist.github.com/sigmavirus24/6faaaeb956dec3f51a9bd630a3490291.git``

    .. attribute:: created_at

        This is a datetime object representing when the gist was created.

    .. attribute:: updated_at
        This is a datetime object representing the last time this gist was
        most recently updated.

    .. attribute:: owner

        This attribute is a :class:`~github3.users.ShortUser` object
        representing the creator of the gist.

    .. attribute:: files

        A dictionary mapping the filename to a
        :class:`~github3.gists.gist.GistFile` object.

        .. versionchanged:: 1.0.0

            Previously this was a list but it has been converted to a
            dictionary to preserve the structure of the API.

    .. attribute:: comments_url

        The URL to retrieve the list of comments on the Gist via the API.
    """

    class_name = "ShortGist"
    _refresh_to = Gist
