"""This module contains the RepoComment class."""
from .. import models
from .. import users
from ..decorators import requires_auth


class _RepoComment(models.GitHubCore):
    """The :class:`RepoComment <RepoComment>` object.

    This stores the information about a comment on a file in a repository.

    Two comment instances can be checked like so::

        c1 == c2
        c1 != c2

    And is equivalent to::

        c1.id == c2.id
        c1.id != c2.id

    """

    class_name = "_RepoComment"

    def _update_attributes(self, comment):
        self._api = comment["url"]
        self.author_association = comment["author_association"]
        self.body = comment["body"]
        self.commit_id = comment["commit_id"]
        self.created_at = self._strptime(comment["created_at"])
        self.html_url = comment["html_url"]
        self.id = comment["id"]
        self.line = comment["line"]
        self.path = comment["path"]
        self.position = comment["position"]
        self.updated_at = self._strptime(comment["updated_at"])
        self.user = users.ShortUser(comment["user"], self)

    def _repr(self):
        return "<{} [{}/{}]>".format(
            self.class_name, self.commit_id[:7], self.user.login or ""
        )

    @requires_auth
    def delete(self):
        """Delete this comment.

        :returns:
            True if successfully deleted, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_auth
    def edit(self, body):
        """Edit this comment.

        :param str body:
            (required), new body of the comment, Markdown formatted
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if body:
            json = self._json(
                self._patch(self._api, json={"body": body}), 200
            )
            if json:
                self._update_attributes(json)
                return True
        return False

    update = edit


class RepoComment(_RepoComment):
    """The representation of the full comment on an object in a repository.

    This object has the same attributes as a
    :class:`~github3.repos.comment.ShortComment` as well as the following:

    .. attribute:: body_html

        The HTML formatted text of this comment.

    .. attribute:: body_text

        The plain-text formatted text of this comment.
    """

    class_name = "Repository Comment"

    def _update_attributes(self, comment):
        super()._update_attributes(comment)
        self.body_text = comment["body_text"]
        self.body_html = comment["body_html"]


class ShortComment(_RepoComment):
    """The representation of an abridged comment on an object in a repo.

    This object has the following attributes:

    .. attribute:: author_association

        The affiliation the author of this comment has with the repository.

    .. attribute:: body

        The Markdown formatted text of this comment.

    .. attribute:: commit_id

        The SHA1 associated with this comment.

    .. attribute:: created_at

        A :class:`~dateteime.datetime` object representing the date and time
        when this comment was created.

    .. attribute:: html_url

        The URL to view this comment in a browser.

    .. attribute:: id

        The unique identifier of this comment.

    .. attribute:: line

        The line number where the comment is located.

    .. attribute:: path

        The path to the file where this comment was made.

    .. attribute:: position

        The position in the diff where the comment was left.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this comment was most recently updated.

    .. attribute:: user

        A :class:`~github3.users.ShortUser` representing the author of this
        comment.
    """

    class_name = "Short Repository Comment"
    _refresh_to = RepoComment
