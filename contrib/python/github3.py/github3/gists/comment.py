"""Module containing the logic for a GistComment."""
from .. import decorators
from .. import models
from .. import users


class GistComment(models.GitHubCore):
    """Representation of a comment left on a :class:`~github3.gists.Gist`.

    See also: http://developer.github.com/v3/gists/comments/

    .. versionchanged:: 1.0.0

        The ``links``, ``html_url``, and ``pull_request_url`` attributes were
        removed as none of them exist in the response from GitHub.

    This object has the following attributes:

    .. attribute:: author_association

        The comment author's (:attr:`user`) association with this gist.

    .. attribute:: body

        The markdown formatted original text written by the author.

    .. attribute:: body_html

        The HTML formatted comment body.

    .. attribute:: body_text

        The plain-text formatted comment body.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this comment was created.

    .. attribute:: id

        The unique identifier for this comment.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this comment was most recently updated.

    .. attribute:: user

        A :class:`~github3.users.ShortUser` representing the author of this
        comment.
    """

    def _update_attributes(self, comment):
        self._api = comment["url"]
        self.author_association = comment["author_association"]
        self.body = comment["body"]
        self.body_html = comment["body_html"]
        self.body_text = comment["body_text"]
        self.created_at = self._strptime(comment["created_at"])
        self.id = comment["id"]
        self.updated_at = self._strptime(comment["updated_at"])
        self.user = users.ShortUser(comment["user"], self)

    def _repr(self):
        return f"<Gist Comment [{self.user.login}]>"

    @decorators.requires_auth
    def delete(self):
        """Delete this comment from the gist.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @decorators.requires_auth
    def edit(self, body):
        """Edit this comment.

        :param str body:
            (required), new body of the comment, Markdown-formatted
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
