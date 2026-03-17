"""Module with class(es) representing issue comments."""
from .. import decorators
from .. import models
from .. import users
from .. import utils


class IssueComment(models.GitHubCore):
    """Representation of a comment left on an issue.

    See also: http://developer.github.com/v3/issues/comments/

    This object has the following attributes:

    .. attribute:: author_association

        The association of the author (:attr:`user`) with the repository
        this issue belongs to.

    .. attribute:: body

        The markdown formatted original text written by the author.

    .. attribute:: body_html

        The HTML formatted comment body.

    .. attribute:: body_text

        The plain-text formatted comment body.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this comment was created.

    .. attribute:: html_url

        The URL to view this comment in a browser.

    .. attribute:: id

        The unique identifier for this comment.

    .. attribute:: issue_url

        The URL of the parent issue in the API.

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
        self.html_url = comment["html_url"]
        self.id = comment["id"]
        self.issue_url = comment["issue_url"]
        self.updated_at = self._strptime(comment["updated_at"])
        self.user = users.ShortUser(comment["user"], self)

    def _repr(self):
        return f"<IssueComment [{self.user.login}]>"

    @decorators.requires_auth
    def delete(self):
        """Delete this comment.

        :returns: bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @decorators.requires_auth
    def edit(self, body):
        """Edit this comment.

        :param str body: (required), new body of the comment, Markdown
            formatted
        :returns: bool
        """
        if body:
            json = self._json(
                self._patch(self._api, json={"body": body}), 200
            )
            if json:
                self._update_attributes(json)
                return True
        return False


def issue_comment_params(sort, direction, since):
    """Properly format parameters for issue comments.

    .. warning::

        This is not a public API designed for users of github3.py.

    """
    params = {}

    if sort in ("created", "updated"):
        params["sort"] = sort

    if direction in ("asc", "desc"):
        params["direction"] = direction

    since = utils.timestamp_parameter(since)
    if since:
        params["since"] = since

    return params
