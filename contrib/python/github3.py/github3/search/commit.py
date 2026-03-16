"""Commit search results implementation."""
from .. import git
from .. import models
from .. import repos
from .. import users


class CommitSearchResult(models.GitHubCore):
    """A representation of a commit search result from the API.

    This object has the following attributes:

    .. attribute:: author

        A :class:`~github3.users.ShortUser` representing the user who
        authored the found commit.

    .. attribute:: comments_url

        The URL to retrieve the comments on the found commit from the API.

    .. attribute:: commit

        A :class:`~github3.git.ShortCommit` representing the found commit.

    .. attribute:: committer

        A :class:`~github3.users.ShortUser` representing the user who
        committed the found commit.

    .. attribute:: html_url

        The URL to view the found commit in a browser.

    .. attribute:: repository

        A :class:`~github3.repos.repo.ShortRepository` representing the
        repository in which the commit was found.

    .. attribute:: score

        The confidence score assigned to the result.

    .. attribute:: sha

        The SHA1 of the found commit.

    .. attribute:: text_matches

        A list of the text matches in the commit that generated this result.

        .. note::

            To receive these, you must pass ``text_match=True`` to
            :meth:`~github3.github.GitHub.search_commit`.
    """

    def _update_attributes(self, data):
        self._api = data["url"]
        self.author = users.ShortUser(data["author"], self)
        self.comments_url = data["comments_url"]
        self.commit = git.ShortCommit(data["commit"], self)
        self.committer = users.ShortUser(data["committer"], self)
        self.html_url = data["html_url"]
        self.repository = repos.ShortRepository(data["repository"], self)
        self.score = data["score"]
        self.sha = data["sha"]
        self.text_matches = data.get("text_matches", [])

    def _repr(self):
        return f"<CommitSearchResult [{self.sha[:7]}]>"
