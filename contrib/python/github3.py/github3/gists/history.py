"""Module containing the GistHistory object."""
from .. import models
from .. import users


class GistHistory(models.GitHubCore):
    """This object represents one version (or revision) of a gist.

    The GitHub API returns the following attributes:

    .. attribute:: url

        The URL to the revision of the gist retrievable through the API.

    .. attribute:: version

        The commit ID of the revision of the gist.

    .. attribute:: user

        The :class:`~github3.users.ShortUser` representation of the user who
        owns this gist.

    .. attribute:: committed_at

        The date and time of the revision's commit.

    .. attribute:: change_status

        A dictionary with the number of deletions, additions, and total
        changes to the gist.

    For convenience, github3.py also exposes the following attributes from the
    :attr:`change_status`:

    .. attribute:: additions

        The number of additions to the gist compared to the previous revision.

    .. attribute:: deletions

        The number of deletions from the gist compared to the previous
        revision.

    .. attribute:: total

        The total number of changes to the gist compared to the previous
        revision.
    """

    def _update_attributes(self, history) -> None:
        self.url = self._api = history["url"]
        self.version = history["version"]
        self.user = users.ShortUser(history["user"], self)
        self.change_status = history["change_status"]
        self.additions = self.change_status.get("additions")
        self.deletions = self.change_status.get("deletions")
        self.total = self.change_status["total"]
        self.committed_at = self._strptime(history["committed_at"])

    def _repr(self) -> str:
        return f"<Gist History [{self.version}]>"

    def gist(self):
        """Retrieve the gist at this version.

        :returns:
            the gist at this point in history or ``None``
        :rtype:
            :class:`Gist <github3.gists.gist.Gist>`
        """
        from .gist import Gist

        json = self._json(self._get(self._api), 200)
        return self._instance_or_null(Gist, json)
