"""This module contains the Status object for GitHub's commit status API."""
from .. import models
from .. import users
from ..models import GitHubCore


class _Status(models.GitHubCore):
    """Representation of a status on a repository."""

    class_name = "_Status"

    def _update_attributes(self, status):
        self._api = status["url"]
        self.context = status["context"]
        self.created_at = self._strptime(status["created_at"])
        self.description = status["description"]
        self.id = status["id"]
        self.state = status["state"]
        self.target_url = status["target_url"]
        self.updated_at = self._strptime(status["updated_at"])

    def _repr(self):
        return "<{s.class_name} [{s.id}:{s.state}]>".format(s=self)


class Status(_Status):
    """Representation of a full status on a repository.

    See also: http://developer.github.com/v3/repos/statuses/

    This object has the same attributes as a
    :class:`~github3.repos.status.ShortStatus` as well as the following
    attributes:

    .. attribute:: creator

        A :class:`~github3.users.ShortUser` representing the user who created
        this status.
    """

    class_name = "Status"

    def _update_attributes(self, status):
        super()._update_attributes(status)
        self.creator = users.ShortUser(status["creator"], self)


class ShortStatus(_Status):
    """Representation of a short status on a repository.

    .. versionadded:: 1.0.0

    This is the representation found in a
    :class:`~github3.repos.status.CombinedStatus` object.

    See also: http://developer.github.com/v3/repos/statuses/

    This object has the following attributes:

    .. attribute:: context

        This is a string that explains the context of this status object.
        For example, ``'continuous-integration/travis-ci/pr'``.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this status was created.

    .. attribute:: creator

        A :class:`~github3.users.ShortUser` representing the user who created
        this status.

    .. attribute:: description

        A short description of the status.

    .. attribute:: id

        The unique identifier of this status object.

    .. attribute:: state

        The state of this status, e.g., ``'success'``, ``'pending'``,
        ``'failure'``.

    .. attribute:: target_url

        The URL to retrieve more information about this status.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this status was most recently updated.
    """

    class_name = "ShortStatus"
    _refresh_to = Status


class CombinedStatus(GitHubCore):
    """A representation of the combined statuses in a repository.

    See also: http://developer.github.com/v3/repos/statuses/

    This object has the following attributes:

    .. attribute:: commit_url

        The URL of the commit this combined status is present on.

    .. attribute:: repository

        A :class:`~gitub3.repos.repo.ShortRepository` representing the
        repository on which this combined status exists.

    .. attribute:: sha

        The SHA1 of the commit this status exists on.

    .. attribute:: state

        The state of the combined status, e.g., ``'success'``, ``'pending'``,
        ``'failure'``.

    .. attribute:: statuses

        The list of :class:`~github3.repos.status.ShortStatus` objects
        representing the individual statuses that is combined in this object.

    .. attribute:: total_count

        The total number of sub-statuses.
    """

    def _update_attributes(self, combined_status):
        from . import repo

        self._api = combined_status["url"]
        self.commit_url = combined_status["commit_url"]
        self.repository = repo.ShortRepository(
            combined_status["repository"], self
        )
        self.sha = combined_status["sha"]
        self.state = combined_status["state"]
        statuses = combined_status["statuses"]
        self.statuses = [ShortStatus(s, self) for s in statuses]
        self.total_count = combined_status["total_count"]

    def _repr(self):
        f = "<CombinedStatus [{s.state}:{s.total_count} sub-statuses]>"
        return f.format(s=self)
