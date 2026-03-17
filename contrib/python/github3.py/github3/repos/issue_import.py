"""This module contains the logic for GitHub's import issue API."""
from .. import models


class ImportedIssue(models.GitHubCore):
    """Represents an issue imported via the unofficial API.

    See also: https://gist.github.com/jonmagic/5282384165e0f86ef105

    This object has the following attributes:

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this imported issue was created.

    .. attribute:: id

        The globally unique identifier for this imported issue.

    .. attribute:: import_issues_url

        The URL used to import more issues via the API.

    .. attribute:: repository_url

        The URL used to retrieve the repository via the API.

    .. attribute:: status

        The status of this imported issue.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing te date and time
        when this imported issue was last updated.
    """

    IMPORT_CUSTOM_HEADERS = {
        "Accept": "application/vnd.github.golden-comet-preview+json"
    }

    def _update_attributes(self, issue):
        self._api = issue["url"]
        self.created_at = self._strptime(issue["created_at"])
        self.id = issue["id"]
        self.import_issues_url = issue["import_issues_url"]
        self.repository_url = issue["repository_url"]
        self.status = issue["status"]
        self.updated_at = self._strptime(issue["updated_at"])
