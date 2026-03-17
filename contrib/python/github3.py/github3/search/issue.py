"""Issue search results implementation."""
from ..issues import ShortIssue
from ..models import GitHubCore


class IssueSearchResult(GitHubCore):
    """A representation of a search result containing an issue.

    This object has the following attributes:

    .. attribute:: issue

        A :class:`~github3.issues.issue.ShortIssue` representing the issue
        found in this search result.

    .. attribute:: score

        The confidence score of this search result.

    .. attribute:: text_matches

        A list of matches in the issue for this search result.

        .. note::

            To receive these, you must pass ``text_match=True`` to
            :meth:`~github3.github.GitHub.search_issues`.
    """

    def _update_attributes(self, data):
        result = data.copy()
        self.score = result.pop("score")
        self.text_matches = result.pop("text_matches", [])
        self.issue = ShortIssue(result, self)

    def _repr(self):
        return f"<IssueSearchResult [{self.issue}]>"
