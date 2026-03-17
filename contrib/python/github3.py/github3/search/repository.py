"""Repository search results implementation."""
from .. import models
from .. import repos


class RepositorySearchResult(models.GitHubCore):
    """A representation of a search result containing a repository.

    This object has the following attributes::

    .. attribute:: repository

        A :class:`~github3.repos.repo.ShortRepository` representing the
        repository found by the search.

    .. attribute:: score

        The confidence score of this search result.

    .. attribute:: text_matches

        A list of the text matches in the repository that generated this
        result.

        .. note::

            To receive these, you must pass ``text_match=True`` to
            :meth:`~github3.github.GitHub.search_code`.
    """

    def _update_attributes(self, data):
        result = data.copy()
        self.score = result.pop("score")
        self.text_matches = result.pop("text_matches", [])
        self.repository = repos.ShortRepository(result, self)

    def _repr(self):
        return f"<RepositorySearchResult [{self.repository}]>"
