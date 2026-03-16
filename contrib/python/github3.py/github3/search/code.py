"""Code search results implementation."""
from .. import models
from .. import repos


class CodeSearchResult(models.GitHubCore):
    """A representation of a code search result from the API.

    This object has the following attributes:

    .. attribute:: git_url

        The URL to retrieve the blob via Git

    .. attribute:: html_url

        The URL to view the blob found in a browser.

    .. attribute:: name

        The name of the file where the search result was found.

    .. attribute:: path

        The path in the repository to the file containing the result.

    .. attribute:: repository

        A :class:`~github3.repos.repo.ShortRepository` representing the
        repository in which the result was found.

    .. attribute:: score

        The confidence score assigned to the result.

    .. attribute:: sha

        The SHA1 of the blob in which the code can be found.

    .. attribute:: text_matches

        A list of the text matches in the blob that generated this result.

        .. note::

            To receive these, you must pass ``text_match=True`` to
            :meth:`~github3.github.GitHub.search_code`.
    """

    def _update_attributes(self, data):
        self._api = data["url"]
        self.git_url = data["git_url"]
        self.html_url = data["html_url"]
        self.name = data["name"]
        self.path = data["path"]
        self.repository = repos.ShortRepository(data["repository"], self)
        self.score = data["score"]
        self.sha = data["sha"]
        self.text_matches = data.get("text_matches", [])

    def _repr(self):
        return f"<CodeSearchResult [{self.path}]>"
