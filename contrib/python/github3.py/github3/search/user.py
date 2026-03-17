from .. import users
from ..models import GitHubCore


class UserSearchResult(GitHubCore):
    """Representation of a search result for a user.

    This object has the following attributes:

    .. attribute:: score

        The confidence score of this result.

    .. attribute:: text_matches

        If present, a list of text strings that match the search string.

    .. attribute:: user

        A :class:`~github3.users.ShortUser` representing the user found
        in this search result.
    """

    def _update_attributes(self, data):
        result = data.copy()
        self.score = result.pop("score")
        self.text_matches = result.pop("text_matches", [])
        self.user = users.ShortUser(result, self)

    def _repr(self):
        return f"<UserSearchResult [{self.user}]>"
