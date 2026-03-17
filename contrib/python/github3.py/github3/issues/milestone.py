from json import dumps

from . import label
from .. import users
from ..decorators import requires_auth
from ..models import GitHubCore


class Milestone(GitHubCore):
    """Representation of milestones on a repository.

    See also: http://developer.github.com/v3/issues/milestones/

    This object has the following attributes:

    .. attribute:: closed_issues_count

        The number of closed issues in this milestone.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this milestone was created.

    .. attribute:: creator

        If present, a :class:`~github3.users.ShortUser` representing the user
        who created this milestone.

    .. attribute:: description

        The written description of this milestone and its purpose.

    .. attribute:: due_on

        If set, a :class:`~datetime.datetime` object representing the date and
        time when this milestone is due.

    .. attribute:: id

        The unique identifier of this milestone in GitHub.

    .. attribute:: number

        The repository-local numeric identifier of this milestone. This starts
        at 1 like issues.

    .. attribute:: open_issues_count

        The number of open issues still in this milestone.

    .. attribute:: state

        The state of this milestone, e.g., ``'open'`` or ``'closed'``.

    .. attribute:: title

        The title of this milestone.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this milestone was last updated.
    """

    def _update_attributes(self, milestone):
        self._api = milestone["url"]
        self.closed_issues_count = milestone["closed_issues"]
        self.closed_issues = self.closed_issues_count
        self.created_at = self._strptime(milestone["created_at"])
        self.creator = milestone["creator"]
        if self.creator:
            self.creator = users.ShortUser(self.creator, self)
        self.description = milestone["description"]
        self.due_on = self._strptime(milestone["due_on"])
        self.id = milestone["id"]
        self.number = milestone["number"]
        self.open_issues_count = milestone["open_issues"]
        self.open_issues = self.open_issues_count
        self.state = milestone["state"]
        self.title = milestone["title"]
        self.updated_at = self._strptime(milestone["updated_at"])

    def _repr(self):
        return f"<Milestone [{self}]>"

    def __str__(self):
        return self.title

    @requires_auth
    def delete(self):
        """Delete this milestone.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    def labels(self, number=-1, etag=None):
        """Iterate over the labels of every associated issue.

        .. versionchanged:: 0.9

            Add etag parameter.

        :param int number:
            (optional), number of labels to return. Default: -1 returns all
            available labels.
        :param str etag:
            (optional), ETag header from a previous response
        :returns:
            generator of labels
        :rtype:
            :class:`~github3.issues.label.ShortLabel`
        """
        url = self._build_url("labels", base_url=self._api)
        return self._iter(int(number), url, label.ShortLabel, etag=etag)

    @requires_auth
    def update(self, title=None, state=None, description=None, due_on=None):
        """Update this milestone.

        All parameters are optional, but it makes no sense to omit all of them
        at once.

        :param str title:
            (optional), new title of the milestone
        :param str state:
            (optional), ('open', 'closed')
        :param str description:
            (optional)
        :param str due_on:
            (optional), ISO 8601 time format: YYYY-MM-DDTHH:MM:SSZ
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        data = {
            "title": title,
            "state": state,
            "description": description,
            "due_on": due_on,
        }
        self._remove_none(data)
        json = None

        if data:
            json = self._json(self._patch(self._api, data=dumps(data)), 200)
        if json:
            self._update_attributes(json)
            return True
        return False
