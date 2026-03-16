"""Issue events logic."""
from .. import users
from ..models import GitHubCore


class IssueEvent(GitHubCore):
    """Representation of an event from a specific issue.

    This object will be instantiated from calling
    :meth:`~github3.issues.issue.Issue.events` which calls
    https://developer.github.com/v3/issues/events/#list-events-for-an-issue

    See also: http://developer.github.com/v3/issues/events

    This object has the following attributes:

    .. attribute:: actor

        A :class:`~github3.users.ShortUser` representing the user who
        generated this event.

    .. attribute:: commit_id

        The string SHA of a commit that referenced the parent issue. If there
        was no commit referencing this issue, then this will be ``None``.

    .. attribute:: commit_url

        The URL to retrieve commit information from the API for the commit
        that references the parent issue. If there was no commit, this will be
        ``None``.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        this event occurred.

    .. attribute:: event

        The issue-specific action that generated this event. Some examples
        are:

        - closed
        - reopened
        - subscribed
        - merged
        - referenced
        - mentioned
        - assigned

        See `this list of events`_ for a full listing.

    .. attribute:: id

        The unique identifier for this event.

    .. _this list of events:
        https://developer.github.com/v3/issues/events/#events-1
    """

    def _update_attributes(self, event):
        self._api = event["url"]
        self.actor = users.ShortUser(event["actor"], self)
        self.commit_id = event["commit_id"]
        self.commit_url = event["commit_url"]
        self.created_at = self._strptime(event["created_at"])
        # Only for 'assigned' and 'unassigned' events.
        self.assignee = event.get("assignee")
        if self.assignee:
            self.assignee = users.ShortUser(self.assignee, self)
        self.assigner = event.get("assigner")
        if self.assigner:
            self.assigner = users.ShortUser(self.assigner, self)

        # Only for 'review_requested' and 'review_request_removed' events.
        self.review_requester = event.get("review_requester")
        if self.review_requester:
            self.review_requester = users.ShortUser(
                self.review_requester, self
            )
        self.requested_reviewers = event.get("requested_reviewers")
        if self.requested_reviewers:
            self.requested_reviewers = [
                users.ShortUser(reviewer, self)
                for reviewer in self.requested_reviewers
            ]

        self.event = event["event"]
        self.id = event["id"]
        self._uniq = self._api

    def _repr(self):
        return f"<Issue Event [{self.event} by {self.actor}]>"


class RepositoryIssueEvent(IssueEvent):
    """Representation of an issue event on the repository level.

    This object will be instantiated from calling
    :meth:`~github3.repos.repo.Repository.issue_events` or
    :meth:`~github3.repos.repo.ShortRepository.issue_events` which call
    https://developer.github.com/v3/issues/events/#list-events-for-a-repository

    See also: http://developer.github.com/v3/issues/events

    This object has all of the attributes of
    :class:`~github3.issues.event.IssueEvent` and the following:

    .. attribute:: issue

        A :class:`~github3.issues.issue.ShortIssue` representing the issue
        where this event originated from.

    """

    def _update_attributes(self, event):
        super()._update_attributes(event)
        from . import issue

        self.issue = issue.ShortIssue(event["issue"], self)

    def _repr(self):
        return "<Repository Issue Event on #{} [{} by {}]>".format(
            self.issue.number, self.event, self.actor.login
        )
