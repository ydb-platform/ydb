"""Module containing the Issue logic."""
from json import dumps

from uritemplate import URITemplate

from . import comment
from . import event
from . import label
from . import milestone
from .. import models
from .. import users
from ..decorators import requires_auth


class _Issue(models.GitHubCore):
    """The :class:`Issue <Issue>` object.

    Please see GitHub's `Issue Documentation`_ for more information.

    .. _Issue Documentation:
        http://developer.github.com/v3/issues
    """

    class_name = "_Issue"

    def _update_attributes(self, issue):
        self._api = issue["url"]
        self.assignee = issue["assignee"]
        if self.assignee:
            self.assignee = users.ShortUser(self.assignee, self)
        self.assignees = issue["assignees"]
        if self.assignees:
            self.assignees = [
                users.ShortUser(assignee, self) for assignee in self.assignees
            ]
        self.body = issue["body"]
        self.closed_at = self._strptime(issue["closed_at"])
        self.comments_count = issue["comments"]
        self.comments_url = issue["comments_url"]
        self.created_at = self._strptime(issue["created_at"])
        self.events_url = issue["events_url"]
        self.html_url = issue["html_url"]
        self.id = issue["id"]
        self.labels_urlt = URITemplate(issue["labels_url"])
        self.locked = issue["locked"]
        self.milestone = issue["milestone"]
        if self.milestone:
            self.milestone = milestone.Milestone(self.milestone, self)
        self.number = issue["number"]
        self.original_labels = issue["labels"]
        if self.original_labels:
            self.original_labels = [
                label.ShortLabel(lbl, self) for lbl in self.original_labels
            ]
        self.pull_request_urls = issue.get("pull_request")
        self.state = issue["state"]
        self.title = issue["title"]
        self.updated_at = self._strptime(issue["updated_at"])
        self.user = users.ShortUser(issue["user"], self)

    def _repr(self):
        return "<{class_name} [#{n}]>".format(
            n=self.number, class_name=self.class_name
        )

    @requires_auth
    def add_assignees(self, users):
        """Assign ``users`` to this issue.

        This is a shortcut for :meth:`~github3.issues.issue.Issue.edit`.

        :param users:
            users or usernames to assign this issue to
        :type users:
            list of :class:`~github3.users.User`
        :type users:
            list of str
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        usernames = {getattr(user, "login", user) for user in users}
        assignees = list({a.login for a in self.assignees} | usernames)
        return self.edit(assignees=assignees)

    @requires_auth
    def add_labels(self, *args):
        """Add labels to this issue.

        :param str args:
            (required), names of the labels you wish to add
        :returns:
            list of labels
        :rtype:
            :class:`~github3.issues.label.ShortLabel`
        """
        url = self._build_url("labels", base_url=self._api)
        json = self._json(self._post(url, data=args), 200)
        return [label.ShortLabel(lbl, self) for lbl in json] if json else []

    @requires_auth
    def close(self):
        """Close this issue.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        assignee = self.assignee.login if self.assignee else ""
        number = self.milestone.number if self.milestone else None
        labels = [lbl.name for lbl in self.original_labels]

        return self.edit(
            self.title, self.body, assignee, "closed", number, labels
        )

    def comment(self, id_num):
        """Get a single comment by its id.

        The catch here is that id is NOT a simple number to obtain. If
        you were to look at the comments on issue #15 in
        sigmavirus24/Todo.txt-python, the first comment's id is 4150787.

        :param int id_num:
            (required), comment id, see example above
        :returns:
            the comment identified by ``id_num``
        :rtype:
            :class:`~github3.issues.comment.IssueComment`
        """
        json = None
        if int(id_num) > 0:  # Might as well check that it's positive
            base_url, _ = self.url.rsplit("/", 1)
            url = self._build_url("comments", str(id_num), base_url=base_url)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(comment.IssueComment, json)

    def comments(self, number=-1, sort="", direction="", since=None):
        """Iterate over the comments on this issue.

        :param int number:
            (optional), number of comments to iterate over
            Default: -1 returns all comments
        :param str sort:
            accepted valuees: ('created', 'updated') api-default: created
        :param str direction:
            accepted values: ('asc', 'desc') Ignored without the sort parameter
        :param since:
            (optional), Only issues after this date will be returned. This can
            be a ``datetime`` or an ISO8601 formatted date string, e.g.,
            ``2012-05-20T23:10:27Z``
        :type since:
            datetime or string
        :returns:
            iterator of comments
        :rtype:
            :class:`~github3.issues.comment.IssueComment`
        """
        url = self._build_url("comments", base_url=self._api)
        params = comment.issue_comment_params(sort, direction, since)
        return self._iter(int(number), url, comment.IssueComment, params)

    @requires_auth
    def create_comment(self, body):
        """Create a comment on this issue.

        :param str body:
            (required), comment body
        :returns:
            the created comment
        :rtype:
            :class:`~github3.issues.comment.IssueComment`
        """
        json = None
        if body:
            url = self._build_url("comments", base_url=self._api)
            json = self._json(self._post(url, data={"body": body}), 201)
        return self._instance_or_null(comment.IssueComment, json)

    @requires_auth
    def edit(
        self,
        title=None,
        body=None,
        assignee=None,
        state=None,
        milestone=None,
        labels=None,
        assignees=None,
    ):
        """Edit this issue.

        :param str title:
            title of the issue
        :param str body:
            markdown formatted body (description) of the issue
        :param str assignee:
            login name of user the issue should be assigned to
        :param str state:
            accepted values: ('open', 'closed')
        :param int milestone:
            the number (not title) of the milestone to assign this to,
            or 0 to remove the milestone

            .. note::

                This is not the milestone's globally unique identifier, it's
                value in :attr:`~github3.issues.milestone.Milestone.number`.
        :param list labels:
            list of labels to apply this to
        :param assignees:
            (optional), login of the users to assign the issue to
        :type assignees:
            list of strings
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        json = None
        data = {
            "title": title,
            "body": body,
            "assignee": assignee,
            "state": state,
            "milestone": milestone,
            "labels": labels,
            "assignees": assignees,
        }
        self._remove_none(data)
        if data:
            if "milestone" in data and data["milestone"] == 0:
                data["milestone"] = None
            json = self._json(self._patch(self._api, data=dumps(data)), 200)
        if json:
            self._update_attributes(json)
            return True
        return False

    def events(self, number=-1):
        """Iterate over events associated with this issue only.

        :param int number:
            (optional), number of events to return. Default: -1 returns all
            events available.
        :returns:
            generator of events on this issues
        :rtype:
            :class:`~github3.issues.event.IssueEvent`
        """
        url = self._build_url("events", base_url=self._api)
        return self._iter(int(number), url, event.IssueEvent)

    def is_closed(self):
        """Check if the issue is closed.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if self.closed_at or (self.state == "closed"):
            return True
        return False

    def labels(self, number=-1, etag=None):
        """Iterate over the labels associated with this issue.

        :param int number:
            (optional), number of labels to return. Default: -1 returns all
            labels applied to this issue.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of labels on this issue
        :rtype:
            :class:`~github3.issues.label.ShortLabel`
        """
        url = self._build_url("labels", base_url=self._api)
        return self._iter(int(number), url, label.ShortLabel, etag=etag)

    @requires_auth
    def lock(self):
        """Lock an issue.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("lock", base_url=self._api)
        return self._boolean(self._put(url), 204, 404)

    def pull_request(self):
        """Retrieve the pull request associated with this issue.

        :returns:
            the pull request associated with this issue
        :rtype:
            :class:`~github3.pulls.PullRequest`
        """
        from .. import pulls

        json = None
        pull_request_url = None
        if self.pull_request_urls is not None:
            pull_request_url = self.pull_request_urls.get("url")
        if pull_request_url:
            json = self._json(self._get(pull_request_url), 200)
        return self._instance_or_null(pulls.PullRequest, json)

    @requires_auth
    def remove_assignees(self, users):
        """Unassign ``users`` from this issue.

        This is a shortcut for :meth:`~github3.issues.issue.Issue.edit`.

        :param users:
            users or usernames to unassign this issue from
        :type users:
            list of :class:`~github3.users.User`
        :type users:
            list of str
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        usernames = {getattr(user, "login", user) for user in users}
        assignees = list({a.login for a in self.assignees} - usernames)
        return self.edit(assignees=assignees)

    @requires_auth
    def remove_label(self, name):
        """Remove label ``name`` from this issue.

        :param str name:
            (required), name of the label to remove
        :returns:
            list of removed labels
        :rtype:
            :class:`~github3.issues.label.ShortLabel`
        """
        url = self._build_url("labels", name, base_url=self._api)
        json = self._json(self._delete(url), 200, 404)
        labels = [label.ShortLabel(lbl, self) for lbl in json] if json else []
        return labels

    @requires_auth
    def remove_all_labels(self):
        """Remove all labels from this issue.

        :returns:
            the list of current labels (empty) if successful
        :rtype:
            list
        """
        # Can either send DELETE or [] to remove all labels
        return self.replace_labels([])

    @requires_auth
    def replace_labels(self, labels):
        """Replace all labels on this issue with ``labels``.

        :param list labels:
            label names
        :returns:
            list of labels
        :rtype:
            :class:`~github3.issues.label.ShortLabel`
        """
        url = self._build_url("labels", base_url=self._api)
        json = self._json(self._put(url, data=dumps(labels)), 200)
        return [label.ShortLabel(lbl, self) for lbl in json] if json else []

    @requires_auth
    def reopen(self):
        """Re-open a closed issue.

        .. note:: This is a short cut to using :meth:`edit`.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        assignee = self.assignee.login if self.assignee else ""
        number = self.milestone.number if self.milestone else None
        labels = [str(lbl) for lbl in self.original_labels]
        return self.edit(
            self.title, self.body, assignee, "open", number, labels
        )

    @requires_auth
    def unlock(self):
        """Unlock an issue.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("lock", base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)


class Issue(_Issue):
    """Object for the full representation of an Issue.

    GitHub's API returns different amounts of information about issues based
    upon how that information is retrieved. This object exists to represent
    the full amount of information returned for a specific issue. For example,
    you would receive this class when calling
    :meth:`~github3.github.GitHub.issue`. To provide a clear
    distinction between the types of issues, github3.py uses different classes
    with different sets of attributes.

    .. versionchanged:: 1.0.0

    This object has all of the same attributes as a
    :class:`~github3.issues.issue.ShortIssue` as well as the following:

    .. attribute:: body_html

        The HTML formatted body of this issue.

    .. attribute:: body_text

        The plain-text formatted body of this issue.

    .. attribute:: closed_by

        If the issue is closed, a :class:`~github3.users.ShortUser`
        representing the user who closed the issue.
    """

    class_name = "Issue"

    def _update_attributes(self, issue):
        super()._update_attributes(issue)
        self.body_html = issue["body_html"]
        self.body_text = issue["body_text"]
        self.closed_by = issue["closed_by"]
        if self.closed_by:
            self.closed_by = users.ShortUser(self.closed_by, self)


class ShortIssue(_Issue):
    """Object for the shortened representation of an Issue.

    GitHub's API returns different amounts of information about issues based
    upon how that information is retrieved. Often times, when iterating over
    several issues, GitHub will return less information. To provide a clear
    distinction between the types of issues, github3.py uses different classes
    with different sets of attributes.

    .. versionadded:: 1.0.0

    This object has the following attributes:

    .. attribute:: assignee

        .. deprecated:: 1.0.0

            While the API still returns this attribute, it's not as useful in
            the context of multiple assignees.

        If a user is assigned to this issue, then it will be represented as a
        :class:`~github3.users.ShortUser`.

    .. attribute:: assignees

        If users are assigned to this issue, then they will be represented as
        a list of :class:`~github3.users.ShortUser`.

    .. attribute:: body

        The markdown formatted text of the issue as writen by the user who
        opened the issue.

    .. attribute:: closed_at

        If this issue is closed, this will be a :class:`~datetime.datetime`
        object representing the date and time this issue was closed. Otherwise
        it will be ``None``.

    .. attribute:: comments_count

        The number of comments on this issue.

    .. attribute:: comments_url

        The URL to retrieve the comments on this issue from the API.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        this issue was created.

    .. attribute:: events_url

        The URL to retrieve the events related to this issue from the API.

    .. attribute:: html_url

        The URL to view this issue in a browser.

    .. attribute:: id

        The unique identifier for this issue in GitHub.

    .. attribute:: labels_urlt

        A :class:`~uritemplate.URITemplate` object that can expand to a URL to
        retrieve the labels on this issue from the API.

    .. attribute:: locked

        A boolean attribute representing whether or not this issue is locked.

    .. attribute:: milestone

        A :class:`~github3.issues.milestone.Milestone` object representing the
        milestone to which this issue was assigned.

    .. attribute:: number

        The number identifying this issue on its parent repository.

    .. attribute:: original_labels

        If any are assigned to this issue, the list of
        :class:`~github3.issues.label.ShortLabel` objects representing the
        labels returned by the API for this issue.

    .. attribute:: pull_request_urls

        If present, a dictionary of URLs for retrieving information about the
        associated pull request for this issue.

    .. attribute:: state

        The current state of this issue, e.g., ``'closed'`` or ``'open'``.

    .. attribute:: title

        The title for this issue.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this issue was last updated.

    .. attribute:: user

        A :class:`~github3.users.ShortUser` representing the user who opened
        this issue.
    """

    class_name = "ShortIssue"
    _refresh_to = Issue
