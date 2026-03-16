"""This module contains the classes related to Events."""
import copy

from . import models


class EventUser(models.GitHubCore):
    """The class that represents the user information returned in Events.

    .. note::

        Refreshing this object will return a :class:`~github3.users.User`.

    .. attribute:: avatar_url

        The URL of the avatar image this user chose.

    .. attribute:: display_login

        The login that is displayed as part of the event.

    .. attribute:: gravatar_id

        The unique ID for the user's gravatar, if they're using gravatar to
        host their avatar.

    .. attribute:: id

        The user's unique ID in GitHub.

    .. attribute:: login

        The user's login (or handle) on GitHub.
    """

    def _update_attributes(self, user):
        self.avatar_url = user["avatar_url"]
        self.display_login = user.get("display_login")
        self.gravatar_id = user["id"]
        self.id = user["id"]
        self.login = user["login"]
        self._api = self.url = user["url"]

    def to_user(self):
        """Retrieve a full User object for this EventUser.

        :returns:
            The full information about this user.
        :rtype:
            :class:`~github3.users.User`
        """
        from . import users

        url = self._build_url("users", self.login)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(users.User, json)

    refresh = to_user


class EventOrganization(models.GitHubCore):
    """Representation of the organization information returned in Events.

    .. note::

        Refreshing this object will return a
        :class:`~github3.orgs.Organization`.

    This object has the following attributes:

    .. attribute:: avatar_url

        The URL to this organization's avatar.

    .. attribute:: gravatar_id

        The unique identifier for this organization on Gravatar, if its
        avatar is hosted there.

    .. attribute:: id

        This organization's unique identifier on GitHub.

    .. attribute:: login

        The unique login for this organization.
    """

    def _update_attributes(self, org):
        self.avatar_url = org["avatar_url"]
        self.gravatar_id = org["id"]
        self.id = org["id"]
        self.login = org["login"]
        self._api = self.url = org["url"]

    def to_org(self):
        """Retrieve a full Organization object for this EventOrganization.

        :returns:
            The full information about this organization.
        :rtype:
            :class:`~github3.orgs.Organization`
        """
        from . import orgs

        url = self._build_url("orgs", self.login)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(orgs.Organization, json)

    refresh = to_org


class EventPullRequest(models.GitHubCore):
    """Representation of a Pull Request returned in Events.

    .. note::

        Refreshing this object returns a :class:`~github3.pulls.PullRequest`.

    This object has the following attributes:

    .. attribute:: id

        The unique id of this pull request across all of GitHub.

    .. attribute:: number

        The number of this pull request on its repository.

    .. attribute:: state

        The state of this pull request during this event.

    .. attribute:: title

        The title of this pull request during this event.

    .. attribute:: locked

        A boolean attribute describing if this pull request was locked.
    """

    def _update_attributes(self, pull):
        self.id = pull["id"]
        self.number = pull["number"]
        self.state = pull["state"]
        self.title = pull["title"]
        self.locked = pull["locked"]
        self._api = self.url = pull["url"]

    def to_pull(self):
        """Retrieve a full PullRequest object for this EventPullRequest.

        :returns:
            The full information about this pull request.
        :rtype:
            :class:`~github3.pulls.PullRequest`
        """
        from . import pulls

        json = self._json(self._get(self.url), 200)
        return self._instance_or_null(pulls.PullRequest, json)

    refresh = to_pull


class EventReviewComment(models.GitHubCore):
    """Representation of review comments in events.

    .. note::

        Refreshing this object will return a new
        :class`~github3.pulls.ReviewComment`

    This object has the following attributes:

    .. attribute:: id

        The unique id of this comment across all of GitHub.

    .. attribute:: author_association

        The association the author has with this project.

    .. attribute:: body

        The markdown body of this review comment.

    .. attribute:: commit_id

        The identifier of the commit that this comment was left on.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        this comment was created.

    .. attribute:: diff_hunk

        The section (or hunk) of the diff this comment was left on.

    .. attribute:: html_url

        The URL to view this comment in a browser.

    .. attribute:: links

        A dictionary of links to various items about this comment.

    .. attribute:: original_commit_id

        The identifier of original commit this comment was left on.

    .. attribute:: original_position

        The original position within the diff this comment was left.

    .. attribute:: path

        The path to the file this comment was left on.

    .. attribute:: position

        The current position within the diff this comment is placed.

    .. attribute:: pull_request_url

        The URL to retrieve the pull request informtation from the API.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        this comment was updated.

    .. attribute:: user

        A :class:`~github3.users.ShortUser` representing the user who authored
        this comment.
    """

    def _update_attributes(self, comment):
        from . import users

        self._api = comment["url"]
        self.id = comment["id"]
        self.author_association = comment["author_association"]
        self.body = comment["body"]
        self.commit_id = comment["commit_id"]
        self.created_at = self._strptime(comment["created_at"])
        self.diff_hunk = comment["diff_hunk"]
        self.html_url = comment["html_url"]
        self.links = comment["_links"]
        self.original_commit_id = comment["original_commit_id"]
        self.original_position = comment["original_position"]
        self.path = comment["path"]
        self.position = comment["position"]
        self.pull_request_url = comment["pull_request_url"]
        self.updated_at = self._strptime(comment["updated_at"])
        self.user = users.ShortUser(comment["user"], self)

    def to_review_comment(self):
        """Retrieve a full ReviewComment object for this EventReviewComment.

        :returns:
            The full information about this review comment
        :rtype:
            :class:`~github3.pulls.ReviewComment`
        """
        from . import pulls

        comment = self._json(self._get(self._api), 200)
        return pulls.ReviewComment(comment, self)

    refresh = to_review_comment


class EventIssue(models.GitHubCore):
    """The class that represents the issue information returned in Events."""

    def _update_attributes(self, issue):
        self.id = issue["id"]
        self.number = issue["number"]
        self.state = issue["state"]
        self.title = issue["title"]
        self.locked = issue["locked"]
        self._api = self.url = issue["url"]

    def to_issue(self):
        """Retrieve a full Issue object for this EventIssue."""
        from . import issues

        json = self._json(self._get(self.url), 200)
        return self._instance_or_null(issues.Issue, json)

    refresh = to_issue


class EventIssueComment(models.GitHubCore):
    """Representation of a comment left on an issue.

    See also: http://developer.github.com/v3/issues/comments/

    This object has the following attributes:

    .. attribute:: author_association

        The association of the author (:attr:`user`) with the repository
        this issue belongs to.

    .. attribute:: body

        The markdown formatted original text written by the author.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this comment was created.

    .. attribute:: html_url

        The URL to view this comment in a browser.

    .. attribute:: id

        The unique identifier for this comment.

    .. attribute:: issue_url

        The URL of the parent issue in the API.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this comment was most recently updated.

    .. attribute:: user

        A :class:`~github3.users.ShortUser` representing the author of this
        comment.
    """

    def _update_attributes(self, comment):
        from . import users

        self._api = comment["url"]
        self.author_association = comment["author_association"]
        self.body = comment["body"]
        self.created_at = self._strptime(comment["created_at"])
        self.html_url = comment["html_url"]
        self.id = comment["id"]
        self.issue_url = comment["issue_url"]
        self.updated_at = self._strptime(comment["updated_at"])
        self.user = users.ShortUser(comment["user"], self)

    def to_issue_comment(self):
        """Retrieve the full IssueComment object for this comment.

        :returns:
            All the information about an IssueComment.
        :rtype:
            :class:`~github3.issues.comment.IssueComment`
        """
        from .issues import comment

        json = self._json(self._get(self.url), 200)
        return self._instance_or_null(comment.IssueComment, json)

    refresh = to_issue_comment


class Event(models.GitHubCore):
    """Represents an event as returned by the API.

    It structures and handles the data returned by via the
    `Events <https://developer.github.com/v3/activity/events>`_ section
    of the GitHub API.

    Two events can be compared like so::

        e1 == e2
        e1 != e2

    And that is equivalent to::

        e1.id == e2.id
        e1.id != e2.id

    .. attribute:: actor

        A :class:`~github3.events.EventUser` that represents the user whose
        action generated this event.

    .. attribute:: created_at

        A :class:`~datetime.datetime` representing when this event was created.

    .. attribute:: id

        The unique identifier for this event.

    .. attribute:: org

        If present, a :class:`~github3.events.EventOrganization` representing
        the organization on which this event occurred.

    .. attribute:: type

        The type of event this is.

        .. seealso::

            `Event Types Documentation`_
                GitHub's documentation of different event types

    .. attribute:: payload

        The payload of the event which has all of the details relevant to this
        event.

    .. attribute:: repo

        The string representation of the repository this event pertains to.

        .. versionchanged:: 1.0.0

            This restores the behaviour of the API. To get a tuple,
            representation, use ``self.repo.split('/', 1)``

    .. attribute:: public

        A boolean representing whether the event is publicly viewable or not.

    .. _Event Types Documentation:
        https://developer.github.com/v3/activity/events/types/
    """

    def _update_attributes(self, event):
        # If we don't copy this, then we end up altering _json_data which we do
        # not want to do:
        event = copy.deepcopy(event)
        self.actor = EventUser(event["actor"], self)
        self.created_at = self._strptime(event["created_at"])
        self.id = event["id"]
        self.org = event.get("org")
        if self.org:
            self.org = EventOrganization(event["org"], self)
        self.type = event["type"]
        handler = _payload_handlers.get(self.type, identity)
        self.payload = handler(event["payload"], self)
        self.repo = event["repo"]
        self.public = event["public"]

    def _repr(self):
        return f"<Event [{self.type[:-5]}]>"

    @staticmethod
    def list_types():
        """List available payload types."""
        return sorted(_payload_handlers.keys())


def _commitcomment(payload, session):
    from .repos.comment import ShortComment

    if payload.get("comment"):
        payload["comment"] = ShortComment(payload["comment"], session)
    return payload


def _follow(payload, session):
    if payload.get("target"):
        payload["target"] = EventUser(payload["target"], session)
    return payload


def _forkev(payload, session):
    from .repos import ShortRepository

    if payload.get("forkee"):
        payload["forkee"] = ShortRepository(payload["forkee"], session)
    return payload


def _gist(payload, session):
    from .gists import Gist

    if payload.get("gist"):
        payload["gist"] = Gist(payload["gist"], session)
    return payload


def _issuecomm(payload, session):
    if payload.get("issue"):
        payload["issue"] = EventIssue(payload["issue"], session)
    if payload.get("comment"):
        payload["comment"] = EventIssueComment(payload["comment"], session)
    return payload


def _issueevent(payload, session):
    if payload.get("issue"):
        payload["issue"] = EventIssue(payload["issue"], session)
    return payload


def _member(payload, session):
    if payload.get("member"):
        payload["member"] = EventUser(payload["member"], session)
    return payload


def _pullreqev(payload, session):
    if payload.get("pull_request"):
        payload["pull_request"] = EventPullRequest(
            payload["pull_request"], session
        )
    return payload


def _pullreqcomm(payload, session):
    # Transform the Pull Request attribute
    pull = payload.get("pull_request")
    if pull:
        payload["pull_request"] = EventPullRequest(pull, session)

    # Transform the Comment attribute
    comment = payload.get("comment")
    if comment:
        payload["comment"] = EventReviewComment(comment, session)
    return payload


def _release(payload, session):
    from .repos.release import Release

    release = payload.get("release")
    if release:
        payload["release"] = Release(release, session)
    return payload


def _team(payload, session):
    from .orgs import ShortTeam
    from .repos import ShortRepository

    if payload.get("team"):
        payload["team"] = ShortTeam(payload["team"], session)
    if payload.get("repo"):
        payload["repo"] = ShortRepository(payload["repo"], session)
    if payload.get("sender"):
        payload["sender"] = EventUser(payload["sender"], session)
    return payload


def identity(x, session):
    """Return the value."""
    return x


_payload_handlers = {
    "CommitCommentEvent": _commitcomment,
    "CreateEvent": identity,
    "DeleteEvent": identity,
    "FollowEvent": _follow,
    "ForkEvent": _forkev,
    "ForkApplyEvent": identity,
    "GistEvent": _gist,
    "GollumEvent": identity,
    "IssueCommentEvent": _issuecomm,
    "IssuesEvent": _issueevent,
    "MemberEvent": _member,
    "PublicEvent": identity,
    "PullRequestEvent": _pullreqev,
    "PullRequestReviewCommentEvent": _pullreqcomm,
    "PushEvent": identity,
    "ReleaseEvent": _release,
    "StatusEvent": identity,
    "TeamAddEvent": _team,
    "WatchEvent": identity,
}
