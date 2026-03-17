"""This module contains all the classes relating to Checks."""
from json import dumps

from . import decorators
from . import models


class CheckPullRequest(models.GitHubCore):
    """Representation of a Pull Request returned in Checks APIs.

    .. versionadded:: 1.3.0

    .. note::

        Refreshing this object returns a :class:`~github3.pulls.PullRequest`.

    This object has the following attributes:

    .. attribute:: id

        The unique id of this pull request across all of GitHub.

    .. attribute:: number

        The number of this pull request on its repository.

    .. attribute:: head

        A dict of minimal head information retrieved from the Check data
        representing the source of the pull request

    .. attribute:: base

        A dict of minimal base information retrieved from the Check data
        representing the pull request destination.
    """

    def _update_attributes(self, pull):
        self.id = pull["id"]
        self.number = pull["number"]
        self.base = pull["base"]
        self.head = pull["head"]
        self._api = self.url = pull["url"]

    def _repr(self):
        return f"<CheckPullRequest [#{self.number}]>"

    def to_pull(self):
        """Retrieve a full PullRequest object for this CheckPullRequest.

        :returns:
            The full information about this pull request.
        :rtype:
            :class:`~github3.pulls.PullRequest`
        """
        from . import pulls

        json = self._json(self._get(self.url), 200)
        return self._instance_or_null(pulls.PullRequest, json)

    refresh = to_pull


class CheckApp(models.GitHubCore):
    """Representation of an App returned in Checks APIs.

    .. versionadded:: 1.3.0

    .. note::

        Refreshing this object returns a :class:`~github3.apps.App`.

    This object has the following attributes:

    .. attribute:: description

        The description of the App provided by the owner.

    .. attribute:: external_url

        The URL provided for the App by the owner.

    .. attribute:: html_url

        The HTML URL provided for the App by the owner.

    .. attribute:: id

        The unique identifier for the App. This is useful in cases where you
        may want to authenticate either as an App or as a specific
        installation of an App.

    .. attribute:: name

        The display name of the App that the user sees.

    .. attribute:: owner

        A dict of minimal user information retrieved from the Check data
        representing the app owner
    """

    CUSTOM_HEADERS = {
        "Accept": "application/vnd.github.machine-man-preview+json"
    }

    def _update_attributes(self, app):
        self.description = app["description"]
        self.external_url = app["external_url"]
        self.html_url = app["html_url"]
        self.id = app["id"]
        self.name = app["name"]
        self.owner = app["owner"]
        _, slug = app["html_url"].rsplit("/", 1)
        self._api = self.url = self._build_url("apps", slug)

    def _repr(self):
        return '<App ["{}" by {}]>'.format(
            self.name, str(self.owner["login"])
        )

    def to_app(self):
        """Retrieve a full App object for this CheckApp.

        :returns:
            The full information about this App.
        :rtype:
            :class:`~github3.apps.App`
        """
        from . import apps

        headers = getattr(self, "CUSTOM_HEADERS", None)
        json = self._json(self._get(self._api, headers=headers), 200)
        return self._instance_or_null(apps.App, json)

    refresh = to_app


class CheckSuite(models.GitHubCore):
    """The :class:`CheckSuite <CheckSuite>` object.

    .. versionadded:: 1.3.0

    Please see GitHub's `CheckSuite Documentation`_ for more information.

    .. attribute:: status

        The status of the Check Suite

    .. attribute:: conclusion

        The highest priority check run conclusion. If it has not completed this
        will be None

    .. attribute:: head_sha

        The sha of the commit at the head of the branch the check was run
        against (the source of the pull request)

    .. attribute:: head_branch

        The branch checked

    .. attribute:: before

        The sha of the pull request target branch at the time of the checks

    .. attribute:: after

        The sha of the target branch after the change is applied

    .. attribute:: repository

        A representation of the repository the suite belongs to as
        :class:`~github3.repos.repo.ShortRepository`.

    .. attribute:: original_pull_requests

        A list of representations of the pull requests the suite belongs to as
        :class:`~github3.checks.CheckPullRequest`.

        .. note::

            This may be empty.

    .. attribute:: id

        The unique GitHub assigned numerical id of this check suite.

    .. attribute:: app

        A :class:`~github3.checks.CheckApp` representing the App
        this suite belongs to.

    .. CheckSuite Documentation:
        http://developer.github.com/v3/checks/suites/
    """

    class_name = "CheckSuite"
    CUSTOM_HEADERS = {"Accept": "application/vnd.github.antiope-preview+json"}

    def _update_attributes(self, suite):
        # Import here, because a toplevel import causes an import loop
        from . import repos

        self._api = suite["url"]
        self.status = suite["status"]
        self.conclusion = suite["conclusion"]
        self.head_branch = suite["head_branch"]
        self.head_sha = suite["head_sha"]
        self.before = suite["before"]
        self.after = suite["after"]
        prs = suite.get("pull_requests", [])
        self.original_pull_requests = [CheckPullRequest(p, self) for p in prs]
        self.repository = repos.ShortRepository(suite["repository"], self)
        self.id = suite["id"]
        self.app = CheckApp(suite["app"], self)

    def _repr(self):
        return "<{s.class_name} [{s.id}:{s.status}]>".format(s=self)

    @decorators.requires_auth
    def rerequest(self):
        """Rerequest the check suite.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("rerequest", base_url=self._api)
        return self._boolean(
            self._post(url, headers=CheckSuite.CUSTOM_HEADERS), 201, 404
        )

    def check_runs(self):
        """Retrieve the check runs for this suite.

        :returns:
            the check runs for this commit
        :rtype:
            :class:`~github3.checks.CheckRun`
        """
        url = self._build_url("check-runs", base_url=self._api)
        return self._iter(
            -1,
            url,
            CheckRun,
            headers=CheckRun.CUSTOM_HEADERS,
            list_key="check_runs",
        )


class CheckRunAnnotation(models.GitHubCore):
    """Representation of an annotation of a check run.

    .. versionadded:: 1.3.0

    .. attribute:: path

        The path of the file of the annotation.

    .. attribute:: start_line

        The start line of the annotation.

    .. attribute:: end_line

        The end line of the annotation.

    .. attribute:: start_column

        The start column of the annotation.

    .. attribute:: end_column

        The end column of the annotation.

    .. attribute:: annotation_level

        The level of the annotation. Can be one of 'notice', 'warning'
        or 'failure'.

    .. attribute:: title

        The title that represents the annotation.

    .. attribute:: message

        The short descript of the feedback for the lines of code.

    .. attribute:: raw_details

        The details about this annotation.
    """

    class_name = "CheckRunAnnotation"
    CUSTOM_HEADERS = {"Accept": "application/vnd.github.antiope-preview+json"}

    def _repr(self):
        return (
            "<{s.class_name} [{s.path}:{s.start_line}-{s.end_line}]>"
        ).format(s=self)

    def _update_attributes(self, note):
        self.path = note["path"]
        self.start_line = note["start_line"]
        self.end_line = note["end_line"]
        self.start_column = note["start_column"]
        self.end_column = note["end_column"]
        self.annotation_level = note["annotation_level"]
        self.title = note["title"]
        self.message = note["message"]
        self.raw_details = note["raw_details"]


class CheckRunOutput(models.GitHubCore):
    """Representation of the output of a check run.

    .. versionadded:: 1.3.0

    .. attribute:: title

        The title for the check run output.

    .. attribute:: summary

        The summary for the check run output.

    .. attribute:: text

        The output text of the check run.

    .. attribute:: annotations_count

        The number of annotations for this check run.

    .. attribute:: annotations_url

        The URL to retrieve the annotations
    """

    class_name = "CheckRunOutput"

    def _update_attributes(self, output):
        self.title = output["title"]
        self.summary = output["summary"]
        self.text = output["text"]
        self.annotations_count = output["annotations_count"]
        self.annotations_url = output["annotations_url"]

    def _repr(self):
        return "<{s.class_name} [{s.title}]>".format(s=self)

    def annotations(self):
        """Retrieve the annotations for a check run.

        :returns:
            the annotations for this check run
        :rtype:
            :class:`~github3.checks.CheckRunAnnotations`
        """
        url = self._build_url(base_url=self.annotations_url)
        return self._iter(
            -1,
            url,
            CheckRunAnnotation,
            headers=CheckRunAnnotation.CUSTOM_HEADERS,
        )


class CheckRun(models.GitHubCore):
    """The :class:`CheckRun <CheckRun>` object.

    .. versionadded:: 1.3.0

    Please see GitHub's `CheckRun Documentation`_ for more information.

    .. attribute:: status

        The current status of the check.

    .. attribute:: conclusion

        The final conclusion of the check. If the run has not concluded
        this will be None.

    .. attribute:: head_sha

        The sha of the commit at the head of the branch checked.

    .. attribute:: name

        The name of the check.

    .. attribute:: started_at

        A :class:`~datetime.datetime` object representing the date and time
        when this check run started.

    .. attribute:: completed_at

        A :class:`~datetime.datetime` object representing the date and time
        when this check run completed. If this run is not completed it will
        be ``None``.

    .. attribute:: original_pull_requests

        A list of representations of the pull requests the run belongs to as
        :class:`~github3.checks.CheckPullRequest`.

        .. note::

            This may be empty.


    .. attribute:: id

        The unique GitHub assigned numerical id of this check run.

    .. attribute:: external_id

        A reference for the run on the integrator's system. This may be None.

    .. attribute:: html_url

        The URL one would use to view this check run in the browser.

    .. attribute:: check_suite

        The ID of the check suite this run belongs to.

    .. attribute:: output

        A :class:`~github3.checks.CheckRunOutput` representing the output
        of this check run.

    .. attribute:: app

        A :class:`~github3.checks.CheckApp` representing the App
        this run belongs to.

    .. CheckRun Documentation:
        http://developer.github.com/v3/checks/runs/
    """

    class_name = "CheckRun"
    CUSTOM_HEADERS = {"Accept": "application/vnd.github.antiope-preview+json"}

    def _update_attributes(self, run):
        self._api = run["url"]
        self.html_url = run["html_url"]
        self.status = run["status"]
        self.conclusion = run["conclusion"]
        self.started_at = self._strptime(run["started_at"])
        self.completed_at = self._strptime(run["completed_at"])
        self.head_sha = run["head_sha"]
        self.name = run["name"]
        prs = run.get("pull_requests", [])
        self.original_pull_requests = [CheckPullRequest(p, self) for p in prs]
        self.id = run["id"]
        self.external_id = run["external_id"]
        self.app = CheckApp(run["app"], self)
        self.check_suite = run["check_suite"]["id"]
        self.output = CheckRunOutput(run["output"], self)

    def _repr(self):
        return "<{s.class_name} [{s.name}:{s.status}]>".format(s=self)

    @decorators.requires_app_installation_auth
    def update(
        self,
        name=None,
        details_url=None,
        external_id=None,
        started_at=None,
        status=None,
        conclusion=None,
        completed_at=None,
        output=None,
        actions=None,
    ):
        """Update this check run.

        All parameters are optional.

        :param str name:
            (optional), new name of the check
        :param str details_url:
            (optional), The URL of the integrator's site that has the full
            details of the check
        :param str external_id:
            (optional), A reference for the run on the integrator's system
        :param str started_at:
            (optional), ISO 8601 time format: YYYY-MM-DDTHH:MM:SSZ
        :param str status:
            (optional), ('queued', 'in_progress', 'completed')
        :param str conclusion:
            (optional), Required if you provide 'completed_at', or a
            'status' of 'completed'. The final conclusion of the check.
            ('success', 'failure', 'neutral', 'cancelled', 'timed_out',
            'action_required')
        :param str completed_at:
            (optional), Required if you provide 'conclusion'. ISO 8601 time
            format: YYYY-MM-DDTHH:MM:SSZ
        :param dict output:
            (optional), key-value pairs representing the output. Format:
            ``{'title': 'string', 'summary', 'text, can be markdown', 'text':
            'text, can be markdown', 'annotations': [{}], 'images': [{}]}``
        :param list actions:
            (optional), list of action objects. Format is:
            ``[{'label': 'text', 'description', 'text', 'identifier', 'text'},
            ...]``
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        data = {
            "name": name,
            "details_url": details_url,
            "external_id": external_id,
            "started_at": started_at,
            "status": status,
            "conclusion": conclusion,
            "completed_at": completed_at,
            "output": output,
            "actions": actions,
        }
        self._remove_none(data)
        json = None

        if data:
            json = self._json(
                self._patch(
                    self._api,
                    data=dumps(data),
                    headers=CheckSuite.CUSTOM_HEADERS,
                ),
                200,
            )
        if json:
            self._update_attributes(json)
            return True
        return False
