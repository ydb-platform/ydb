"""This module contains the RepoCommit classes."""
from . import status
from .. import checks
from .. import git
from .. import models
from .. import users
from .comment import RepoComment


class _RepoCommit(models.GitHubCore):
    """The :class:`RepoCommit <RepoCommit>` object.

    This represents a commit as
    viewed by a :class:`Repository`. This is different from a Commit object
    returned from the git data section.

    Two commit instances can be checked like so::

        c1 == c2
        c1 != c2

    And is equivalent to::

        c1.sha == c2.sha
        c1.sha != c2.sha

    """

    PREVIEW_HEADERS = {
        "Accept": "application/vnd.github.groot-preview+json,"
        "application/vnd.github.v3.full+json"
    }

    class_name = "_RepoCommit"

    def _update_attributes(self, commit):
        self._api = commit["url"]
        #: SHA of this commit.
        self._uniq = self.sha = commit["sha"]

    def _repr(self):
        return f"<{self.class_name} [{self.sha[:7]}]>"

    def check_runs(self):
        """Retrieve the check runs for this commit.

        .. versionadded:: 1.3.0

        :returns:
            the check runs for this commit
        :rtype:
            :class:`~github3.checks.CheckRun`
        """
        url = self._build_url("check-runs", base_url=self._api)
        return self._iter(
            -1,
            url,
            checks.CheckRun,
            headers=checks.CheckRun.CUSTOM_HEADERS,
            list_key="check_runs",
        )

    def check_suites(self):
        """Retrieve the check suites for this commit.

        .. versionadded:: 1.3.0

        :returns:
            the check suites for this commit
        :rtype:
            :class:`~github3.checks.CheckSuite`
        """
        url = self._build_url("check-suites", base_url=self._api)
        return self._iter(
            -1,
            url,
            checks.CheckSuite,
            headers=checks.CheckSuite.CUSTOM_HEADERS,
            list_key="check_suites",
        )

    def diff(self):
        """Retrieve the diff for this commit.

        :returns:
            the diff as a bytes object
        :rtype:
            bytes
        """
        resp = self._get(
            self._api, headers={"Accept": "application/vnd.github.diff"}
        )
        return resp.content if self._boolean(resp, 200, 404) else b""

    def patch(self):
        """Retrieve the patch formatted diff for this commit.

        :returns:
            the patch as a bytes object
        :rtype:
            bytes
        """
        resp = self._get(
            self._api, headers={"Accept": "application/vnd.github.patch"}
        )
        return resp.content if self._boolean(resp, 200, 404) else b""

    def status(self):
        """Retrieve the combined status for this commit.

        :returns:
            the combined status for this commit
        :rtype:
            :class:`~github3.repos.status.CombinedStatus`
        """
        url = self._build_url("status", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(status.CombinedStatus, json)

    def statuses(self):
        """Retrieve the statuses for this commit.

        :returns:
            the statuses for this commit
        :rtype:
            :class:`~github3.repos.status.Status`
        """
        url = self._build_url("statuses", base_url=self._api)
        return self._iter(-1, url, status.Status)

    def comments(self, number=-1, etag=None):
        """Iterate over comments for this commit.

        :param int number:
            (optional), number of comments to return. Default: -1 returns all
            comments
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of comments
        :rtype:
            :class:~github3.repos.comment.RepoComment`
        """
        url = self._build_url("comments", base_url=self._api)
        return self._iter(int(number), url, RepoComment, etag=etag)

    def associated_pull_requests(self, number=-1, etag=None):
        """Iterate pull requests associated with a commit.

        :param int number:
            (optional), number of comments to return. Default: -1 returns all
            pull requests
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of pull requests
        :rtype:
            :class:~github3.pulls.PullRequest`
        """
        from .. import pulls

        url = self._build_url("pulls", base_url=self._api)
        return self._iter(
            number,
            url,
            pulls.ShortPullRequest,
            etag=etag,
            headers=self.PREVIEW_HEADERS,
        )


class RepoCommit(_RepoCommit):
    """Representation of a commit with repository and git data."""

    class_name = "Repository Commit"

    def _update_attributes(self, commit):
        super()._update_attributes(commit)
        #: The number of additions made in the commit.
        self.additions = 0
        #: The number of deletions made in the commit.
        self.deletions = 0
        #: The files that were modified by this commit.
        self.files = commit["files"]
        #: Total number of changes in the files.
        self.total = 0
        self.stats = commit["stats"]
        if self.stats:
            self.additions = self.stats["additions"]
            self.deletions = self.stats["deletions"]
            self.total = self.stats["total"]


class MiniCommit(_RepoCommit):
    """A commit returned on a ShortBranch."""

    class_name = "Mini Repository Commit"
    _refresh_to = RepoCommit


class ShortCommit(_RepoCommit):
    """Representation of an incomplete commit in a collection."""

    class_name = "Short Repository Commit"
    _refresh_to = RepoCommit

    def _update_attributes(self, commit):
        super()._update_attributes(commit)
        self.author = commit["author"]
        if self.author:
            self.author = users.ShortUser(self.author, self)
        self.comments_url = commit["comments_url"]
        self.commit = git.ShortCommit(commit["commit"], self)
        self.committer = commit["committer"]
        if self.committer:
            self.committer = users.ShortUser(self.committer, self)
        self.html_url = commit["html_url"]
        #: List of parents to this commit.
        self.parents = commit["parents"]
        #: The commit message
        self.message = getattr(self.commit, "message", None)
