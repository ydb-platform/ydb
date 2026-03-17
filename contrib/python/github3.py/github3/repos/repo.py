"""This module contains Repository objects.

The Repository objects represent various different repository representations
returned by GitHub.

"""
import base64
import json as jsonlib

import uritemplate as urit

from . import branch
from . import comment
from . import commit
from . import comparison
from . import contents
from . import deployment
from . import hook
from . import invitation
from . import issue_import
from . import pages
from . import release
from . import stats
from . import status
from . import tag
from . import topics
from . import traffic
from .. import checks
from .. import decorators
from .. import events
from .. import exceptions
from .. import git
from .. import issues
from .. import licenses
from .. import models
from .. import notifications
from .. import projects
from .. import pulls
from .. import users
from .. import utils
from ..issues import event as ievent
from ..issues import label
from ..issues import milestone


class _Repository(models.GitHubCore):
    """This class serves as the base for all other Repository objects.

    Sub-classes should need only to override the ``_update_attributes``
    method to ensure that all attributes are present on the object.
    """

    PREVIEW_HEADERS = {"Accept": "application/vnd.github.mercy-preview+json"}

    STAR_HEADERS = {"Accept": "application/vnd.github.v3.star+json"}

    class_name = "_Repository"

    def _update_attributes(self, repo):
        self.url = self._api = repo["url"]
        self.archive_urlt = urit.URITemplate(repo["archive_url"])
        self.assignees_urlt = urit.URITemplate(repo["assignees_url"])
        self.blobs_urlt = urit.URITemplate(repo["blobs_url"])
        self.branches_urlt = urit.URITemplate(repo["branches_url"])
        self.collaborators_urlt = urit.URITemplate(repo["collaborators_url"])
        self.comments_urlt = urit.URITemplate(repo["comments_url"])
        self.commits_urlt = urit.URITemplate(repo["commits_url"])
        self.compare_urlt = urit.URITemplate(repo["compare_url"])
        self.contents_urlt = urit.URITemplate(repo["contents_url"])
        self.contributors_url = repo["contributors_url"]
        self.deployments_url = repo["deployments_url"]
        self.description = repo["description"]
        self.downloads_url = repo["downloads_url"]
        self.events_url = repo["events_url"]
        self.fork = repo["fork"]
        self.forks_url = repo["forks_url"]
        self.full_name = repo["full_name"]
        self.git_commits_urlt = urit.URITemplate(repo["git_commits_url"])
        self.git_refs_urlt = urit.URITemplate(repo["git_refs_url"])
        self.git_tags_urlt = urit.URITemplate(repo["git_tags_url"])
        self.hooks_url = repo["hooks_url"]
        self.html_url = repo["html_url"]
        self.id = repo["id"]
        self.issue_comment_urlt = urit.URITemplate(repo["issue_comment_url"])
        self.issue_events_urlt = urit.URITemplate(repo["issue_events_url"])
        self.issues_urlt = urit.URITemplate(repo["issues_url"])
        self.keys_urlt = urit.URITemplate(repo["keys_url"])
        self.labels_urlt = urit.URITemplate(repo["labels_url"])
        self.languages_url = repo["languages_url"]
        self.merges_url = repo["merges_url"]
        self.milestones_urlt = urit.URITemplate(repo["milestones_url"])
        self.name = repo["name"]
        self.notifications_urlt = urit.URITemplate(repo["notifications_url"])
        self.owner = users.ShortUser(repo["owner"], self)
        self.private = repo["private"]
        self.pulls_urlt = urit.URITemplate(repo["pulls_url"])
        self.releases_urlt = urit.URITemplate(repo["releases_url"])
        self.stargazers_url = repo["stargazers_url"]
        self.statuses_urlt = urit.URITemplate(repo["statuses_url"])
        self.subscribers_url = repo["subscribers_url"]
        self.subscription_url = repo["subscription_url"]
        self.tags_url = repo["tags_url"]
        self.teams_url = repo["teams_url"]
        self.trees_urlt = urit.URITemplate(repo["trees_url"])

    def _repr(self):
        return f"<{self.class_name} [{self}]>"

    def __str__(self):
        return self.full_name

    def _create_pull(self, data):
        self._remove_none(data)
        json = None
        if data:
            url = self._build_url("pulls", base_url=self._api)
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(pulls.ShortPullRequest, json)

    @decorators.requires_auth
    def add_collaborator(self, username, permission=None):
        """Add ``username`` as a collaborator to a repository.

        :param username:
            (required), username of the user
        :type username:
            str or :class:`~github3.users.User`
        :param str permission:
            (optional), permission to grant the collaborator, valid on
            organization repositories only.
            Can be 'pull', 'triage', 'push', 'maintain', 'admin' or an
            organization-defined custom role name.
        :returns:
            True if successful, False otherwise
        :rtype:
        """
        if not username:
            return False
        url = self._build_url(
            "collaborators", str(username), base_url=self._api
        )
        if permission:
            data = {"permission": permission}
            resp = self._put(url, data=jsonlib.dumps(data))
        else:
            resp = self._put(url)
        return self._boolean(resp, 201, 404)

    def archive(self, format, path="", ref="master"):
        """Get the tarball or zipball archive for this repo at ref.

        See: http://developer.github.com/v3/repos/contents/#get-archive-link

        :param str format:
            (required), accepted values: ('tarball', 'zipball')
        :param path:
            (optional), path where the file should be saved
            to, default is the filename provided in the headers and will be
            written in the current directory.
            it can take a file-like object as well
        :type path:
            str, file
        :param str ref:
            (optional)
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        resp = None
        if format in ("tarball", "zipball"):
            url = self._build_url(format, ref, base_url=self._api)
            resp = self._get(url, allow_redirects=True, stream=True)

        if resp and self._boolean(resp, 200, 404):
            utils.stream_response_to_file(resp, path)
            return True
        return False

    def asset(self, id):
        """Return a single asset.

        :param int id:
            (required), id of the asset
        :returns:
            the asset
        :rtype:
            :class:`~github3.repos.release.Asset`
        """
        data = None
        if int(id) > 0:
            url = self._build_url(
                "releases", "assets", str(id), base_url=self._api
            )
            data = self._json(self._get(url), 200)
        return self._instance_or_null(release.Asset, data)

    def assignees(self, number=-1, etag=None):
        """Iterate over all assignees to which an issue may be assigned.

        :param int number:
            (optional), number of assignees to return. Default:
            -1 returns all available assignees
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of users
        :rtype:
            :class:`~github3.users.ShortUser`
        """
        url = self._build_url("assignees", base_url=self._api)
        return self._iter(int(number), url, users.ShortUser, etag=etag)

    def blob(self, sha):
        """Get the blob indicated by ``sha``.

        :param str sha:
            (required), sha of the blob
        :returns:
            the git blob
        :rtype:
            :class:`~github3.git.Blob`
        """
        url = self._build_url("git", "blobs", sha, base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(git.Blob, json)

    def branch(self, name):
        """Get the branch ``name`` of this repository.

        :param str name:
            (required), branch name
        :returns:
            the branch
        :rtype:
            :class:`~github3.repos.branch.Branch`
        """
        json = None
        if name:
            url = self._build_url("branches", name, base_url=self._api)
            json = self._json(
                self._get(url, headers=branch.Branch.PREVIEW_HEADERS), 200
            )
        return self._instance_or_null(branch.Branch, json)

    def branches(self, number=-1, protected=False, etag=None):
        """Iterate over the branches in this repository.

        :param int number:
            (optional), number of branches to return. Default: -1 returns all
            branches
        :param bool protected:
            (optional), True lists only protected branches.
            Default: False
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of branches
        :rtype:
            :class:`~github3.repos.branch.Branch`
        """
        url = self._build_url("branches", base_url=self._api)
        params = {"protected": "1"} if protected else None
        return self._iter(
            int(number),
            url,
            branch.ShortBranch,
            params,
            etag=etag,
            headers=branch.Branch.PREVIEW_HEADERS,
        )

    def check_run(self, id):
        """Return a single check run.

        .. versionadded:: 1.3.0

        :param int id:
            (required), id of the check run
        :returns:
            the check run
        :rtype:
            :class:`~github3.checks.CheckRun`
        """
        data = None
        if int(id) > 0:
            url = self._build_url("check-runs", str(id), base_url=self._api)
            data = self._json(
                self._get(url, headers=checks.CheckRun.CUSTOM_HEADERS), 200
            )
        return self._instance_or_null(checks.CheckRun, data)

    def check_suite(self, id):
        """Return a single check suite.

        .. versionadded:: 1.3.0

        :param int id:
            (required), id of the check suite
        :returns:
            the check suite
        :rtype:
            :class:`~github3.checks.CheckSuite`
        """
        data = None
        if int(id) > 0:
            url = self._build_url("check-suites", str(id), base_url=self._api)
            data = self._json(
                self._get(url, headers=checks.CheckSuite.CUSTOM_HEADERS), 200
            )
        return self._instance_or_null(checks.CheckSuite, data)

    def code_frequency(self, number=-1, etag=None):
        """Iterate over the code frequency per week.

        .. versionadded:: 0.7

        Returns a weekly aggregate of the number of additions and deletions
        pushed to this repository.

        .. note::

            All statistics methods may return a 202. On those occasions,
            you will not receive any objects. You should store your
            iterator and check the new ``last_status`` attribute. If it
            is a 202 you should wait before re-requesting.

        :param int number:
            (optional), number of weeks to return. Default: -1
            returns all weeks
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of lists ``[seconds_from_epoch, additions, deletions]``
        :rtype:
            list
        """
        url = self._build_url("stats", "code_frequency", base_url=self._api)
        return self._iter(int(number), url, list, etag=etag)

    def collaborators(self, affiliation="all", number=-1, etag=None):
        """Iterate over the collaborators of this repository.

        :param str affiliation:
            (optional), affiliation of the collaborator to the repository.
            Default: "all" returns contributors with all affiliations
        :param int number:
            (optional), number of collaborators to return.
            Default: -1 returns all comments
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of collaborators
        :rtype:
            :class:`~github3.users.Collaborator`
        """
        url = self._build_url("collaborators", base_url=self._api)
        affiliations = {"outside", "direct", "all"}
        if affiliation not in affiliations:
            raise ValueError(
                (
                    "Invalid affiliation value {!r} parameter passed, must "
                    "be 'outside', 'direct', or 'all' (defaults to 'all')."
                ).format(affiliation)
            )
        params = {"affiliation": affiliation}
        return self._iter(
            int(number), url, users.Collaborator, params, etag=etag
        )

    def comments(self, number=-1, etag=None):
        """Iterate over comments on all commits in the repository.

        :param int number:
            (optional), number of comments to return. Default:
            -1 returns all comments
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of comments on commits
        :rtype:
            :class:`~github3.repos.comment.RepoComment`
        """
        url = self._build_url("comments", base_url=self._api)
        return self._iter(int(number), url, comment.RepoComment, etag=etag)

    def commit(self, sha):
        """Get a single (repo) commit.

        See :meth:`git_commit` for the Git Data Commit.

        :param str sha:
            (required), sha of the commit
        :returns:
            the commit
        :rtype:
            :class:`~github3.repos.commit.RepoCommit`
        """
        url = self._build_url("commits", sha, base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(commit.RepoCommit, json)

    def commit_activity(self, number=-1, etag=None):
        """Iterate over last year of commit activity by week.

        .. versionadded:: 0.7

        See: http://developer.github.com/v3/repos/statistics/

        .. note::

            All statistics methods may return a 202. On those occasions,
            you will not receive any objects. You should store your
            iterator and check the new ``last_status`` attribute. If it
            is a 202 you should wait before re-requesting.

        :param int number:
            (optional), number of weeks to return. Default -1
            will return all of the weeks.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of dictionaries
        :rtype:
            dict
        """
        url = self._build_url("stats", "commit_activity", base_url=self._api)
        return self._iter(int(number), url, dict, etag=etag)

    def commit_comment(self, comment_id):
        """Get a single commit comment.

        :param int comment_id:
            (required), id of the comment used by GitHub
        :returns:
            the comment on the commit
        :rtype:
            :class:`~github3.repos.comment.RepoComment`
        """
        url = self._build_url("comments", str(comment_id), base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(comment.RepoComment, json)

    def commits(
        self,
        sha=None,
        path=None,
        author=None,
        number=-1,
        etag=None,
        since=None,
        until=None,
        per_page=None,
    ):
        """Iterate over commits in this repository.

        :param str sha:
            (optional), sha or branch to start listing commits from
        :param str path:
            (optional), commits containing this path will be listed
        :param str author:
            (optional), GitHub login, real name, or email to
            filter commits by (using commit author)
        :param int number:
            (optional), number of commits to return. Default:
            -1 returns all commits
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :param since:
            (optional), Only commits after this date will be returned.
            This can be a ``datetime`` or an ``ISO8601`` formatted
            date string.
        :type since:
            :class:`~datetime.datetime` or str
        :param until:
            (optional), Only commits before this date will
            be returned. This can be a ``datetime`` or an ``ISO8601`` formatted
            date string.
        :type until:
            :class:`~datetime.datetime` or str
        :param int per_page:
            (optional), commits listing page size
        :returns:
            generator of commits
        :rtype:
            :class:`~github3.repos.commit.RepoCommit`
        """
        params = {
            "sha": sha,
            "path": path,
            "author": author,
            "since": utils.timestamp_parameter(since),
            "until": utils.timestamp_parameter(until),
            "per_page": per_page,
        }

        self._remove_none(params)
        url = self._build_url("commits", base_url=self._api)
        return self._iter(int(number), url, commit.ShortCommit, params, etag)

    def compare_commits(self, base, head):
        """Compare two commits.

        :param str base:
            (required), base for the comparison
        :param str head:
            (required), compare this against base
        :returns:
            the comparison of the commits
        :rtype:
            :class:`~github3.repos.comparison.Comparison`
        """
        url = self._build_url(
            "compare", base + "..." + head, base_url=self._api
        )
        json = self._json(self._get(url), 200)
        return self._instance_or_null(comparison.Comparison, json)

    def contributor_statistics(self, number=-1, etag=None):
        """Iterate over the contributors list.

        .. versionadded:: 0.7

        See also: http://developer.github.com/v3/repos/statistics/

        .. note::

            All statistics methods may return a 202. On those occasions,
            you will not receive any objects. You should store your
            iterator and check the new ``last_status`` attribute. If it
            is a 202 you should wait before re-requesting.

        :param int number:
            (optional), number of weeks to return. Default -1
            will return all of the weeks.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of contributor statistics for each contributor
        :rtype:
            :class:`~github3.repos.stats.ContributorStats`
        """
        url = self._build_url("stats", "contributors", base_url=self._api)
        return self._iter(int(number), url, stats.ContributorStats, etag=etag)

    def contributors(self, anon=False, number=-1, etag=None):
        """Iterate over the contributors to this repository.

        :param bool anon:
            (optional), True lists anonymous contributors as well
        :param int number:
            (optional), number of contributors to return.
            Default: -1 returns all contributors
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of contributor users
        :rtype:
            :class:`~github3.users.Contributor`
        """
        url = self._build_url("contributors", base_url=self._api)
        params = {}
        if anon:
            params = {"anon": "true"}
        return self._iter(int(number), url, users.Contributor, params, etag)

    def views(self, per="day"):
        """Get the total number of repository views and breakdown per day or
        week for the last 14 days.

        .. versionadded:: 1.4.0

        See also: https://developer.github.com/v3/repos/traffic/

        :param str per:
            (optional), ('day', 'week'), views reporting period. Default 'day'
            will return views per day for the last 14 days.
        :returns:
            views data
        :rtype:
            :class:`~github3.repos.traffic.ViewsStats`
        :raises:
            ValueError if per is not a valid choice
        """
        params = {}
        if per in ("day", "week"):
            params.update(per=per)
        else:
            raise ValueError("per must be 'day' or 'week'")
        url = self._build_url("traffic", "views", base_url=self._api)
        json = self._json(self._get(url, params=params), 200)
        return self._instance_or_null(traffic.ViewsStats, json)

    def clones(self, per="day"):
        """Get the total number of repository clones and breakdown per day or
        week for the last 14 days.

        .. versionadded:: 1.4.0

        See also: https://developer.github.com/v3/repos/traffic/

        :param str per:
            (optional), ('day', 'week'), clones reporting period. Default 'day'
            will return clones per day for the last 14 days.
        :returns:
            clones data
        :rtype:
            :class:`~github3.repos.traffic.ClonesStats`
        :raises:
            ValueError if per is not a valid choice
        """
        params = {}
        if per in ("day", "week"):
            params.update(per=per)
        else:
            raise ValueError("per must be 'day' or 'week'")
        url = self._build_url("traffic", "clones", base_url=self._api)
        json = self._json(self._get(url, params=params), 200)
        return self._instance_or_null(traffic.ClonesStats, json)

    @decorators.requires_app_installation_auth
    def create_check_run(
        self,
        name,
        head_sha,
        details_url=None,
        external_id=None,
        started_at=None,
        status=None,
        conclusion=None,
        completed_at=None,
        output=None,
        actions=None,
    ):
        """Create a check run object on a commit

        .. versionadded:: 1.3.0

        :param str name:
            (required), The name of the check
        :param str head_sha:
            (required), The SHA of the commit
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
            ``{'title': 'string', 'summary': 'text, can be markdown', 'text':
            'text, can be markdown', 'annotations': [{}], 'images': [{}]}``
        :param list actions:
            (optional), list of action objects. Format is:
            ``[{'label': 'text', 'description', 'text', 'identifier', 'text'},
            ...]``
        :returns:
            the created check run
        :rtype:
            :class:`~github3.checks.CheckRun`
        """
        json = None
        # TODO: Cleanse output dict, actions array
        if name and head_sha:
            data = {
                "name": name,
                "head_sha": head_sha,
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
            url = self._build_url("check-runs", base_url=self._api)
            json = self._json(
                self._post(
                    url, headers=checks.CheckRun.CUSTOM_HEADERS, data=data
                ),
                201,
            )
        return self._instance_or_null(checks.CheckRun, json)

    @decorators.requires_app_installation_auth
    def create_check_suite(self, head_sha):
        """Create a check suite object on a commit

        .. versionadded:: 1.3.0

        :param str head_sha:
            The sha of the head commit.
        :returns:
            the created check suite
        :rtype:
            :class:`~github3.checks.CheckSuite`
        """
        json = None
        if head_sha:
            data = {"head_sha": head_sha}
            self._remove_none(data)
            url = self._build_url("check-suites", base_url=self._api)
            json = self._json(
                self._post(
                    url, data=data, headers=checks.CheckSuite.CUSTOM_HEADERS
                ),
                201,
            )
        return self._instance_or_null(checks.CheckSuite, json)

    @decorators.requires_auth
    def create_blob(self, content, encoding):
        """Create a blob with ``content``.

        :param str content:
            (required), content of the blob
        :param str encoding:
            (required), ('base64', 'utf-8')
        :returns:
            string of the SHA returned
        :returns:
            str
        """
        sha = ""
        if encoding in ("base64", "utf-8"):
            url = self._build_url("git", "blobs", base_url=self._api)
            data = {"content": content, "encoding": encoding}
            json = self._json(self._post(url, data=data), 201)
            if json:
                sha = json.get("sha")
        return sha

    @decorators.requires_auth
    def create_branch_ref(self, name, sha=None):
        """Create a branch git reference.

        This is a shortcut for calling
        :meth:`github3.repos.repo.Repository.create_ref`.

        :param str branch:
            (required), the branch to create
        :param str sha:
            the commit to base the branch from
        :returns:
            a reference object representing the branch
        :rtype:
            :class:`~github3.git.Reference`
        """
        ref = "refs/heads/%s" % name
        return self.create_ref(ref, sha)

    @decorators.requires_auth
    def create_comment(self, body, sha, path=None, position=None, line=1):
        """Create a comment on a commit.

        :param str body:
            (required), body of the message
        :param str sha:
            (required), commit id
        :param str path:
            (optional), relative path of the file to comment on
        :param str position:
            (optional), line index in the diff to comment on
        :param int line:
            (optional), line number of the file to comment on, default: 1
        :returns:
            the created comment
        :rtype:
            :class:`~github3.repos.comment.RepoComment`
        """
        json = None
        if body and sha and (line and int(line) > 0):
            data = {
                "body": body,
                "line": line,
                "path": path,
                "position": position,
            }
            self._remove_none(data)
            url = self._build_url(
                "commits", sha, "comments", base_url=self._api
            )
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(comment.RepoComment, json)

    @decorators.requires_auth
    def create_commit(
        self, message, tree, parents, author=None, committer=None
    ):
        """Create a commit on this repository.

        :param str message:
            (required), commit message
        :param str tree:
            (required), SHA of the tree object this commit points to
        :param list parents:
            (required), SHAs of the commits that were parents
            of this commit. If empty, the commit will be written as the root
            commit.  Even if there is only one parent, this should be an
            array.
        :param dict author:
            (optional), if omitted, GitHub will
            use the authenticated user's credentials and the current
            time. Format: {'name': 'Committer Name', 'email':
            'name@example.com', 'date': 'YYYY-MM-DDTHH:MM:SS+HH:00'}
        :param dict committer:
            (optional), if ommitted, GitHub will use the
            author parameters. Should be the same format as the author
            parameter.
        :returns:
            the created commit
        :rtype:
            :class:`~github3.git.Commit`
        """
        json = None
        if message and tree and isinstance(parents, list):
            url = self._build_url("git", "commits", base_url=self._api)
            data = {
                "message": message,
                "tree": tree,
                "parents": parents,
                "author": author,
                "committer": committer,
            }
            self._remove_none(data)
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(git.Commit, json)

    @decorators.requires_auth
    def create_deployment(
        self,
        ref,
        required_contexts=None,
        payload="",
        auto_merge=False,
        description="",
        environment=None,
    ):
        """Create a deployment.

        :param str ref:
            (required), The ref to deploy. This can be a branch, tag, or sha.
        :param list required_contexts:
            Optional array of status contexts
            verified against commit status checks. To bypass checking
            entirely pass an empty array. Default: []
        :param str payload:
            Optional JSON payload with extra information about
            the deployment. Default: ""
        :param bool auto_merge:
            Optional parameter to merge the default branch
            into the requested deployment branch if necessary. Default: False
        :param str description:
            Optional short description. Default: ""
        :param str environment:
            Optional name for the target deployment
            environment (e.g., production, staging, qa). Default: "production"
        :returns:
            the created deployment
        :rtype:
            :class:`~github3.repos.deployment.Deployment`
        """
        json = None
        if ref:
            if required_contexts is None:
                required_contexts = []
            url = self._build_url("deployments", base_url=self._api)
            data = {
                "ref": ref,
                "required_contexts": required_contexts,
                "payload": payload,
                "auto_merge": auto_merge,
                "description": description,
                "environment": environment,
            }
            self._remove_none(data)
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(deployment.Deployment, json)

    @decorators.requires_auth
    def create_file(
        self, path, message, content, branch=None, committer=None, author=None
    ):
        """Create a file in this repository.

        See also: http://developer.github.com/v3/repos/contents/#create-a-file

        :param str path:
            (required), path of the file in the repository
        :param str message:
            (required), commit message
        :param bytes content:
            (required), the actual data in the file
        :param str branch:
            (optional), branch to create the commit on.
            Defaults to the default branch of the repository
        :param dict committer:
            (optional), if no information is given the
            authenticated user's information will be used. You must specify
            both a name and email.
        :param dict author:
            (optional), if omitted this will be filled in with
            committer information. If passed, you must specify both a name and
            email.
        :returns:
            dictionary of contents and commit for created file
        :rtype:
            :class:`~github3.repos.contents.Contents`,
            :class:`~github3.git.Commit`
        """
        if content and not isinstance(content, bytes):
            raise ValueError(  # (No coverage)
                "content must be a bytes object"
            )  # (No coverage)

        json = None
        if path and message and content:
            url = self._build_url("contents", path, base_url=self._api)
            content = base64.b64encode(content).decode("utf-8")
            data = {
                "message": message,
                "content": content,
                "branch": branch,
                "committer": contents.validate_commmitter(committer),
                "author": contents.validate_commmitter(author),
            }
            self._remove_none(data)
            json = self._json(self._put(url, data=jsonlib.dumps(data)), 201)
            if json and "content" in json and "commit" in json:
                json["content"] = contents.Contents(json["content"], self)
                json["commit"] = git.Commit(json["commit"], self)
        return json

    @decorators.requires_auth
    def create_fork(self, organization=None):
        """Create a fork of this repository.

        :param str organization:
            (required), login for organization to create the fork under
        :returns:
            the fork of this repository
        :rtype:
            :class:`~github3.repos.repo.Repository`
        """
        url = self._build_url("forks", base_url=self._api)
        if organization:
            resp = self._post(url, data={"organization": organization})
        else:
            resp = self._post(url)

        json = self._json(resp, 202)
        return self._instance_or_null(Repository, json)

    @decorators.requires_auth
    def create_hook(self, name, config, events=["push"], active=True):
        """Create a hook on this repository.

        :param str name:
            (required), name of the hook
        :param dict config:
            (required), key-value pairs which act as settings for this hook
        :param list events:
            (optional), events the hook is triggered for
        :param bool active:
            (optional), whether the hook is actually triggered
        :returns:
            the created hook
        :rtype:
            :class:`~github3.repos.hook.Hook`
        """
        json = None
        if name and config and isinstance(config, dict):
            url = self._build_url("hooks", base_url=self._api)
            data = {
                "name": name,
                "config": config,
                "events": events,
                "active": active,
            }
            json = self._json(self._post(url, data=data), 201)
        return hook.Hook(json, self) if json else None

    @decorators.requires_auth
    def create_issue(
        self,
        title,
        body=None,
        assignee=None,
        milestone=None,
        labels=None,
        assignees=None,
    ):
        """Create an issue on this repository.

        :param str title:
            (required), title of the issue
        :param str body:
            (optional), body of the issue
        :param str assignee:
            (optional), login of the user to assign the issue to
        :param int milestone:
            (optional), id number of the milestone to
            attribute this issue to (e.g. ``m`` is a
            :class:`~github3.issues.milestone.Milestone` object, ``m.number``
            is what you pass here.)
        :param labels:
            (optional), labels to apply to this issue
        :type labels:
            [str]
        :param assignees:
            (optional), login of the users to assign the issue to
        :type assignees:
            [str]
        :returns:
            the created issue
        :rtype:
            :class:`~github3.issues.issue.ShortIssue`
        """
        issue = {
            "title": title,
            "body": body,
            "assignee": assignee,
            "milestone": milestone,
            "labels": labels,
            "assignees": assignees,
        }
        self._remove_none(issue)
        json = None

        if issue:
            url = self._build_url("issues", base_url=self._api)
            json = self._json(self._post(url, data=issue), 201)

        return self._instance_or_null(issues.ShortIssue, json)

    @decorators.requires_auth
    def create_key(self, title, key, read_only=False):
        """Create a deploy key.

        :param str title:
            (required), title of key
        :param str key:
            (required), key text
        :param bool read_only:
            (optional), restrict key access to read-only, default is False
        :returns:
            the created key
        :rtype:
            :class:`~github3.users.Key`
        """
        json = None
        if title and key:
            data = {"title": title, "key": key, "read_only": read_only}
            url = self._build_url("keys", base_url=self._api)
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(users.Key, json)

    @decorators.requires_auth
    def create_label(self, name, color, description=None):
        """Create a label for this repository.

        :param str name:
            (required), name to give to the label
        :param str color:
            (required), value of the color to assign to the
            label, e.g., '#fafafa' or 'fafafa' (the latter is what is sent)
        :param str description:
            (optional), description to give to the label
        :returns:
            the created label
        :rtype:
            :class:`~github3.issues.label.Label`
        """
        json = None
        if name and color:
            data = {"name": name, "color": color.strip("#")}
            if description is not None:
                data["description"] = description
            url = self._build_url("labels", base_url=self._api)
            resp = self._post(
                url, data=data, headers=label.Label.SYMMETRA_PREVIEW_HEADERS
            )
            json = self._json(resp, 201)
        return self._instance_or_null(label.Label, json)

    @decorators.requires_auth
    def create_milestone(
        self, title, state=None, description=None, due_on=None
    ):
        """Create a milestone for this repository.

        :param str title:
            (required), title of the milestone
        :param str state:
            (optional), state of the milestone, accepted
            values: ('open', 'closed'), default: 'open'
        :param str description:
            (optional), description of the milestone
        :param str due_on:
            (optional), ISO 8601 formatted due date
        :returns:
            the created milestone
        :rtype:
            :class:`~github3.issues.milestone.Milestone`
        """
        url = self._build_url("milestones", base_url=self._api)
        if state not in ("open", "closed"):
            state = None
        data = {
            "title": title,
            "state": state,
            "description": description,
            "due_on": due_on,
        }
        self._remove_none(data)
        json = None
        if data:
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(milestone.Milestone, json)

    @decorators.requires_auth
    def create_project(self, name, body=None):
        """Create a project for this repository.

        :param str name:
            (required), name of the project
        :param str body:
            (optional), body of the project
        :returns:
            the created project
        :rtype:
            :class:`~github3.projects.Project`
        """
        url = self._build_url("projects", base_url=self._api)
        data = {"name": name, "body": body}
        self._remove_none(data)
        json = None
        if data:
            json = self._json(
                self._post(
                    url, data=data, headers=projects.Project.CUSTOM_HEADERS
                ),
                201,
            )
        return self._instance_or_null(projects.Project, json)

    @decorators.requires_auth
    def create_pull(
        self, title, base, head, body=None, maintainer_can_modify=None
    ):
        """Create a pull request of ``head`` onto ``base`` branch in this repo.

        :param str title:
            (required)
        :param str base:
            (required), e.g., 'master'
        :param str head:
            (required), e.g., 'username:branch'
        :param str body:
            (optional), markdown formatted description
        :param bool maintainer_can_modify:
            (optional), Indicates whether a maintainer is allowed to modify the
            pull request or not.
        :returns:
            the created pull request
        :rtype:
            :class:`~github3.pulls.ShortPullRequest`
        """
        data = {"title": title, "body": body, "base": base, "head": head}
        if maintainer_can_modify is not None:
            data["maintainer_can_modify"] = maintainer_can_modify
        return self._create_pull(data)

    @decorators.requires_auth
    def create_pull_from_issue(self, issue, base, head):
        """Create a pull request from issue #``issue``.

        :param int issue:
            (required), issue number
        :param str base:
            (required), e.g., 'master'
        :param str head:
            (required), e.g., 'username:branch'
        :returns:
            the created pull request
        :rtype:
            :class:`~github3.pulls.ShortPullRequest`
        """
        if int(issue) > 0:
            data = {"issue": issue, "base": base, "head": head}
            return self._create_pull(data)
        return None

    @decorators.requires_auth
    def create_ref(self, ref, sha):
        """Create a reference in this repository.

        :param str ref:
            (required), fully qualified name of the reference,
            e.g. ``refs/heads/master``. If it doesn't start with ``refs`` and
            contain at least two slashes, GitHub's API will reject it.
        :param str sha:
            (required), SHA1 value to set the reference to
        :returns:
            the created ref
        :rtype:
            :class:`~github3.git.Reference`
        """
        sha = getattr(sha, "sha", sha)

        json = None
        if ref and ref.startswith("refs") and ref.count("/") >= 2 and sha:
            data = {"ref": ref, "sha": sha}
            url = self._build_url("git", "refs", base_url=self._api)
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(git.Reference, json)

    @decorators.requires_auth
    def create_release(
        self,
        tag_name,
        target_commitish=None,
        name=None,
        body=None,
        draft=False,
        prerelease=False,
    ):
        """Create a release for this repository.

        :param str tag_name:
            (required), name to give to the tag
        :param str target_commitish:
            (optional), vague concept of a target, either a SHA or a branch
            name.
        :param str name:
            (optional), name of the release
        :param str body:
            (optional), description of the release
        :param bool draft:
            (optional), whether this release is a draft or not
        :param bool prerelease:
            (optional), whether this is a prerelease or not
        :returns:
            the created release
        :rtype:
            :class:`~github3.repos.release.Release`
        """
        data = {
            "tag_name": str(tag_name),
            "target_commitish": target_commitish,
            "name": name,
            "body": body,
            "draft": draft,
            "prerelease": prerelease,
        }
        self._remove_none(data)

        url = self._build_url("releases", base_url=self._api)
        json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(release.Release, json)

    @decorators.requires_auth
    def create_status(
        self, sha, state, target_url=None, description=None, context="default"
    ):
        """Create a status object on a commit.

        :param str sha:
            (required), SHA of the commit to create the status on
        :param str state:
            (required), state of the test; only the following
            are accepted: 'pending', 'success', 'error', 'failure'
        :param str target_url:
            (optional), URL to associate with this status.
        :param str description:
            (optional), short description of the status
        :param str context:
            (optional), A string label to differentiate this
            status from the status of other systems
        :returns:
            the created status
        :rtype:
            :class:`~github3.repos.status.Status`
        """
        json = None
        if sha and state:
            data = {
                "state": state,
                "target_url": target_url,
                "description": description,
                "context": context,
            }
            url = self._build_url("statuses", sha, base_url=self._api)
            self._remove_none(data)
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(status.Status, json)

    @decorators.requires_auth
    def create_tag(
        self, tag, message, sha, obj_type, tagger, lightweight=False
    ):
        """Create a tag in this repository.

        By default, this method creates an annotated tag. If you wish to
        create a lightweight tag instead, pass ``lightweight=True``.

        If you are creating an annotated tag, this method makes **2 calls** to
        the API:

        1. Creates the tag object
        2. Creates the reference for the tag

        This behaviour is required by the GitHub API.

        :param str tag:
            (required), name of the tag
        :param str message:
            (required), tag message
        :param str sha:
            (required), SHA of the git object this is tagging
        :param str obj_type:
            (required), type of object being tagged, e.g., 'commit', 'tree',
            'blob'
        :param dict tagger:
            (required), containing the name, email of the
            tagger and optionally the date it was tagged
        :param bool lightweight:
            (optional), if False, create an annotated
            tag, otherwise create a lightweight tag (a Reference).
        :returns:
            if creating a lightweight tag, this will return a
            :class:`~github3.git.Reference`, otherwise it will return a
            :class:`~github3.git.Tag`
        :rtype:
            :class:`~github3.git.Tag` or :class:`~github3.git.Reference`
        """
        if lightweight and tag and sha:
            return self.create_ref("refs/tags/" + tag, sha)

        json = None
        if tag and message and sha and obj_type and len(tagger) >= 2:
            data = {
                "tag": tag,
                "message": message,
                "object": sha,
                "type": obj_type,
                "tagger": tagger,
            }
            url = self._build_url("git", "tags", base_url=self._api)
            json = self._json(self._post(url, data=data), 201)
            if json:
                self.create_ref("refs/tags/" + tag, json.get("sha"))
        return self._instance_or_null(git.Tag, json)

    @decorators.requires_auth
    def create_tree(self, tree, base_tree=None):
        """Create a tree on this repository.

        :param list tree:
            (required), specifies the tree structure.
            Format: [{'path': 'path/file', 'mode':
            'filemode', 'type': 'blob or tree', 'sha': '44bfc6d...'}]
        :param str base_tree:
            (optional), SHA1 of the tree you want to update with new data
        :returns:
            the created tree
        :rtype:
            :class:`~github3.git.Tree`
        """
        json = None
        if tree and isinstance(tree, list):
            data = {"tree": tree}
            if base_tree:
                data["base_tree"] = base_tree
            url = self._build_url("git", "trees", base_url=self._api)
            json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(git.Tree, json)

    @decorators.requires_auth
    def delete(self):
        """Delete this repository.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @decorators.requires_auth
    def delete_key(self, key_id):
        """Delete the key with the specified id from your deploy keys list.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if int(key_id) <= 0:
            return False
        url = self._build_url("keys", str(key_id), base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    @decorators.requires_auth
    def delete_subscription(self):
        """Delete the user's subscription to this repository.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("subscription", base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    def deployment(self, id):
        """Retrieve the deployment identified by ``id``.

        :param int id:
            (required), id for deployments.
        :returns:
            the deployment
        :rtype:
            :class:`~github3.repos.deployment.Deployment`
        """
        json = None
        if int(id) > 0:
            url = self._build_url("deployments", str(id), base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(deployment.Deployment, json)

    def deployments(self, number=-1, etag=None):
        """Iterate over deployments for this repository.

        :param int number:
            (optional), number of deployments to return.
            Default: -1, returns all available deployments
        :param str etag:
            (optional), ETag from a previous request for all deployments
        :returns:
            generator of deployments
        :rtype:
            :class:`~github3.repos.deployment.Deployment`
        """
        url = self._build_url("deployments", base_url=self._api)
        i = self._iter(int(number), url, deployment.Deployment, etag=etag)
        return i

    def directory_contents(self, directory_path, ref=None, return_as=list):
        """Get the contents of each file in ``directory_path``.

        If the path provided is actually a directory, you will receive a
        list back of the form::

            [('filename.md', Contents(...)),
             ('github.py', Contents(...)),
             # ...
             ('fiz.py', Contents(...))]

        You can either then transform it into a dictionary::

            contents = dict(repo.directory_contents('path/to/dir/'))

        Or you can use the ``return_as`` parameter to have it return a
        dictionary for you::

            contents = repo.directory_contents('path/to/dir/', return_as=dict)

        :param str path:
            (required), path to file, e.g.  github3/repos/repo.py
        :param str ref:
            (optional), the string name of a commit/branch/tag.
            Default: master
        :param return_as:
            (optional), how to return the directory's contents.
            Default: :class:`list`
        :returns:
            list of tuples of the filename and the Contents returned
        :rtype:
            [(str, :class:`~github3.repos.contents.Contents`)]
        :raises github3.exceptions.UnprocessableResponseBody:
            When the requested directory is not actually a directory
        """
        url = self._build_url("contents", directory_path, base_url=self._api)
        json = self._json(self._get(url, params={"ref": ref}), 200) or []
        if not (
            isinstance(json, list) and all(isinstance(j, dict) for j in json)
        ):
            raise exceptions.UnprocessableResponseBody(
                "The contents returned do not appear to be a directory. "
                "You may have requested a non-directory.",
                json,
            )
        return return_as(
            (j.get("name"), contents.Contents(j, self)) for j in json
        )

    @decorators.requires_auth
    def edit(
        self,
        name,
        description=None,
        homepage=None,
        private=None,
        has_issues=None,
        has_wiki=None,
        has_downloads=None,
        default_branch=None,
        archived=None,
        allow_merge_commit=None,
        allow_squash_merge=None,
        allow_rebase_merge=None,
        has_projects=None,
    ):
        """Edit this repository.

        :param str name:
            (required), name of the repository
        :param str description:
            (optional), If not ``None``, change the
            description for this repository. API default: ``None`` - leave
            value unchanged.
        :param str homepage:
            (optional), If not ``None``, change the homepage
            for this repository. API default: ``None`` - leave value unchanged.
        :param bool private:
            (optional), If ``True``, make the repository
            private. If ``False``, make the repository public. API default:
            ``None`` - leave value unchanged.
        :param bool has_issues:
            (optional), If ``True``, enable issues for
            this repository. If ``False``, disable issues for this repository.
            API default: ``None`` - leave value unchanged.
        :param bool has_wiki:
            (optional), If ``True``, enable the wiki for
            this repository. If ``False``, disable the wiki for this
            repository. API default: ``None`` - leave value unchanged.
        :param bool has_downloads:
            (optional), If ``True``, enable downloads
            for this repository. If ``False``, disable downloads for this
            repository. API default: ``None`` - leave value unchanged.
        :param str default_branch:
            (optional), If not ``None``, change the
            default branch for this repository. API default: ``None`` - leave
            value unchanged.
        :param bool archived:
            (optional), If not ``None``, toggle the archived
            attribute on the repository to control whether it is archived or
            not.
        :param bool allow_rebase_merge:
            (optional), If not ``None``, change whether the merge strategy
            that allows adding all commits from the head branch onto the base
            branch individually is enabled for this repository.  API default:
            ``None`` - leave value unchanged.
        :param bool allow_squash_merge:
            (optional), If not ``None``, change whether combining all commits
            from the head branch into a single commit in the base branch is
            allowed. API default: ``None`` - leave value unchanged.
        :param bool allow_merge_commit:
            (optional), If not ``None``, change whether adding all commits
            from the head branch to the base branch with a merge commit is
            allowed. API default: ``None`` - leave value unchanged.
        :param bool has_projects:
            (optional), If ``True``, enable projects for this repository.
            If ``False``, disable projects projects for this repository.
            API default: ``None`` - leave value unchanged.
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        edit = {
            "name": name,
            "description": description,
            "homepage": homepage,
            "private": private,
            "has_issues": has_issues,
            "has_wiki": has_wiki,
            "has_downloads": has_downloads,
            "default_branch": default_branch,
            "archived": archived,
            "default_branch": default_branch,
            "allow_merge_commit": allow_merge_commit,
            "allow_squash_merge": allow_squash_merge,
            "allow_rebase_merge": allow_rebase_merge,
            "has_projects": has_projects,
        }
        self._remove_none(edit)
        json = None
        if edit:
            json = self._json(
                self._patch(self._api, data=jsonlib.dumps(edit)), 200
            )
            self._update_attributes(json)
            return True
        return False

    @decorators.requires_auth
    def auto_trigger_checks(self, app_id, enabled=True):
        """Change preferences for automatic creation of check suites.

        .. versionadded:: 1.3.0

        Enable/disable the automatic flow when creating check suites.
        By default, the check suite is automatically created each time code
        is pushed. When the automatic creation is disable they can be created
        manually.

        :param int app_id:
            (required), the id of the GitHub App
        :param bool enabled:
            (optional), enable automatic creation of check suites
            Default: True
        :returns:
            the check suite settings for this repository
        :rtype:
            dict
        """
        url = self._build_url(
            "check-suites", "preferences", base_url=self._api
        )
        headers = {"Accept": "application/vnd.github.antiope-preview+json"}
        json = self._json(
            self._patch(
                url,
                headers=headers,
                data=jsonlib.dumps(
                    {
                        "auto_trigger_checks": [
                            {"app_id": app_id, "setting": enabled}
                        ]
                    }
                ),
            ),
            200,
            include_cache_info=False,
        )
        if json and json.get("repository"):
            del json["repository"]
        return json

    def events(self, number=-1, etag=None):
        """Iterate over events on this repository.

        :param int number:
            (optional), number of events to return. Default: -1
            returns all available events
        :param str etag:
            (optional), ETag from a previous request to the same
            endpoint
        :returns:
            generator of events
        :rtype:
            :class:`~github3.events.Event`
        """
        url = self._build_url("events", base_url=self._api)
        return self._iter(int(number), url, events.Event, etag=etag)

    def file_contents(self, path, ref=None):
        """Get the contents of the file pointed to by ``path``.

        :param str path:
            (required), path to file, e.g.  github3/repos/repo.py
        :param str ref:
            (optional), the string name of a commit/branch/tag.
            Default: master
        :returns:
            the contents of the file requested
        :rtype:
            :class:`~github3.repos.contents.Contents`
        """
        url = self._build_url("contents", path, base_url=self._api)
        json = self._json(self._get(url, params={"ref": ref}), 200)
        return self._instance_or_null(contents.Contents, json)

    def forks(self, sort="", number=-1, etag=None):
        """Iterate over forks of this repository.

        :param str sort:
            (optional), accepted values:
            ('newest', 'oldest', 'stargazers'), API default: 'newest'
        :param int number:
            (optional), number of forks to return. Default: -1
            returns all forks
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of forks of this repository
        :rtype:
            :class:`~github3.repos.repo.ShortRepository`
        """
        url = self._build_url("forks", base_url=self._api)
        params = {}
        if sort in ("newest", "oldest", "stargazers"):
            params = {"sort": sort}
        return self._iter(int(number), url, ShortRepository, params, etag)

    def git_commit(self, sha):
        """Get a single (git) commit.

        :param str sha:
            (required), sha of the commit
        :returns:
            the single commit data from git
        :rtype:
            :class:`~github3.git.Commit`
        """
        json = {}
        if sha:
            url = self._build_url("git", "commits", sha, base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(git.Commit, json)

    @decorators.requires_auth
    def hook(self, hook_id):
        """Get a single hook.

        :param int hook_id:
            (required), id of the hook
        :returns:
            the hook
        :rtype:
            :class:`~github3.repos.hook.Hook`
        """
        json = None
        if int(hook_id) > 0:
            url = self._build_url("hooks", str(hook_id), base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(hook.Hook, json)

    @decorators.requires_auth
    def hooks(self, number=-1, etag=None):
        """Iterate over hooks registered on this repository.

        :param int number:
            (optional), number of hoks to return. Default: -1
            returns all hooks
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of hooks
        :rtype:
            :class:`~github3.repos.hook.Hook`
        """
        url = self._build_url("hooks", base_url=self._api)
        return self._iter(int(number), url, hook.Hook, etag=etag)

    @decorators.requires_auth
    def ignore(self):
        """Ignore notifications from this repository for the user.

        .. versionadded:: 1.0

        This replaces ``Repository#set_subscription``.

        :returns:
            the new repository subscription
        :rtype:
            :class:~github3.notifications.RepositorySubscription`
        """
        url = self._build_url("subscription", base_url=self._api)
        json = self._json(
            self._put(url, data=jsonlib.dumps({"ignored": True})), 200
        )
        return self._instance_or_null(
            notifications.RepositorySubscription, json
        )

    @decorators.requires_auth
    def imported_issue(self, imported_issue_id):
        """Retrieve imported issue specified by imported issue id.

        :param int imported_issue_id:
            (required) id of imported issue
        :returns:
            the imported issue
        :rtype:
            :class:`~github3.repos.issue_import.ImportedIssue`
        """
        url = self._build_url(
            "import/issues", imported_issue_id, base_url=self._api
        )
        data = self._get(
            url, headers=issue_import.ImportedIssue.IMPORT_CUSTOM_HEADERS
        )
        json = self._json(data, 200)
        return self._instance_or_null(issue_import.ImportedIssue, json)

    @decorators.requires_auth
    def imported_issues(self, number=-1, since=None, etag=None):
        """Retrieve the collection of imported issues via the API.

        See also: https://gist.github.com/jonmagic/5282384165e0f86ef105

        :param int number:
            (optional), number of imported issues to return.
            Default: -1 returns all branches
        :param since:
            (optional), Only imported issues after this date will
            be returned. This can be a ``datetime`` instance, ISO8601
            formatted date string, or a string formatted like so:
            ``2016-02-04`` i.e. ``%Y-%m-%d``
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of imported issues
        :rtype:
            :class:`~github3.repos.issue_import.ImportedIssue`
        """
        data = {"since": utils.timestamp_parameter(since)}

        self._remove_none(data)
        url = self._build_url("import/issues", base_url=self._api)

        return self._iter(
            int(number),
            url,
            issue_import.ImportedIssue,
            etag=etag,
            params=data,
            headers=issue_import.ImportedIssue.IMPORT_CUSTOM_HEADERS,
        )

    @decorators.requires_auth
    def import_issue(
        self,
        title,
        body,
        created_at,
        assignee=None,
        milestone=None,
        closed=None,
        labels=None,
        comments=None,
    ):
        """Import an issue into the repository.

        See also: https://gist.github.com/jonmagic/5282384165e0f86ef105

        :param string title:
            (required) Title of issue
        :param string body:
            (required) Body of issue
        :param created_at:
            (required) Creation timestamp
        :type created_at:
            :class:`~datetime.datetime` or str
        :param string assignee:
            (optional) Username to assign issue to
        :param int milestone:
            (optional) Milestone ID
        :param boolean closed:
            (optional) Status of issue is Closed if True
        :param list labels:
            (optional) List of labels containing string names
        :param list comments:
            (optional) List of dictionaries which contain
            created_at and body attributes
        :returns:
            the imported issue
        :rtype:
            :class:`~github3.repos.issue_import.ImportedIssue`
        """
        issue = {
            "issue": {
                "title": title,
                "body": body,
                "created_at": utils.timestamp_parameter(created_at),
                "assignee": assignee,
                "milestone": milestone,
                "closed": closed,
                "labels": labels,
            },
            "comments": comments,
        }

        self._remove_none(issue)
        self._remove_none(issue["issue"])
        url = self._build_url("import/issues", base_url=self._api)

        data = self._post(
            url,
            data=issue,
            headers=issue_import.ImportedIssue.IMPORT_CUSTOM_HEADERS,
        )

        json = self._json(data, 202)
        return self._instance_or_null(issue_import.ImportedIssue, json)

    @decorators.requires_auth
    def invitations(self, number=-1, etag=None):
        """Iterate over the invitations to this repository.

        :param int number:
            (optional), number of invitations to return. Default: -1 returns
            all available invitations
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of repository invitation objects
        :rtype:
            :class:`~github3.repos.invitation.Invitation`
        """
        url = self._build_url("invitations", base_url=self._api)
        return self._iter(int(number), url, invitation.Invitation, etag=etag)

    def is_assignee(self, username):
        """Check if the user can be assigned an issue on this repository.

        :param username:
            name of the user to check
        :type username:
            str or :class:`~github3.users.User`
        :returns:
            bool
        """
        if not username:
            return False
        url = self._build_url("assignees", str(username), base_url=self._api)
        return self._boolean(self._get(url), 204, 404)

    @decorators.requires_auth
    def is_collaborator(self, username):
        """Check to see if ``username`` is a collaborator on this repository.

        :param username:
            (required), login for the user
        :type username:
            str or :class:`~github3.users.User`
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if not username:
            return False
        url = self._build_url(
            "collaborators", str(username), base_url=self._api
        )
        return self._boolean(self._get(url), 204, 404)

    def issue(self, number):
        """Get the issue specified by ``number``.

        :param int number:
            (required), number of the issue on this repository
        :returns:
            the issue
        :rtype:
            :class:`~github3.issues.issue.Issue`
        """
        json = None
        if int(number) > 0:
            url = self._build_url("issues", str(number), base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(issues.Issue, json)

    def issue_events(self, number=-1, etag=None):
        """Iterate over issue events on this repository.

        :param int number:
            (optional), number of events to return. Default: -1
            returns all available events
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of events on issues
        :rtype:
            :class:`~github3.issues.event.IssueEvent`
        """
        url = self._build_url("issues", "events", base_url=self._api)
        return self._iter(
            int(number), url, ievent.RepositoryIssueEvent, etag=etag
        )

    def issues(
        self,
        milestone=None,
        state=None,
        assignee=None,
        mentioned=None,
        labels=None,
        sort=None,
        direction=None,
        since=None,
        number=-1,
        etag=None,
    ):
        """Iterate over issues on this repo based upon parameters passed.

        .. versionchanged:: 0.9.0

            The ``state`` parameter now accepts 'all' in addition to 'open'
            and 'closed'.

        :param int milestone:
            (optional), 'none', or '*'
        :param str state:
            (optional), accepted values: ('all', 'open', 'closed')
        :param str assignee:
            (optional), 'none', '*', or login name
        :param str mentioned:
            (optional), user's login name
        :param str labels:
            (optional), comma-separated list of labels, e.g.  'bug,ui,@high'
        :param sort:
            (optional), accepted values:
            ('created', 'updated', 'comments', 'created')
        :param str direction:
            (optional), accepted values: ('asc', 'desc')
        :param since:
            (optional), Only issues after this date will
            be returned. This can be a ``datetime`` or an ``ISO8601`` formatted
            date string, e.g., 2012-05-20T23:10:27Z
        :type since:
            :class:`~datetime.datetime` or str
        :param int number:
            (optional), Number of issues to return.
            By default all issues are returned
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of issues
        :rtype:
            :class:`~github3.issues.ShortIssue`
        """
        url = self._build_url("issues", base_url=self._api)

        params = repo_issue_params(
            milestone,
            state,
            assignee,
            mentioned,
            labels,
            sort,
            direction,
            since,
        )

        return self._iter(int(number), url, issues.ShortIssue, params, etag)

    @decorators.requires_auth
    def key(self, id_num):
        """Get the specified deploy key.

        :param int id_num:
            (required), id of the key
        :returns:
            the deploy key
        :rtype:
            :class:`~github3.users.Key`
        """
        json = None
        if int(id_num) > 0:
            url = self._build_url("keys", str(id_num), base_url=self._api)
            json = self._json(self._get(url), 200)
        return users.Key(json, self) if json else None

    @decorators.requires_auth
    def keys(self, number=-1, etag=None):
        """Iterate over deploy keys on this repository.

        :param int number:
            (optional), number of keys to return. Default: -1
            returns all available keys
        :param str etag:
            (optional), ETag from a previous request to the same
            endpoint
        :returns:
            generator of keys
        :rtype:
            :class:`~github3.users.Key`
        """
        url = self._build_url("keys", base_url=self._api)
        return self._iter(int(number), url, users.Key, etag=etag)

    def label(self, name):
        """Get the label specified by ``name``.

        :param str name:
            (required), name of the label
        :returns:
            the label
        :rtype:
            :class:`~github3.issues.label.Label`
        """
        json = None
        if name:
            url = self._build_url("labels", name, base_url=self._api)
            resp = self._get(
                url, headers=label.Label.SYMMETRA_PREVIEW_HEADERS
            )
            json = self._json(resp, 200)
        return self._instance_or_null(label.Label, json)

    def labels(self, number=-1, etag=None):
        """Iterate over labels on this repository.

        :param int number:
            (optional), number of labels to return. Default: -1
            returns all available labels
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of labels
        :rtype:
            :class:`~github3.issues.label.Label`
        """
        url = self._build_url("labels", base_url=self._api)
        return self._iter(
            int(number),
            url,
            label.Label,
            etag=etag,
            headers=label.Label.SYMMETRA_PREVIEW_HEADERS,
        )

    def languages(self, number=-1, etag=None):
        """Iterate over the programming languages used in the repository.

        :param int number:
            (optional), number of languages to return. Default:
            -1 returns all used languages
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of tuples
        :rtype:
            tuple
        """
        url = self._build_url("languages", base_url=self._api)
        return self._iter(int(number), url, tuple, etag=etag)

    @decorators.requires_auth
    def latest_pages_build(self):
        """Get the build information for the most recent Pages build.

        :returns:
            the information for the most recent build
        :rtype:
            :class:`~github3.repos.pages.PagesBuild`
        """
        url = self._build_url("pages", "builds", "latest", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(pages.PagesBuild, json)

    def latest_release(self):
        """Get the latest release.

        Draft releases and prereleases are not returned by this endpoint.

        :returns:
            the release
        :rtype:
            :class:`~github3.repos.release.Release`
        """
        url = self._build_url("releases", "latest", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(release.Release, json)

    def license(self):
        """Get the contents of a license for the repo.

        :returns:
            the license
        :rtype:
            :class:`~github3.licenses.RepositoryLicense`
        """
        url = self._build_url("license", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(licenses.RepositoryLicense, json)

    @decorators.requires_auth
    def mark_notifications(self, last_read=""):
        """Mark all notifications in this repository as read.

        :param str last_read:
            (optional), Describes the last point that
            notifications were checked. Anything updated since this time will
            not be updated. Default: Now. Expected in ISO 8601 format:
            ``YYYY-MM-DDTHH:MM:SSZ``. Example: "2012-10-09T23:39:01Z".
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("notifications", base_url=self._api)
        mark = {"read": True}
        if last_read:
            mark["last_read_at"] = last_read
        return self._boolean(
            self._put(url, data=jsonlib.dumps(mark)), 205, 404
        )

    @decorators.requires_auth
    def merge(self, base, head, message=""):
        """Perform a merge from ``head`` into ``base``.

        :param str base:
            (required), where you're merging into
        :param str head:
            (required), where you're merging from
        :param str message:
            (optional), message to be used for the commit
        :returns:
            the commit resulting from the merge
        :rtype:
            :class:`~github3.repos.commit.RepoCommit`
        """
        url = self._build_url("merges", base_url=self._api)
        data = {"base": base, "head": head}
        if message:
            data["commit_message"] = message
        json = self._json(self._post(url, data=data), 201)
        return self._instance_or_null(commit.ShortCommit, json)

    def milestone(self, number):
        """Get the milestone indicated by ``number``.

        :param int number:
            (required), unique id number of the milestone
        :returns:
            the milestone
        :rtype:
            :class:`~github3.issues.milestone.Milestone`
        """
        json = None
        if int(number) > 0:
            url = self._build_url(
                "milestones", str(number), base_url=self._api
            )
            json = self._json(self._get(url), 200)
        return self._instance_or_null(milestone.Milestone, json)

    def milestones(
        self, state=None, sort=None, direction=None, number=-1, etag=None
    ):
        """Iterate over the milestones on this repository.

        :param str state:
            (optional), state of the milestones, accepted
            values: ('open', 'closed')
        :param str sort:
            (optional), how to sort the milestones, accepted
            values: ('due_date', 'completeness')
        :param str direction:
            (optional), direction to sort the milestones,
            accepted values: ('asc', 'desc')
        :param int number:
            (optional), number of milestones to return.
            Default: -1 returns all milestones
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of milestones
        :rtype:
            :class:`~github3.issues.milestone.Milestone`
        """
        url = self._build_url("milestones", base_url=self._api)
        accepted = {
            "state": ("open", "closed", "all"),
            "sort": ("due_date", "completeness"),
            "direction": ("asc", "desc"),
        }
        params = {"state": state, "sort": sort, "direction": direction}
        for k, v in list(params.items()):
            if not (v and (v in accepted[k])):  # e.g., '' or None
                del params[k]
        if not params:
            params = None
        return self._iter(int(number), url, milestone.Milestone, params, etag)

    def network_events(self, number=-1, etag=None):
        r"""Iterate over events on a network of repositories.

        :param int number:
            (optional), number of events to return. Default: -1
            returns all available events
        :param str etag:
            (optional), ETag from a previous request to the same
            endpoint
        :returns:
            generator of events
        :rtype:
            :class:`~github3.events.Event`
        """
        base = self._api.replace("repos", "networks", 1)
        url = self._build_url("events", base_url=base)
        return self._iter(int(number), url, events.Event, etag)

    @decorators.requires_auth
    def notifications(
        self, all=False, participating=False, since=None, number=-1, etag=None
    ):
        """Iterate over the notifications for this repository.

        :param bool all:
            (optional), show all notifications, including ones
            marked as read
        :param bool participating:
            (optional), show only the notifications the
            user is participating in directly
        :param since:
            (optional), filters out any notifications updated
            before the given time. This can be a `datetime` or an `ISO8601`
            formatted date string, e.g., 2012-05-20T23:10:27Z
        :type since:
            :class:`~datetime.datetime` or str
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of notification threads
        :rtype:
            :class:`~github3.notifications.Thread`
        """
        url = self._build_url("notifications", base_url=self._api)
        params = {
            "all": str(all).lower(),
            "participating": str(participating).lower(),
            "since": utils.timestamp_parameter(since),
        }
        self._remove_none(params)
        return self._iter(
            int(number), url, notifications.Thread, params, etag
        )

    @decorators.requires_auth
    def pages(self):
        """Get information about this repository's pages site.

        :returns:
            information about this repository's GitHub pages site
        :rtype:
            :class:`~github3.repos.pages.PagesInfo`
        """
        url = self._build_url("pages", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(pages.PagesInfo, json)

    @decorators.requires_auth
    def pages_builds(self, number=-1, etag=None):
        """Iterate over pages builds of this repository.

        :param int number:
            (optional) the number of builds to return
        :param str etag:
            (optional), ETag value from a previous request
        :returns:
            generator of builds
        :rtype:
            :class:`~github3.repos.pages.PagesBuild`
        """
        url = self._build_url("pages", "builds", base_url=self._api)
        return self._iter(int(number), url, pages.PagesBuild, etag=etag)

    def project(self, id, etag=None):
        """Return the organization project with the given ID.

        :param int id:
            (required), ID number of the project
        :returns:
            the project
        :rtype:
            :class:`~github3.projects.Project`
        """
        url = self._build_url("projects", id)
        json = self._json(
            self._get(url, headers=projects.Project.CUSTOM_HEADERS), 200
        )
        return self._instance_or_null(projects.Project, json)

    def projects(self, number=-1, etag=None):
        """Iterate over projects for this organization.

        :param int number:
            (optional), number of members to return. Default:
            -1 will return all available.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of projects
        :rtype:
            :class:`~github3.projects.Project`
        """
        url = self._build_url("projects", base_url=self._api)
        return self._iter(
            int(number),
            url,
            projects.Project,
            etag=etag,
            headers=projects.Project.CUSTOM_HEADERS,
        )

    def pull_request(self, number):
        """Get the pull request indicated by ``number``.

        :param int number:
            (required), number of the pull request.
        :returns:
            the pull request
        :rtype:
            :class:`~github3.pulls.PullRequest`
        """
        json = None
        if int(number) > 0:
            url = self._build_url("pulls", str(number), base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(pulls.PullRequest, json)

    def pull_requests(
        self,
        state=None,
        head=None,
        base=None,
        sort="created",
        direction="desc",
        number=-1,
        etag=None,
    ):
        """List pull requests on repository.

        .. versionchanged:: 0.9.0

            - The ``state`` parameter now accepts 'all' in addition to 'open'
              and 'closed'.

            - The ``sort`` parameter was added.

            - The ``direction`` parameter was added.

        :param str state:
            (optional), accepted values: ('all', 'open', 'closed')
        :param str head:
            (optional), filters pulls by head user and branch
            name in the format ``user:ref-name``, e.g., ``seveas:debian``
        :param str base:
            (optional), filter pulls by base branch name.
            Example: ``develop``.
        :param str sort:
            (optional), Sort pull requests by ``created``,
            ``updated``, ``popularity``, ``long-running``. Default: 'created'
        :param str direction:
            (optional), Choose the direction to list pull
            requests. Accepted values: ('desc', 'asc'). Default: 'desc'
        :param int number:
            (optional), number of pulls to return. Default: -1
            returns all available pull requests
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of pull requests
        :rtype:
            :class:`~github3.pulls.ShortPullRequest`
        """
        url = self._build_url("pulls", base_url=self._api)
        params = {}

        if state:
            state = state.lower()
            if state in ("all", "open", "closed"):
                params["state"] = state

        params.update(head=head, base=base, sort=sort, direction=direction)
        self._remove_none(params)
        return self._iter(
            int(number), url, pulls.ShortPullRequest, params, etag
        )

    def readme(self):
        """Get the README for this repository.

        :returns:
            this repository's readme
        :rtype:
            :class:`Contents <github3.repos.contents.Contents>`
        """
        url = self._build_url("readme", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(contents.Contents, json)

    def ref(self, ref):
        """Get a reference pointed to by ``ref``.

        The most common will be branches and tags. For a branch, you must
        specify 'heads/branchname' and for a tag, 'tags/tagname'. Essentially,
        the system should return any reference you provide it in the namespace,
        including notes and stashes (provided they exist on the server).

        :param str ref:
            (required)
        :returns:
            the reference
        :rtype:
            :class:`~github3.git.Reference`
        """
        json = None
        if ref:
            url = self._build_url("git", "ref", ref, base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(git.Reference, json)

    def refs(self, subspace="", number=-1, etag=None):
        """Iterate over references for this repository.

        :param str subspace:
            (optional), e.g. 'tags', 'stashes', 'notes'
        :param int number:
            (optional), number of refs to return. Default: -1
            returns all available refs
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of references
        :rtype:
            :class:`~github3.git.Reference`
        """
        if subspace:
            args = ("git", "refs", subspace)
        else:
            args = ("git", "refs")
        url = self._build_url(*args, base_url=self._api)
        return self._iter(int(number), url, git.Reference, etag=etag)

    def release(self, id):
        """Get a single release.

        :param int id:
            (required), id of release
        :returns:
            the release
        :rtype:
            :class:`~github3.repos.release.Release`
        """
        json = None
        if int(id) > 0:
            url = self._build_url("releases", str(id), base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(release.Release, json)

    def release_from_tag(self, tag_name):
        """Get a release by tag name.

        release_from_tag() returns a release with specified tag
        while release() returns a release with specified release id

        :param str tag_name:
            (required) name of tag
        :returns:
            the release
        :rtype:
            :class:`~github3.repos.release.Release`
        """
        url = self._build_url(
            "releases", "tags", tag_name, base_url=self._api
        )
        json = self._json(self._get(url), 200)
        return self._instance_or_null(release.Release, json)

    def releases(self, number=-1, etag=None):
        """Iterate over releases for this repository.

        :param int number:
            (optional), number of refs to return. Default: -1
            returns all available refs
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of releases
        :rtype:
            :class:`~github3.repos.release.Release`
        """
        url = self._build_url("releases", base_url=self._api)
        return self._iter(int(number), url, release.Release, etag=etag)

    @decorators.requires_auth
    def remove_collaborator(self, username):
        """Remove collaborator ``username`` from the repository.

        :param username:
            (required), login name of the collaborator
        :type username:
            str or :class:`~github3.users.User`
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if not username:
            return False

        url = self._build_url(
            "collaborators", str(username), base_url=self._api
        )
        return self._boolean(self._delete(url), 204, 404)

    @decorators.requires_auth
    def replace_topics(self, new_topics):
        """Replace the repository topics with ``new_topics``.

        :param topics:
            (required), new topics of the repository
        :type topics:
            list
        :returns:
            new topics of the repository
        :rtype:
            :class:`~github3.repos.topics.Topics`
        """
        url = self._build_url("topics", base_url=self._api)
        data = {"names": new_topics}
        json = self._json(
            self._put(
                url, data=jsonlib.dumps(data), headers=self.PREVIEW_HEADERS
            ),
            200,
        )
        return self._instance_or_null(topics.Topics, json)

    def stargazers(self, number=-1, etag=None):
        """List users who have starred this repository.

        :param int number:
            (optional), number of stargazers to return.
            Default: -1 returns all subscribers available
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of users
        :rtype:
            :class:`~github3.users.Stargazer`
        """
        url = self._build_url("stargazers", base_url=self._api)
        return self._iter(
            int(number),
            url,
            users.Stargazer,
            etag=etag,
            headers={"Accept": "application/vnd.github.v3.star+json"},
        )

    def statuses(self, sha, number=-1, etag=None):
        """Iterate over the statuses for a specific SHA.

        .. warning::

            Deprecated in v1.0. Also deprecated upstream
            https://developer.github.com/v3/repos/statuses/

        :param str sha:
            SHA of the commit to list the statuses of
        :param int number:
            (optional), return up to number statuses. Default:
            -1 returns all available statuses.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of statuses
        :rtype:
            :class:`~github3.repos.status.Status`
        """
        url = ""
        if sha:
            url = self._build_url("statuses", sha, base_url=self._api)
        return self._iter(int(number), url, status.Status, etag=etag)

    @decorators.requires_auth
    def subscribe(self):
        """Subscribe the user to this repository's notifications.

        .. versionadded:: 1.0

        This replaces ``Repository#set_subscription``

        :returns:
            the new repository subscription
        :rtype:
            :class:`~github3.notifications.RepositorySubscription`
        """
        url = self._build_url("subscription", base_url=self._api)
        json = self._json(
            self._put(url, data=jsonlib.dumps({"subscribed": True})), 200
        )
        return self._instance_or_null(
            notifications.RepositorySubscription, json
        )

    def subscribers(self, number=-1, etag=None):
        """Iterate over users subscribed to this repository.

        :param int number:
            (optional), number of subscribers to return.
            Default: -1 returns all subscribers available
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of users subscribed to this repository
        :rtype:
            :class:`~github3.users.ShortUser`
        """
        url = self._build_url("subscribers", base_url=self._api)
        return self._iter(int(number), url, users.ShortUser, etag=etag)

    @decorators.requires_auth
    def subscription(self):
        """Return subscription for this Repository.

        :returns:
            the user's subscription to this repository
        :rtype:
            :class:`~github3.notifications.RepositorySubscription`
        """
        url = self._build_url("subscription", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(
            notifications.RepositorySubscription, json
        )

    def tag(self, sha):
        """Get an annotated tag.

        http://learn.github.com/p/tagging.html

        :param str sha:
            (required), sha of the object for this tag
        :returns:
            the annotated tag
        :rtype:
            :class:`~github3.git.Tag`
        """
        json = None
        if sha:
            url = self._build_url("git", "tags", sha, base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(git.Tag, json)

    def tags(self, number=-1, etag=None):
        """Iterate over tags on this repository.

        :param int number:
            (optional), return up to at most number tags.
            Default: -1 returns all available tags.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of tags with GitHub repository specific information
        :rtype:
            :class:`~github3.repos.tag.RepoTag`
        """
        url = self._build_url("tags", base_url=self._api)
        return self._iter(int(number), url, tag.RepoTag, etag=etag)

    @decorators.requires_auth
    def teams(self, number=-1, etag=None):
        """Iterate over teams with access to this repository.

        :param int number:
            (optional), return up to number Teams. Default: -1
            returns all Teams.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of teams
        :rtype:
            :class:`~github3.orgs.Team`
        """
        from .. import orgs

        url = self._build_url("teams", base_url=self._api)
        return self._iter(int(number), url, orgs.ShortTeam, etag=etag)

    def topics(self):
        """Get the topics of this repository.

        :returns:
            this repository's topics
        :rtype:
            :class:`~github3.repos.topics.Topics`
        """
        url = self._build_url("topics", base_url=self._api)
        json = self._json(self._get(url, headers=self.PREVIEW_HEADERS), 200)
        return self._instance_or_null(topics.Topics, json)

    def tree(self, sha, recursive=False):
        """Get a tree.

        :param str sha:
            (required), sha of the object for this tree
        :param bool recursive:
            (optional), whether to fetch the tree recursively
        :returns:
            the tree
        :rtype:
            :class:`~github3.git.Tree`
        """
        json = None
        if sha:
            url = self._build_url("git", "trees", sha, base_url=self._api)
            params = {"recursive": 1} if recursive else None
            json = self._json(self._get(url, params=params), 200)
        return self._instance_or_null(git.Tree, json)

    @decorators.requires_auth
    def unignore(self):
        """Unignore notifications from this repository for the user.

        .. versionadded:: 1.3

        This replaces ``Repository#set_subscription``.

        :returns:
            the new repository subscription
        :rtype:
            :class:~github3.notifications.RepositorySubscription`
        """
        url = self._build_url("subscription", base_url=self._api)
        json = self._json(
            self._put(url, data=jsonlib.dumps({"ignored": False})), 200
        )
        return self._instance_or_null(
            notifications.RepositorySubscription, json
        )

    @decorators.requires_auth
    def unsubscribe(self):
        """Unsubscribe the user to this repository's notifications.

        .. versionadded:: 1.3

        This replaces ``Repository#set_subscription``

        :returns:
            the new repository subscription
        :rtype:
            :class:`~github3.notifications.RepositorySubscription`
        """
        url = self._build_url("subscription", base_url=self._api)
        json = self._json(
            self._put(url, data=jsonlib.dumps({"subscribed": False})), 200
        )
        return self._instance_or_null(
            notifications.RepositorySubscription, json
        )

    def weekly_commit_count(self):
        """Retrieve the total commit counts.

        .. note::

            All statistics methods may return a 202. If github3.py
            receives a 202 in this case, it will return an emtpy dictionary.
            You should give the API a moment to compose the data and then re
            -request it via this method.

        ..versionadded:: 0.7

        The dictionary returned has two entries: ``all`` and ``owner``. Each
        has a fifty-two element long list of commit counts. (Note: ``all``
        includes the owner.) ``d['all'][0]`` will be the oldest week,
        ``d['all'][51]`` will be the most recent.

        :returns:
            the commit count as a dictionary
        :rtype:
            dict
        """
        url = self._build_url("stats", "participation", base_url=self._api)
        resp = self._get(url)
        if resp and resp.status_code == 202:
            return {}
        json = self._json(resp, 200)
        if json and json.get("ETag"):
            del json["ETag"]
        if json and json.get("Last-Modified"):
            del json["Last-Modified"]
        return json


class Repository(_Repository):
    """This organizes the full representation of a single Repository.

    The full representation of a Repository is not returned in collections but
    instead in individual requests, e.g.,
    :meth:`~github3.github.GitHub.repository`.

    This object has all the same attributes as
    :class:`~github3.repos.repo.ShortRepository` as well as:

    .. attribute:: allow_merge_commit

        .. note::

            This attribute is not guaranteed to be present.

        Whether the repository allows creating a merge commit when merging
        when a pull request.

    .. attribute:: allow_rebase_merge

        .. note::

            This attribute is not guaranteed to be present.

        Whether the repository allows rebasing when merging a pull request.

    .. attribute:: allow_squash_merge

        .. note::

            This attribute is not guaranteed to be present.

        Whether the repository allows squashing commits when merging a pull
        request.

    .. attribute:: archived

        A boolean attribute that describes whether the current repository has
        been archived or not.

    .. attribute:: clone_url

        This is the URL that can be used to clone the repository via HTTPS,
        e.g., ``https://github.com/sigmavirus24/github3.py.git``.

    .. attribute:: created_at

        A parsed :class:`~datetime.datetime` object representing the date the
        repository was created.

    .. attribute:: default_branch

        This is the default branch of the repository as configured by its
        administrator(s).

    .. attribute:: forks_count

        This is the number of forks of the repository.

    .. attribute:: git_url

        This is the URL that can be used to clone the repository via the Git
        protocol, e.g., ``git://github.com/sigmavirus24/github3.py``.

    .. attribute:: has_downloads

        This is a boolean attribute that conveys whether or not the repository
        has downloads.

    .. attribute:: has_issues

        This is a boolean attribute that conveys whether or not the repository
        has issues.

    .. attribute:: has_pages

        This is a boolean attribute that conveys whether or not the repository
        has pages.

    .. attribute:: has_wiki

        This is a boolean attribute that conveys whether or not the repository
        has a wiki.

    .. attribute:: homepage

        This is the administrator set homepage URL for the project. This may
        not be provided.

    .. attribute:: language

        This is the language GitHub has detected for the repository.

    .. attribute:: original_license

        .. note::

            When used with a Github Enterprise instance <= 2.12.7, this
            attribute will not be returned. To handle these situations
            sensitively, the attribute will be set to ``None``.
            Repositories may still have a license associated with them
            in these cases.

        This is the :class:`~github3.license.ShortLicense` returned as part of
        the repository. To retrieve the most recent license, see the
        :meth:`~github3.repos.repo.Repository.license` method.

    .. attribute:: mirror_url

        The URL that GitHub is mirroring the repository from.

    .. attribute:: network_count

        The size of the repository's "network".

    .. attribute:: open_issues_count

        The number of issues currently open on the repository.

    .. attribute:: parent

        A representation of the parent repository as
        :class:`~github3.repos.repo.ShortRepository`. If this Repository has
        no parent then this will be ``None``.

    .. attribute:: pushed_at

        A parsed :class:`~datetime.datetime` object representing the date a
        push was last made to the repository.

    .. attribute:: size

        The size of the repository.

    .. attribute:: source

        A representation of the source repository as
        :class:`~github3.repos.repo.ShortRepository`. If this Repository has
        no source then this will be ``None``.

    .. attribute:: ssh_url

        This is the URL that can be used to clone the repository via the SSH
        protocol, e.g., ``ssh@github.com:sigmavirus24/github3.py.git``.

    .. attribute:: stargazers_count

        The number of people who have starred this repository.

    .. attribute:: subscribers_count

        The number of people watching (or who have subscribed to notifications
        about) this repository.

    .. attribute:: svn_url

        This is the URL that can be used to clone the repository via SVN,
        e.g., ``ssh@github.com:sigmavirus24/github3.py.git``.

    .. attribute:: updated_at

        A parsed :class:`~datetime.datetime` object representing the date a
        the repository was last updated by its administrator(s).

    .. attribute:: watchers_count

        The number of people watching this repository.


    See also: http://developer.github.com/v3/repos/
    """

    class_name = "Repository"

    def _update_attributes(self, repo):
        super()._update_attributes(repo)
        self.allow_merge_commit = repo.get("allow_merge_commit")
        self.allow_rebase_merge = repo.get("allow_rebase_merge")
        self.allow_squash_merge = repo.get("allow_squash_merge")
        self.archived = repo["archived"]
        self.clone_url = repo["clone_url"]
        self.created_at = self._strptime(repo["created_at"])
        self.default_branch = repo["default_branch"]
        self.forks_count = repo["forks_count"]
        self.fork_count = self.forks_count
        self.git_url = repo["git_url"]
        self.has_downloads = repo["has_downloads"]
        self.has_issues = repo["has_issues"]
        self.has_pages = repo["has_pages"]
        self.has_projects = repo["has_projects"]
        self.has_wiki = repo["has_wiki"]
        self.homepage = repo["homepage"]
        self.language = repo["language"]
        self.original_license = repo.get("license")
        if self.original_license is not None:
            self.original_license = licenses.ShortLicense(
                self.original_license, self
            )
        self.mirror_url = repo["mirror_url"]
        self.network_count = repo["network_count"]
        self.open_issues_count = repo["open_issues_count"]
        self.parent = repo.get("parent")
        if self.parent is not None:
            self.parent = ShortRepository(self.parent, self)
        self.pushed_at = self._strptime(repo["pushed_at"])
        self.size = repo["size"]
        self.source = repo.get("source")
        if self.source is not None:
            self.source = ShortRepository(self.source, self)
        self.ssh_url = repo["ssh_url"]
        self.stargazers_count = repo["stargazers_count"]
        self.subscribers_count = repo["subscribers_count"]
        self.svn_url = repo["svn_url"]
        self.updated_at = self._strptime(repo["updated_at"])
        self.watchers_count = self.watchers = repo["watchers_count"]


class ShortRepository(_Repository):
    """This represents a Repository object returned in collections.

    GitHub's API returns different amounts of information about repositories
    based upon how that information is retrieved. This object exists to
    represent the full amount of information returned for a specific
    repository. For example, you would receive this class when calling
    :meth:`~github3.github.GitHub.repository`. To provide a clear distinction
    between the types of repositories, github3.py uses different classes with
    different sets of attributes.

    This object only has the following attributes:

    .. attribute:: url

        The GitHub API URL for this repository, e.g.,
        ``https://api.github.com/repos/sigmavirus24/github3.py``.

    .. attribute:: archive_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``archive_urlt.variables`` for the list of variables that can
        be passed to ``archive_urlt.expand()``.

    .. attribute:: assignees_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``assignees_urlt.variables`` for the list of variables that can
        be passed to ``assignees_urlt.expand()``.

    .. attribute:: blobs_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``blobs_urlt.variables`` for the list of variables that can
        be passed to ``blobs_urlt.expand()``.

    .. attribute:: branches_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``branches_urlt.variables`` for the list of variables that can
        be passed to ``branches_urlt.expand()``.

    .. attribute:: collaborators_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``collaborators_urlt.variables`` for the list of variables that can
        be passed to ``collaborators_urlt.expand()``.

    .. attribute:: comments_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``comments_urlt.variables`` for the list of variables that can
        be passed to ``comments_urlt.expand()``.

    .. attribute:: commits_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``commits_urlt.variables`` for the list of variables that can
        be passed to ``commits_urlt.expand()``.

    .. attribute:: compare_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``compare_urlt.variables`` for the list of variables that can
        be passed to ``compare_urlt.expand()``.

    .. attribute:: contents_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``contents_urlt.variables`` for the list of variables that can
        be passed to ``contents_urlt.expand()``.

    .. attribute:: contributors_url

        The URL to retrieve this repository's list of contributors.

    .. attribute:: deployments_url

        The URL to retrieve this repository's list of deployments.

    .. attribute:: description

        The administrator created description of the repository.

    .. attribute:: downloads_url

        The URL to retrieve this repository's list of downloads.

    .. attribute:: events_url

        The URL to retrieve this repository's list of events.

    .. attribute:: fork

        Whether or not this repository is a fork of another.

    .. attribute:: forks_url

        The URL to retrieve this repository's list of forks.

    .. attribute:: full_name

        The full name of this repository, e.g., ``sigmavirus24/github3.py``.

    .. attribute:: git_commits_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``git_commits_urlt.variables`` for the list of variables that can
        be passed to ``git_commits_urlt.expand()``.

    .. attribute:: git_refs_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``git_refs_urlt.variables`` for the list of variables that can
        be passed to ``git_refs_urlt.expand()``.

    .. attribute:: git_tags_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``git_tags_urlt.variables`` for the list of variables that can
        be passed to ``git_tags_urlt.expand()``.

    .. attribute:: hooks_url

        The URL to retrieve this repository's list of hooks.

    .. attribute:: html_url

        The HTML URL of this repository, e.g.,
        ``https://github.com/sigmavirus24/github3.py``.

    .. attribute:: id

        The unique GitHub assigned numerical id of this repository.

    .. attribute:: issue_comment_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``issue_comment_urlt.variables`` for the list of variables that can
        be passed to ``issue_comment_urlt.expand()``.

    .. attribute:: issue_events_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``issue_events_urlt.variables`` for the list of variables that can
        be passed to ``issue_events_urlt.expand()``.

    .. attribute:: issues_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``issues_urlt.variables`` for the list of variables that can
        be passed to ``issues_urlt.expand()``.

    .. attribute:: keys_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``keys_urlt.variables`` for the list of variables that can
        be passed to ``keys_urlt.expand()``.

    .. attribute:: labels_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``labels_urlt.variables`` for the list of variables that can
        be passed to ``labels_urlt.expand()``.

    .. attribute:: languages_url

        The URL to retrieve this repository's list of languages.

    .. attribute:: merges_url

        The URL to retrieve this repository's list of merges.

    .. attribute:: milestones_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``milestones_urlt.variables`` for the list of variables that can
        be passed to ``milestones_urlt.expand()``.

    .. attribute:: name

        The name of the repository, e.g., ``github3.py``.

    .. attribute:: notifications_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``notifications_urlt.variables`` for the list of variables that can
        be passed to ``notifications_urlt.expand()``.

    .. attribute:: owner

        A :class:`~github3.users.ShortUser` object representing the owner of
        the repository.

    .. attribute:: private

        Whether the repository is private or public.

    .. attribute:: pulls_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``pulls_urlt.variables`` for the list of variables that can
        be passed to ``pulls_urlt.expand()``.

    .. attribute:: releases_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``releases_urlt.variables`` for the list of variables that can
        be passed to ``releases_urlt.expand()``.

    .. attribute:: stargazers_url

        The URL to retrieve this repository's list of stargazers.

    .. attribute:: statuses_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``statuses_urlt.variables`` for the list of variables that can
        be passed to ``statuses_urlt.expand()``.

    .. attribute:: subscribers_url

        The URL to retrieve this repository's list of subscribers.

    .. attribute:: subscription_url

        The URL to modify subscription to this repository.

    .. attribute:: tags_url

        The URL to retrieve this repository's list of tags.

    .. attribute:: teams_url

        The URL to retrieve this repository's list of teams.

    .. attribute:: trees_urlt

        The :class:`~uritemplate.URITemplate` object representing the
        URI template returned by GitHub's API. Check
        ``trees_urlt.variables`` for the list of variables that can
        be passed to ``trees_urlt.expand()``.

    .. versionadded:: 1.0.0
    """

    class_name = "ShortRepository"
    _refresh_to = Repository


class StarredRepository(models.GitHubCore):
    """This object represents the data returned about a user's starred repos.

    GitHub used to send back the ``starred_at`` attribute on Repositories but
    then changed the structure to a new object that separates that from the
    Repository representation. This consolidates the two.

    Attributes:

    .. attribute:: starred_at

        A parsed :class:`~datetime.datetime` object representing the date a
        the repository was starred.

    .. attribute:: repository

        The :class:`Repository` that was starred by the user.

    See also:
    https://developer.github.com/v3/activity/starring/#list-repositories-being-starred

    """

    def _update_attributes(self, starred_repository):
        self.starred_at = self._strptime(starred_repository["starred_at"])
        self.repository = ShortRepository(starred_repository["repo"], self)
        self.repo = self.repository

    def _repr(self):
        return f"<StarredRepository [{self.repository!r}]>"


def repo_issue_params(
    milestone=None,
    state=None,
    assignee=None,
    mentioned=None,
    labels=None,
    sort=None,
    direction=None,
    since=None,
    number=-1,
    etag=None,
):
    """Validate and filter issue method parameters in one place."""
    params = {"assignee": assignee, "mentioned": mentioned}
    if milestone in ("*", "none") or isinstance(milestone, int):
        params["milestone"] = milestone
    Repository._remove_none(params)
    params.update(
        issues.issue_params(None, state, labels, sort, direction, since)
    )
    return params
