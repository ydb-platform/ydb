"""Implementation of a branch on a repository."""
import typing as t

from . import commit
from .. import decorators
from .. import models

if t.TYPE_CHECKING:
    from .. import apps as tapps
    from .. import users as tusers
    from . import orgs


class _Branch(models.GitHubCore):
    """A representation of a branch on a repository.

    See also https://developer.github.com/v3/repos/branches/

    This object has the following attributes:
    """

    # The Accept header will likely be removable once the feature is out of
    # preview mode. See: http://git.io/v4O1e
    PREVIEW_HEADERS = {"Accept": "application/vnd.github.loki-preview+json"}

    class_name = "Repository Branch"

    def _update_attributes(self, branch):
        self.commit = commit.MiniCommit(branch["commit"], self)
        self.name = branch["name"]
        base = self.commit.url.split("/commit", 1)[0]
        self._api = self._build_url("branches", self.name, base_url=base)

    def _repr(self):
        return f"<{self.class_name} [{self.name}]>"

    def latest_sha(self, differs_from=""):
        """Check if SHA-1 is the same as the remote branch.

        See: https://git.io/vaqIw

        :param str differs_from:
            (optional), sha to compare against
        :returns:
            string of the SHA or None
        :rtype:
            str on Python 3
        """
        # If-None-Match returns 200 instead of 304 value does not have quotes
        headers = {
            "Accept": "application/vnd.github.v3.sha",
            "If-None-Match": f'"{differs_from}"',
        }
        base = self._api.split("/branches", 1)[0]
        url = self._build_url("commits", self.name, base_url=base)
        resp = self._get(url, headers=headers)
        if self._boolean(resp, 200, 304):
            return resp.text
        return None

    @decorators.requires_auth
    def protection(self) -> "BranchProtection":
        """Retrieve the protections enabled for this branch.

        See:
        https://developer.github.com/v3/repos/branches/#get-branch-protection

        :returns:
            The protections enabled for this branch.
        :rtype:
            :class:`~github3.repos.branch.BranchProtection`
        """
        url = self._build_url("protection", base_url=self._api)
        resp = self._get(url)
        json = self._json(resp, 200)
        return BranchProtection(json, self)

    @decorators.requires_auth
    def protect(
        self,
        required_status_checks: t.Optional[t.Mapping[str, t.Any]],
        enforce_admins: t.Optional[bool],
        required_pull_request_reviews: t.Optional[t.Mapping[str, t.Any]],
        restrictions: t.Optional[t.Mapping[str, t.Sequence[str]]],
        required_linear_history: t.Optional[bool] = None,
        allow_force_pushes: t.Optional[bool] = None,
        allow_deletions: t.Optional[bool] = None,
        required_conversation_resolution: t.Optional[bool] = None,
    ) -> "BranchProtection":
        """Enable force push protection and configure status check enforcement.

        See also:
        https://docs.github.com/en/rest/reference/repos#update-branch-protection

        .. versionchanged:: 3.0.0

            The GitHub API changed since the last release this was updated in.
            As such the parameters have to change here.

        :param requireed_status_checks:
            Required. Require status checks to pass before merging. Set to null
            to disable.
        :param enforce_admins:
            Required. Enforce all configured restrictions for administrators.
            Set to true to enforce required status checks for repository
            administrators. Set to null to disable.
        :param required_pull_request_reviews:
            Required. Require at least one approving review on a pull request,
            before merging. Set to null to disable.
        :param restrictions:
            Required. Restrict who can push to the protected branch. User,
            app, and team restrictions are only available for
            organization-owned repositories. Set to null to disable.
        :param required_linear_history:
            Enforces a linear commit Git history, which prevents anyone from
            pushing merge commits to a branch. Set to true to enforce a linear
            commit history. Set to false to disable a linear commit Git
            history. Your repository must allow squash merging or rebase
            merging before you can enable a linear commit history. Default:
            false. For more information, see "Requiring a linear commit
            history" in the GitHub Help documentation.
        :param allow_force_pushes:
            Permits force pushes to the protected branch by anyone with write
            access to the repository. Set to true to allow force pushes. Set
            to false or null to block force pushes. Default: false. For more
            information, see "Enabling force pushes to a protected branch" in
            the GitHub Help documentation."
        :param allow_deletions:
            Allows deletion of the protected branch by anyone with write
            access to the repository. Set to false to prevent deletion of the
            protected branch. Default: false. For more information, see
            "Enabling force pushes to a protected branch" in the GitHub Help
            documentation.
        :param required_conversation_resolution:
            Requires all conversations on code to be resolved before a pull
            request can be merged into a branch that matches this rule. Set to
            false to disable. Default: false.
        :returns:
            BranchProtection if successful
        :rtype:
            :class:`BranchProtection`
        """
        edit = {
            "required_status_checks": required_status_checks,
            "enforce_admins": enforce_admins,
            "required_pull_request_reviews": required_pull_request_reviews,
            "restrictions": restrictions,
        }
        if required_linear_history is not None:
            edit["required_linear_history"] = required_linear_history
        if allow_force_pushes is not None:
            edit["allow_force_pushes"] = allow_force_pushes
        if allow_deletions is not None:
            edit["allow_deletions"] = allow_deletions
        if required_conversation_resolution is not None:
            edit[
                "required_conversation_resolution"
            ] = required_conversation_resolution
        url = self._build_url("protection", base_url=self._api)
        resp = self._put(url, json=edit)
        json = self._json(resp, 200)
        return BranchProtection(json, self)

    @decorators.requires_auth
    def sync_with_upstream(self) -> t.Mapping[str, str]:
        """Synchronize this branch with the upstream.

        .. warning::

            This API endpoint is still in Beta per gitHub

        .. versionadded:: 3.0.0

        Sync a branch of a forked repository to keep it up-to-date with the
        upstream repository.

        See also:
        https://docs.github.com/en/rest/reference/repos#sync-a-fork-branch-with-the-upstream-repository

        :returns:
            The dictionary described in the documentation
        :rtype:
            dict
        """
        base = self._api.split("/branches", 1)[0]
        url = self._build_url("merge-upstream", base_url=base)
        json = self._json(self._post(url), 200)
        return json

    @decorators.requires_auth
    def unprotect(self) -> bool:
        """Disable protections on this branch."""
        return self._boolean(
            self._delete(self._build_url("protection", base_url=self._api)),
            200,
            403,
        )


class Branch(_Branch):
    """The representation of a branch returned in a collection.

    GitHub's API returns different amounts of information about repositories
    based upon how that information is retrieved. This object exists to
    represent the limited amount of information returned for a specific
    branch in a collection. For example, you would receive this class when
    calling :meth:`~github3.repos.repo.Repository.branches`. To provide a
    clear distinction between the types of branches, github3.py uses different
    classes with different sets of attributes.

    This object has the same attributes as a
    :class:`~github3.repos.branch.ShortBranch` as well as the following:

    .. attribute:: links

        The dictionary of URLs returned by the API as ``_links``.

    .. attribute:: protected

        A boolean attribute that describes whether this branch is protected or
        not.

    .. attribute:: original_protection

        .. versionchanged:: 1.1.0

            To support a richer branch protection API, this is the new name
            for the information formerly stored under the attribute
            ``protection``.

        A dictionary with details about the protection configuration of this
        branch.

    .. attribute:: protection_url

        The URL to access and manage details about this branch's protection.
    """

    class_name = "Repository Branch"

    def _update_attributes(self, branch):
        super()._update_attributes(branch)
        self.commit = commit.ShortCommit(branch["commit"], self)
        #: Returns '_links' attribute.
        self.links = branch["_links"]
        #: Provides the branch's protection status.
        self.protected = branch["protected"]
        self.original_protection = branch["protection"]
        self.protection_url = branch["protection_url"]
        if self.links and "self" in self.links:
            self._api = self.links["self"]


class ShortBranch(_Branch):
    """The representation of a branch returned in a collection.

    GitHub's API returns different amounts of information about repositories
    based upon how that information is retrieved. This object exists to
    represent the limited amount of information returned for a specific
    branch in a collection. For example, you would receive this class when
    calling :meth:`~github3.repos.repo.Repository.branches`. To provide a
    clear distinction between the types of branches, github3.py uses different
    classes with different sets of attributes.

    This object has the following attributes:

    .. attribute:: commit

        A :class:`~github3.repos.commit.MiniCommit` representation of the
        newest commit on this branch with the associated repository metadata.

    .. attribute:: name

        The name of this branch.
    """

    class_name = "Short Repository Branch"
    _refresh_to = Branch

    @t.overload
    def refresh(self, conditional: bool = False) -> Branch:  # noqa: D102
        ...


class BranchProtection(models.GitHubCore):
    """The representation of a branch's protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection

    .. versionchanged:: 3.0.0

        Added ``required_linear_history``, ``allow_force_pushes``,
        ``allow_deletions``, and ``required_conversation_resolution``.

    This object has the following attributes:

    .. attribute:: enforce_admins

        A :class:`~github3.repos.branch.ProtectionEnforceAdmins` instance
        representing whether required status checks are required for admins.

    .. attribute:: restrictions

        A :class:`~github3.repos.branch.ProtectionRestrictions` representing
        who can push to this branch. Team and user restrictions are only
        available for organization-owned repositories.

    .. attribute:: required_pull_request_reviews

        A :class:`~github3.repos.branch.ProtectionRequiredPullRequestReviews`
        representing the protection provided by requiring pull request
        reviews.

    .. attribute:: required_status_checks

        A :class:`~github3.repos.branch.ProtectionRequiredStatusChecks`
        representing the protection provided by requiring status checks.

    .. attribute:: required_linear_history

        .. versionadded:: 3.0.0

        A :class:`~github3.repos.branch.ProtectionRequiredLinearHistory`
        representing the information returned by the API about this
        protection.

    .. attribute:: allow_force_pushes

        .. versionadded:: 3.0.0

        A :class:`~github3.repos.branch.ProtectionAllowForcePushes`
        representing the information returned by the API about this
        protection.

    .. attribute:: allow_deletions

        .. versionadded:: 3.0.0

        A :class:`~github3.repos.branch.ProtectionAllowDeletions`
        representing the information returned by the API about this
        protection.

    .. attribute:: required_conversation_resolution

        .. versionadded:: 3.0.0

        A
        :class:`~github3.repos.branch.ProtectionRequiredConversationResolution`
        representing the information returned by the API about this
        protection.

    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    """

    def _update_attributes(self, protection):
        self._api = protection["url"]

        def _set_conditional_attr(name, cls):
            value = protection.get(name)
            setattr(self, name, value)
            if getattr(self, name):
                setattr(self, name, cls(value, self))

        _set_conditional_attr("enforce_admins", ProtectionEnforceAdmins)
        _set_conditional_attr("restrictions", ProtectionRestrictions)
        _set_conditional_attr(
            "required_pull_request_reviews",
            ProtectionRequiredPullRequestReviews,
        )
        _set_conditional_attr(
            "required_status_checks", ProtectionRequiredStatusChecks
        )
        _set_conditional_attr(
            "required_linear_history", ProtectionRequiredLinearHistory
        )
        _set_conditional_attr(
            "allow_force_pushes", ProtectionAllowForcePushes
        )
        _set_conditional_attr("allow_deleteions", ProtectionAllowDeletions)
        _set_conditional_attr(
            "required_conversation_resolution",
            ProtectionRequiredConversationResolution,
        )

    @decorators.requires_auth
    def update(
        self,
        enforce_admins=None,
        required_status_checks=None,
        required_pull_request_reviews=None,
        restrictions=None,
    ):
        """Enable force push protection and configure status check enforcement.

        See: http://git.io/v4Gvu

        :param str enforce_admins:
            (optional), Specifies the enforcement level of the status checks.
            Must be one of 'off', 'non_admins', or 'everyone'. Use `None` or
            omit to use the already associated value.
        :param list required_status_checks:
            (optional), A list of strings naming status checks that must pass
            before merging. Use `None` or omit to use the already associated
            value.
        :param obj required_pull_request_reviews:
            (optional), Object representing the configuration of Request Pull
            Request Reviews settings. Use `None` or omit to use the already
            associated value.
        :param obj restrictions:
            (optional), Object representing the configuration of Restrictions.
            Use `None` or omit to use the already associated value.
        :returns:
            Updated branch protection
        :rtype:
            :class:`~github3.repos.branch.BranchProtection`
        """
        current_status = {
            "enforce_admins": getattr(self.enforce_admins, "enabled", False),
            "required_status_checks": (
                self.required_status_checks.as_dict()
                if self.required_status_checks is not None
                else None
            ),
            "required_pull_request_reviews": (
                self.required_pull_request_reviews.as_dict()
                if self.required_pull_request_reviews is not None
                else None
            ),
            "restrictions": (
                self.restrictions.as_dict()
                if self.restrictions is not None
                else None
            ),
        }
        edit = {
            "enabled": True,
            "enforce_admins": (
                enforce_admins
                if enforce_admins is not None
                else current_status["enforce_admins"]
            ),
            "required_status_checks": (
                required_status_checks
                if required_status_checks is not None
                else current_status["required_status_checks"]
            ),
            "required_pull_request_reviews": (
                required_pull_request_reviews
                if required_pull_request_reviews is not None
                else current_status["required_pull_request_reviews"]
            ),
            "restrictions": (
                restrictions
                if restrictions is not None
                else current_status["restrictions"]
            ),
        }

        json = self._json(self._put(self._api, json=edit), 200)
        self._update_attributes(json)
        return self

    @decorators.requires_auth
    def delete(self) -> bool:
        """Remove branch protection.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        resp = self._delete(self._api)
        return self._boolean(resp, 204, 404)

    @decorators.requires_auth
    def requires_signatures(self) -> bool:
        """Check if commit signatures are presently required.

        :returns:
            True if enabled, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("required_signatures", base_url=self._api)
        resp = self._get(url)
        if resp.status_code == 200:
            return resp.json()["enabled"]
        return False

    @decorators.requires_auth
    def require_signatures(self) -> bool:
        """Require commit signatures for commits to this branch.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("required_signatures", base_url=self._api)
        resp = self._post(url)
        return self._boolean(resp, 200, 404)

    @decorators.requires_auth
    def delete_signature_requirements(self) -> bool:
        """Stop requiring commit signatures for commits to this branch.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("required_signatures", base_url=self._api)
        resp = self._delete(url)
        return self._boolean(resp, 200, 404)


class ProtectionEnforceAdmins(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection
        `Admin enforcement of protected branch`_
            GitHub's documentation of protecting a branch with admins

    This object has the following attributes:

    .. attribute:: enabled

        A boolean attribute indicating whether the ``enforce_admins``
        protection is enabled or disabled.

    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    .. _Admin enforcement of protected branch:
        https://developer.github.com/v3/repos/branches/#get-admin-enforcement-of-protected-branch
    """

    def _update_attributes(self, protection):
        self._api = protection["url"]
        self.enabled = protection["enabled"]

    @decorators.requires_auth
    def enable(self):
        """Enable Admin enforcement for protected branch."""
        resp = self._post(self._api)
        return self._boolean(resp, 200, 404)

    @decorators.requires_auth
    def disable(self):
        """Disable Admin enforcement for protected branch."""
        resp = self._delete(self._api)
        return self._boolean(resp, 204, 404)


class ProtectionRestrictions(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection

        `Branch restriction documentation`_
            GitHub's description of branch restriction

    This object has the following attributes:

    .. attribute:: original_teams

        List of :class:`~github3.orgs.ShortTeam` objects representing
        the teams allowed to push to the protected branch.

    .. attribute:: original_users

        List of :class:`~github3.users.ShortUser` objects representing
        the users allowed to push to the protected branch.

    .. attribute:: teams_url

        The URL to retrieve the list of teams allowed to push to the
        protected branch.

    .. attribute:: users_url

        The URL to retrieve the list of users allowed to push to the
        protected branch.


    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    .. _Branch restriction documentation:
        https://help.github.com/articles/about-branch-restrictions
    """

    def _update_attributes(self, protection):
        from .. import apps, orgs, users

        self._api = protection["url"]
        self.users_url = protection["users_url"]
        self.teams_url = protection["teams_url"]
        self.apps_url = protection.get("apps_url")
        self.original_users = protection["users"]
        if self.original_users:
            self.original_users = [
                users.ShortUser(user, self) for user in self.original_users
            ]

        self.original_teams = protection["teams"]
        if self.original_teams:
            self.original_teams = [
                orgs.ShortTeam(team, self) for team in self.original_teams
            ]

        self.original_apps = protection.get("apps")
        if self.original_apps:
            self.original_apps = [
                apps.App(app, self) for app in self.original_apps
            ]

    @decorators.requires_auth
    def add_teams(
        self, teams: t.Sequence[str]
    ) -> t.Sequence["orgs.ShortTeam"]:
        """Add teams to the protected branch.

        See:
        https://developer.github.com/v3/repos/branches/#add-team-restrictions-of-protected-branch

        .. warning::

            This will not update the object to replace the ``original_teams``
            attribute.

        :param list teams:
            The list of the team names to have access to interact with
            protected branch.
        :returns:
            List of added teams
        :rtype:
            List[github3.orgs.ShortTeam]
        """
        from .. import orgs

        resp = self._post(self.teams_url, data=teams)
        json = self._json(resp, 200)
        return [orgs.ShortTeam(team, self) for team in json] if json else []

    @decorators.requires_auth
    def add_users(
        self, users: t.Sequence[str]
    ) -> t.Sequence["tusers.ShortUser"]:
        """Add users to protected branch.

        See
        https://developer.github.com/v3/repos/branches/#add-user-restrictions-of-protected-branch

        .. warning::

            This will not update the object to replace the ``original_users``
            attribute.

        :param list users:
            The list of the user logins to have access to interact with
            protected branch.
        :returns:
            List of added users
        :rtype:
            List[github3.users.ShortUser]
        """
        from .. import users as _users

        json = self._json(self._post(self.users_url, data=users), 200)
        return [_users.ShortUser(user, self) for user in json] if json else []

    @decorators.requires_auth
    def apps(self, number: int = -1) -> t.Generator["tapps.App", None, None]:
        """Retrieve current list of apps with access to the protected branch.

        See
        https://docs.github.com/en/rest/reference/repos#get-apps-with-access-to-the-protected-branch

        .. warning::

            This will not update the object to replace the ``original_apps``
            attribute.

        :param int number:
            Limit the number of apps returned
        :returns:
            An iterator of apps
        :rtype:
            :class:`~github3.apps.App`
        """
        from .. import apps

        return self._iter(int(number), self.apps_url, apps.App)

    @decorators.requires_auth
    def add_app_restrictions(
        self, apps: t.Sequence[t.Union["tapps.App", str]]
    ) -> t.List["tapps.App"]:
        """Grant app push access to the current branch.

        See
        https://docs.github.com/en/rest/reference/repos#add-app-access-restrictions

        Per GitHub's documentation above:

            Grants the specified apps push access for this branch. Only
            installed GitHub Apps with write access to the contents permission
            can be added as authorized actors on a protected branch.

        :param list apps:
            List of slugs of apps to grant push access to the protected
            branch. If you pass a list of :class:`~github3.apps.App` then the
            library will retrieve the slug for you.
        :returns:
            List of apps with push access to the protected branch
        :rtype:
            List[:class:`~github3.apps.App`]
        """
        from .. import apps as _apps

        apps = [getattr(a, "slug", a) for a in apps]
        json = self._json(self._post(self.apps_url, data=apps), 200)
        return [_apps.App(a, self) for a in json]

    @decorators.requires_auth
    def replace_app_restrictions(
        self, apps: t.Sequence[t.Union["tapps.App", str]]
    ) -> t.List["tapps.App"]:
        """Replace existing app push access with only those specified.

        See
        https://docs.github.com/en/rest/reference/repos#set-app-access-restrictions

        Per GitHub's documentation above:

            Replaces the list of apps that have push access to this branch.
            This removes all apps that previously had push access and grants
            push access to the new list of apps. Only installed GitHub Apps
            with write access to the contents permission can be added as
            authorized actors on a protected branch.

        :param list apps:
            List of slugs of apps to grant push access to the protected
            branch. If you pass a list of :class:`~github3.apps.App` then the
            library will retrieve the slug for you.
        :returns:
            List of apps with push access to the protected branch
        :rtype:
            List[:class:`~github3.apps.App`]
        """
        from .. import apps as _apps

        apps = [getattr(a, "slug", a) for a in apps]
        json = self._json(self._put(self.apps_url, data=apps), 200)
        return [_apps.App(a, self) for a in json]

    @decorators.requires_auth
    def remove_app_restrictions(
        self, apps: t.Sequence[t.Union["tapps.App", str]]
    ) -> t.List["tapps.App"]:
        """Remove the apps' push access to the protected branch.

        See
        https://docs.github.com/en/rest/reference/repos#remove-app-access-restrictions

        :param list apps:
            List of slugs of apps to revoke push access to the protected
            branch. If you pass a list of :class:`~github3.apps.App` then the
            library will retrieve the slug for you.
        :returns:
            List of apps that still have push access
        :rtype:
            List[:class:`~github3.apps.App`]
        """
        from .. import apps as _apps

        apps = [getattr(a, "slug", a) for a in apps]
        json = self._json(self._delete(self.apps_url, data=apps), 200)
        return [_apps.App(a, self) for a in json]

    @decorators.requires_auth
    def delete(self) -> bool:
        """Completely remove restrictions of the protected branch.

        See
        https://developer.github.com/v3/repos/branches/#remove-user-restrictions-of-protected-branch

        :returns:
            True if successful, False otherwise.
        :rtype:
            bool
        """
        resp = self._delete(self._api)
        return self._boolean(resp, 204, 404)

    @decorators.requires_auth
    def remove_teams(
        self, teams: t.Sequence[str]
    ) -> t.Sequence["orgs.ShortTeam"]:
        """Remove teams from protected branch.

        See
        https://developer.github.com/v3/repos/branches/#remove-team-restrictions-of-protected-branch

        :param list teams:
            The list of the team names to stop having access to interact with
            protected branch.
        :returns:
            List of removed teams
        :rtype:
            List[github3.orgs.ShortTeam]
        """
        from .. import orgs

        resp = self._delete(self.teams_url, json=teams)
        json = self._json(resp, 200)
        return [orgs.ShortTeam(team, self) for team in json] if json else []

    @decorators.requires_auth
    def remove_users(
        self, users: t.Sequence[str]
    ) -> t.Sequence["tusers.ShortUser"]:
        """Remove users from protected branch.

        See
        https://developer.github.com/v3/repos/branches/#remove-user-restrictions-of-protected-branch

        :param list users:
            The list of the user logins to stop having access to interact with
            protected branch.
        :returns:
            List of removed users
        :rtype:
            List[github3.users.ShortUser]
        """
        resp = self._delete(self.users_url, json=users)
        json = self._json(resp, 200)
        from .. import users as _users

        return [_users.ShortUser(user, self) for user in json] if json else []

    @decorators.requires_auth
    def replace_teams(
        self, teams: t.Sequence[str]
    ) -> t.Sequence["orgs.ShortTeam"]:
        """Replace teams that will have access to protected branch.

        See
        https://developer.github.com/v3/repos/branches/#replace-team-restrictions-of-protected-branch

        :param list teams:
            The list of the team names to have access to interact with
            protected branch.
        :returns:
            List of teams that now have access to the protected branch
        :rtype:
            List[github3.orgs.ShortTeam]
        """
        from .. import orgs

        resp = self._put(self.teams_url, json=teams)
        json = self._json(resp, 200)
        return [orgs.ShortTeam(team, self) for team in json] if json else []

    @decorators.requires_auth
    def replace_users(
        self, users: t.Sequence[str]
    ) -> t.Sequence["tusers.ShortUser"]:
        """Replace users that will have access to protected branch.

        See
        https://developer.github.com/v3/repos/branches/#replace-user-restrictions-of-protected-branch

        :param list users:
            The list of the user logins to have access to interact with
            protected branch.
        :returns:
            List of users that now have access to the protected branch
        :rtype:
            List[github3.users.ShortUser]
        """
        users_resp = self._put(self.users_url, json=users)
        return self._boolean(users_resp, 200, 404)

    def teams(
        self, number: int = -1
    ) -> t.Generator["orgs.ShortTeam", None, None]:
        """Retrieve an up-to-date listing of teams.

        :returns:
            An iterator of teams
        :rtype:
            :class:`~github3.orgs.ShortTeam`
        """
        from .. import orgs

        return self._iter(
            int(number),
            self.teams_url,
            orgs.ShortTeam,
        )

    def users(
        self, number: int = -1
    ) -> t.Generator["tusers.ShortUser", None, None]:
        """Retrieve an up-to-date listing of users.

        :returns:
            An iterator of users
        :rtype:
            :class:`~github3.users.ShortUser`
        """
        from .. import users

        return self._iter(int(number), self.users_url, users.ShortUser)


class ProtectionRequiredPullRequestReviews(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection.
        `Branch Required Pull Request Reviews`_
            GitHub's documentation of required pull request review protections

    This object has the folllowing attributes:

    .. attribute:: dismiss_stale_reviews

        A boolean attribute describing whether stale pull request reviews
        should be automatically dismissed by GitHub.

    .. attribute:: dismissal_restrictions

        If specified, a :class:`~github3.repos.branch.ProtectionRestrictions`
        object describing the dismissal restrictions for pull request reviews.

    .. attribute:: require_code_owner_reviews

        A boolean attribute describing whether to require "code owners" to
        review a pull request before it may be merged.

    .. attribute:: required_approving_review_count

        An integer describing the number (between 1 and 6) of reviews required
        before a pull request may be merged.

    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    .. _Branch Required Pull Request Reviews:
        https://developer.github.com/v3/repos/branches/#get-pull-request-review-enforcement-of-protected-branch
    """

    def _update_attributes(self, protection):
        self._api = protection["url"]
        self.dismiss_stale_reviews = protection["dismiss_stale_reviews"]
        # Use a temporary value to stay under line-length restrictions
        value = protection["require_code_owner_reviews"]
        self.require_code_owner_reviews = value
        # Use a temporary value to stay under line-length restrictions
        value = protection["required_approving_review_count"]
        self.required_approving_review_count = value
        self.dismissal_restrictions = None
        if "dismissal_restrictions" in protection:
            self.dismissal_restrictions = ProtectionRestrictions(
                protection["dismissal_restrictions"], self
            )

    @decorators.requires_auth
    def update(
        self,
        dismiss_stale_reviews=None,
        require_code_owner_reviews=None,
        required_approving_review_count=None,
        dismissal_restrictions=None,
    ):
        """Update the configuration for the Required Pull Request Reviews.

        :param bool dismiss_stale_reviews:
            Whether or not to dismiss stale pull request reviews automatically
        :param bool require_code_owner_reviews:
            Blocks merging pull requests until code owners review them
        :param int required_approving_review_count:
            The number of reviewers required to approve pull requests.
            Acceptable values are between 1 and 6.
        :param dict dismissal_restrictions:
            An empty dictionary will disable this. This must have the
            following keys: ``users`` and ``teams`` each mapping to a list
            of user logins and team slugs respectively.
        :returns:
            A updated instance of the required pull request reviews.
        :rtype:
            :class:`~github3.repos.branch.ProtectionRequiredPullRequestReviews`
        """
        existing_values = {
            "dismiss_stale_reviews": self.dismiss_stale_reviews,
            "dismissal_restrictions": {
                "users": [
                    getattr(u, "login", u)
                    for u in getattr(
                        self.dismissal_restrictions, "original_users", []
                    )
                ],
                "teams": [
                    getattr(t, "slug", t)
                    for t in getattr(
                        self.dismissal_restrictions, "original_teams", []
                    )
                ],
            },
            "require_code_owner_reviews": self.require_code_owner_reviews,
            "required_approving_review_count": (
                self.required_approving_review_count
            ),
        }

        update_json = {
            "dismiss_stale_reviews": (
                dismiss_stale_reviews
                if dismiss_stale_reviews is not None
                else existing_values["dismiss_stale_reviews"]
            ),
            "require_code_owner_reviews": (
                require_code_owner_reviews
                if require_code_owner_reviews is not None
                else existing_values["require_code_owner_reviews"]
            ),
            "required_approving_review_count": (
                required_approving_review_count
                if required_approving_review_count is not None
                else existing_values["required_approving_review_count"]
            ),
            "dismissal_restrictions": (
                dismissal_restrictions
                if dismissal_restrictions is not None
                else existing_values["dismissal_restrictions"]
            ),
        }
        resp = self._patch(self._api, json=update_json)
        json = self._json(resp, 200)
        if json:
            self._update_attributes(json)
        return self

    @decorators.requires_auth
    def delete(self):
        """Remove the Required Pull Request Reviews.

        :returns:
            Whether the operation finished successfully or not
        :rtype:
            bool
        """
        resp = self._delete(self._api)
        return self._boolean(resp, 204, 404)


class ProtectionRequiredStatusChecks(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection
        `Required Status Checks documentation`_
            GitHub's description of required status checks
        `Required Status Checks API documentation`_
            The API documentation for required status checks


    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    .. _Required Status Checks documentation:
        https://help.github.com/articles/about-required-status-checks
    .. _Required Status Checks API documentation:
        https://developer.github.com/v3/repos/branches/#get-required-status-checks-of-protected-branch
    """

    def _update_attributes(self, protection):
        self._api = protection["url"]
        self.strict = protection["strict"]
        self.original_contexts = protection["contexts"]
        self.contexts_url = protection["contexts_url"]

    @decorators.requires_auth
    def add_contexts(self, contexts):
        """Add contexts to the existing list of required contexts.

        See:
        https://developer.github.com/v3/repos/branches/#add-required-status-checks-contexts-of-protected-branch

        :param list contexts:
            The list of contexts to append to the existing list.
        :returns:
            The updated list of contexts.
        :rtype:
            list
        """
        resp = self._post(self.contexts_url, data=contexts)
        json = self._json(resp, 200)
        return json

    @decorators.requires_auth
    def contexts(self):
        """Retrieve the list of contexts required as status checks.

        See:
        https://developer.github.com/v3/repos/branches/#list-required-status-checks-contexts-of-protected-branch

        :returns:
            A list of context names which are required status checks.
        :rtype:
            list
        """
        resp = self._get(self.contexts_url)
        json = self._json(resp, 200)
        return json

    @decorators.requires_auth
    def remove_contexts(self, contexts):
        """Remove the specified contexts from the list of required contexts.

        See:
        https://developer.github.com/v3/repos/branches/#remove-required-status-checks-contexts-of-protected-branch

        :param list contexts:
            The context names to remove
        :returns:
            The updated list of contexts required as status checks.
        :rtype:
            list
        """
        resp = self._delete(self.contexts_url, json=contexts)
        json = self._json(resp, 200)
        return json

    @decorators.requires_auth
    def replace_contexts(self, contexts):
        """Replace the existing contexts required as status checks.

        See
        https://developer.github.com/v3/repos/branches/#replace-required-status-checks-contexts-of-protected-branch

        :param list contexts:
            The names of the contexts to be required as status checks
        :returns:
            The new list of contexts required as status checks.
        :rtype:
            list
        """
        resp = self._put(self.contexts_url, json=contexts)
        json = self._json(resp, 200)
        return json

    @decorators.requires_auth
    def delete_contexts(self, contexts):
        """Delete the contexts required as status checks.

        See
        https://developer.github.com/v3/repos/branches/#replace-required-status-checks-contexts-of-protected-branch

        :param list contexts:
            The names of the contexts to be required as status checks
        :returns:
            The updated list of contexts required as status checks.
        :rtype:
            list
        """
        resp = self._delete(self.contexts_url, json=contexts)
        return self._boolean(resp, 204, 404)

    @decorators.requires_auth
    def update(self, strict=None, contexts=None):
        """Update required status checks for the branch.

        This requires admin or owner permissions to the repository and
        branch protection to be enabled.

        .. seealso::

            `API docs`_
                Descrption of how to update the required status checks.

        :param bool strict:
            Whether this should be strict protection or not.
        :param list contexts:
            A list of context names that should be required.
        :returns:
            A new instance of this class with the updated information
        :rtype:
            :class:`~github3.repos.branch.ProtectionRequiredStatusChecks`


        .. links
        .. _API docs:
            https://developer.github.com/v3/repos/branches/#update-required-status-checks-of-protected-branch
        """
        update_data = {}
        json = None
        if strict is not None:
            update_data["strict"] = strict
        if contexts is not None:
            update_data["contexts"] = contexts
        if update_data:
            resp = self._patch(self.url, json=update_data)
            json = self._json(resp, 200)
        if json is not None:
            self._update_attributes(json)
        return self

    @decorators.requires_auth
    def delete(self):
        """Remove required status checks from this branch.

        See:
        https://developer.github.com/v3/repos/branches/#remove-required-status-checks-of-protected-branch

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        resp = self._delete(self.url)
        return self._boolean(resp, 204, 404)


class ProtectionRequiredLinearHistory(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection

    This object has the following attributes:

    .. attribute:: enabled

        A boolean attribute indicating whether the ``required_linear_history``
        protection is enabled or disabled.

    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    """

    def _update_attributes(self, protection):
        self.enabled = protection["enabled"]


class ProtectionAllowForcePushes(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection

    This object has the following attributes:

    .. attribute:: enabled

        A boolean attribute indicating whether the ``allow_force_pushes``
        protection is enabled or disabled.

    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    """

    def _update_attributes(self, protection):
        self.enabled = protection["enabled"]


class ProtectionAllowDeletions(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection

    This object has the following attributes:

    .. attribute:: enabled

        A boolean attribute indicating whether the ``allow_deletions``
        protection is enabled or disabled.

    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    """

    def _update_attributes(self, protection):
        self.enabled = protection["enabled"]


class ProtectionRequiredConversationResolution(models.GitHubCore):
    """The representation of a sub-portion of branch protection.

    .. seealso::

        `Branch protection API documentation`_
            GitHub's documentation of branch protection

    This object has the following attributes:

    .. attribute:: enabled

        A boolean attribute indicating whether the
        ``required_conversation_resolution`` protection is enabled or
        disabled.

    .. links
    .. _Branch protection API documentation:
        https://developer.github.com/v3/repos/branches/#get-branch-protection
    """

    def _update_attributes(self, protection):
        self.enabled = protection["enabled"]
