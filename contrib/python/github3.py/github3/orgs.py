"""This module contains all of the classes related to organizations."""
import typing as t
from json import dumps

from uritemplate import URITemplate

from . import models
from . import users
from .decorators import requires_auth
from .events import Event
from .projects import Project
from .repos import Repository
from .repos import ShortRepository

if t.TYPE_CHECKING:
    from . import users as _users


class ShortRepositoryWithPermissions(ShortRepository):
    class_name = "ShortRepositoryWithPermissions"

    def _update_attributes(self, repo) -> None:
        super()._update_attributes(repo)
        self.permissions = repo["permissions"]


class _Team(models.GitHubCore):
    """Base class for Team representations."""

    class_name = "_Team"
    # Roles available to members on a team.
    member_roles = frozenset(["member", "maintainer"])
    filterable_member_roles = member_roles.union(["all"])

    def _update_attributes(self, team):
        self._api = team["url"]
        self.id = team["id"]
        self.members_urlt = URITemplate(team["members_url"])
        self.name = team["name"]
        self.permission = team["permission"]
        self.privacy = team.get(
            "privacy"
        )  # TODO: Re-record cassettes to ensure this exists
        self.repositories_url = team["repositories_url"]
        self.slug = team["slug"]
        self.parent = None
        parent = team.get("parent")
        if parent:
            self.parent = ShortTeam(parent, self)

    def _repr(self):
        return "<{s.class_name} [{s.name}]>".format(s=self)

    @requires_auth
    def add_or_update_membership(self, username, role="member"):
        """Add or update the user's membership in this team.

        This returns a dictionary like so::

            {
                'state': 'pending',
                'url': 'https://api.github.com/teams/...',
                'role': 'member',
            }

        :param str username:
            (required), login of user whose membership is being modified
        :param str role:
            (optional), the role the user should have once their membership
            has been modified. Options: 'member', 'maintainer'. Default:
            'member'
        :returns:
            dictionary of the invitation response
        :rtype:
            dict
        """
        if role not in self.member_roles:
            raise ValueError(
                "'role' must be one of {}".format(
                    ", ".join(sorted(self.member_roles))
                )
            )
        data = {"role": role}
        url = self._build_url("memberships", username, base_url=self._api)
        return self._json(self._put(url, json=data), 200)

    @requires_auth
    def add_repository(self, repository, permission=""):
        """Add ``repository`` to this team.

        If a permission is not provided, the team's default permission
        will be assigned, by GitHub.

        :param str repository:
            (required), form: 'user/repo'
        :param str permission:
            (optional), ('pull', 'push', 'admin')
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        data = {}
        if permission:
            data = {"permission": permission}
        url = self._build_url("repos", repository, base_url=self._api)
        return self._boolean(self._put(url, data=dumps(data)), 204, 404)

    @requires_auth
    def delete(self):
        """Delete this team.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_auth
    def edit(
        self,
        name: str,
        permission: str = "",
        parent_team_id: t.Optional[int] = None,
        privacy: t.Optional[str] = None,
    ):
        """Edit this team.

        :param str name:
            (required), the new name of this team
        :param str permission:
            (optional), one of ('pull', 'push', 'admin')
            .. deprecated:: 3.0.0

                This was deprecated by the GitHub API.
        :param int parent_team_id:
            (optional), id of the parent team for this team
        :param str privacy:
            (optional), one of "closed" or "secret"
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if name:
            data: t.Dict[str, t.Union[str, int]] = {"name": name}
            if permission:
                data["permission"] = permission
            if parent_team_id is not None:
                data["parent_team_id"] = parent_team_id
            if privacy in {"closed", "secret"}:
                data["privacy"] = privacy
            json = self._json(self._patch(self._api, data=dumps(data)), 200)
            if json:
                self._update_attributes(json)
                return True
        return False

    @requires_auth
    def has_repository(self, repository):
        """Check if this team has access to ``repository``.

        :param str repository:
            (required), form: 'user/repo'
        :returns:
            True if the team can access the repository, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("repos", repository, base_url=self._api)
        return self._boolean(self._get(url), 204, 404)

    @requires_auth
    def members(self, role=None, number=-1, etag=None):
        """Iterate over the members of this team.

        :param str role:
            (optional), filter members returned by their role in the team.
            Can be one of: ``"member"``, ``"maintainer"``, ``"all"``. Default:
            ``"all"``.
        :param int number:
            (optional), number of users to iterate over.  Default: -1 iterates
            over all values
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of the members of this team
        :rtype:
            :class:`~github3.users.ShortUser`
        """
        headers = {}
        params = {}
        if role in self.filterable_member_roles:
            params["role"] = role
            headers["Accept"] = "application/vnd.github.ironman-preview+json"
        url = self._build_url("members", base_url=self._api)
        return self._iter(
            int(number),
            url,
            users.ShortUser,
            params=params,
            etag=etag,
            headers=headers,
        )

    @requires_auth
    def permissions_for(
        self, repository: str
    ) -> ShortRepositoryWithPermissions:
        headers = {"Accept": "application/vnd.github.v3.repository+json"}
        url = self._build_url("repos", repository, base_url=self._api)
        json = self._json(self._get(url, headers=headers), 200)
        return ShortRepositoryWithPermissions(json, self)

    @requires_auth
    def repositories(self, number=-1, etag=None):
        """Iterate over the repositories this team has access to.

        :param int number:
            (optional), number of repos to iterate over. Default: -1 iterates
            over all values
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of repositories this team has access to
        :rtype:
            :class:`~github3.orgs.ShortRepositoryWithPermissions`
        """
        url = self._build_url("repos", base_url=self._api)
        return self._iter(
            int(number),
            url,
            ShortRepositoryWithPermissions,
            etag=etag,
        )

    @requires_auth
    def membership_for(self, username):
        """Retrieve the membership information for the user.

        :param str username:
            (required), name of the user
        :returns:
            dictionary with the membership
        :rtype:
            dict
        """
        url = self._build_url("memberships", username, base_url=self._api)
        json = self._json(self._get(url), 200)
        return json or {}

    @requires_auth
    def revoke_membership(self, username):
        """Revoke this user's team membership.

        :param str username:
            (required), name of the team member
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("memberships", username, base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    @requires_auth
    def remove_repository(self, repository):
        """Remove ``repository`` from this team.

        :param str repository:
            (required), form: 'user/repo'
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("repos", repository, base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)


class Team(_Team):
    """Object representing a team in the GitHub API.

    In addition to the attributes on a :class:`~github3.orgs.ShortTeam` a Team
    has the following attribute:

    .. attribute:: created_at

        A :class:`~datetime.datetime` instance representing the time and date
        when this team was created.

    .. attribute:: members_count

        The number of members in this team.

    .. attribute:: organization

        A :class:`~github3.orgs.ShortOrganization` representing the
        organization this team belongs to.

    .. attribute:: repos_count

        The number of repositories this team can access.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` instance representing the time and date
        when this team was updated.

    Please see GitHub's `Team Documentation`_ for more information.

    .. _Team Documentation:
        http://developer.github.com/v3/orgs/teams/
    """

    class_name = "Team"

    def _update_attributes(self, team):
        super()._update_attributes(team)
        self.created_at = self._strptime(team["created_at"])
        self.members_count = team["members_count"]
        self.organization = ShortOrganization(team["organization"], self)
        self.repos_count = team["repos_count"]
        self.updated_at = self._strptime(team["updated_at"])


class ShortTeam(_Team):
    """Object representing a team in the GitHub API.

    .. attribute:: id

        Unique identifier for this team across all of GitHub.

    .. attribute:: members_count

        The number of members in this team.

    .. attribute:: members_urlt

        A :class:`~uritemplate.URITemplate` instance to either retrieve all
        members in this team or to test if a user is a member.

    .. attribute:: name

        The human-readable name provided to this team.

    .. attribute:: permission

        The level of permissions this team has, e.g., ``push``, ``pull``,
        or ``admin``.

    .. attribute:: privacy

        The privacy level of this team inside the organization.

    .. attribute:: repos_count

        The number of repositories this team can access.

    .. attribute:: repositories_url

        The URL of the resource to enumerate all repositories this team can
        access.

    .. attribute:: slug

        The handle for this team or the portion you would use in an
        at-mention after the ``/``, e.g., in ``@myorg/myteam`` the
        slug is ``myteam``.

    Please see GitHub's `Team Documentation`_ for more information.

    .. _Team Documentation:
        http://developer.github.com/v3/orgs/teams/
    """

    class_name = "ShortTeam"
    _refresh_to = Team


class _Organization(models.GitHubCore):
    """The :class:`Organization <Organization>` object.

    Please see GitHub's `Organization Documentation`_ for more information.

    .. _Organization Documentation:
        http://developer.github.com/v3/orgs/
    """

    class_name = "_Organization"

    # Filters available when listing members. Note: ``"2fa_disabled"``
    # is only available for organization owners.
    members_filters = frozenset(["2fa_disabled", "all"])

    # Roles available to members in an organization.
    member_roles = frozenset(["admin", "member"])
    filterable_member_roles = member_roles.union(["all"])

    # Roles for invitations, see also:
    # https://developer.github.com/v3/orgs/members/#create-organization-invitation
    invitation_roles = frozenset(
        ["admin", "direct_member", "billing_manager"]
    )

    def _update_attributes(self, org):
        self.avatar_url = org["avatar_url"]
        self.description = org["description"]
        self.events_url = org["events_url"]
        self.hooks_url = org["hooks_url"]
        self.id = org["id"]
        self.issues_url = org["issues_url"]
        self.login = org["login"]
        self.members_url = org["members_url"]
        self.public_members_urlt = URITemplate(org["public_members_url"])
        self.repos_url = org["repos_url"]
        self.url = self._api = org["url"]
        self.type = "Organization"

    def _repr(self):
        display_name = ""
        name = getattr(self, "name", None)
        if name is not None:
            display_name = f":{name}"
        return "<{s.class_name} [{s.login}{display}]>".format(
            s=self, display=display_name
        )

    @requires_auth
    def add_or_update_membership(self, username, role="member"):
        """Add a member or update their role.

        :param str username:
            (required), user to add or update.
        :param str role:
            (optional), role to give to the user. Options are ``member``,
            ``admin``. Defaults to ``member``.
        :returns:
            the created or updated membership
        :rtype:
            :class:`~github3.orgs.Membership`
        :raises:
            ValueError if role is not a valid choice
        """
        if role not in self.member_roles:
            raise ValueError(
                "'role' must be one of {}".format(
                    ", ".join(sorted(self.member_roles))
                )
            )
        data = {"role": role}
        url = self._build_url(
            "memberships", str(username), base_url=self._api
        )
        json = self._json(self._put(url, json=data), 200)
        return self._instance_or_null(Membership, json)

    @requires_auth
    def add_repository(self, repository, team_id):  # FIXME(jlk): add perms
        """Add ``repository`` to ``team``.

        .. versionchanged:: 1.0

            The second parameter used to be ``team`` but has been changed to
            ``team_id``. This parameter is now required to be an integer to
            improve performance of this method.

        :param str repository:
            (required), form: 'user/repo'
        :param int team_id:
            (required), team id
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if int(team_id) < 0:
            return False

        url = self._build_url(
            "organizations",
            str(self.id),
            "team",
            str(team_id),
            "repos",
            str(repository),
        )
        return self._boolean(self._put(url), 204, 404)

    @requires_auth
    def blocked_users(
        self, number: int = -1, etag: t.Optional[str] = None
    ) -> t.Generator[users.ShortUser, None, None]:
        """Iterate over the users blocked by this organization.

        .. versionadded:: 2.1.0

        :param int number:
            (optional), number of users to iterate over.  Default: -1 iterates
            over all values
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of the members of this team
        :rtype:
            :class:`~github3.users.ShortUser`
        """
        url = self._build_url("blocks", base_url=self._api)
        return self._iter(int(number), url, users.ShortUser, etag=etag)

    @requires_auth
    def block(self, username: users.UserLike) -> bool:
        """Block a specific user from an organization.

        .. versionadded:: 2.1.0

        :parameter str username:
            Name (or user-like instance) of the user to block.
        :returns:
            True if successful, Fales otherwise
        :rtype:
            bool
        """
        url = self._build_url("blocks", str(username), base_url=self._api)
        return self._boolean(self._put(url), 204, 404)

    @requires_auth
    def unblock(self, username: users.UserLike) -> bool:
        """Unblock a specific user from an organization.

        .. versionadded:: 2.1.0

        :parameter str username:
            Name (or user-like instance) of the user to unblock.
        :returns:
            True if successful, Fales otherwise
        :rtype:
            bool
        """
        url = self._build_url("blocks", str(username), base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    @requires_auth
    def is_blocking(self, username: users.UserLike) -> bool:
        """Check if this organization is blocking a specific user.

        .. versionadded:: 2.1.0

        :parameter str username:
            Name (or user-like instance) of the user to unblock.
        :returns:
            True if successful, Fales otherwise
        :rtype:
            bool
        """
        url = self._build_url("blocks", str(username), base_url=self._api)
        return self._boolean(self._get(url), 204, 404)

    @requires_auth
    def create_hook(self, name, config, events=["push"], active=True):
        """Create a hook on this organization.

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
            :class:`~github3.orgs.OrganizationHook`
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
        return OrganizationHook(json, self) if json else None

    @requires_auth
    def create_project(self, name, body=""):
        """Create a project for this organization.

        If the client is authenticated and a member of the organization, this
        will create a new project in the organization.

        :param str name:
            (required), name of the project
        :param str body:
            (optional), the body of the project
        :returns:
            the new project
        :rtype:
            :class:`~github3.projects.Project`
        """
        url = self._build_url("projects", base_url=self._api)
        data = {"name": name, "body": body}
        json = self._json(
            self._post(url, data, headers=Project.CUSTOM_HEADERS), 201
        )
        return self._instance_or_null(Project, json)

    @requires_auth
    def create_repository(
        self,
        name,
        description="",
        homepage="",
        private=False,
        has_issues=True,
        has_wiki=True,
        team_id=0,
        auto_init=False,
        gitignore_template="",
        license_template="",
        has_projects=True,
    ):
        """Create a repository for this organization.

        If the client is authenticated and a member of the organization, this
        will create a new repository in the organization.

            ``name`` should be no longer than 100 characters

        :param str name:
            (required), name of the repository

            .. warning:: this should be no longer than 100 characters
        :param str description:
            (optional)
        :param str homepage:
            (optional)
        :param bool private:
            (optional), If ``True``, create a private repository. API default:
            ``False``
        :param bool has_issues:
            (optional), If ``True``, enable issues for this repository. API
            default: ``True``
        :param bool has_wiki:
            (optional), If ``True``, enable the wiki for this repository. API
            default: ``True``
        :param int team_id:
            (optional), id of the team that will be granted access to this
            repository
        :param bool auto_init:
            (optional), auto initialize the repository.
        :param str gitignore_template:
            (optional), name of the template; this is ignored if auto_int is
            False.
        :param str license_template:
            (optional), name of the license; this is ignored if auto_int is
            False.
        :param bool has_projects:
            (optional), If ``True``, enable projects for this repository. API
            default: ``True``
        :returns:
            the created repository
        :rtype:
            :class:`~github3.repos.Repository`
        """
        url = self._build_url("repos", base_url=self._api)
        data = {
            "name": name,
            "description": description,
            "homepage": homepage,
            "private": private,
            "has_issues": has_issues,
            "has_wiki": has_wiki,
            "license_template": license_template,
            "auto_init": auto_init,
            "gitignore_template": gitignore_template,
            "has_projects": has_projects,
        }
        if int(team_id) > 0:
            data.update({"team_id": team_id})
        json = self._json(self._post(url, data), 201)
        return self._instance_or_null(Repository, json)

    @requires_auth
    def conceal_member(self, username):
        """Conceal ``username``'s membership in this organization.

        :param str username:
            username of the organization member to conceal
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("public_members", username, base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    @requires_auth
    def create_team(
        self,
        name: str,
        repo_names: t.Optional[t.Sequence[str]] = [],
        maintainers: t.Optional[
            t.Union[t.Sequence[str], t.Sequence["_users._User"]]
        ] = [],
        permission: str = "pull",
        parent_team_id: t.Optional[int] = None,
        privacy: str = "secret",
    ):
        """Create a new team and return it.

        This only works if the authenticated user owns this organization.

        :param str name:
            (required), name to be given to the team
        :param list repo_names:
            (optional) repositories, e.g.  ['github/dotfiles']
        :param list maintainers:
            (optional) list of usernames who will be maintainers
        :param str permission:
            (optional), options:

            - ``pull`` -- (default) members can not push or administer
                repositories accessible by this team
            - ``push`` -- members can push and pull but not administer
                repositories accessible by this team
            - ``admin`` -- members can push, pull and administer
                repositories accessible by this team
        :param int parent_team_id:
            (optional), the ID of a team to set as the parent team.
        :param str privacy:
            (optional), options:

            - ``secret`` -- (default) only visible to organization
                owners and members of this team
            - ``closed`` -- visible to all members of this organization
        :returns:
            the created team
        :rtype:
            :class:`~github3.orgs.Team`
        """
        data: t.Dict[str, t.Union[t.List[str], str, int]] = {
            "name": name,
            "repo_names": [
                getattr(r, "full_name", r) for r in (repo_names or [])
            ],
            "maintainers": [
                getattr(m, "login", m) for m in (maintainers or [])
            ],
            "permission": permission,
            "privacy": privacy,
        }
        if parent_team_id:
            data.update({"parent_team_id": parent_team_id})

        url = self._build_url("teams", base_url=self._api)
        json = self._json(self._post(url, data), 201)
        return self._instance_or_null(Team, json)

    @requires_auth
    def edit(
        self,
        billing_email=None,
        company=None,
        email=None,
        location=None,
        name=None,
        description=None,
        has_organization_projects=None,
        has_repository_projects=None,
        default_repository_permission=None,
        members_can_create_repositories=None,
    ):
        """Edit this organization.

        :param str billing_email:
            (optional) Billing email address (private)
        :param str company:
            (optional)
        :param str email:
            (optional) Public email address
        :param str location:
            (optional)
        :param str name:
            (optional)
        :param str description:
            (optional) The description of the company.
        :param bool has_organization_projects:
            (optional) Toggles whether organization projects are enabled for
            the organization.
        :param bool has_repository_projects:
            (optional) Toggles whether repository projects are enabled for
            repositories that belong to the organization.
        :param string default_repository_permission:
            (optional) Default permission level members have for organization
            repositories:

            - ``read`` -- (default) can pull, but not push to or administer
                this repository.
            - ``write`` -- can pull and push, but not administer this
                repository.
            - ``admin`` -- can pull, push, and administer this repository.
            - ``none`` -- no permissions granted by default.
        :param bool members_can_create_repositories:
            (optional) Toggles ability of non-admin organization members to
            create repositories:

            - ``True`` -- (default) all organization members can create
                repositories.
            - ``False`` -- only admin members can create repositories.
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        json = None
        data = {
            "billing_email": billing_email,
            "company": company,
            "email": email,
            "location": location,
            "name": name,
            "description": description,
            "has_organization_projects": has_organization_projects,
            "has_repository_projects": has_repository_projects,
            "default_repository_permission": default_repository_permission,
            "members_can_create_repositories": members_can_create_repositories,
        }
        self._remove_none(data)

        if data:
            json = self._json(self._patch(self._api, data=dumps(data)), 200)

        if json:
            self._update_attributes(json)
            return True
        return False

    @requires_auth
    def hook(self, hook_id):
        """Get a single hook.

        :param int hook_id:
            (required), id of the hook
        :returns:
            the hook
        :rtype:
            :class:`~github3.orgs.OrganizationHook`
        """
        json = None
        if int(hook_id) > 0:
            url = self._build_url("hooks", str(hook_id), base_url=self._api)
            json = self._json(self._get(url), 200)
        return self._instance_or_null(OrganizationHook, json)

    @requires_auth
    def hooks(self, number=-1, etag=None):
        """Iterate over hooks registered on this organization.

        :param int number:
            (optional), number of hoks to return. Default: -1
            returns all hooks
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of hooks
        :rtype:
            :class:`~github3.orgs.OrganizationHook`
        """
        url = self._build_url("hooks", base_url=self._api)
        return self._iter(int(number), url, OrganizationHook, etag=etag)

    @requires_auth
    def invite(
        self, team_ids, invitee_id=None, email=None, role="direct_member"
    ):
        """Invite the user to join this organization.

        :param list[int] team_ids:
            (required), list of team identifiers to invite the user to
        :param int invitee_id:
            (required if email is not specified), the identifier for the user
            being invited
        :param str email:
            (required if invitee_id is not specified), the email address of
            the user being invited
        :param str role:
            (optional) role to provide to the invited user. Must be one of
        :returns:
            the created invitation
        :rtype:
            :class:`~github3.orgs.Invitation`
        """
        if (invitee_id is None and email is None) or (
            invitee_id is not None and email is not None
        ):
            raise ValueError(
                "One of either 'invitee_id' or 'email' must be specified"
            )
        if not team_ids:
            raise ValueError(
                "'team_ids' must be a non-empty list of integers"
            )
        data = {"team_ids": team_ids}
        if invitee_id is not None:
            data["invitee_id"] = invitee_id
        else:
            data["email"] = email
        if role not in self.invitation_roles:
            raise ValueError(
                "'role' must be one of {}".format(
                    ", ".join(sorted(self.invitation_roles))
                )
            )
        headers = {"Accept": "application/vnd.github.dazzler-preview.json"}
        data["role"] = role
        url = self._build_url("invitations", base_url=self._api)
        json = self._json(self._post(url, data=data, headers=headers), 200)
        return self._instance_or_null(Invitation, json)

    @requires_auth
    def cancel_invite(self, invitee_id):
        """Cancel the invitation using ``invitee_id``
        of the user from the organization.

        :param int invitee_id:
            the identifier for the user being invited, to cancel its invitation
        :returns: bool
        """
        url = self._build_url("invitations", invitee_id, base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    @requires_auth
    def failed_invitations(self):
        """List failed organization invitations.

        :returns: bool
        """
        url = self._build_url("failed_invitations", base_url=self._api)
        return self._json(self._get(url), 200, 404)

    def is_member(self, username):
        """Check if the user named ``username`` is a member.

        :param str username:
            name of the user you'd like to check
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("members", username, base_url=self._api)
        return self._boolean(self._get(url), 204, 404)

    def is_public_member(self, username):
        """Check if the user named ``username`` is a public member.

        :param str username:
            name of the user you'd like to check
        :returns:
            True if the user is a public member, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("public_members", username, base_url=self._api)
        return self._boolean(self._get(url), 204, 404)

    def all_events(self, username, number=-1, etag=None):
        """Iterate over all org events visible to the authenticated user.

        :param str username:
            (required), the username of the currently authenticated user.
        :param int number:
            (optional), number of events to return. Default: -1 iterates over
            all events available.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of events
        :rtype:
            :class:`~github3.events.Event`
        """
        url = self._build_url("users", username, "events", "orgs", self.login)
        return self._iter(int(number), url, Event, etag=etag)

    def public_events(self, number=-1, etag=None):
        """Iterate over public events for this org.

        :param int number:
            (optional), number of events to return. Default: -1 iterates over
            all events available.
        :param str etag:
            (optional), ETag from a previous request to the same
            endpoint
        :returns:
            generator of public events
        :rtype:
            :class:`~github3.events.Event`
        """
        url = self._build_url("events", base_url=self._api)
        return self._iter(int(number), url, Event, etag=etag)

    @requires_auth
    def invitations(self, number=-1, etag=None):
        """Iterate over outstanding invitations to this organization.

        :returns:
            generator of invitation objects
        :rtype:
            :class:`~github3.orgs.Invitation`
        """
        headers = {"Accept": "application/vnd.github.korra-preview"}
        url = self._build_url("invitations", base_url=self._api)
        return self._iter(
            int(number), url, Invitation, etag=etag, headers=headers
        )

    def members(self, filter=None, role=None, number=-1, etag=None):
        """Iterate over members of this organization.

        :param str filter:
            (optional), filter members returned by this method. Can be one of:
            ``"2fa_disabled"``, ``"all",``. Default: ``"all"``. Filtering by
            ``"2fa_disabled"`` is only available for organization owners with
            private repositories.
        :param str role:
            (optional), filter members returned by their role. Can be one of:
            ``"all"``, ``"admin"``, ``"member"``. Default: ``"all"``.
        :param int number:
            (optional), number of members to return. Default: -1 will return
            all available.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of members of this organization
        :rtype:
            :class:`~github3.users.ShortUser`
        """
        headers = {}
        params = {}
        if filter in self.members_filters:
            params["filter"] = filter
        if role in self.filterable_member_roles:
            params["role"] = role
            # TODO(sigmavirus24): Determine if the preview header is still
            # necessary
            headers["Accept"] = "application/vnd.github.ironman-preview+json"
        url = self._build_url("members", base_url=self._api)
        return self._iter(
            int(number),
            url,
            users.ShortUser,
            params=params,
            etag=etag,
            headers=headers,
        )

    @requires_auth
    def membership_for(self, username):
        """Obtain the membership status of ``username``.

        Implements
        https://developer.github.com/v3/orgs/members/#get-organization-membership

        :param str username:
            (required), username name of the user
        :returns:
            the membership information
        :rtype:
            :class:`~github3.orgs.Membership`
        """
        url = self._build_url("memberships", username, base_url=self._api)
        json = self._json(self._get(url), 200, 404)
        return self._instance_or_null(Membership, json)

    def public_members(self, number=-1, etag=None):
        """Iterate over public members of this organization.

        :param int number:
            (optional), number of members to return. Default: -1 will return
            all available.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of public members
        :rtype:
            :class:`~github3.users.ShortUser`
        """
        url = self._build_url("public_members", base_url=self._api)
        return self._iter(int(number), url, users.ShortUser, etag=etag)

    def project(self, id, etag=None):
        """Return the organization project with the given ID.

        :param int id:
            (required), ID number of the project
        :returns:
            requested project
        :rtype:
            :class:`~github3.projects.Project`
        """
        url = self._build_url("projects", id)
        json = self._json(self._get(url, headers=Project.CUSTOM_HEADERS), 200)
        return self._instance_or_null(Project, json)

    def projects(self, number=-1, etag=None):
        """Iterate over projects for this organization.

        :param int number:
            (optional), number of members to return. Default: -1 will return
            all available.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of organization projects
        :rtype:
            :class:`~github3.projects.Project`
        """
        url = self._build_url("projects", base_url=self._api)
        return self._iter(
            int(number),
            url,
            Project,
            etag=etag,
            headers=Project.CUSTOM_HEADERS,
        )

    @requires_auth
    def remove_membership(self, username):
        """Remove ``username`` from this organization.

        Unlike ``remove_member``, this will cancel a pending invitation.

        :param str username: (required), username of the member to remove
        :returns: bool
        """
        url = self._build_url("memberships", username, base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    def repositories(self, type="", number=-1, etag=None):
        """Iterate over repos for this organization.

        :param str type:
            (optional), accepted values: ('all', 'public', 'member', 'private',
            'forks', 'sources'), API default: 'all'
        :param int number:
            (optional), number of members to return. Default: -1 will return
            all available.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of repositories in this organization
        :rtype:
            :class:`~github3.repos.Repository`
        """
        url = self._build_url("repos", base_url=self._api)
        params = {}
        if type in ("all", "public", "member", "private", "forks", "sources"):
            params["type"] = type
        return self._iter(int(number), url, ShortRepository, params, etag)

    @requires_auth
    def teams(self, number=-1, etag=None):
        """Iterate over teams that are part of this organization.

        :param int number:
            (optional), number of teams to return. Default: -1 returns all
            available teams.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of this organization's teams
        :rtype:
            :class:`~github3.orgs.ShortTeam`
        """
        url = self._build_url("teams", base_url=self._api)
        return self._iter(int(number), url, ShortTeam, etag=etag)

    @requires_auth
    def publicize_member(self, username: str) -> bool:
        """Make ``username``'s membership in this organization public.

        :param str username:
            the name of the user whose membership you wish to publicize
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("public_members", username, base_url=self._api)
        return self._boolean(self._put(url), 204, 404)

    @requires_auth
    def remove_member(self, username: str) -> bool:
        """Remove the user named ``username`` from this organization.

        .. note::

            Only a user may publicize their own membership. See also:
            https://developer.github.com/v3/orgs/members/#publicize-a-users-membership

        :param str username:
            name of the user to remove from the org
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("members", username, base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    @requires_auth
    def remove_repository(
        self,
        repository: t.Union[Repository, ShortRepository, str],
        team_id: int,
    ):
        """Remove ``repository`` from the team with ``team_id``.

        :param str repository:
            (required), form: 'user/repo'
        :param int team_id:
            (required), the unique identifier of the team
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if int(team_id) > 0:
            url = self._build_url(
                "organizations",
                str(self.id),
                "team",
                str(team_id),
                "repos",
                str(repository),
            )
            return self._boolean(self._delete(url), 204, 404)
        return False

    @requires_auth
    def team(self, team_id: int) -> t.Optional[Team]:
        """Return the team specified by ``team_id``.

        :param int team_id:
            (required), unique id for the team
        :returns:
            the team identified by the id in this organization
        :rtype:
            :class:`~github3.orgs.Team`
        """
        json = None
        if int(team_id) > 0:
            url = self._build_url(
                "organizations", str(self.id), "team", str(team_id)
            )
            json = self._json(self._get(url), 200)
        return self._instance_or_null(Team, json)

    @requires_auth
    def team_by_name(self, team_slug: str) -> t.Optional[Team]:
        """Return the team specified by ``team_slug``.

        :param str team_slug:
            (required), slug for the team
        :returns:
            the team identified by the slug in this organization
        :rtype:
            :class:`~github3.orgs.Team`
        """
        url = self._build_url("teams", str(team_slug), base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(Team, json)


class Organization(_Organization):
    """Object for the full representation of a Organization.

    GitHub's API returns different amounts of information about orgs based
    upon how that information is retrieved. This object exists to represent
    the full amount of information returned for a specific org. For example,
    you would receive this class when calling
    :meth:`~github3.github.GitHub.organization`. To provide a clear
    distinction between the types of orgs, github3.py uses different classes
    with different sets of attributes.

    .. versionchanged:: 1.0.0

    This object includes all attributes on
    :class:`~github3.orgs.ShortOrganization` as well as the following:

    .. attribute:: blog

        If set, the URL of this organization's blog.

    .. attribute:: company

        The name of the company that is associated with this organization.

    .. attribute:: created_at

        A :class:`~datetime.datetime` instance representing the time and date
        when this organization was created.

    .. attribute:: email

        The email address associated with this organization.

    .. attribute:: followers_count

        The number of users following this organization. Organizations no
        longer have followers so this number will always be 0.

    .. attribute:: following_count

        The number of users this organization follows. Organizations no
        longer follow users so this number will always be 0.

    .. attribute:: html_url

        The URL used to view this organization in a browser.

    .. attribute:: location

        The location of this organization, e.g., New York, NY.

    .. attribute:: name

        The display name of this organization.

    .. attribute:: public_repos_count

        The number of public repositories owned by thi sorganization.
    """

    class_name = "Organization"

    def _update_attributes(self, org):
        super()._update_attributes(org)
        self.created_at = self._strptime(org["created_at"])
        self.followers_count = org["followers"]
        self.following_count = org["following"]
        self.html_url = org["html_url"]
        self.public_repos_count = org["public_repos"]
        # GitHub just doesn't return these 4 attributes sometimes. Compare
        # /orgs/github3py to /orgs/testgh3py
        self.blog = org.get("blog")
        self.company = org.get("company")
        self.email = org.get("email")
        self.location = org.get("location")
        self.name = org.get("name")


class ShortOrganization(_Organization):
    """Object for the shortened representation of an Organization.

    GitHub's API returns different amounts of information about orgs based
    upon how that information is retrieved. Often times, when iterating over
    several orgs, GitHub will return less information. To provide a clear
    distinction between the types of orgs, github3.py uses different classes
    with different sets of attributes.

    .. versionadded:: 1.0.0

    .. attribute:: avatar_url

        The URL of the avatar image for this organization.

    .. attribute:: description

        The user-provided description of this organization.

    .. attribute:: events_url

        The URL to retrieve the events related to this organization.

    .. attribute:: hooks_url

        The URL for the resource to manage organization hooks.

    .. attribute:: id

        The unique identifier for this organization across GitHub.

    .. attribute:: issues_url

        The URL to retrieve the issues across this organization's
        repositories.

    .. attribute:: login

        The unique username of the organization.

    .. attribute:: members_url

        The URL to retrieve the members of this organization.

    .. attribute:: public_members_urlt

        A :class:`uritemplate.URITemplate` that can be expanded to either list
        the public members of this organization or verify a user is a public
        member.

    .. attribute:: repos_url

        The URL to retrieve the repositories in this organization.

    .. attribute:: url

        The URL to retrieve this organization from the GitHub API.

    .. attribute:: type

        .. deprecated:: 1.0.0

            This will be removed in a future release.

        Previously returned by the API to indicate the type of the account.
    """

    class_name = "ShortOrganization"
    _refresh_to = Organization


class Invitation(models.GitHubCore):
    """Object representing an invitation to an organization.

    .. attribute:: created_at

        A :class:`~datetime.datetime` instance representing the time and date
        when this invitation was created.

    .. attribute:: email

        The email address of the user invited to the organization.

    .. attribute:: id

        The unique identifier for this invitation.

    .. attribute:: invitation_team_url

        The API URL to retrieve the :class:`~github3.orgs.ShortTeam` objects
        associated with this invitation.

    .. attribute:: inviter

        A :class:`~github3.users.ShortUser` representing the user who invited
        the user identified by ``login``.

    .. attribute:: login

        The username of the user invited to the organization.

    .. attribute:: team_count

        The number of teams involved in this invitation.
    """

    def _update_attributes(self, json):
        self.created_at = self._strptime(json["created_at"])
        self.email = json["email"]
        self.id = json["id"]
        self.inviter = users.ShortUser(json["inviter"], self)
        self.login = json["login"]
        # NOTE(sigmavirus24): GitHub docs claim these should be present but
        # in testing it is not.
        self.invitation_team_url = json.get("invitation_team_url")
        self.team_count = json.get("team_count")

    def _repr(self):
        return "<Invitation {} for [{}] from [{}]>".format(
            self.id, self.login, self.inviter.login
        )

    @requires_auth
    def teams(self):
        """Retrieve the list of teams associated with this invite.

        :returns:
            generator of teams associated with this invitation
        :rtype:
            :class:`~github3.orgs.ShortTeam`
        """
        return self._iter(
            -1,
            self.invitation_team_url,
            ShortTeam,
            headers={"Accept": "application/vnd.github.dazzler-preview.json"},
        )


class Membership(models.GitHubCore):
    """Object describing a user's membership in teams and organizations.

    .. attribute:: organization

        A :class:`~github3.orgs.ShortOrganization` instance representing the
        organization this membership is part of.

    .. attribute:: organization_url

        The URL of the organization resource in the API that this membership
        is part of.

    .. attribute:: state

        The state of this membership, e.g., active or pending.

    .. attribute:: active

        .. warning::

            This is a computed attribute, it is not returned by the API.

        A boolean attribute equivalent to ``self.state.lower() == 'active'``.

    .. attribute:: pending

        .. warning::

            This is a computed attribute, it is not returned by the API.

        A boolean attribute equivalent to ``self.state.lower() == 'pending'``.
    """

    def _repr(self):
        return f"<Membership [{self.organization}]>"

    def _update_attributes(self, membership):
        self._api = membership["url"]
        self.organization = ShortOrganization(
            membership["organization"], self
        )
        self.organization_url = membership["organization_url"]
        self.role = membership["role"]
        self.state = membership["state"]
        self.user = users.ShortUser(membership["user"], self)

        self.active = self.state
        if self.active:
            self.active = self.state.lower() == "active"
        self.pending = self.state
        if self.pending:
            self.pending = self.state.lower() == "pending"

    @requires_auth
    def edit(self, state):
        """Edit the user's membership.

        :param str state:
            (required), the state the membership should be in. Only accepts
            ``"active"``.
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        json = None
        if state and state.lower() == "active":
            data = dumps({"state": state.lower()})
            json = self._json(self._patch(self._api, data=data), 200)
        if json is not None:
            self._update_attributes(json)
            return True
        return False


class OrganizationHook(models.GitHubCore):
    """The representation of a hook on an organization.

    See also: https://developer.github.com/v3/orgs/hooks/

    This object has the following attributes:

    .. attribute:: active

        A boolean attribute describing whether the hook is active or not.

    .. attribute:: config

        A dictionary containing the configuration for this hook.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this hook was created.

    .. attribute:: events

        The list of events which trigger this hook.

    .. attribute:: id

        The unique identifier for this hook.

    .. attribute:: name

        The name provided to this hook.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this hook was updated.
    """

    def _update_attributes(self, hook, session=None):
        self._api = hook["url"]
        self.active = hook["active"]
        self.config = hook["config"]
        self.created_at = self._strptime(hook["created_at"])
        self.events = hook["events"]
        self.id = hook["id"]
        self.name = hook["name"]
        self.updated_at = self._strptime(hook["updated_at"])

    def _repr(self):
        return f"<OrganizationHook [{self.name}]>"

    @requires_auth
    def delete(self):
        """Delete this hook.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_auth
    def edit(self, config={}, events=[], active=True):
        """Edit this hook.

        :param dict config:
            (required), key-value pairs which act as settings for this hook
        :param list events:
            (optional), which events should this be triggered for
        :param bool active:
            (optional), should this event be active
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        data = {"config": config, "active": active}
        if events:
            data["events"] = events

        json = self._json(self._patch(self._api, data=dumps(data)), 200)

        if json:
            self._update_attributes(json)
            return True

        return False

    @requires_auth
    def ping(self):
        """Ping this hook.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("pings", base_url=self._api)
        return self._boolean(self._post(url), 204, 404)
