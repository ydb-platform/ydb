"""This module contains everything relating to Users."""
import typing as t
from json import dumps

from uritemplate import URITemplate

from . import models
from .decorators import requires_auth
from .events import Event
from github3.auths import Authorization


class GPGKey(models.GitHubCore):
    """The object representing a user's GPG key.

    .. versionadded:: 1.2.0

    Please see GitHub's `GPG Key Documentation` for more information.

    .. _GPG Key Documentation:
        https://developer.github.com/v3/users/gpg_keys/

    .. attribute:: can_certify

        Whether this GPG key can be used to sign a key.

    .. attribute:: can_encrypt_comms

        Whether this GPG key can be used to encrypt communications.

    .. attribute:: can_encrypt_storage

        Whether this GPG key can be used to encrypt storage.

    .. attribute:: can_sign

        Whether this GPG key can be used to sign some data.

    .. attribute:: created_at

        A :class:`~datetime.datetime` representing the date and time when
        this GPG key was created.

    .. attribute:: emails

        A list of :class:`~github3.users.ShortEmail` attached to this GPG
        key.

    .. attribute:: expires_at

        A :class:`~datetime.datetime` representing the date and time when
        this GPG key will expire.

    .. attribute:: id

        The unique identifier of this GPG key.

    .. attribute:: key_id

        A hexadecimal string that identifies this GPG key.

    .. attribute:: primary_key_id

        The unique identifier of the primary key of this GPG key.

    .. attribute:: public_key

        The public key contained in this GPG key. This is not a GPG formatted
        key, and is not suitable to be used directly in programs like GPG.

    .. attribute:: subkeys

        A list of :class:`~github3.users.GPGKey` of the subkeys of this GPG
        key.
    """

    def _update_attributes(self, key):
        self.can_certify = key["can_certify"]
        self.can_encrypt_comms = key["can_encrypt_comms"]
        self.can_encrypt_storage = key["can_encrypt_storage"]
        self.can_sign = key["can_sign"]
        self.created_at = self._strptime(key["created_at"])
        self.emails = [ShortEmail(email, self) for email in key["emails"]]
        self.expires_at = self._strptime(key["expires_at"])
        self.id = key["id"]
        self.key_id = key["key_id"]
        self.primary_key_id = key["primary_key_id"]
        self.public_key = key["public_key"]
        self.subkeys = [GPGKey(subkey, self) for subkey in key["subkeys"]]

    def _repr(self):
        return f"<GPG Key [{self.key_id}]>"

    def __str__(self):
        return self.key_id

    @requires_auth
    def delete(self):
        """Delete this GPG key.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("user", "gpg_keys", self.id)
        return self._boolean(self._delete(url), 204, 404)


class Key(models.GitHubCore):
    """The object representing a user's SSH key.

    Please see GitHub's `Key Documentation`_ for more information.

    .. _Key Documentation:
        http://developer.github.com/v3/users/keys/

    .. versionchanged:: 1.0.0

        Removed ``title`` attribute

    .. attribute:: key

        A string containing the actual text of the SSH Key

    .. attribute:: id

        GitHub's unique ID for this key
    """

    def _update_attributes(self, key, session=None):
        self._api = key.get("url")
        self.key = key["key"]
        self.id = key["id"]

    def _repr(self):
        return f"<User Key [{self.title}]>"

    def __str__(self):
        return self.key

    @requires_auth
    def delete(self):
        """Delete this key."""
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_auth
    def update(self, title, key):
        """Update this key.

        .. warning::

            As of 20 June 2014, the API considers keys to be immutable.
            This will soon begin to return MethodNotAllowed errors.

        :param str title: (required), title of the key
        :param str key: (required), text of the key file
        :returns: bool
        """
        json = None
        if title and key:
            data = {"title": title, "key": key}
            json = self._json(self._patch(self._api, data=dumps(data)), 200)
        if json:
            self._update_attributes(json)
            return True
        return False


class Plan(models.GitHubCore):
    """The :class:`Plan <Plan>` object.

    Please see GitHub's `Authenticated User`_ documentation for more details.

    .. _Authenticated User:
        http://developer.github.com/v3/users/#get-the-authenticated-user

    .. attribute:: collaborators_count

        .. versionchanged:: 1.0.0

        The number of collaborators allowed on this plan

    .. attribute:: name

        The name of the plan on GitHub

    .. attribute:: private_repos_count

        .. versionchanged:: 1.0.0

        The number of allowed private repositories

    .. attribute:: space

        The amount of space allotted by this plan
    """

    def _update_attributes(self, plan):
        self.collaborators = plan["collaborators"]
        self.name = plan["name"]
        self.private_repos_count = plan["private_repos"]
        self.space = plan["space"]

    def _repr(self):
        return f"<Plan [{self.name}]>"  # (No coverage)

    def __str__(self):
        return self.name

    def is_free(self):
        """Check if this is a free plan.

        :returns: bool
        """
        return self.name == "free"  # (No coverage)


class _Email(models.GitHubCore):
    """Base email object."""

    class_name = "_Email"

    def _update_attributes(self, email):
        self.email = email["email"]
        self.verified = email["verified"]

    def _repr(self):
        return f"<{self.class_name} [{self.email}]>"

    def __str__(self):
        return self.email


class ShortEmail(_Email):
    """The object used to represent an email attached to a GPG key.

    This object has the following attributes:

    .. attribute:: email

        The email address as a string

    .. attribute:: verified

        A boolean value representing whether the address has been verified or
        not
    """

    class_name = "ShortEmail"


class Email(_Email):
    """The object used to represent an AuthenticatedUser's email.

    Please see GitHub's `Emails documentation`_ for more information.

    .. _Emails documentation:
        https://developer.github.com/v3/users/emails/

    This object has all of the attributes of :class:`ShortEmail` as well as
    the following attributes:

    .. attribute:: primary

        A boolean value representing whether the address is the primary
        address for the user or not

    .. attribute:: visibility

        A string value representing whether an authenticated user can view the
        email address. Use ``public`` to allow it, ``private`` to disallow it.
    """

    def _update_attributes(self, email):
        super()._update_attributes(email)
        self.primary = email["primary"]
        self.visibility = email["visibility"]

    def _repr(self):
        return f"<Email [{self.email}]>"

    def __str__(self):
        return self.email


class _User(models.GitHubCore):
    """The :class:`User <User>` object.

    This handles and structures information in the `User section`_.

    Two user instances can be checked like so::

        u1 == u2
        u1 != u2

    And is equivalent to::

        u1.id == u2.id
        u1.id != u2.id

    .. _User section:
        http://developer.github.com/v3/users/
    """

    class_name = "_User"

    def _update_attributes(self, user):
        self.avatar_url = user["avatar_url"]
        self.events_urlt = URITemplate(user["events_url"])
        self.followers_url = user["followers_url"]
        self.following_urlt = URITemplate(user["following_url"])
        self.gists_urlt = URITemplate(user["gists_url"])
        self.gravatar_id = user["gravatar_id"]
        self.html_url = user["html_url"]
        self.id = user["id"]
        self.login = user["login"]
        self.organizations_url = user["organizations_url"]
        self.received_events_url = user["received_events_url"]
        self.repos_url = user["repos_url"]
        self.site_admin = user.get("site_admin")
        self.starred_urlt = URITemplate(user["starred_url"])
        self.subscriptions_url = user["subscriptions_url"]
        self.type = user["type"]
        self.url = self._api = user["url"]
        self._uniq = self.id

    def __str__(self):
        return self.login

    def _repr(self):
        full_name = ""
        name = getattr(self, "name", None)
        if name is not None:
            full_name = f":{name}"
        return "<{s.class_name} [{s.login}{full_name}]>".format(
            s=self, full_name=full_name
        )

    def is_assignee_on(self, username, repository):
        """Check if this user can be assigned to issues on username/repository.

        :param str username: owner's username of the repository
        :param str repository: name of the repository
        :returns: True if the use can be assigned, False otherwise
        :rtype: :class:`bool`
        """
        url = self._build_url(
            "repos", username, repository, "assignees", self.login
        )
        return self._boolean(self._get(url), 204, 404)

    def is_following(self, username):
        """Check if this user is following ``username``.

        :param str username: (required)
        :returns: bool

        """
        url = self.following_urlt.expand(other_user=username)
        return self._boolean(self._get(url), 204, 404)

    def events(self, public=False, number=-1, etag=None):
        r"""Iterate over events performed by this user.

        :param bool public: (optional), only list public events for the
            authenticated user
        :param int number: (optional), number of events to return. Default: -1
            returns all available events.
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`Event <github3.events.Event>`\ s
        """
        path = ["events"]
        if public:
            path.append("public")
        url = self._build_url(*path, base_url=self._api)
        return self._iter(int(number), url, Event, etag=etag)

    def followers(self, number=-1, etag=None):
        r"""Iterate over the followers of this user.

        :param int number: (optional), number of followers to return. Default:
            -1 returns all available
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`User <User>`\ s
        """
        url = self._build_url("followers", base_url=self._api)
        return self._iter(int(number), url, ShortUser, etag=etag)

    def following(self, number=-1, etag=None):
        r"""Iterate over the users being followed by this user.

        :param int number: (optional), number of users to return. Default: -1
            returns all available users
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`User <User>`\ s
        """
        url = self._build_url("following", base_url=self._api)
        return self._iter(int(number), url, ShortUser, etag=etag)

    def gpg_keys(self, number=-1, etag=None):
        r"""Iterate over the GPG keys of this user.

        .. versionadded:: 1.2.0

        :param int number: (optional), number of GPG keys to return. Default:
            -1 returns all available GPG keys
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`GPGKey <GPGKey>`\ s
        """
        url = self._build_url("gpg_keys", base_url=self._api)
        return self._iter(int(number), url, GPGKey, etag=etag)

    def keys(self, number=-1, etag=None):
        r"""Iterate over the public keys of this user.

        .. versionadded:: 0.5

        :param int number: (optional), number of keys to return. Default: -1
            returns all available keys
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`Key <Key>`\ s
        """
        url = self._build_url("keys", base_url=self._api)
        return self._iter(int(number), url, Key, etag=etag)

    @requires_auth
    def organization_events(self, org, number=-1, etag=None):
        r"""Iterate over events from the user's organization dashboard.

        .. note:: You must be authenticated to view this.

        :param str org: (required), name of the organization
        :param int number: (optional), number of events to return. Default: -1
            returns all available events
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`Event <github3.events.Event>`\ s
        """
        url = ""
        if org:
            url = self._build_url("events", "orgs", org, base_url=self._api)
        return self._iter(int(number), url, Event, etag=etag)

    def received_events(self, public=False, number=-1, etag=None):
        r"""Iterate over events that the user has received.

        If the user is the authenticated user, you will see private and public
        events, otherwise you will only see public events.

        :param bool public: (optional), determines if the authenticated user
            sees both private and public or just public
        :param int number: (optional), number of events to return. Default: -1
            returns all events available
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`Event <github3.events.Event>`\ s
        """
        path = ["received_events"]
        if public:
            path.append("public")
        url = self._build_url(*path, base_url=self._api)
        return self._iter(int(number), url, Event, etag=etag)

    def organizations(self, number=-1, etag=None):
        r"""Iterate over organizations the user is member of.

        :param int number: (optional), number of organizations to return.
            Default: -1 returns all available organization
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of
            :class:`ShortOrganization <github3.orgs.ShortOrganization>`\ s
        """
        # Import here, because a toplevel import causes an import loop
        from .orgs import ShortOrganization

        url = self._build_url("orgs", base_url=self._api)
        return self._iter(int(number), url, ShortOrganization, etag=etag)

    def starred_repositories(
        self, sort=None, direction=None, number=-1, etag=None
    ):
        """Iterate over repositories starred by this user.

        .. versionchanged:: 0.5
           Added sort and direction parameters (optional) as per the change in
           GitHub's API.

        :param int number: (optional), number of starred repos to return.
            Default: -1, returns all available repos
        :param str sort: (optional), either 'created' (when the star was
            created) or 'updated' (when the repository was last pushed to)
        :param str direction: (optional), either 'asc' or 'desc'. Default:
            'desc'
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`~github3.repos.repo.StarredRepository`
        """
        from .repos import Repository, StarredRepository

        params = {"sort": sort, "direction": direction}
        self._remove_none(params)
        url = self.starred_urlt.expand(owner=None, repo=None)
        return self._iter(
            int(number),
            url,
            StarredRepository,
            params,
            etag,
            headers=Repository.STAR_HEADERS,
        )

    def subscriptions(self, number=-1, etag=None):
        """Iterate over repositories subscribed to by this user.

        :param int number: (optional), number of subscriptions to return.
            Default: -1, returns all available
        :param str etag: (optional), ETag from a previous request to the same
            endpoint
        :returns: generator of :class:`Repository <github3.repos.Repository>`
        """
        from .repos import ShortRepository

        url = self._build_url("subscriptions", base_url=self._api)
        return self._iter(int(number), url, ShortRepository, etag=etag)

    @requires_auth
    def rename(self, login):
        """Rename the user.

        .. note::

            This is only available for administrators of a GitHub Enterprise
            instance.

        :param str login: (required), new name of the user
        :returns: bool
        """
        url = self._build_url("admin", "users", self.login)
        payload = {"login": login}
        resp = self._boolean(self._patch(url, data=payload), 202, 403)
        return resp

    @requires_auth
    def impersonate(self, scopes=None):
        """Obtain an impersonation token for the user.

        The retrieved token will allow impersonation of the user.
        This is only available for admins of a GitHub Enterprise instance.

        :param list scopes: (optional), areas you want this token to apply to,
            i.e., 'gist', 'user'
        :returns: :class:`Authorization <Authorization>`
        """
        url = self._build_url("admin", "users", self.login, "authorizations")
        data = {}

        if scopes:
            data["scopes"] = scopes

        json = self._json(self._post(url, data=data), 201)

        return self._instance_or_null(Authorization, json)

    @requires_auth
    def revoke_impersonation(self):
        """Revoke all impersonation tokens for the current user.

        This is only available for admins of a GitHub Enterprise instance.

        :returns: bool -- True if successful, False otherwise
        """
        url = self._build_url("admin", "users", self.login, "authorizations")

        return self._boolean(self._delete(url), 204, 403)

    @requires_auth
    def promote(self):
        """Promote a user to site administrator.

        This is only available for admins of a GitHub Enterprise instance.

        :returns: bool -- True if successful, False otherwise
        """
        url = self._build_url("site_admin", base_url=self._api)

        return self._boolean(self._put(url), 204, 403)

    @requires_auth
    def demote(self):
        """Demote a site administrator to simple user.

        You can demote any user account except your own.

        This is only available for admins of a GitHub Enterprise instance.

        :returns: bool -- True if successful, False otherwise
        """
        url = self._build_url("site_admin", base_url=self._api)

        return self._boolean(self._delete(url), 204, 403)

    @requires_auth
    def suspend(self):
        """Suspend the user.

        This is only available for admins of a GitHub Enterprise instance.

        This API is disabled if you use LDAP, check the GitHub API dos for more
        information.

        :returns: bool -- True if successful, False otherwise
        """
        url = self._build_url("suspended", base_url=self._api)

        return self._boolean(self._put(url), 204, 403)

    @requires_auth
    def unsuspend(self):
        """Unsuspend the user.

        This is only available for admins of a GitHub Enterprise instance.

        This API is disabled if you use LDAP, check the GitHub API dos for more
        information.

        :returns: bool -- True if successful, False otherwise
        """
        url = self._build_url("suspended", base_url=self._api)

        return self._boolean(self._delete(url), 204, 403)

    @requires_auth
    def delete(self):
        """Delete the user.

        Per GitHub API documentation, it is often preferable to suspend the
        user.

        .. note::

            This is only available for admins of a GitHub Enterprise instance.

        :returns: bool -- True if successful, False otherwise
        """
        url = self._build_url("admin", "users", self.login)
        return self._boolean(self._delete(url), 204, 403)


class User(_User):
    """Object for the full representation of a User.

    GitHub's API returns different amounts of information about users based
    upon how that information is retrieved. This object exists to represent
    the full amount of information returned for a specific user. For example,
    you would receive this class when calling
    :meth:`~github3.github.GitHub.user`. To provide a clear distinction
    between the types of users, github3.py uses different classes with
    different sets of attributes.

    This object no longer contains information about the currently
    authenticated user (e.g., :meth:`~github3.github.GitHub.me`).

    .. versionchanged:: 1.0.0

    This object contains all of the attributes available on
    :class:`~github3.users.ShortUser` as well as the following:

    .. attribute:: bio

        The markdown formatted User's biography

    .. attribute:: blog

        The URL of the user's blog

    .. attribute:: company

        The name or GitHub handle of the user's company

    .. attribute:: created_at

        A parsed :class:`~datetime.datetime` object representing the date the
        user was created

    .. attribute:: email

        The email address the user has on their public profile page

    .. attribute:: followers_count

        The number of followers of this user

    .. attribute:: following_count

        The number of users this user follows

    .. attribute:: hireable

        Whether or not the user has opted into GitHub jobs advertising

    .. attribute:: location

        The location specified by the user on their public profile

    .. attribute:: name

        The name specified by their user on their public profile

    .. attribute:: public_gists_count

        The number of public gists owned by this user

    .. attribute: public_repos_count

        The number of public repositories owned by this user

    .. attribute:: updated_at

        A parsed :class:`~datetime.datetime` object representing the date
        the user was last updated
    """

    class_name = "User"

    def _update_attributes(self, user):
        super()._update_attributes(user)
        self.bio = user["bio"]
        self.blog = user["blog"]
        self.company = user["company"]
        self.created_at = self._strptime(user["created_at"])
        self.email = user["email"]
        self.followers_count = user["followers"]
        self.following_count = user["following"]
        self.hireable = user["hireable"]
        self.location = user["location"]
        self.name = user["name"]
        self.public_gists_count = user["public_gists"]
        self.public_repos_count = user["public_repos"]
        self.updated_at = self._strptime(user["updated_at"])


class ShortUser(_User):
    """Object for the shortened representation of a User.

    GitHub's API returns different amounts of information about users based
    upon how that information is retrieved. Often times, when iterating over
    several users, GitHub will return less information. To provide a clear
    distinction between the types of users, github3.py uses different classes
    with different sets of attributes.

    .. versionadded:: 1.0.0


    .. attribute:: avatar_url

        The URL of the avatar (possibly from Gravatar)

    .. attribute:: events_urlt

        A URITemplate object from ``uritemplate`` that can be used to generate
        an events URL

    .. attribute:: followers_url

        A string representing the resource to retrieve a User's followers

    .. attribute:: following_urlt

        A URITemplate object from ``uritemplate`` that can be used to generate
        the URL to check if this user is following ``other_user``

    .. attribute:: gists_urlt

        A URITemplate object from ``uritemplate`` that can be used to generate
        the URL to retrieve a Gist by its id

    .. attribute:: gravatar_id

        The identifier for the user's gravatar

    .. attribute:: html_url

        The URL of the user's publicly visible profile. For example,
        ``https://github.com/sigmavirus24``

    .. attribute:: id

        The unique ID of the account

    .. attribute:: login

        The username of the user, e.g., ``sigmavirus24``

    .. attribute:: organizations_url

        A string representing the resource to retrieve the organizations to
        which a user belongs

    .. attribute:: received_events_url

        A string representing the resource to retrieve the events a user
        received

    .. attribute:: repos_url

        A string representing the resource to list a user's repositories

    .. attribute:: site_admin

        A boolean attribute indicating whether the user is a member of
        GitHub's staff

    .. attribute:: starred_urlt

        A URITemplate object from ``uritemplate`` that can be used to generate
        a URL to retrieve whether the user has starred a repository.

    .. attribute:: subscriptions_url

        A string representing the resource to list a user's subscriptions

    .. attribute:: type

        A string representing the type of User account this. In all cases
        should be "User"

    .. attribute:: url

        A string of this exact resource retrievable from GitHub's API
    """

    class_name = "ShortUser"
    _refresh_to = User


class Stargazer(_User):
    """Object representing a user that has starred a repository.

    .. versionadded:: 3.0.0

    This object contains all of the attributes available on
    :class:`~github3.users.ShortUser` as well as the following:

    .. attribute:: starred_at

        The time and date that the user starred the repository this was
        queried from.
    """

    class_name = "Stargazer"
    _refresh_to = User

    def _update_attributes(self, stargazer):
        super()._update_attributes(stargazer["user"])
        self.starred_at = self._strptime(stargazer["starred_at"])


class AuthenticatedUser(User):
    """Object to represent the currently authenticated user.

    This is returned by :meth:`~github3.github.GitHub.me`. It contains the
    extra informtation that is not returned for other users such as the
    currently authenticated user's plan and private email information.

    .. versionadded:: 1.0.0

    .. versionchanged:: 1.0.0

        The ``total_private_gists`` attribute is no longer returned by
        GitHub's API and so is removed.

    This object has all of the same attribute as the
    :class:`~github3.users.ShortUser` and :class:`~github3.users.User` objects
    as well as:

    .. attribute:: disk_usage

        The amount of repository space that has been used by you, the user

    .. attribute:: owned_private_repos_count

        The number of private repositories owned by you, the user

    .. attribute:: plan

        .. note::

            When used with a Github Enterprise instance <= 2.12.7, this
            attribute will not be returned. To handle these situations
            sensitively, the attribute will be set to ``None``.
            Repositories may still have a license associated with them
            in these cases.

        The name of the plan that you, the user, have purchased
    """

    class_name = "AuthenticatedUser"

    def _update_attributes(self, user):
        super()._update_attributes(user)
        self.disk_usage = user.get("disk_usage")
        self.owned_private_repos_count = user.get("owned_private_repos")
        self.total_private_repos_count = user.get("total_private_repos")
        self.plan = user.get("plan")
        if self.plan is not None:
            self.plan = Plan(self.plan, self)


class Collaborator(_User):
    """Object for the representation of a collaborator.

    .. versionadded:: 1.1.0

    When retrieving a repository's contributors, GitHub returns the same
    information as a :class:`~github3.users.ShortUser` with an additional
    attribute:

    .. attribute:: permissions

        Admin, push, and pull permissions of a collaborator
    """

    class_name = "Collaborator"
    _refresh_to = User

    def _update_attributes(self, user):
        super()._update_attributes(user)

        self.permissions = user["permissions"]


class Contributor(_User):
    """Object for the specialized representation of a contributor.

    .. versionadded:: 1.0.0

    .. versionchanged:: 1.1.0

        This class now refreshes to a :class:`~github3.users.User`.

        The attribute ``contributions`` was renamed to ``contributions_count``,
        the documentation already declared it as ``contributions_count``, it
        was the implementation now reflects this as well.

    When retrieving a repository's contributors, GitHub returns the same
    information as a :class:`~github3.users.ShortUser` with an additional
    attribute:

    .. attribute:: contributions_count

        The number of contributions a contributor has made to the repository

    """

    class_name = "Contributor"
    _refresh_to = User

    def _update_attributes(self, contributor):
        super()._update_attributes(contributor)
        self.contributions_count = contributor["contributions"]


UserLike = t.Union[
    ShortUser, User, AuthenticatedUser, Collaborator, Contributor, str
]
