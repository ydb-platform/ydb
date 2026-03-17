"""Invitation related logic."""
from json import dumps

from .. import models
from .. import users
from ..decorators import requires_auth


class Invitation(models.GitHubCore):
    """Representation of an invitation to collaborate on a repository.

    .. attribute:: created_at

        A :class:`~datetime.datetime` instance representing the time and date
        when this invitation was created.

    .. attribute:: html_url

        The URL to view this invitation in a browser.

    .. attribute:: id

        The unique identifier for this invitation.

    .. attribute:: invitee

        A :class:`~github3.users.ShortUser` representing the user who was
        invited to collaborate.

    .. attribute:: inviter

        A :class:`~github3.users.ShortUser` representing the user who invited
        the ``invitee``.

    .. attribute:: permissions

        The permissions that the ``invitee`` will have on the repository. Valid
        values are ``read``, ``write``, and ``admin``.

    .. attribute:: repository

        A :class:`~github3.repos.ShortRepository` representing the repository
        on which the ``invitee` was invited to collaborate.

    .. attribute:: url

        The API URL that the ``invitee`` can use to respond to the invitation.
        Note that the ``inviter`` must use a different URL, not returned by
        the API, to update or cancel the invitation.
    """

    class_name = "Invitation"
    allowed_permissions = frozenset(["admin", "read", "write"])

    def _update_attributes(self, invitation):
        from . import repo

        self.created_at = self._strptime(invitation["created_at"])
        self.html_url = invitation["html_url"]
        self.id = invitation["id"]
        self.invitee = users.ShortUser(invitation["invitee"], self)
        self.inviter = users.ShortUser(invitation["inviter"], self)
        self.permissions = invitation["permissions"]
        self.repository = repo.ShortRepository(invitation["repository"], self)
        self.url = invitation["url"]

    def _repr(self):
        return f"<Invitation [{self.repository.full_name}]>"

    @requires_auth
    def accept(self):
        """Accept this invitation.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._patch(self.url), 204, 404)

    @requires_auth
    def decline(self):
        """Decline this invitation.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self.url), 204, 404)

    @requires_auth
    def delete(self):
        """Delete this invitation.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url(
            "invitations", self.id, base_url=self.repository.url
        )
        return self._boolean(self._delete(url), 204, 404)

    @requires_auth
    def update(self, permissions):
        """Update this invitation.

        :param str permissions:
            (required), the permissions that will be granted by this invitation
            once it has been updated. Options: 'admin', 'read', 'write'
        :returns:
            The updated invitation
        :rtype:
            :class:`~github3.repos.invitation.Invitation`
        """
        if permissions not in self.allowed_permissions:
            raise ValueError(
                "'permissions' must be one of {}".format(
                    ", ".join(sorted(self.allowed_permissions))
                )
            )
        url = self._build_url(
            "invitations", self.id, base_url=self.repository.url
        )
        data = {"permissions": permissions}
        json = self._json(self._patch(url, data=dumps(data)), 200)
        return self._instance_or_null(Invitation, json)
