"""This module contains the Authorization object."""
from .decorators import requires_basic_auth
from .models import GitHubCore


class Authorization(GitHubCore):
    """Representation of an OAuth Authorization.

    See also: https://developer.github.com/v3/oauth_authorizations/

    This object has the following attributes:

    .. attribute:: app

        Details about the application the authorization was created for.

    .. attribute:: created_at

        A :class:`~datetime.datetime` representing when this authorization was
        created.

    .. attribute:: fingerprint

        .. versionadded:: 1.0

        The optional parameter that is used to allow an OAuth application to
        create multiple authorizations for the same user. This will help
        distinguish two authorizations for the same app.

    .. attribute:: hashed_token

        .. versionadded:: 1.0

        This is the base64 of the SHA-256 digest of the token.

        .. seealso::

            `Removing Authorization Tokens`_
                The blog post announcing the removal of :attr:`token`.

    .. attribute:: id

        The unique identifier for this authorization.

    .. attribute:: note_url

        The URL that points to a longer description about the purpose of this
        autohrization.

    .. attribute:: note

        The short note provided when this authorization was created.

    .. attribute:: scopes

        The list of scopes assigned to this token.

        .. seealso::

            `Scopes for OAuth Applications`_
                GitHub's documentation around available scopes and what they
                mean

    .. attribute:: token

        If this authorization was created, this will contain the full token.
        Otherwise, this attribute will be an empty string.

    .. attribute:: token_last_eight

        .. versionadded:: 1.0

        The last eight characters of the token. This allows users to identify
        a token after the initial retrieval.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` representing when this authorization was
        most recently updated.

    .. _Scopes for OAuth Applications:
        https://developer.github.com/apps/building-oauth-apps/scopes-for-oauth-apps/
    .. _Removing Authorization Tokens:
        https://developer.github.com/changes/2014-12-08-removing-authorizations-token/#what-should-you-do
    """

    def _update_attributes(self, auth):
        self._api = auth["url"]
        self.app = auth["app"]
        self.created_at = self._strptime(auth["created_at"])
        self.fingerprint = auth["fingerprint"]
        self.id = auth["id"]
        self.note_url = auth["note_url"]
        self.note = auth["note"]
        self.scopes = auth["scopes"]
        self.token = auth["token"]
        self.token_last_eight = auth["token_last_eight"]
        self.updated_at = self._strptime(auth["updated_at"])

    def _repr(self):
        return f"<Authorization [{self.name}]>"

    def _update(self, scopes_data, note, note_url):
        """Helper for add_scopes, replace_scopes, remove_scopes."""
        if note is not None:
            scopes_data["note"] = note
        if note_url is not None:
            scopes_data["note_url"] = note_url
        json = self._json(self._post(self._api, data=scopes_data), 200)

        if json:
            self._update_attributes(json)
            return True

        return False

    @requires_basic_auth
    def add_scopes(self, scopes, note=None, note_url=None):
        """Add the scopes to this authorization.

        .. versionadded:: 1.0

        :param list scopes:
            Adds these scopes to the ones present on this authorization
        :param str note:
            (optional), Note about the authorization
        :param str note_url:
            (optional), URL to link to when the user views the authorization
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._update({"add_scopes": scopes}, note, note_url)

    @requires_basic_auth
    def delete(self):
        """Delete this authorization.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_basic_auth
    def remove_scopes(self, scopes, note=None, note_url=None):
        """Remove the scopes from this authorization.

        .. versionadded:: 1.0

        :param list scopes:
            Remove these scopes from the ones present on this authorization
        :param str note:
            (optional), Note about the authorization
        :param str note_url:
            (optional), URL to link to when the user views the authorization
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._update({"rm_scopes": scopes}, note, note_url)

    @requires_basic_auth
    def replace_scopes(self, scopes, note=None, note_url=None):
        """Replace the scopes on this authorization.

        .. versionadded:: 1.0

        :param list scopes:
            Use these scopes instead of the previous list
        :param str note:
            (optional), Note about the authorization
        :param str note_url:
            (optional), URL to link to when the user views the authorization
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._update({"scopes": scopes}, note, note_url)
