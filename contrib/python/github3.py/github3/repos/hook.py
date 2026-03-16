"""This module contains only the Hook object for GitHub's Hook API."""
from json import dumps

from ..decorators import requires_auth
from ..models import GitHubCore


class Hook(GitHubCore):
    """The representation of a hook on a repository.

    See also: http://developer.github.com/v3/repos/hooks/

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
        return f"<Hook [{self.name}]>"

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
    def edit(
        self, config={}, events=[], add_events=[], rm_events=[], active=True
    ):
        """Edit this hook.

        :param dict config:
            (optional), key-value pairs of settings for this hook
        :param list events:
            (optional), which events should this be triggered for
        :param list add_events:
            (optional), events to be added to the list of events that this hook
            triggers for
        :param list rm_events:
            (optional), events to be removed from the list of events that this
            hook triggers for
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

        if add_events:
            data["add_events"] = add_events

        if rm_events:
            data["remove_events"] = rm_events

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

    @requires_auth
    def test(self):
        """Test this hook.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("tests", base_url=self._api)
        return self._boolean(self._post(url), 204, 404)
