"""GitHub Pages related logic."""
from .. import models


class PagesInfo(models.GitHubCore):
    """Representation of the information about a GitHub pages website.

    .. attribute:: cname

        The cname in use for the pages site, if one is set.

    .. attribute:: custom_404

        A boolean attribute indicating whether the user configured a custom
        404 page for this site.

    .. attribute:: status

        The current status of the pages site, e.g., ``built``.
    """

    def _update_attributes(self, info):
        self._api = info["url"]
        self.cname = info["cname"]
        self.custom_404 = info["custom_404"]
        self.status = info["status"]

    def _repr(self):
        info = self.cname or ""
        if info:
            info += "/"
        info += self.status or ""
        return f"<Pages Info [{info}]>"


class PagesBuild(models.GitHubCore):
    """Representation of a single build of a GitHub pages website.

    .. attribute:: commit

        The SHA of the commit that triggered this build.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this build was created.

    .. attribute:: duration

        The time it spent processing this build.

    .. attribute:: error

        If this build errored, a dictionary containing the error message and
        details about the error.

    .. attribute:: pusher

        A :class:`~github3.users.ShortUser` representing the user who pushed
        the commit that triggered this build.

    .. attribute:: status

        The current statues of the build, e.g., ``building``.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this build was last updated.
    """

    def _update_attributes(self, build):
        from .. import users

        self._api = build["url"]
        self.commit = build["commit"]
        self.created_at = self._strptime(build["created_at"])
        self.duration = build["duration"]
        self.error = build["error"]
        self.pusher = users.ShortUser(build["pusher"], self)
        self.status = build["status"]
        self.updated_at = self._strptime(build["updated_at"])

    def _repr(self):
        return f"<Pages Build [{self.commit}/{self.status}]>"
