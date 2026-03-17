"""This module contains the Contents object."""
from base64 import b64decode
from base64 import b64encode
from json import dumps

from .. import models
from ..decorators import requires_auth
from ..git import Commit


class Contents(models.GitHubCore):
    """A representation of file contents returned via the API.

    See also: http://developer.github.com/v3/repos/contents/

    This object has the following attributes:

    .. attribute:: content

        The body of the file. If this is present, it may be base64 encoded.

    .. attribute:: encoding

        The encoding used on the :attr:`content` when returning the data from
        the API, e.g., ``base64``. If :attr:`content` is not present this will
        not be present either.

    .. attribute:: decoded

        .. note:: This is a computed attribute which isn't returned by the API.
        .. versionchanged:: 0.5.2

        Decoded content of the file as a bytes object. If we try to decode
        to character set for you, we might encounter an exception which
        will prevent the object from being created. On python2 this is the
        same as a string, but on python3 you should call the decode method
        with the character set you wish to use, e.g.,
        ``content.decoded.decode('utf-8')``.

    .. attribute:: git_url

        The URL for the Git API pertaining to this file.

    .. attribute:: html_url

        The URL to open this file in a browser.

    .. attribute:: links

        A dictionary of links returned about the contents and related
        resources.

    .. attribute:: name

        The name of the file.

    .. attribute:: path

        The path to this file.

    .. attribute:: sha

        The SHA1 of the contents of this file.

    .. attribute:: size

        The size of file in bytes.

    .. attribute:: submodule_git_url

        The URL of the git submodule (if this is a git submodule).

    .. attribute:: target

        If the file is a symlink, this will be present and provides the type
        of file that the symlink points to.

    .. attribute:: type

        Type of content, e.g., ``'file'``, ``'symlink'``, or ``'submodule'``.
    """

    def _update_attributes(self, content):
        self._api = content["url"]
        self.content = content.get("content")
        self.encoding = content.get("encoding")
        self.decoded = self.content
        if self.encoding == "base64" and self.content is not None:
            self.decoded = b64decode(self.content.encode())
        self.download_url = content["download_url"]
        self.git_url = content["git_url"]
        self.html_url = content["html_url"]
        self.links = content["_links"]
        self.name = content["name"]
        self.path = content["path"]
        self._uniq = self.sha = content["sha"]
        self.size = content["size"]
        self.submodule_git_url = content.get("submodule_git_url")
        self.target = content.get("target")
        self.type = content["type"]

    def _repr(self):
        return f"<Contents [{self.path}]>"

    def __eq__(self, other):
        return self.decoded == other

    def __ne__(self, other):
        return self.sha != other

    @requires_auth
    def delete(self, message, branch=None, committer=None, author=None):
        """Delete this file.

        :param str message:
            (required), commit message to describe the removal
        :param str branch:
            (optional), branch where the file exists.
            Defaults to the default branch of the repository.
        :param dict committer:
            (optional), if no information is given the authenticated user's
            information will be used. You must specify both a name and email.
        :param dict author:
            (optional), if omitted this will be filled in with committer
            information. If passed, you must specify both a name and email.
        :returns:
            dictionary of new content and associated commit
        :rtype:
            :class:`~github3.repos.contents.Contents` and
            :class:`~github3.git.Commit`
        """
        json = {}
        if message:
            data = {
                "message": message,
                "sha": self.sha,
                "branch": branch,
                "committer": validate_commmitter(committer),
                "author": validate_commmitter(author),
            }
            self._remove_none(data)
            json = self._json(self._delete(self._api, data=dumps(data)), 200)
            if json and "commit" in json:
                json["commit"] = Commit(json["commit"], self)
            if json and "content" in json:
                json["content"] = self._instance_or_null(
                    Contents, json["content"]
                )
        return json

    @requires_auth
    def update(
        self, message, content, branch=None, committer=None, author=None
    ):
        """Update this file.

        :param str message:
            (required), commit message to describe the update
        :param str content:
            (required), content to update the file with
        :param str branch:
            (optional), branch where the file exists.
            Defaults to the default branch of the repository.
        :param dict committer:
            (optional), if no information is given the authenticated user's
            information will be used. You must specify both a name and email.
        :param dict author:
            (optional), if omitted this will be filled in with committer
            information. If passed, you must specify both a name and email.
        :returns:
            dictionary containing the updated contents object and the
            commit in which it was changed.
        :rtype:
            dictionary of :class:`~github3.repos.contents.Contents` and
            :class:`~github3.git.Commit`
        """
        if content and not isinstance(content, bytes):
            raise ValueError(  # (No coverage)
                "content must be a bytes object"
            )  # (No coverage)

        json = None
        if message and content:
            content = b64encode(content).decode("utf-8")
            data = {
                "message": message,
                "content": content,
                "branch": branch,
                "sha": self.sha,
                "committer": validate_commmitter(committer),
                "author": validate_commmitter(author),
            }
            self._remove_none(data)
            json = self._json(self._put(self._api, data=dumps(data)), 200)
            if json and "content" in json:
                self._update_attributes(json["content"])
                json["content"] = self
            if json and "commit" in json:
                json["commit"] = Commit(json["commit"], self)
        return json


def validate_commmitter(d):
    """Validate that there are enough details in the dictionary.

    When sending data to GitHub, we need to ensure we're sending the name and
    email for committer and author data.
    """
    if d and d.get("name") and d.get("email"):
        return d
    return None
