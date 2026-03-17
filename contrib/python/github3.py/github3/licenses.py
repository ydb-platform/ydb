"""
This module contains the classes relating to licenses.

See also: https://developer.github.com/v3/licenses/
"""
import base64

from . import models


class _License(models.GitHubCore):
    """Base license object."""

    class_name = "_License"

    def _update_attributes(self, license):
        self._api = license["url"]
        self.key = license["key"]
        self.name = license["name"]
        self.spdx_id = license["spdx_id"]

    def _repr(self):
        return f"<{self.class_name} [{self.name}]>"


class License(_License):
    """This object represents a license as returned by the GitHub API.

    See https://developer.github.com/v3/licenses/ for more information.

    This object has all of the attributes of :class:`ShortLicense` as well as
    the following attributes:

    .. attribute:: body

        The full text of this license.

    .. attribute:: conditions

        A list of the conditions of this license.

    .. attribute:: description

        The short description of this license.

    .. attribute:: featured

        A boolean attribute describing whether this license is featured on
        GitHub or not.

    .. attribute:: html_url

        The URL to view this license on GitHub.

    .. attribute:: implementation

        The short description of how a user applies this license to their
        original work.

    .. attribute:: limitations

        A list of limitations of this license.

    .. attribute:: permissions

        A list of the permissions granted by this license.
    """

    class_name = "License"

    def _update_attributes(self, license):
        super()._update_attributes(license)
        self.body = license["body"]
        self.conditions = license["conditions"]
        self.description = license["description"]
        self.featured = license["featured"]
        self.html_url = license["html_url"]
        self.implementation = license["implementation"]
        self.limitations = license["limitations"]
        self.permissions = license["permissions"]

    def _repr(self):
        return f"<License [{self.name}]>"


class ShortLicense(_License):
    """This object represents a license returned in a collection.

    GitHub's API returns different representations of objects in different
    contexts. This object reprsents a license that would be returned in a
    collection, e.g., retrieving all licenses from ``/licenses``.

    This object has the following attributes:

    .. attribute:: key

        The short, API name, for this license.

    .. attribute:: name

        The long form, legal name for this license.

    .. attribute:: spdx_id

        The Software Package Data Exchange (a.k.a, SPDX) identifier for this
        license.
    """

    class_name = "ShortLicense"
    _refresh_to = License


class RepositoryLicense(models.GitHubCore):
    """The representation of the repository's retrieved license.

    This object will be returned from
    :meth:`~github3.repos.repo.Repository.license` and behaves like
    :class:`~github3.repos.contents.Contents` with a few differences.
    This also includes a :attr:`license` attribute to access the licenses API.

    This object has the following attributes:

    .. attribute:: name

        The name of the file this license is stored in on the repository.

    .. attribute:: path

        The path to the file of this license in the repository.

    .. attribute:: sha

        The current SHA of this license file in the repository.

    .. attribute:: size

        The size in bytes of this file.

    .. attribute:: html_url

        The URL used to view this file in a browser.

    .. attribute:: git_url

        The URL to retrieve this license file via the git protocol.

    .. attribute:: download_url

        The URL used to download this license file from the repository.

    .. attribute:: type

        Analogous to :attr:`github3.repos.contents.Contents.type`, this should
        indicate whether this is a file, directory, or some other type.

    .. attribute:: content

        The content as returned by the API. This may be base64 encoded. See
        :meth:`decode_content` to retrieve the content in plain-text.

    .. attribute:: encoding

        The encoding of the content. For example, ``base64``.

    .. attribute:: links

        The dictionary of URLs returned in the ``_links`` key by the API.

    .. attribute:: license

        A :class:`github3.licenses.ShortLicense` instance representing the
        metadata GitHub knows about the license.
    """

    def _update_attributes(self, license):
        self._api = license["url"]
        self.name = license["name"]
        self.path = license["path"]
        self.sha = license["sha"]
        self.size = license["size"]
        self.html_url = license["html_url"]
        self.git_url = license["git_url"]
        self.download_url = license["download_url"]
        self.type = license["type"]
        self.content = license["content"]
        self.encoding = license["encoding"]
        self.links = license["_links"]
        self.license = ShortLicense(license["license"], self)

    def _repr(self):
        return f"<RepositoryLicense [{self.name}]>"

    def decode_content(self):
        """Decode the :attr:`content` attribute.

        If ``content`` is base64 encoded, decode this and return it.
        Otherwise, return ``content``.

        :returns:
            plain-text content of this license
        :rtype:
            text (str on Python 3)
        """
        if self.encoding == "base64":
            return base64.b64decode(self.content.encode("utf-8")).decode(
                "utf-8"
            )
        return self.content
