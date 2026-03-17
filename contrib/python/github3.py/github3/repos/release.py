"""Release logic for the GitHub API."""
import json

from uritemplate import URITemplate

from .. import models
from .. import users
from .. import utils
from ..decorators import requires_auth
from ..exceptions import error_for


class Release(models.GitHubCore):
    """Representation of a GitHub release.

    It holds the information GitHub returns about a release from a
    :class:`Repository <github3.repos.repo.Repository>`.

    Please see GitHub's `Releases Documentation`_ for more information.

    This object has the following attributes:

    .. attribute:: original_assets

        A list of :class:`~github3.repos.release.Asset` objects representing
        the assets uploaded for this relesae.

    .. attribute:: assets_url

        The URL to retrieve the assets from the API.

    .. attribute:: author

        A :class:`~github3.users.ShortUser` representing the creator of this
        release.

    .. attribute:: body

        The description of this release as written by the release creator.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this release was created.

    .. attribute:: draft

        A boolean attribute describing whether this release is a draft.

    .. attribute:: html_url

        The URL to view this release in a browser.

    .. attribute:: id

        The unique identifier of this release.

    .. attribute:: name

        The name given to this release by the :attr:`author`.

    .. attribute:: prerelease

        A boolean attribute indicating whether the release is a pre-release.

    .. attribute:: published_at

        A :class:`~datetime.datetime` object representing the date and time
        when this release was publisehd.

    .. attribute:: tag_name

        The name of the tag associated with this release.

    .. attribute:: tarball_url

        The URL to retrieve a GitHub generated tarball for this release from
        the API.

    .. attribute:: target_commitish

        The reference (usually a commit) that is targetted by this release.

    .. attribute:: upload_urlt

        A :class:`~uritemplate.URITemplate` object that expands to form the
        URL to upload assets to.

    .. attribute:: zipball_url

        The URL to retrieve a GitHub generated zipball for this release from
        the API.

    .. _Releases Documentation:
        https://developer.github.com/v3/repos/releases/
    """

    def _update_attributes(self, release):
        self._api = self.url = release["url"]
        self.original_assets = [Asset(i, self) for i in release["assets"]]
        self.assets_url = release["assets_url"]
        self.author = users.ShortUser(release["author"], self)
        self.body = release["body"]
        self.created_at = self._strptime(release["created_at"])
        self.draft = release["draft"]
        self.html_url = release["html_url"]
        self.id = release["id"]
        self.name = release["name"]
        self.prerelease = release["prerelease"]
        self.published_at = self._strptime(release["published_at"])
        self.tag_name = release["tag_name"]
        self.tarball_url = release["tarball_url"]
        self.target_commitish = release["target_commitish"]
        self.upload_urlt = URITemplate(release["upload_url"])
        self.zipball_url = release["zipball_url"]

    def _repr(self):
        return f"<Release [{self.name}]>"

    def archive(self, format, path=""):
        """Get the tarball or zipball archive for this release.

        :param str format:
            (required), accepted values: ('tarball', 'zipball')
        :param path:
            (optional), path where the file should be saved to, default is the
            filename provided in the headers and will be written in the current
            directory. It can take a file-like object as well
        :type path:
            str, file
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        resp = None
        if format in ("tarball", "zipball"):
            repo_url = self._api[: self._api.rfind("/releases")]
            url = self._build_url(format, self.tag_name, base_url=repo_url)
            resp = self._get(url, allow_redirects=True, stream=True)

        if resp and self._boolean(resp, 200, 404):
            utils.stream_response_to_file(resp, path)
            return True
        return False

    def asset(self, asset_id):
        """Retrieve the asset from this release with ``asset_id``.

        :param int asset_id:
            ID of the Asset to retrieve
        :returns:
            the specified asset, if it exists
        :rtype:
            :class:`~github3.repos.release.Asset`
        """
        json = None
        if int(asset_id) > 0:
            i = self._api.rfind("/")
            url = self._build_url(
                "assets", str(asset_id), base_url=self._api[:i]
            )
            json = self._json(self._get(url), 200)
        return self._instance_or_null(Asset, json)

    def assets(self, number=-1, etag=None):
        """Iterate over the assets available for this release.

        :param int number:
            (optional), Number of assets to return
        :param str etag:
            (optional), last ETag header sent
        :returns:
            generator of asset objects
        :rtype:
            :class:`~github3.repos.release.Asset`
        """
        url = self._build_url("assets", base_url=self._api)
        return self._iter(number, url, Asset, etag=etag)

    @requires_auth
    def delete(self):
        """Delete this release.

        Only users with push access to the repository can delete a release.

        :returns:
            True if successful; False if not successful
        :rtype:
            bool
        """
        url = self._api
        return self._boolean(self._delete(url), 204, 404)

    @requires_auth
    def edit(
        self,
        tag_name=None,
        target_commitish=None,
        name=None,
        body=None,
        draft=None,
        prerelease=None,
    ):
        """Edit this release.

        Only users with push access to the repository can edit a release.

        If the edit is successful, this object will update itself.

        :param str tag_name:
            (optional), Name of the tag to use
        :param str target_commitish:
            (optional), The "commitish" value that determines where the Git tag
            is created from. Defaults to the repository's default branch.
        :param str name:
            (optional), Name of the release
        :param str body:
            (optional), Description of the release
        :param boolean draft:
            (optional), True => Release is a draft
        :param boolean prerelease:
            (optional), True => Release is a prerelease
        :returns:
            True if successful; False if not successful
        :rtype:
            bool
        """
        url = self._api
        data = {
            "tag_name": tag_name,
            "target_commitish": target_commitish,
            "name": name,
            "body": body,
            "draft": draft,
            "prerelease": prerelease,
        }
        self._remove_none(data)

        r = self.session.patch(url, data=json.dumps(data))

        successful = self._boolean(r, 200, 404)
        if successful:
            # If the edit was successful, let's update the object.
            self._update_attributes(r.json())

        return successful

    @requires_auth
    def upload_asset(self, content_type, name, asset, label=None):
        """Upload an asset to this release.

        .. note:: All parameters are required.

        :param str content_type:
            The content type of the asset. Wikipedia has a list of common media
            types
        :param str name:
            The name of the file
        :param asset:
            The file or bytes object to upload.
        :param label:
            (optional), An alternate short description of the asset.
        :returns:
            the created asset
        :rtype:
            :class:`~github3.repos.release.Asset`
        """
        headers = {"Content-Type": content_type}
        params = {"name": name, "label": label}
        self._remove_none(params)
        url = self.upload_urlt.expand(params)
        r = self._post(url, data=asset, json=False, headers=headers)
        if r.status_code in (201, 202):
            return Asset(r.json(), self)
        raise error_for(r)


class Asset(models.GitHubCore):
    """Representation of an asset in a release.

    .. seealso::

        `List Assets`_, :meth:`~github3.repos.release.Release.assets`
            Documentation around listing assets of a release
        `Upload Assets`_, :meth:`~github3.repos.release.Release.upload_asset`
            Documentation around uploading assets to a release
        `Get a Single Asset`_, :meth:`~github3.repos.release.Release.asset`
            Documentation around retrieving an asset
        `Edit an Asset`_, :meth:`~github3.repos.release.Asset.edit`
            Documentation around editing an asset
        `Delete an Asset`_, :meth:`~github3.repos.release.Asset.delete`
            Documentation around deleting an asset

    This object has the following attributes:

    .. attribute:: browser_download_url

        The user-friendly URL to download this asset via a browser.

    .. attribute:: content_type

        The Content-Type provided by the uploader when this asset was created.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when this asset was created.

    .. attribute:: download_count

        The number of times this asset has been downloaded.

    .. attribute:: download_url

        The URL to retrieve this uploaded asset via the API, e.g., tarball,
        zipball, etc.

    .. attribute:: id

        The unique identifier of this asset.

    .. attribute:: label

        The short description of this asset.

    .. attribute:: name

        The name provided to this asset.

    .. attribute:: size

        The file size of this asset.

    .. attribute:: state

        The state of this asset, e.g., ``'uploaded'``.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this asset was most recently updated.

    .. _List Assets:
        https://developer.github.com/v3/repos/releases/#list-assets-for-a-release
    .. _Upload Assets:
        https://developer.github.com/v3/repos/releases/#upload-a-release-asset
    .. _Get a Single Asset:
        https://developer.github.com/v3/repos/releases/#get-a-single-release-asset
    .. _Edit an Asset:
        https://developer.github.com/v3/repos/releases/#edit-a-release-asset
    .. _Delete an Asset:
        https://developer.github.com/v3/repos/releases/#delete-a-release-asset
    """

    def _update_attributes(self, asset):
        self._api = asset["url"]
        self.browser_download_url = asset["browser_download_url"]
        self.content_type = asset["content_type"]
        self.created_at = self._strptime(asset["created_at"])
        self.download_count = asset["download_count"]
        self.download_url = self._api
        self.id = asset["id"]
        self.label = asset["label"]
        self.name = asset["name"]
        self.size = asset["size"]
        self.state = asset["state"]
        self.updated_at = self._strptime(asset["updated_at"])

    def _repr(self):
        return f"<Asset [{self.name}]>"

    def download(self, path=""):
        """Download the data for this asset.

        :param path:
            (optional), path where the file should be saved to, default is the
            filename provided in the headers and will be written in the current
            directory. It can take a file-like object as well
        :type path:
            str, file
        :returns:
            name of the file, if successful otherwise ``None``
        :rtype:
            str
        """
        headers = {"Accept": "application/octet-stream"}
        resp = self._get(
            self._api, allow_redirects=False, stream=True, headers=headers
        )
        if resp.status_code == 302:
            # Amazon S3 will reject the redirected request unless we omit
            # certain request headers
            headers.update({"Content-Type": None})

            with self.session.no_auth():
                resp = self._get(
                    resp.headers["location"], stream=True, headers=headers
                )

        if self._boolean(resp, 200, 404):
            return utils.stream_response_to_file(resp, path)
        return None

    @requires_auth
    def delete(self):
        """Delete this asset if the user has push access.

        :returns:
            True if successful; False if not successful
        :rtype:
            bool
        """
        url = self._api
        return self._boolean(self._delete(url), 204, 404)

    def edit(self, name, label=None):
        """Edit this asset.

        :param str name:
            (required), The file name of the asset
        :param str label:
            (optional), An alternate description of the asset
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if not name:
            return False
        edit_data = {"name": name, "label": label}
        self._remove_none(edit_data)
        r = self._patch(self._api, data=json.dumps(edit_data))
        successful = self._boolean(r, 200, 404)
        if successful:
            self._update_attributes(r.json())

        return successful
