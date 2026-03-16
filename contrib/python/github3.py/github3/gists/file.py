"""Module containing the GistFile object."""
from .. import models


class _GistFile(models.GitHubCore):
    """Base for GistFile classes."""

    def _update_attributes(self, gistfile):
        self.raw_url = gistfile["raw_url"]
        self.filename = gistfile["filename"]
        self.language = gistfile["language"]
        self.size = gistfile["size"]
        self.type = gistfile["type"]

    def _repr(self):
        return "<{s.class_name} [{s.filename}]>".format(s=self)

    def content(self):
        """Retrieve contents of file from key 'raw_url'.

        :returns:
            unaltered, untruncated contents of file.
        :rtype:
            bytes
        """
        resp = self._get(self.raw_url)
        if self._boolean(resp, 200, 404):
            return resp.content
        return None


class GistFile(_GistFile):
    """This represents the full file object returned by interacting with gists.

    The object has all of the attributes as returned by the API for a
    ShortGistFile as well as:

    .. attribute:: truncated

        A boolean attribute that indicates whether :attr:`original_content`
        contains all of the file's contents.

    .. attribute:: original_content

        The contents of the file (potentially truncated) returned by the API.
        If the file was truncated use :meth:`content` to retrieve it in its
        entirety.

    """

    class_name = "GistFile"

    def _update_attributes(self, gistfile):
        super()._update_attributes(gistfile)
        self.original_content = gistfile["content"]
        self.truncated = gistfile["truncated"]


class ShortGistFile(_GistFile):
    """This represents the file object returned by interacting with gists.

    The object has the following attributes as returned by the API for a Gist:

    .. attribute:: raw_url

        This URL provides access to the complete, untruncated content of the
        file represented by this object.

    .. attribute:: filename

        The string for the filename.

    .. attribute:: language

        The GitHub detected language for the file, e.g., Erlang, Python, text.

    .. attribute:: type

        The mime-type of the file. Related to :attr:`language`.

    .. attribute:: size

        The file size in bytes.
    """

    class_name = "ShortGistFile"
    _refresh_to = GistFile
