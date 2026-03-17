"""Module containing the logic for labels."""
from json import dumps

from ..decorators import requires_auth
from ..models import GitHubCore


class _Label(GitHubCore):
    SYMMETRA_PREVIEW_HEADERS = {
        "Accept": "application/vnd.github.symmetra-preview+json"
    }

    def _update_attributes(self, label):
        self._api = label["url"]
        self.color = label["color"]
        self.name = label["name"]
        self._uniq = self._api

    def _repr(self):
        return "<{0.class_name} [{0.name}]>".format(self)

    def __str__(self):
        return self.name

    @requires_auth
    def delete(self):
        """Delete this label.

        :returns:
            True if successfully deleted, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    @requires_auth
    def update(self, name, color, description=None):
        """Update this label.

        :param str name:
            (required), new name of the label
        :param str color:
            (required), color code, e.g., 626262, no leading '#'
        :param str description:
            (optional), new description of the label
        :returns:
            True if successfully updated, False otherwise
        :rtype:
            bool
        """
        json = None

        if name and color:
            if color[0] == "#":
                color = color[1:]
            data = {"name": name, "color": color}
            if description is not None:
                data["description"] = description
            resp = self._patch(
                self._api,
                data=dumps(data),
                headers=self.SYMMETRA_PREVIEW_HEADERS,
            )
            json = self._json(resp, 200)

        if json:
            self._update_attributes(json)
            return True

        return False


class ShortLabel(_Label):
    """A representation of a label object defined on a repository.

    See also: http://developer.github.com/v3/issues/labels/

    This object has the following attributes::

    .. attribute:: color

        The hexadecimeal representation of the background color of this label.

    .. attribute:: name

        The name (display label) for this label.
    """

    class_name = "ShortLabel"


class Label(_Label):
    """A representation of a label object defined on a repository.

    See also: http://developer.github.com/v3/issues/labels/

    This object has the following attributes::

    .. attribute:: color

        The hexadecimeal representation of the background color of this label.

    .. attribute:: description

        The description for this label.

    .. attribute:: name

        The name (display label) for this label.
    """

    class_name = "Label"

    def _update_attributes(self, label):
        super()._update_attributes(label)
        self.description = label["description"]
