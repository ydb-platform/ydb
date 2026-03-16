"""Topics related logic."""
from .. import models


class Topics(models.GitHubCore):
    """Representation of the repository topics.

    .. attribute:: names

        The names of the topics.
    """

    def _update_attributes(self, topics):
        self.names = topics["names"]

    def _repr(self):
        return "<Topics [{}]>".format(", ".join(self.names))
