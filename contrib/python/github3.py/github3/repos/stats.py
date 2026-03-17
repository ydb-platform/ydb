"""Repository and contributor stats logic."""
import datetime

import dateutil.tz

from .. import models
from .. import users


def alternate_week(week):
    """Map GitHub 'short' data to usable data.

    .. note:: This is not meant for public consumption

    :param dict week:
        The week's statistical data from GitHub
    :returns:
        Huamnized week statistical data
    :rtype:
        dict
    """
    start_of_week = datetime.datetime.utcfromtimestamp(int(week["w"]))
    return {
        "start of week": start_of_week.replace(tzinfo=dateutil.tz.tzutc()),
        "additions": week["a"],
        "deletions": week["d"],
        "commits": week["c"],
    }


class ContributorStats(models.GitHubCore):
    """Representation of a user's contributor statistics to a repository.

    See also http://developer.github.com/v3/repos/statistics/

    This object has the following attributes:

    .. attribute:: author

        A :class:`~github3.users.ShortUser` representing the contributor
        whose stats this object represents.

    .. attribute:: total

        The total number of commits authored by :attr:`author`.

    .. attribute:: weeks

        A list of dictionaries containing weekly statistical data.

    .. attribute:: alternate_weeks

        .. note::

            :mod:`github3` generates this data for a more humane interface
            to the data in :attr:`weeks`.

        A list of dictionaries that provide an easier to remember set of
        keys as well as a :class:`~datetime.datetime` object representing the
        start of the week. The dictionary looks vaguely like:

        .. code-block:: python

            {
                'start of week': datetime(2013, 5, 5, 5, 0, tzinfo=tzutc())
                'additions': 100,
                'deletions': 150,
                'commits': 5,
            }

    """

    def _update_attributes(self, stats_object):
        self.author = users.ShortUser(stats_object["author"], self)
        self.total = stats_object["total"]
        self.weeks = stats_object["weeks"]
        alt_weeks = self.weeks
        if alt_weeks:
            alt_weeks = [alternate_week(w) for w in self.weeks]
        self.alternate_weeks = self.alt_weeks = alt_weeks

    def _repr(self):
        return f"<Contributor Statistics [{self.author}]>"
