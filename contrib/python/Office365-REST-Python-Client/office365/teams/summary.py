from office365.runtime.client_value import ClientValue


class TeamSummary(ClientValue):
    """Contains information about a team in Microsoft Teams, including number of owners, members, and guests."""

    def __init__(self, guests_count=None, members_count=None, owners_count=None):
        """
        :param int guests_count: Count of guests in a team.
        :param int members_count: Count of members in a team.
        :param int owners_count: Count of owners in a team.
        """
        self.guestsCount = guests_count
        self.membersCount = members_count
        self.ownersCount = owners_count
