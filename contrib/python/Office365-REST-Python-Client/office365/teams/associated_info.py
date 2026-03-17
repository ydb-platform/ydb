from office365.teams.info import TeamInfo


class AssociatedTeamInfo(TeamInfo):
    """
    Represents a team that is associated with a user.

    Currently, a user can be associated with a team in two different ways:

        - A user can be a direct member of a team.
        - A user can be a member of a shared channel that is hosted inside a team.
    """
