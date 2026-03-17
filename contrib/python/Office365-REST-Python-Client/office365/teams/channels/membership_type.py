class ChannelMembershipType:
    standard = "standard"
    """Channel inherits the list of members of the parent team."""

    private = "private"
    """Channel can have members that are a subset of all the members on the parent team."""

    shared = "shared"
    """Members can be directly added to the channel without adding them to the team."""
