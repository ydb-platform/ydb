from office365.entity import Entity


class TeamworkTag(Entity):
    """
    Represents a tag associated with a team.

    Tags provide a flexible way for customers to classify users or groups based on a auth attribute within a team.
    For example, a Nurse, Manager, or Designer tag will enable users to reach groups of people in Teams without
    having to type every single name.

    When a tag is added, users can @mention it in a channel. Everyone who has been assigned that tag will receive
    a notification just as they would if they were @mentioned individually. Users can also use a tag to start
    a new chat with the members of that tag.
    """
