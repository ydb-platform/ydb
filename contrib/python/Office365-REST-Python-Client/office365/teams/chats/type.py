class ChatType:

    oneOnOne = "oneOnOne"
    """Indicates that the chat is a 1:1 chat. The roster size is fixed for this type of chat; members can't be
    removed/added."""

    group = "group"
    """Indicates that the chat is a group chat. The roster size (of at least two people) can be updated for this
    type of chat. Members can be removed/added later."""

    meeting = "meeting"
    """Indicates that the chat is associated with an online meeting. This type of chat is only created as
    part of the creation of an online meeting."""
