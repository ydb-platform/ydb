import datetime


def timedelta_to_milliseconds(timedelta: datetime.timedelta) -> int:
    """Convert ``timedelta`` object to milliseconds."""
    return int(timedelta.total_seconds() * 1000)
