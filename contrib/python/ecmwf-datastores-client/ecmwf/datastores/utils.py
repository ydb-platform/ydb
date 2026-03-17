import datetime


def string_to_datetime(string: str) -> datetime.datetime:
    string = string.replace("Z", "+00:00")  # Support python<3.11
    date_time = datetime.datetime.fromisoformat(string)
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=datetime.timezone.utc)
    return date_time
