import pydantic


def is_second_version() -> bool:
    return int(pydantic.VERSION.split(".")[0]) >= 2
