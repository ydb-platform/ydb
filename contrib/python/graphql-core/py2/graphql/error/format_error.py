from .base import GraphQLError

# Necessary for static type checking
if False:  # flake8: noqa
    from typing import Any, Dict


def format_error(error):
    # type: (Exception) -> Dict[str, Any]
    # Protect against UnicodeEncodeError when run in py2 (#216)
    try:
        message = str(error)
    except UnicodeEncodeError:
        message = error.message.encode("utf-8")  # type: ignore
    formatted_error = {"message": message}  # type: Dict[str, Any]
    if isinstance(error, GraphQLError):
        if error.locations is not None:
            formatted_error["locations"] = [
                {"line": loc.line, "column": loc.column} for loc in error.locations
            ]
        if error.path is not None:
            formatted_error["path"] = error.path

        if error.extensions is not None:
            formatted_error["extensions"] = error.extensions

    return formatted_error
