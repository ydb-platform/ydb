import logging
import re
from argparse import Namespace
from typing import List

logger = logging.getLogger(__name__)

# Matches Mode/Jinja-style template parameters: {{ param_name }}
# Two patterns handled:
#   1. Already quoted: '{{ param }}' -> '__TEMPLATE_PARAM__' (keep surrounding quotes)
#   2. Unquoted: {{ param }} -> '__TEMPLATE_PARAM__' (add quotes)
_QUOTED_TEMPLATE_PARAM_RE = re.compile(r"'(\{\{.*?\}\})'")
_UNQUOTED_TEMPLATE_PARAM_RE = re.compile(r"\{\{.*?\}\}")


def escape_identifier_name(name: str):
    if name is not None:
        if name.startswith("[") and name.endswith("]"):
            return name.strip("[]")
        return name.strip("`").strip('"').strip("'")
    return None


def sanitize_template_params(sql: str) -> str:
    """Replace template parameters (e.g. {{ param }}) with a placeholder literal.

    Tools like Mode Analytics use Jinja-style ``{{ param_name }}`` syntax for
    runtime parameters.  These tokens are opaque values (dates, thresholds, …)
    that never affect table or column lineage, but they *do* break SQL parsers
    that don't understand template syntax.

    Replacing them with a single-quoted string placeholder keeps the SQL
    structurally valid for every parser while preserving lineage accuracy.
    """
    # First pass: handle already-quoted params '{{ x }}' -> keep the surrounding quotes
    sql = _QUOTED_TEMPLATE_PARAM_RE.sub("'__TEMPLATE_PARAM__'", sql)
    # Second pass: handle unquoted params {{ x }} -> wrap in quotes
    return _UNQUOTED_TEMPLATE_PARAM_RE.sub("'__TEMPLATE_PARAM__'", sql)


def extract_sql_from_args(args: Namespace) -> str:
    sql = ""
    if getattr(args, "f", None):
        try:
            with open(args.f) as f:
                sql = f.read()
        except IsADirectoryError:
            logger.exception("%s is a directory", args.f)
            exit(1)
        except FileNotFoundError:
            logger.exception("No such file: %s", args.f)
            exit(1)
        except PermissionError:
            # On Windows, open a directory as file throws PermissionError
            logger.exception("Permission denied when reading file '%s'", args.f)
            exit(1)
    elif getattr(args, "e", None):
        sql = args.e
    return sql


def split(sql: str) -> List[str]:
    # TODO: we need a parser independent split function
    import sqlparse

    # sometimes sqlparse split out a statement that is comment only, we want to exclude that
    return [s.value for s in sqlparse.parse(sql) if s.token_first(skip_cm=True)]


def trim_comment(sql: str) -> str:
    # TODO: we need a parser independent trim_comment function
    import sqlparse

    return str(sqlparse.format(sql, strip_comments=True))
