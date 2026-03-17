from __future__ import unicode_literals
from contextlib import contextmanager
import re
import sys
import logging
import click
import io
import shlex
import sqlparse
import psycopg2
from os.path import expanduser
from .namedqueries import NamedQueries
from . import export
from .main import show_extra_help_command, special_command

NAMED_QUERY_PLACEHOLDERS = frozenset({"$1", "$*", "$@"})

DEFAULT_WATCH_SECONDS = 2

_logger = logging.getLogger(__name__)


@export
def editor_command(command):
    """
    Is this an external editor command?  (\\e or \\ev)

    :param command: string

    Returns the specific external editor command found.
    """
    # It is possible to have `\e filename` or `SELECT * FROM \e`. So we check
    # for both conditions.

    stripped = command.strip()
    for sought in ("\\e ", "\\ev ", "\\ef "):
        if stripped.startswith(sought):
            return sought.strip()
    for sought in ("\\e",):
        if stripped.endswith(sought):
            return sought


@export
def get_filename(sql):
    if sql.strip().startswith("\\e"):
        command, _, filename = sql.partition(" ")
        return filename.strip() or None


@export
@show_extra_help_command(
    "\\watch",
    f"\\watch [sec={DEFAULT_WATCH_SECONDS}]",
    "Execute query every `sec` seconds.",
)
def get_watch_command(command):
    match = re.match(r"(.*?)[\s]*\\watch(\s+\d+)?\s*;?\s*$", command, re.DOTALL)
    if match:
        groups = match.groups(default=f"{DEFAULT_WATCH_SECONDS}")
        return groups[0], int(groups[1])
    return None, None


@export
def get_editor_query(sql):
    """Get the query part of an editor command."""
    sql = sql.strip()

    # The reason we can't simply do .strip('\e') is that it strips characters,
    # not a substring. So it'll strip "e" in the end of the sql also!
    # Ex: "select * from style\e" -> "select * from styl".
    pattern = re.compile(r"(^\\e|\\e$)")
    while pattern.search(sql):
        sql = pattern.sub("", sql)

    return sql


@export
def open_external_editor(filename=None, sql=None):
    """
    Open external editor, wait for the user to type in his query,
    return the query.
    :return: list with one tuple, query as first element.
    """

    message = None
    filename = filename.strip().split(" ", 1)[0] if filename else None

    sql = sql or ""
    MARKER = "# Type your query above this line.\n"

    # Populate the editor buffer with the partial sql (if available) and a
    # placeholder comment.
    query = click.edit(
        "{sql}\n\n{marker}".format(sql=sql, marker=MARKER),
        filename=filename,
        extension=".sql",
    )

    if filename:
        try:
            query = read_from_file(filename)
        except IOError:
            message = "Error reading file: %s." % filename

    if query is not None:
        query = query.split(MARKER, 1)[0].rstrip("\n")
    else:
        # Don't return None for the caller to deal with.
        # Empty string is ok.
        query = sql

    return (query, message)


def read_from_file(path):
    with io.open(expanduser(path), encoding="utf-8") as f:
        contents = f.read()
    return contents


@contextmanager
def _paused_thread():
    try:
        thread = psycopg2.extensions.get_wait_callback()
        psycopg2.extensions.set_wait_callback(None)
        yield
    finally:
        psycopg2.extensions.set_wait_callback(thread)


def _index_of_file_name(tokenlist):
    for (idx, token) in reversed(list(enumerate(tokenlist[:-2]))):
        if token.is_keyword and token.value.upper() in ("TO", "FROM"):
            return idx + 2
    raise Exception("Missing keyword in \\copy command. Either TO or FROM is required.")


@special_command(
    "\\copy",
    "\\copy [tablename] to/from [filename]",
    "Copy data between a file and a table.",
)
def copy(cur, pattern, verbose):
    """Copies table data to/from files"""

    # Replace the specified file destination with STDIN or STDOUT
    parsed = sqlparse.parse(pattern)
    tokenlist = parsed[0].tokens
    idx = _index_of_file_name(tokenlist)
    file_name = tokenlist[idx].value
    before_file_name = "".join(t.value for t in tokenlist[:idx])
    after_file_name = "".join(t.value for t in tokenlist[idx + 1 :])

    direction = tokenlist[idx - 2].value.upper()
    replacement_file_name = "STDIN" if direction == "FROM" else "STDOUT"
    query = "{0} {1} {2}".format(
        before_file_name, replacement_file_name, after_file_name
    )
    open_mode = "r" if direction == "FROM" else "w"
    if file_name.startswith("'") and file_name.endswith("'"):
        file = io.open(
            expanduser(file_name.strip("'")), mode=open_mode, encoding="utf-8"
        )
    elif "stdin" in file_name.lower():
        file = sys.stdin
    elif "stdout" in file_name.lower():
        file = sys.stdout
    else:
        raise Exception("Enclose filename in single quotes")

    # pg3: I don't know what is pause_thread for here.
    with _paused_thread():
        # pg3: COPY changed in psycopg3 and is no more file based: examples at
        # pg3: https://www.psycopg.org/psycopg3/docs/basic/copy.html#copying-block-by-block
        cur.copy_expert("copy " + query, file)

    if cur.description:
        # pg3 (and 2): x.name is probably more readable than x[0]
        headers = [x[0] for x in cur.description]
        return [(None, cur, headers, cur.statusmessage)]
    else:
        return [(None, None, None, cur.statusmessage)]


def subst_favorite_query_args(query, args):
    """replace positional parameters ($1,$2,...$n) in query."""
    is_query_with_aggregation = ("$*" in query) or ("$@" in query)

    # In case of arguments aggregation we replace all positional arguments until the
    # first one not present in the query. Then we aggregate all the remaining ones and
    # replace the placeholder with them.
    for idx, val in enumerate(args, start=1):
        subst_var = "$" + str(idx)
        if subst_var not in query:
            if is_query_with_aggregation:
                # remove consumed arguments ( - 1 to include current value)
                args = args[idx - 1 :]
                break

            return [
                None,
                "query does not have substitution parameter "
                + subst_var
                + ":\n  "
                + query,
            ]

        query = query.replace(subst_var, val)
    # we consumed all arguments
    else:
        args = []

    if is_query_with_aggregation and not args:
        return [None, "missing substitution for $* or $@ in query:\n" + query]

    if "$*" in query:
        query = query.replace("$*", ", ".join(args))
    elif "$@" in query:
        query = query.replace("$@", ", ".join(map("'{}'".format, args)))

    match = re.search("\\$\\d+", query)
    if match:
        return [
            None,
            "missing substitution for " + match.group(0) + " in query:\n  " + query,
        ]

    return [query, None]


@special_command(
    "\\n", "\\n[+] [name] [param1 param2 ...]", "List or execute named queries."
)
def execute_named_query(cur, pattern, **_):
    """Returns (title, rows, headers, status)"""
    if pattern == "":
        return list_named_queries(True)

    params = shlex.split(pattern)
    pattern = params.pop(0)

    query = NamedQueries.instance.get(pattern)
    title = "> {}".format(query)
    if query is None:
        message = "No named query: {}".format(pattern)
        return [(None, None, None, message)]

    try:
        if any(p in query for p in NAMED_QUERY_PLACEHOLDERS):
            query, params = subst_favorite_query_args(query, params)
            if query is None:
                raise Exception("Bad arguments\n" + params)
        cur.execute(query)
    # pg3: (but psycopg2 too): you can use `except psycopg.errors.SyntaxError`
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.SYNTAX_ERROR and "%s" in query:
            raise Exception(
                "Bad arguments: "
                'please use "$1", "$2", etc. for named queries instead of "%s"'
            )
        else:
            raise
    except (IndexError, TypeError):
        raise Exception("Bad arguments")

    if cur.description:
        headers = [x[0] for x in cur.description]
        return [(title, cur, headers, cur.statusmessage)]
    else:
        return [(title, None, None, cur.statusmessage)]


def list_named_queries(verbose):
    """List of all named queries.
    Returns (title, rows, headers, status)"""
    if not verbose:
        rows = [[r] for r in NamedQueries.instance.list()]
        headers = ["Name"]
    else:
        headers = ["Name", "Query"]
        rows = [[r, NamedQueries.instance.get(r)] for r in NamedQueries.instance.list()]

    if not rows:
        status = NamedQueries.instance.usage
    else:
        status = ""
    return [("", rows, headers, status)]


@special_command("\\np", "\\np name_pattern", "Print a named query.")
def get_named_query(pattern, **_):
    """Get a named query that matches name_pattern.

    The named pattern can be a regular expression. Returns (title,
    rows, headers, status)

    """

    usage = "Syntax: \\np name.\n\n" + NamedQueries.instance.usage
    if not pattern:
        return [(None, None, None, usage)]

    name = pattern.strip()
    if not name:
        return [(None, None, None, usage + "Err: A name is required.")]

    headers = ["Name", "Query"]
    rows = [
        (r, NamedQueries.instance.get(r))
        for r in NamedQueries.instance.list()
        if re.search(name, r)
    ]

    status = ""
    if not rows:
        status = "No match found"

    return [("", rows, headers, status)]


@special_command("\\ns", "\\ns name query", "Save a named query.")
def save_named_query(pattern, **_):
    """Save a new named query.
    Returns (title, rows, headers, status)"""

    usage = "Syntax: \\ns name query.\n\n" + NamedQueries.instance.usage
    if not pattern:
        return [(None, None, None, usage)]

    name, _, query = pattern.partition(" ")

    # If either name or query is missing then print the usage and complain.
    if (not name) or (not query):
        return [(None, None, None, usage + "Err: Both name and query are required.")]

    NamedQueries.instance.save(name, query)
    return [(None, None, None, "Saved.")]


@special_command("\\nd", "\\nd [name]", "Delete a named query.")
def delete_named_query(pattern, **_):
    """Delete an existing named query."""
    usage = "Syntax: \\nd name.\n\n" + NamedQueries.instance.usage
    if not pattern:
        return [(None, None, None, usage)]

    status = NamedQueries.instance.delete(pattern)

    return [(None, None, None, status)]
