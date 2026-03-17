import copy
import functools
import inspect
import os
import socketserver
import sys
import time

import bson
import pymongo.collection
import pymongo.command_cursor
import pymongo.cursor
import pymongo.helpers

__all__ = [
    "queries",
    "inserts",
    "updates",
    "removes",
    "install_tracker",
    "uninstall_tracker",
    "reset",
    "response_sizes",
]

_original_methods = {
    "insert": pymongo.collection.Collection.insert,
    "update": pymongo.collection.Collection.update,
    "remove": pymongo.collection.Collection.remove,
    "refresh": pymongo.cursor.Cursor._refresh,
    "_unpack_response": pymongo.command_cursor.CommandCursor._unpack_response,
}

queries = []
inserts = []
updates = []
removes = []
response_sizes = []

if sys.version_info >= (3, 0):
    unicode = str


# Wrap helpers._unpack_response for getting response size
@functools.wraps(_original_methods["_unpack_response"])
def _unpack_response(response, *args, **kwargs):
    result = _original_methods["_unpack_response"](response, *args, **kwargs)
    response_sizes.append(sys.getsizeof(response, len(response)) / 1024.0)
    return result


# Wrap Cursor.insert for getting queries
@functools.wraps(_original_methods["insert"])
def _insert(
    collection_self, doc_or_docs, manipulate=True, safe=None, check_keys=True, **kwargs
):
    start_time = time.time()
    result = _original_methods["insert"](
        collection_self, doc_or_docs, check_keys=check_keys, **kwargs
    )
    total_time = (time.time() - start_time) * 1000

    __traceback_hide__ = True  # noqa
    stack_trace, internal = _tidy_stacktrace()
    inserts.append(
        {
            "document": doc_or_docs,
            "time": total_time,
            "stack_trace": stack_trace,
            "size": response_sizes[-1] if response_sizes else 0,
            "internal": internal,
        }
    )
    return result


# Wrap Cursor.update for getting queries
@functools.wraps(_original_methods["update"])
def _update(
    collection_self,
    spec,
    document,
    upsert=False,
    maniuplate=False,
    safe=None,
    multi=False,
    **kwargs,
):
    start_time = time.time()
    result = _original_methods["update"](
        collection_self, spec, document, upsert=upsert, multi=multi, **kwargs
    )
    total_time = (time.time() - start_time) * 1000

    __traceback_hide__ = True  # noqa
    stack_trace, internal = _tidy_stacktrace()
    updates.append(
        {
            "document": document,
            "upsert": upsert,
            "multi": multi,
            "spec": spec,
            "time": total_time,
            "stack_trace": stack_trace,
            "size": response_sizes[-1] if response_sizes else 0,
            "internal": internal,
        }
    )
    return result


# Wrap Cursor.remove for getting queries
@functools.wraps(_original_methods["remove"])
def _remove(collection_self, spec_or_id, safe=None, **kwargs):
    start_time = time.time()
    result = _original_methods["remove"](collection_self, spec_or_id, **kwargs)
    total_time = (time.time() - start_time) * 1000

    __traceback_hide__ = True  # noqa
    stack_trace, internal = _tidy_stacktrace()
    removes.append(
        {
            "spec_or_id": spec_or_id,
            "time": total_time,
            "   ": stack_trace,
            "size": response_sizes[-1] if response_sizes else 0,
            "internal": internal,
        }
    )
    return result


# Wrap Cursor._refresh for getting queries
@functools.wraps(_original_methods["refresh"])
def _cursor_refresh(cursor_self):
    # Look up __ private instance variables
    def privar(name):
        return getattr(cursor_self, "_Cursor__{0}".format(name), None)

    if privar("id") is not None:
        # getMore not query - move on
        return _original_methods["refresh"](cursor_self)

    # NOTE: See pymongo/cursor.py+557 [_refresh()] and
    # pymongo/message.py for where information is stored

    # Time the actual query
    start_time = time.time()
    result = _original_methods["refresh"](cursor_self)
    total_time = (time.time() - start_time) * 1000

    query_son = privar("query_spec")()
    if not isinstance(query_son, bson.SON):

        if "$query" not in query_son:
            query_son = {"$query": query_son}

        data = privar("data")
        if data:
            query_son["data"] = data

        orderby = privar("ordering")
        if orderby:
            query_son["$orderby"] = orderby

        hint = privar("hint")
        if hint:
            query_son["$hint"] = hint

        snapshot = privar("snapshot")
        if snapshot:
            query_son["$snapshot"] = snapshot

        maxScan = privar("max_scan")
        if maxScan:
            query_son["$maxScan"] = maxScan

    __traceback_hide__ = True  # noqa
    stack_trace, internal = _tidy_stacktrace()
    query_data = {
        "time": total_time,
        "operation": "query",
        "stack_trace": stack_trace,
        "size": response_sizes[-1] if response_sizes else 0,
        "data": copy.copy(privar("data")),
        "internal": internal,
    }

    # Collection in format <db_name>.<collection_name>
    collection_name = privar("collection")
    query_data["collection"] = collection_name.full_name.split(".")[1]

    if query_data["collection"] == "$cmd":
        query_data["operation"] = "command"
        # Handle count as a special case
        if "count" in query_son:
            # Information is in a different format to a standard query
            query_data["collection"] = query_son["count"]
            query_data["operation"] = "count"
            query_data["skip"] = query_son.get("skip")
            query_data["limit"] = query_son.get("limit")
            query_data["query"] = query_son["query"]
    else:
        # Normal Query
        query_data["skip"] = privar("skip")
        query_data["limit"] = privar("limit")
        query_data["query"] = query_son["$query"]
        query_data["ordering"] = _get_ordering(query_son)

    queries.append(query_data)

    return result


def install_tracker():
    if pymongo.collection.Collection.insert != _insert:
        pymongo.collection.Collection.insert = _insert
    if pymongo.collection.Collection.update != _update:
        pymongo.collection.Collection.update = _update
    if pymongo.collection.Collection.remove != _remove:
        pymongo.collection.Collection.remove = _remove
    if pymongo.cursor.Cursor._refresh != _cursor_refresh:
        pymongo.cursor.Cursor._refresh = _cursor_refresh
    if pymongo.command_cursor.CommandCursor._unpack_response != _unpack_response:
        pymongo.command_cursor.CommandCursor._unpack_response = _unpack_response


def uninstall_tracker():
    if pymongo.collection.Collection.insert == _insert:
        pymongo.collection.Collection.insert = _original_methods["insert"]
    if pymongo.collection.Collection.update == _update:
        pymongo.collection.Collection.update = _original_methods["update"]
    if pymongo.collection.Collection.remove == _remove:
        pymongo.collection.Collection.remove = _original_methods["remove"]
    if pymongo.cursor.Cursor._refresh == _cursor_refresh:
        pymongo.cursor.Cursor._refresh = _original_methods["cursor_refresh"]
    if pymongo.command_cursor.CommandCursor._unpack_response == _unpack_response:
        pymongo.command_cursor.CommandCursor._unpack_response = _original_methods[
            "_unpack_response"
        ]


def reset():
    global queries, inserts, updates, removes, response_sizes
    queries = []
    inserts = []
    updates = []
    removes = []
    response_sizes = []


def _get_ordering(son):
    """Helper function to extract formatted ordering from dict."""

    def fmt(field, direction):
        return "{0}{1}".format({-1: "-", 1: "+"}[direction], field)

    if "$orderby" in son:
        return ", ".join(fmt(f, d) for f, d in son["$orderby"].items())


def _tidy_stacktrace():
    """
    Tidy the stack_trace
    """
    socketserver_path = os.path.realpath(os.path.dirname(socketserver.__file__))
    pymongo_path = os.path.realpath(os.path.dirname(pymongo.__file__))
    paths = ["/site-packages/", "/flaskext/", socketserver_path, pymongo_path]
    internal = False

    # Check html templates
    fnames = []
    for i in range(100):
        try:
            fname = sys._getframe(i).f_code.co_filename
            if ".html" in fname:
                fnames.append(fname)
        except Exception:
            break
    fnames = list(set(fnames))
    trace = []

    for path in fnames:
        if "flask_debugtoolbar" in path:
            internal = True
        trace.append((path, "?", "?", "?", False))

    if trace:
        return trace, internal

    stack = inspect.stack()
    reversed(stack)

    trace = []
    for frame, path, line_no, func_name, text in (f[:5] for f in stack):
        s_path = os.path.realpath(path)
        # Support hiding of frames -- used in various utilities that provide
        # inspection.
        if "__traceback_hide__" in frame.f_locals:
            continue
        hidden = False
        if func_name == "<genexpr>":
            hidden = True
        if any([p for p in paths if p in s_path]):
            hidden = True
        if not text:
            text = ""
        else:
            if sys.version_info >= (3, 0):
                text = "".join(text).strip()
            else:
                try:
                    text = unicode("".join(text).strip())
                except Exception:
                    pass
        trace.append((path, line_no, func_name, text, hidden))
    return trace, internal
