#
# Copyright (c), 2025-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Callable, Iterator, Sequence
from functools import partial
from typing import Any, Optional
from xml.etree import ElementTree

from xmlschema.aliases import AncestorsType, IOType, IterParseType, ElementType, NsmapType
from xmlschema.exceptions import XMLResourceParseError, XMLSchemaValueError
from xmlschema.xpath import ElementPathSelector

FilterFunctionType = Callable[[ElementType, ElementType, AncestorsType], bool]
ClearFunctionType = Callable[[ElementType, ElementType, AncestorsType], None]


###
# Default filter and clear functions

def no_filter(root: ElementType, elem: ElementType,  ancestors: AncestorsType) -> bool:
    return True


def no_cleanup(root: ElementType, elem: ElementType,  ancestors: AncestorsType) -> None:
    return


def clear_elem(root: ElementType, elem: ElementType,  ancestors: AncestorsType) -> None:
    elem.clear()
    if ancestors is not None:
        if elem in ancestors[-1]:
            ancestors[-1].remove(elem)


###
# Iterparse generator functions

def generic_iterparse(fp: IOType,
                      events: Optional[Sequence[str]] = None,
                      filter_fn: Optional[FilterFunctionType] = None,
                      clear_fn: Optional[ClearFunctionType] = None,
                      ancestors: AncestorsType = None,
                      depth: int = -1,
                      limit: int = -1) -> Iterator[tuple[str, Any]]:
    """
    An event-based parser for filtering XML elements during parsing.

    :param fp: an open file-like object to read from.
    :param events: an optional sequence of events to filter on.
    :param filter_fn: a function that takes the root element, the \
    current element and an optional list of ancestors elements and \
    returns a boolean.
    :param clear_fn: a function that takes the root element, the \
    current element and an optional list of ancestors elements.
    :param ancestors: an optional sequence of ancestors to track.
    :param depth: an optional integer specifying the depth of the tree \
    at where to clean elements. The default value means no cleanup.
    :param limit: an optional integer specifying the maximum number of \
    parser events to process. The default value means no limit.
    """
    if events is None:
        events = 'start-ns', 'end-ns', 'start', 'end', 'comment', 'pi'
    elif 'start' not in events or 'end' not in events:
        events = tuple(events) + ('start', 'end')

    if filter_fn is None:
        filter_fn = no_filter
    if clear_fn is None:
        clear_fn = no_cleanup

    level = 0
    stop_node: Any = None
    root: Any = None
    node: Any

    try:
        for event, node in ElementTree.iterparse(fp, events):
            if not limit:
                raise StopIteration
            limit -= 1

            if event == 'end':
                level -= 1
                if level < depth:
                    if ancestors is not None:
                        ancestors.pop()
                elif level == depth and stop_node is node:
                    stop_node = None
                    clear_fn(root, node, ancestors)
            elif event == 'start':
                if level < depth:
                    if not level:
                        root = node
                    if ancestors is not None:
                        ancestors.append(node)
                elif level == depth and not filter_fn(root, node, ancestors):
                    stop_node = node
                    level += 1
                    continue

                level += 1
                if stop_node is None:
                    yield event, node
            else:
                yield event, node

    except SyntaxError as err:
        raise XMLResourceParseError("invalid XML syntax: {}".format(err)) from err


def iterfind_parser(path: str,
                    namespaces: Optional[NsmapType] = None,
                    ancestors: AncestorsType = None,
                    limit: int = -1) -> IterParseType:
    """
    Returns an iterparse function that yields elements that match the given path.
    """
    selector = ElementPathSelector(path, namespaces)

    def filter_fn(r: ElementType, e: ElementType, a: AncestorsType) -> bool:
        return selector.select_all or e in selector.iter_select(r)

    return partial(
        generic_iterparse,
        filter_fn=filter_fn,
        clear_fn=clear_elem,
        ancestors=ancestors,
        depth=selector.depth,
        limit=limit
    )


def limited_parser(limit: int,
                   ancestors: AncestorsType = None,
                   depth: int = -1) -> IterParseType:
    """
    Returns an iterparse function that process a limited number of parser events.
    """
    if limit < 0:
        raise XMLSchemaValueError("limit argument must be >= 0")
    clear_fn = None if depth > 0 else clear_elem

    return partial(
        generic_iterparse,
        filter_fn=None,
        clear_fn=clear_fn,
        ancestors=ancestors,
        depth=depth,
        limit=limit,
    )
