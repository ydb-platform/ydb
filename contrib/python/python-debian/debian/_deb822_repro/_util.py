import collections
import collections.abc
import logging
import sys
import textwrap
from abc import ABC

from typing import (
    Optional,
    Union,
    Iterable,
    Callable,
    TYPE_CHECKING,
    Iterator,
    Type,
    cast,
    List,
    Generic,
)
from debian._util import T
from debian._deb822_repro.types import TE, R, TokenOrElement

_combine_parts_ret_type = Callable[
    [Iterable[Union[TokenOrElement, TE]]], Iterable[Union[TokenOrElement, R]]
]

if TYPE_CHECKING:
    from debian._deb822_repro.parsing import Deb822Element
    from debian._deb822_repro.tokens import Deb822Token


def print_ast(
    ast_tree,  # type: Union[Iterable[TokenOrElement], 'Deb822Element']
    *,
    end_marker_after=5,  # type: Optional[int]
    output_function=None,  # type: Optional[Callable[[str], None]]
):
    # type: (...) -> None
    """Debugging aid, which can dump a Deb822Element or a list of tokens/elements

    :param ast_tree: Either a Deb822Element or an iterable Deb822Token/Deb822Element entries
      (both types may be mixed in the same iterable, which enable it to dump the
      ast tree at different stages of parse_deb822_file method)
    :param end_marker_after: The dump will add "end of element" markers if a
      given element spans at least this many tokens/elements. Can be disabled
      with by passing None as value. Use 0 for unconditionally marking all
      elements (note that tokens never get an "end of element" marker as they
      are not an elements).
    :param output_function: Callable that receives a single str argument and is responsible
      for "displaying" that line. The callable may be invoked multiple times (one per line
      of output).  Defaults to logging.info if omitted.

    """
    # Avoid circular dependency
    # pylint: disable=import-outside-toplevel
    from debian._deb822_repro.parsing import Deb822Element

    prefix = None
    if isinstance(ast_tree, Deb822Element):
        ast_tree = [ast_tree]
    stack = [(0, "", iter(ast_tree))]
    current_no = 0
    if output_function is None:
        output_function = logging.info
    while stack:
        start_no, name, current_iter = stack[-1]
        for current in current_iter:
            current_no += 1
            if prefix is None:
                prefix = "  " * len(stack)
            if isinstance(current, Deb822Element):
                stack.append(
                    (current_no, current.__class__.__name__, iter(current.iter_parts()))
                )
                output_function(prefix + current.__class__.__name__)
                prefix = None
                break
            output_function(prefix + str(current))
        else:
            # current_iter is depleted
            stack.pop()
            prefix = None
            if (
                end_marker_after is not None
                and start_no + end_marker_after <= current_no
                and name
            ):
                if prefix is None:
                    prefix = "  " * len(stack)
                output_function(prefix + "# <-- END OF " + name)


def combine_into_replacement(
    source_class,  # type: Type[TE]
    replacement_class,  # type: Type[R]
    *,
    constructor=None,  # type: Optional[Callable[[List[TE]], R]]
):
    # type: (...) -> _combine_parts_ret_type[TE, R]
    """Combines runs of one type into another type

    This is primarily useful for transforming tokens (e.g, Comment tokens) into
    the relevant element (such as the Comment element).
    """
    if constructor is None:
        _constructor = cast("Callable[[List[TE]], R]", replacement_class)
    else:
        # Force mypy to see that constructor is no longer optional
        _constructor = constructor

    def _impl(token_stream):
        # type: (Iterable[Union[TokenOrElement, TE]]) -> Iterable[Union[TokenOrElement, R]]
        tokens = []
        for token in token_stream:
            if isinstance(token, source_class):
                tokens.append(token)
                continue

            if tokens:
                yield _constructor(list(tokens))
                tokens.clear()
            yield token

        if tokens:
            yield _constructor(tokens)

    return _impl


if sys.version_info >= (3, 9) or TYPE_CHECKING:
    _bufferingIterator_Base = collections.abc.Iterator[T]
else:
    # Python 3.5 - 3.8 compat - we are not allowed to subscript the abc.Iterator
    # - use this little hack to work around it
    class _bufferingIterator_Base(collections.abc.Iterator, Generic[T], ABC):
        pass


class BufferingIterator(_bufferingIterator_Base[T], Generic[T]):

    def __init__(self, stream):
        # type: (Iterable[T]) -> None
        self._stream = iter(stream)  # type: Iterator[T]
        self._buffer = collections.deque()  # type: collections.deque[T]
        self._expired = False  # type: bool

    def __next__(self):
        # type: () -> T
        if self._buffer:
            return self._buffer.popleft()
        if self._expired:
            raise StopIteration
        return next(self._stream)

    def takewhile(self, predicate):
        # type: (Callable[[T], bool]) -> Iterable[T]
        """Variant of itertools.takewhile except it does not discard the first non-matching token"""
        buffer = self._buffer
        while buffer or self._fill_buffer(5):
            v = buffer[0]
            if predicate(v):
                buffer.popleft()
                yield v
            else:
                break

    def consume_many(self, count):
        # type: (int) -> List[T]
        self._fill_buffer(count)
        buffer = self._buffer
        if len(buffer) == count:
            ret = list(buffer)
            buffer.clear()
        else:
            ret = []
            while buffer and count:
                ret.append(buffer.popleft())
                count -= 1
        return ret

    def peek_buffer(self):
        # type: () -> List[T]
        return list(self._buffer)

    def peek_find(
        self,
        predicate,  # type: Callable[[T], bool]
        limit=None,  # type: Optional[int]
    ):
        # type: (...) -> Optional[int]
        buffer = self._buffer
        i = 0
        while limit is None or i < limit:
            if i >= len(buffer):
                self._fill_buffer(i + 5)
                if i >= len(buffer):
                    return None
            v = buffer[i]
            if predicate(v):
                return i + 1
            i += 1
        return None

    def _fill_buffer(self, number):
        # type: (int) -> bool
        if not self._expired:
            while len(self._buffer) < number:
                try:
                    self._buffer.append(next(self._stream))
                except StopIteration:
                    self._expired = True
                    break
        return bool(self._buffer)

    def peek(self):
        # type: () -> Optional[T]
        return self.peek_at(1)

    def peek_at(self, tokens_ahead):
        # type: (int) -> Optional[T]
        self._fill_buffer(tokens_ahead)
        return (
            self._buffer[tokens_ahead - 1]
            if len(self._buffer) >= tokens_ahead
            else None
        )

    def peek_many(self, number):
        # type: (int) -> List[T]
        self._fill_buffer(number)
        buffer = self._buffer
        if len(buffer) == number:
            ret = list(buffer)
        elif number:
            ret = []
            for t in buffer:
                ret.append(t)
                number -= 1
                if not number:
                    break
        else:
            ret = []
        return ret


def len_check_iterator(
    content,  # type: str
    stream,  # type: Iterable[TE]
    content_len=None,  # type: Optional[int]
):
    # type: (...) -> Iterable[TE]
    """Flatten a parser's output into tokens and verify it covers the entire line/text"""
    if content_len is None:
        content_len = len(content)
    # Fail-safe to ensure none of the value parsers incorrectly parse a value.
    covered = 0
    for token_or_element in stream:
        # We use the AttributeError to discriminate between elements and tokens
        # The cast()s are here to assist / workaround mypy not realizing that.
        try:
            tokens = cast("Deb822Element", token_or_element).iter_tokens()
        except AttributeError:
            token = cast("Deb822Token", token_or_element)
            covered += len(token.text)
        else:
            for token in tokens:
                covered += len(token.text)
        yield token_or_element
    if covered != content_len:
        if covered < content_len:
            msg = textwrap.dedent(
                """\
            Value parser did not fully cover the entire line with tokens (
            missing range {covered}..{content_len}).  Occurred when parsing "{content}"
            """
            ).format(covered=covered, content_len=content_len, content=content)
            raise ValueError(msg)
        msg = textwrap.dedent(
            """\
                    Value parser emitted tokens for more text than was present?  Should have
                     emitted {content_len} characters, got {covered}. Occurred when parsing
                     "{content}"
                    """
        ).format(covered=covered, content_len=content_len, content=content)
        raise ValueError(msg)
