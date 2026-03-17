# -*- coding: utf-8 -*- vim: fileencoding=utf-8 :

import collections.abc
import contextlib
import sys
import textwrap
import weakref
from abc import ABC
from types import TracebackType
from typing import (
    Iterable,
    Iterator,
    List,
    Union,
    Dict,
    Optional,
    Callable,
    Any,
    Generic,
    Type,
    Tuple,
    IO,
    cast,
    overload,
    Mapping,
    TYPE_CHECKING,
    Sequence,
)
from weakref import ReferenceType

from debian._deb822_repro._util import (
    combine_into_replacement,
    BufferingIterator,
    len_check_iterator,
)
from debian._deb822_repro.formatter import (
    FormatterContentToken,
    one_value_per_line_trailing_separator,
    format_field,
)
from debian._deb822_repro.locatable import Locatable, START_POSITION, Position, Range
from debian._deb822_repro.tokens import (
    Deb822Token,
    Deb822ValueToken,
    Deb822SemanticallySignificantWhiteSpace,
    Deb822SpaceSeparatorToken,
    Deb822CommentToken,
    Deb822WhitespaceToken,
    Deb822ValueContinuationToken,
    Deb822NewlineAfterValueToken,
    Deb822CommaToken,
    Deb822FieldNameToken,
    Deb822FieldSeparatorToken,
    Deb822ErrorToken,
    tokenize_deb822_file,
    comma_split_tokenizer,
    whitespace_split_tokenizer,
)
from debian._deb822_repro.types import AmbiguousDeb822FieldKeyError, SyntaxOrParseError
from debian._util import (
    resolve_ref,
    LinkedList,
    LinkedListNode,
    OrderedSet,
    _strI,
    default_field_sort_key,
)

from debian._util import T

# for some reason, pylint does not see that Commentish is used in typing
from debian._deb822_repro.types import (  # pylint: disable=unused-import
    ST,
    VE,
    TE,
    ParagraphKey,
    TokenOrElement,
    Commentish,
    ParagraphKeyBase,
    FormatterCallback,
)

StreamingValueParser = Callable[[Deb822Token, BufferingIterator[Deb822Token]], VE]
StrToValueParser = Callable[[str], Iterable[Union["Deb822Token", VE]]]
KVPNode = LinkedListNode["Deb822KeyValuePairElement"]


class ValueReference(Generic[TE]):
    """Reference to a value inside a Deb822 paragraph

    This is useful for cases where want to modify values "in-place" or maybe
    conditionally remove a value after looking at it.

    ValueReferences can be invalidated by various changes or actions performed
    to the underlying provider of the value reference.  As an example, sorting
    a list of values will generally invalidate all ValueReferences related to
    that list.

    The ValueReference will raise validity issues where it detects them but most
    of the time it will not notice.  As a means to this end,  the ValueReference
    will *not* keep a strong reference to the underlying value.  This enables it
    to detect when the container goes out of scope.  However, keep in mind that
    the timeliness of garbage collection is implementation defined (e.g., pypy
    does not use ref-counting).
    """

    __slots__ = (
        "_node",
        "_render",
        "_value_factory",
        "_removal_handler",
        "_mutation_notifier",
    )

    def __init__(
        self,
        node,  # type: LinkedListNode[TE]
        render,  # type: Callable[[TE], str]
        value_factory,  # type: Callable[[str], TE]
        removal_handler,  # type: Callable[[LinkedListNode[TokenOrElement]], None]
        mutation_notifier,  # type: Optional[Callable[[], None]]
    ):
        self._node = weakref.ref(
            node
        )  # type: Optional[ReferenceType[LinkedListNode[TE]]]
        self._render = render
        self._value_factory = value_factory
        self._removal_handler = removal_handler
        self._mutation_notifier = mutation_notifier

    def _resolve_node(self):
        # type: () -> LinkedListNode[TE]
        # NB: We check whether the "ref" itself is None (instead of the ref resolving to None)
        # This enables us to tell the difference between "known removal" vs. "garbage collected"
        if self._node is None:
            raise RuntimeError("Cannot use ValueReference after remove()")
        node = self._node()
        if node is None:
            raise RuntimeError("ValueReference is invalid (garbage collected)")
        return node

    @property
    def value(self) -> str:
        """Resolve the reference into a str"""
        return self._render(self._resolve_node().value)

    @value.setter
    def value(self, new_value):
        # type: (str) -> None
        """Update the reference value

        Updating the value via this method will *not* invalidate the reference (or other
        references to the same container).

        This can raise an exception if the new value does not follow the requirements
        for the referenced values.  As an example, values in whitespace separated
        lists cannot contain spaces and would trigger an exception.
        """
        self._resolve_node().value = self._value_factory(new_value)
        if self._mutation_notifier is not None:
            self._mutation_notifier()

    @property
    def locatable(self) -> Locatable:
        """Reference to a locatable that can be used to determine where this value is"""
        return self._resolve_node().value

    def remove(self) -> None:
        """Remove the underlying value

        This will invalidate the ValueReference (and any other ValueReferences pointing
        to that exact value).  The validity of other ValueReferences to that container
        remains unaffected.
        """
        self._removal_handler(
            cast("LinkedListNode[TokenOrElement]", self._resolve_node())
        )
        self._node = None


if sys.version_info >= (3, 9) or TYPE_CHECKING:
    _Deb822ParsedTokenList_ContextManager = contextlib.AbstractContextManager[T]
else:
    # Python 3.5 - 3.8 compat - we are not allowed to subscript the abc.Iterator
    # - use this little hack to work around it
    # Note that Python 3.5 is so old that it does not have AbstractContextManager,
    # so we re-implement it here.
    class _Deb822ParsedTokenList_ContextManager(Generic[T]):

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None


class Deb822ParsedTokenList(
    Generic[VE, ST],
    _Deb822ParsedTokenList_ContextManager["Deb822ParsedTokenList[VE, ST]"],
):

    def __init__(
        self,
        kvpair_element,  # type: 'Deb822KeyValuePairElement'
        interpreted_value_element,  # type: Deb822InterpretationProxyElement
        vtype,  # type: Type[VE]
        stype,  # type: Type[ST]
        str2value_parser,  # type: StrToValueParser[VE]
        default_separator_factory,  # type: Callable[[], ST]
        render,  # type: Callable[[VE], str]
    ):
        # type: (...) -> None
        self._kvpair_element = kvpair_element
        self._proxy_element = interpreted_value_element
        self._token_list = LinkedList(interpreted_value_element.parts)
        self._vtype = vtype
        self._stype = stype
        self._str2value_parser = str2value_parser
        self._default_separator_factory = default_separator_factory
        self._value_factory = _parser_to_value_factory(str2value_parser, vtype)
        self._render = render
        self._format_preserve_original_formatting = True
        self._formatter = (
            one_value_per_line_trailing_separator
        )  # type: FormatterCallback
        self._changed = False
        self.__continuation_line_char = None  # type: Optional[str]
        assert self._token_list
        last_token = self._token_list.tail

        if last_token is not None and isinstance(
            last_token, Deb822NewlineAfterValueToken
        ):
            # We always remove the last newline (if present), because then
            # adding values will happen after the last value rather than on
            # a new line by default.
            #
            # On write, we always ensure the value ends on a newline (even
            # if it did not before).  This is simpler and should be a
            # non-issue in practise.
            self._token_list.pop()

    def __iter__(self):
        # type: () -> Iterator[str]
        yield from (self._render(v) for v in self.value_parts)

    def __bool__(self) -> bool:
        return next(iter(self), None) is not None

    def __exit__(
        self,
        exc_type,  # type: Optional[Type[BaseException]]
        exc_val,  # type: Optional[BaseException]
        exc_tb,  # type: Optional[TracebackType]
    ):
        # type: (...) -> Optional[bool]
        if exc_type is None and self._changed:
            self._update_field()
        return super().__exit__(exc_type, exc_val, exc_tb)

    @property
    def value_parts(self):
        # type: () -> Iterator[VE]
        yield from (v for v in self._token_list if isinstance(v, self._vtype))

    def iter_parts(self) -> Iterable[TokenOrElement]:
        yield from self._token_list

    def _mark_changed(self) -> None:
        self._changed = True

    def iter_value_references(self):
        # type: () -> Iterator[ValueReference[VE]]
        """Iterate over all values in the list (as ValueReferences)

        This is useful for doing inplace modification of the values or even
        streaming removal of field values.  It is in general also more
        efficient when more than one value is updated or removed.
        """
        yield from (
            ValueReference(
                cast("LinkedListNode[VE]", n),
                self._render,
                self._value_factory,
                self._remove_node,
                self._mark_changed,
            )
            for n in self._token_list.iter_nodes()
            if isinstance(n.value, self._vtype)
        )

    def append_separator(self, space_after_separator=True):
        # type: (bool) -> None

        separator_token = self._default_separator_factory()
        if separator_token.is_whitespace:
            space_after_separator = False

        self._changed = True
        self._append_continuation_line_token_if_necessary()
        self._token_list.append(separator_token)

        if space_after_separator and not separator_token.is_whitespace:
            self._token_list.append(Deb822WhitespaceToken(" "))

    def replace(self, orig_value, new_value):
        # type: (str, str) -> None
        """Replace the first instance of a value with another

        This method will *not* affect the validity of ValueReferences.
        """
        vtype = self._vtype
        for node in self._token_list.iter_nodes():
            if isinstance(node.value, vtype) and self._render(node.value) == orig_value:
                node.value = self._value_factory(new_value)
                self._changed = True
                break
        else:
            raise ValueError("list.replace(x, y): x not in list")

    def remove(self, value):
        # type: (str) -> None
        """Remove the first instance of a value

        Removal will invalidate ValueReferences to the value being removed.
        ValueReferences to other values will be unaffected.
        """
        vtype = self._vtype
        for node in self._token_list.iter_nodes():
            if isinstance(node.value, vtype) and self._render(node.value) == value:
                node_to_remove = node
                break
        else:
            raise ValueError("list.remove(x): x not in list")

        return self._remove_node(node_to_remove)

    def _remove_node(self, node_to_remove):
        # type: (LinkedListNode[TokenOrElement]) -> None
        vtype = self._vtype
        self._changed = True

        # We naively want to remove the node and every thing to the left of it
        # until the previous value.  That is the basic idea for now (ignoring
        # special-cases for now).
        #
        # Example:
        #
        # """
        # Multiline-Keywords: bar[
        # # Comment about foo
        #                     foo]
        #                     baz
        # Keywords: bar[ foo] baz
        # Comma-List: bar[, foo], baz,
        # Multiline-Comma-List: bar[,
        # # Comment about foo
        #                       foo],
        #                       baz,
        # """
        #
        # Assuming we want to remove "foo" for the lists, the []-markers
        # show what we aim to remove.  This has the nice side-effect of
        # preserving whether nor not the value has a trailing separator.
        # Note that we do *not* attempt to repair missing separators but
        # it may fix duplicated separators by "accident".
        #
        # Now, there are two special cases to be aware of, where this approach
        # has short comings:
        #
        # 1) If foo is the only value (in which case, "delete everything"
        #    is the only option).
        # 2) If foo is the first value
        # 3) If foo is not the only value on the line and we see a comment
        #    inside the deletion range.
        #
        # For 2) + 3), we attempt to flip and range to delete and every
        # thing after it (up to but exclusion "baz") instead.  This
        # definitely fixes 3), but 2) has yet another corner case, namely:
        #
        # """
        # Multiline-Comma-List: foo,
        # # Remark about bar
        #                       bar,
        # Another-Case: foo
        # # Remark, also we use leading separator
        #             , bar
        # """
        #
        # The options include:
        #
        #  A) Discard the comment - brain-dead simple
        #  B) Hoist the comment up to a field comment, but then what if the
        #     field already has a comment?
        #  C) Clear the first value line leaving just the newline and
        #     replace the separator before "bar" (if present) with a space.
        #     (leaving you with the value of the form "\n# ...\n      bar")
        #

        first_value_on_lhs = None  # type: Optional[LinkedListNode[TokenOrElement]]
        first_value_on_rhs = None  # type: Optional[LinkedListNode[TokenOrElement]]
        comment_before_previous_value = False
        comment_before_next_value = False
        for past_node in node_to_remove.iter_previous(skip_current=True):
            past_token = past_node.value
            if isinstance(past_token, Deb822Token) and past_token.is_comment:
                comment_before_previous_value = True
                continue
            if isinstance(past_token, vtype):
                first_value_on_lhs = past_node
                break

        for future_node in node_to_remove.iter_next(skip_current=True):
            future_token = future_node.value
            if isinstance(future_token, Deb822Token) and future_token.is_comment:
                comment_before_next_value = True
                continue
            if isinstance(future_token, vtype):
                first_value_on_rhs = future_node
                break

        if first_value_on_rhs is None and first_value_on_lhs is None:
            # This was the last value, just remove everything.
            self._token_list.clear()
            return

        if first_value_on_lhs is not None and not comment_before_previous_value:
            # Delete left
            delete_lhs_of_node = True
        elif first_value_on_rhs is not None and not comment_before_next_value:
            # Delete right
            delete_lhs_of_node = False
        else:
            # There is a comment on either side (or no value on one and a
            # comment and the other). Keep it simple, we just delete to
            # one side (preferring deleting to left if possible).
            delete_lhs_of_node = first_value_on_lhs is not None

        if delete_lhs_of_node:
            first_remain_lhs = first_value_on_lhs
            first_remain_rhs = node_to_remove.next_node
        else:
            first_remain_lhs = node_to_remove.previous_node
            first_remain_rhs = first_value_on_rhs

        # Actual deletion - with some manual labour to update HEAD/TAIL of
        # the list in case we do a "delete everything left/right this node".
        if first_remain_lhs is None:
            self._token_list.head_node = first_remain_rhs
        if first_remain_rhs is None:
            self._token_list.tail_node = first_remain_lhs
        LinkedListNode.link_nodes(first_remain_lhs, first_remain_rhs)

    def append(self, value):
        # type: (str) -> None
        vt = self._value_factory(value)
        self.append_value(vt)

    def append_value(self, vt):
        # type: (VE) -> None
        value_parts = self._token_list
        if value_parts:
            needs_separator = False
            stype = self._stype
            vtype = self._vtype
            for t in reversed(value_parts):
                if isinstance(t, vtype):
                    needs_separator = True
                    break
                if isinstance(t, stype):
                    break

            if needs_separator:
                self.append_separator()
        else:
            # Looks nicer if there is a space before the very first value
            self._token_list.append(Deb822WhitespaceToken(" "))
        self._append_continuation_line_token_if_necessary()
        self._changed = True
        value_parts.append(vt)

    def _previous_is_newline(self) -> bool:
        tail = self._token_list.tail
        return tail is not None and tail.convert_to_text().endswith("\n")

    def append_newline(self) -> None:
        if self._previous_is_newline():
            raise ValueError(
                "Cannot add a newline after a token that ends on a newline"
            )
        self._token_list.append(Deb822NewlineAfterValueToken())

    def append_comment(self, comment_text):
        # type: (str) -> None
        tail = self._token_list.tail
        if tail is None or not tail.convert_to_text().endswith("\n"):
            self.append_newline()
        comment_token = Deb822CommentToken(_format_comment(comment_text))
        self._token_list.append(comment_token)

    @property
    def _continuation_line_char(self) -> str:
        char = self.__continuation_line_char
        if char is None:
            # Use ' ' by default but match the existing field if possible.
            char = " "
            for token in self._token_list:
                if isinstance(token, Deb822ValueContinuationToken):
                    char = token.text
                    break
            self.__continuation_line_char = char
        return char

    def _append_continuation_line_token_if_necessary(self) -> None:
        tail = self._token_list.tail
        if tail is not None and tail.convert_to_text().endswith("\n"):
            self._token_list.append(
                Deb822ValueContinuationToken(self._continuation_line_char)
            )

    def reformat_when_finished(self) -> None:
        self._enable_reformatting()
        self._changed = True

    def _enable_reformatting(self) -> None:
        self._format_preserve_original_formatting = False

    def no_reformatting_when_finished(self) -> None:
        self._format_preserve_original_formatting = True

    def value_formatter(
        self,
        formatter,  # type: FormatterCallback
        force_reformat=False,  # type: bool
    ):
        # type: (...) -> None
        """Use a custom formatter when formatting the value

        :param formatter: A formatter (see debian._deb822_repro.formatter.format_field
          for details)
        :param force_reformat: If True, always reformat the field even if there are
          no (other) changes performed.  By default, fields are only reformatted if
          they are changed.
        """
        self._formatter = formatter
        self._format_preserve_original_formatting = False
        if force_reformat:
            self._changed = True

    def clear(self) -> None:
        """Like list.clear() - removes all content (including comments and spaces)"""
        if self._token_list:
            self._changed = True
        self._token_list.clear()

    def _iter_content_as_tokens(self):
        # type: () -> Iterable[Deb822Token]
        for te in self._token_list:
            if isinstance(te, Deb822Element):
                yield from te.iter_tokens()
            else:
                yield te

    def _generate_reformatted_field_content(self) -> str:
        separator_token = self._default_separator_factory()
        vtype = self._vtype
        stype = self._stype
        token_list = self._token_list

        def _token_iter():
            # type: () -> Iterator[FormatterContentToken]
            text = ""  # type: str
            for te in token_list:
                if isinstance(te, Deb822Token):
                    if te.is_comment:
                        yield FormatterContentToken.comment_token(te.text)
                    elif isinstance(te, stype):
                        text = te.text
                        yield FormatterContentToken.separator_token(text)
                else:
                    assert isinstance(te, vtype)
                    text = te.convert_to_text()
                    yield FormatterContentToken.value_token(text)

        return format_field(
            self._formatter,
            self._kvpair_element.field_name,
            FormatterContentToken.separator_token(separator_token.text),
            _token_iter(),
        )

    def _generate_field_content(self) -> str:
        return "".join(t.text for t in self._iter_content_as_tokens())

    def _generate_kvpair(self) -> "Deb822KeyValuePairElement":
        kvpair_element = self._kvpair_element
        field_name = kvpair_element.field_name
        token_list = self._token_list
        tail = token_list.tail
        had_tokens = False

        for t in self._iter_content_as_tokens():
            had_tokens = True
            if not t.is_comment and not t.is_whitespace:
                break
        else:
            if had_tokens:
                raise ValueError(
                    "Field must be completely empty or have content "
                    "(i.e. non-whitespace and non-comments)"
                )
        if tail is not None:
            if isinstance(tail, Deb822Token) and tail.is_comment:
                raise ValueError("Fields must not end on a comment")
            if not tail.convert_to_text().endswith("\n"):
                # Always end on a newline
                self.append_newline()

            if self._format_preserve_original_formatting:
                value_text = self._generate_field_content()
                text = ":".join((field_name, value_text))
            else:
                text = self._generate_reformatted_field_content()

            new_content = text.splitlines(keepends=True)
        else:
            # Special-case for the empty list which will be mapped to
            # an empty field.  Always end on a newline (avoids errors
            # if there is a field after this)
            new_content = [field_name + ":\n"]

        # As absurd as it might seem, it is easier to just use the parser to
        # construct the AST correctly
        deb822_file = parse_deb822_file(iter(new_content))
        error_token = deb822_file.find_first_error_element()
        if error_token:
            # _print_ast(deb822_file)
            raise ValueError("Syntax error in new field value for " + field_name)
        paragraph = next(iter(deb822_file))
        assert isinstance(paragraph, Deb822NoDuplicateFieldsParagraphElement)
        new_kvpair_element = paragraph.get_kvpair_element(field_name)
        assert new_kvpair_element is not None
        return new_kvpair_element

    def convert_to_text(self, *, with_field_name: bool = False) -> str:
        kvpair = self._generate_kvpair()
        element = kvpair if with_field_name else kvpair.value_element
        return element.convert_to_text()

    def _update_field(self) -> None:
        kvpair_element = self._kvpair_element
        kvpair_element.value_element = self._generate_kvpair().value_element
        self._changed = False

    def sort_elements(
        self,
        *,
        key=None,  # type: Optional[Callable[[VE], Any]]
        reverse=False,  # type: bool
    ):
        # type: (...) -> None
        """Sort the elements (abstract values) in this list.

        This method will sort the logical values of the list. It will
        attempt to preserve comments associated with a given value where
        possible.  Whether space and separators are preserved depends on
        the contents of the field as well as the formatting settings.

        Sorting (without reformatting) is likely to leave you with "awkward"
        whitespace. Therefore, you almost always want to apply reformatting
        such as the reformat_when_finished() method.

        Sorting will invalidate all ValueReferences.
        """
        comment_start_node = None
        vtype = self._vtype
        stype = self._stype

        def key_func(x):
            # type: (Tuple[VE, List[TokenOrElement]]) -> Any
            if key:
                return key(x[0])
            return x[0].convert_to_text()

        parts = []

        for node in self._token_list.iter_nodes():
            value = node.value
            if isinstance(value, Deb822Token) and value.is_comment:
                if comment_start_node is None:
                    comment_start_node = node
                continue

            if isinstance(value, vtype):
                comments = []
                if comment_start_node is not None:
                    for keep_node in comment_start_node.iter_next(skip_current=False):
                        if keep_node is node:
                            break
                        comments.append(keep_node.value)
                parts.append((value, comments))
                comment_start_node = None

        parts.sort(key=key_func, reverse=reverse)

        self._changed = True
        self._token_list.clear()
        first_value = True

        separator_is_space = self._default_separator_factory().is_whitespace

        for value, comments in parts:
            if first_value:
                first_value = False
                if comments:
                    # While unlikely, there could be a separator between the comments.
                    # It would be in the way and we remove it.
                    comments = [x for x in comments if not isinstance(x, stype)]
                    # Comments cannot start the field, so inject a newline to
                    # work around that
                    self.append_newline()
            else:
                if not separator_is_space and not any(
                    isinstance(x, stype) for x in comments
                ):
                    # While unlikely, you can hide a comma between two comments and expect
                    # us to preserve it.  However, the more common case is that the separator
                    # appeared before the comments and was thus omitted (leaving us to re-add
                    # it here).
                    self.append_separator(space_after_separator=False)
                if comments:
                    self.append_newline()
                else:
                    self._token_list.append(Deb822WhitespaceToken(" "))

            self._token_list.extend(comments)
            self.append_value(value)

    def sort(
        self,
        *,
        key=None,  # type: Optional[Callable[[str], Any]]
        **kwargs,  # type: Any
    ):
        # type: (...) -> None
        """Sort the values (rendered as str) in this list.

        This method will sort the logical values of the list. It will
        attempt to preserve comments associated with a given value where
        possible.  Whether space and separators are preserved depends on
        the contents of the field as well as the formatting settings.

        Sorting (without reformatting) is likely to leave you with "awkward"
        whitespace. Therefore, you almost always want to apply reformatting
        such as the reformat_when_finished() method.

        Sorting will invalidate all ValueReferences.
        """
        if key is not None:
            render = self._render
            kwargs["key"] = lambda vt: key(render(vt))
        self.sort_elements(**kwargs)


class Interpretation(Generic[T]):

    def interpret(
        self,
        kvpair_element,  # type: Deb822KeyValuePairElement
        discard_comments_on_read=True,  # type: bool
    ):
        # type: (...) -> T
        raise NotImplementedError  # pragma: no cover


class GenericContentBasedInterpretation(Interpretation[T], Generic[T, VE]):

    def __init__(
        self,
        tokenizer,  # type: Callable[[str], Iterable['Deb822Token']]
        value_parser,  # type: StreamingValueParser[VE]
    ):
        # type: (...) -> None
        super().__init__()
        self._tokenizer = tokenizer
        self._value_parser = value_parser

    def _high_level_interpretation(
        self,
        kvpair_element,  # type: Deb822KeyValuePairElement
        proxy_element,  # type: Deb822InterpretationProxyElement
        discard_comments_on_read=True,  # type: bool
    ):
        # type: (...) -> T
        raise NotImplementedError  # pragma: no cover

    def _parse_stream(
        self, buffered_iterator  # type: BufferingIterator[Deb822Token]
    ):
        # type: (...) -> Iterable[Union[Deb822Token, VE]]

        value_parser = self._value_parser
        for token in buffered_iterator:
            if isinstance(token, Deb822ValueToken):
                yield value_parser(token, buffered_iterator)
            else:
                yield token

    def _parse_kvpair(
        self, kvpair  # type: Deb822KeyValuePairElement
    ):
        # type: (...) -> Deb822InterpretationProxyElement
        value_element = kvpair.value_element
        content = value_element.convert_to_text()
        token_list = []  # type: List['TokenOrElement']
        token_list.extend(self._parse_str(content))
        return Deb822InterpretationProxyElement(value_element, token_list)

    def _parse_str(self, content):
        # type: (str) -> Iterable[Union[Deb822Token, VE]]
        content_len = len(content)
        biter = BufferingIterator(
            len_check_iterator(
                content,
                self._tokenizer(content),
                content_len=content_len,
            )
        )
        yield from len_check_iterator(
            content,
            self._parse_stream(biter),
            content_len=content_len,
        )

    def interpret(
        self,
        kvpair_element,  # type: Deb822KeyValuePairElement
        discard_comments_on_read=True,  # type: bool
    ):
        # type: (...) -> T
        proxy_element = self._parse_kvpair(kvpair_element)
        return self._high_level_interpretation(
            kvpair_element,
            proxy_element,
            discard_comments_on_read=discard_comments_on_read,
        )


def _parser_to_value_factory(
    parser,  # type: StrToValueParser[VE]
    vtype,  # type: Type[VE]
):
    # type: (...) -> Callable[[str], VE]
    def _value_factory(v):
        # type: (str) -> VE
        if v == "":
            raise ValueError("The empty string is not a value")
        token_iter = iter(parser(v))
        t1 = next(token_iter, None)  # type: Optional[Union[TokenOrElement]]
        t2 = next(token_iter, None)
        assert t1 is not None, (
            'Bad parser - it returned None (or no TE) for "' + v + '"'
        )
        if t2 is not None:
            msg = textwrap.dedent(
                """\
            The input "{v}" should have been exactly one element, but the parser provided at
             least two.  This can happen with unnecessary leading/trailing whitespace
             or including commas the value for a comma list.
            """
            ).format(v=v)
            raise ValueError(msg)
        if not isinstance(t1, vtype):
            if isinstance(t1, Deb822Token) and (t1.is_comment or t1.is_whitespace):
                raise ValueError(
                    'The input "{v}" is whitespace or a comment: Expected a value'
                )
            msg = (
                'The input "{v}" should have produced a element of type {vtype_name}, but'
                " instead it produced {t1}"
            )
            raise ValueError(msg.format(v=v, vtype_name=vtype.__name__, t1=t1))

        assert len(t1.convert_to_text()) == len(v), (
            "Bad tokenizer - the token did not cover the input text"
            " exactly ({t1_len} != {v_len}".format(
                t1_len=len(t1.convert_to_text()), v_len=len(v)
            )
        )
        return t1

    return _value_factory


class ListInterpretation(
    GenericContentBasedInterpretation[Deb822ParsedTokenList[VE, ST], VE]
):

    def __init__(
        self,
        tokenizer,  # type: Callable[[str], Iterable['Deb822Token']]
        value_parser,  # type: StreamingValueParser[VE]
        vtype,  # type: Type[VE]
        stype,  # type: Type[ST]
        default_separator_factory,  # type: Callable[[], ST]
        render_factory,  # type: Callable[[bool], Callable[[VE], str]]
    ):
        # type: (...) -> None
        super().__init__(tokenizer, value_parser)
        self._vtype = vtype
        self._stype = stype
        self._default_separator_factory = default_separator_factory
        self._render_factory = render_factory

    def _high_level_interpretation(
        self,
        kvpair_element,  # type: Deb822KeyValuePairElement
        proxy_element,  # type: Deb822InterpretationProxyElement
        discard_comments_on_read=True,  # type: bool
    ):
        # type: (...) -> Deb822ParsedTokenList[VE, ST]
        return Deb822ParsedTokenList(
            kvpair_element,
            proxy_element,
            self._vtype,
            self._stype,
            self._parse_str,
            self._default_separator_factory,
            self._render_factory(discard_comments_on_read),
        )


def _parse_whitespace_list_value(token, _):
    # type: (Deb822Token, BufferingIterator[Deb822Token]) -> Deb822ParsedValueElement
    return Deb822ParsedValueElement([token])


def _is_comma_token(v):
    # type: (TokenOrElement) -> bool
    # Consume tokens until the next comma
    return isinstance(v, Deb822CommaToken)


def _parse_separator_list_value(
    is_separator_token: Callable[[TokenOrElement], bool],
) -> Callable[
    [Deb822Token, BufferingIterator[Deb822Token]], "Deb822ParsedValueElement"
]:
    def _parse_list_value(
        token: Deb822Token, buffered_iterator: BufferingIterator[Deb822Token]
    ) -> "Deb822ParsedValueElement":
        separator_offset = buffered_iterator.peek_find(is_separator_token)
        value_parts = [token]
        if separator_offset is not None:
            # The value is followed by a separator, and now we know where it ends
            value_parts.extend(buffered_iterator.peek_many(separator_offset - 1))
        else:
            # The value is the last value there is.  Consume all remaining tokens
            # and then trim from the right.
            value_parts.extend(buffered_iterator.peek_buffer())
        while value_parts and not isinstance(value_parts[-1], Deb822ValueToken):
            value_parts.pop()

        buffered_iterator.consume_many(len(value_parts) - 1)
        return Deb822ParsedValueElement(value_parts)

    return _parse_list_value


_parse_comma_list_value = _parse_separator_list_value(_is_comma_token)


def _parse_uploaders_list_value(token, buffered_iterator):
    # type: (Deb822Token, BufferingIterator[Deb822Token]) -> Deb822ParsedValueElement

    # This is similar to _parse_comma_list_value *except* that there is an extra special
    # case.  Namely, comma only counts as a true separator if it follows ">"
    value_parts = [token]
    comma_offset = -1  # type: Optional[int]
    while comma_offset is not None:
        comma_offset = buffered_iterator.peek_find(_is_comma_token)
        if comma_offset is not None:
            # The value is followed by a comma.  Verify that this is a terminating
            # comma (comma may appear in the name or email)
            #
            # We include value_parts[-1] to easily cope with the common case of
            # "foo <a@b.com>," where we will have 0 peeked element to examine.
            peeked_elements = [value_parts[-1]]
            peeked_elements.extend(buffered_iterator.peek_many(comma_offset - 1))
            comma_was_separator = False
            i = len(peeked_elements) - 1
            while i >= 0:
                token = peeked_elements[i]
                if isinstance(token, Deb822ValueToken):
                    if token.text.endswith(">"):
                        # The comma terminates the value
                        value_parts.extend(buffered_iterator.consume_many(i))
                        assert isinstance(
                            value_parts[-1], Deb822ValueToken
                        ) and value_parts[-1].text.endswith(">"), "Got: " + str(
                            value_parts
                        )
                        comma_was_separator = True
                    break
                i -= 1
            if comma_was_separator:
                break
            value_parts.extend(buffered_iterator.consume_many(comma_offset))
            assert isinstance(value_parts[-1], Deb822CommaToken)
        else:
            # The value is the last value there is.  Consume all remaining tokens
            # and then trim from the right.
            remaining_part = buffered_iterator.peek_buffer()
            consume_elements = len(remaining_part)
            value_parts.extend(remaining_part)
            while value_parts and not isinstance(value_parts[-1], Deb822ValueToken):
                value_parts.pop()
                consume_elements -= 1
            buffered_iterator.consume_many(consume_elements)

    return Deb822ParsedValueElement(value_parts)


class Deb822Element(Locatable):
    """Composite elements (consists of 1 or more tokens)"""

    __slots__ = ("_parent_element", "_full_size_cache", "__weakref__")

    def __init__(self) -> None:
        self._parent_element = None  # type: Optional[ReferenceType['Deb822Element']]
        self._full_size_cache = None  # type: Optional[Range]

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        raise NotImplementedError  # pragma: no cover

    def iter_parts_of_type(self, only_element_or_token_type):
        # type: (Type[TE]) -> Iterable[TE]
        for part in self.iter_parts():
            if isinstance(part, only_element_or_token_type):
                yield part

    def iter_tokens(self):
        # type: () -> Iterable[Deb822Token]
        for part in self.iter_parts():
            # Control check to catch bugs early
            assert part._parent_element is not None
            if isinstance(part, Deb822Element):
                yield from part.iter_tokens()
            else:
                yield part

    def iter_recurse(
        self, *, only_element_or_token_type=None  # type: Optional[Type[TE]]
    ):
        # type: (...) -> Iterable[TE]
        for part in self.iter_parts():
            if only_element_or_token_type is None or isinstance(
                part, only_element_or_token_type
            ):
                yield cast("TE", part)
            if isinstance(part, Deb822Element):
                yield from part.iter_recurse(
                    only_element_or_token_type=only_element_or_token_type
                )

    @property
    def is_error(self) -> bool:
        return False

    @property
    def is_comment(self) -> bool:
        return False

    @property
    def is_whitespace(self) -> bool:
        return False

    @property
    def is_separator(self) -> bool:
        return False

    @property
    def parent_element(self):
        # type: () -> Optional[Deb822Element]
        return resolve_ref(self._parent_element)

    @parent_element.setter
    def parent_element(self, new_parent):
        # type: (Optional[Deb822Element]) -> None
        self._parent_element = (
            weakref.ref(new_parent) if new_parent is not None else None
        )

    def _init_parent_of_parts(self) -> None:
        for part in self.iter_parts():
            part.parent_element = self

    # Deliberately not a "text" property, to signal that it is not necessary cheap.
    def convert_to_text(self) -> str:
        return "".join(t.text for t in self.iter_tokens())

    def clear_parent_if_parent(self, parent):
        # type: (Deb822Element) -> None
        if parent is self.parent_element:
            self._parent_element = None

    def size(self) -> Range:
        size_cache = self._full_size_cache
        if size_cache is None:
            size_cache = Range.from_position_and_sizes(
                START_POSITION,
                (p.size() for p in self.iter_parts()),
            )
            self._full_size_cache = size_cache
        return size_cache


class Deb822InterpretationProxyElement(Deb822Element):

    __slots__ = ("parts",)

    def __init__(
        self, real_element: Deb822Element, parts: List[TokenOrElement]
    ) -> None:
        super().__init__()
        self.parent_element = real_element
        self.parts = parts
        for p in parts:
            p.parent_element = self

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        return iter(self.parts)

    def position_in_parent(self) -> Position:
        parent = self.parent_element
        if parent is None:
            raise RuntimeError("parent was garbage collected")
        return parent.position_in_parent()

    def position_in_file(self) -> Position:
        parent = self.parent_element
        if parent is None:
            raise RuntimeError("parent was garbage collected")
        return parent.position_in_file()

    def size(self) -> Range:
        # Same as parent except we never use a cache.
        sizes = (p.size() for p in self.iter_parts())
        return Range.from_position_and_sizes(START_POSITION, sizes)


class Deb822ErrorElement(Deb822Element):
    """Element representing elements or tokens that are out of place

    Commonly, it will just be instances of Deb822ErrorToken, but it can be other
    things.  As an example if a parser discovers out of order elements/tokens,
    it can bundle them in a Deb822ErrorElement to signal that the sequence of
    elements/tokens are invalid (even if the tokens themselves are valid).
    """

    __slots__ = ("_parts",)

    def __init__(self, parts):
        # type: (Sequence[TokenOrElement]) -> None
        super().__init__()
        self._parts = tuple(parts)
        self._init_parent_of_parts()

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        yield from self._parts

    @property
    def is_error(self) -> bool:
        return True


class Deb822ValueLineElement(Deb822Element):
    """Consists of one "line" of a value"""

    __slots__ = (
        "_comment_element",
        "_continuation_line_token",
        "_leading_whitespace_token",
        "_value_tokens",
        "_trailing_whitespace_token",
        "_newline_token",
    )

    def __init__(
        self,
        comment_element,  # type: Optional[Deb822CommentElement]
        continuation_line_token,  # type: Optional[Deb822ValueContinuationToken]
        leading_whitespace_token,  # type: Optional[Deb822WhitespaceToken]
        value_parts,  # type: List[TokenOrElement]
        trailing_whitespace_token,  # type: Optional[Deb822WhitespaceToken]
        # only optional if it is the last line of the file and the file does not
        # end with a newline.
        newline_token,  # type: Optional[Deb822WhitespaceToken]
    ):
        # type: (...) -> None
        super().__init__()
        if comment_element is not None and continuation_line_token is None:
            raise ValueError("Only continuation lines can have comments")
        self._comment_element = comment_element  # type: Optional[Deb822CommentElement]
        self._continuation_line_token = continuation_line_token
        self._leading_whitespace_token = (
            leading_whitespace_token
        )  # type: Optional[Deb822WhitespaceToken]
        self._value_tokens = value_parts  # type: List[TokenOrElement]
        self._trailing_whitespace_token = trailing_whitespace_token
        self._newline_token = newline_token  # type: Optional[Deb822WhitespaceToken]
        self._init_parent_of_parts()

    @property
    def comment_element(self):
        # type: () -> Optional[Deb822CommentElement]
        return self._comment_element

    @property
    def continuation_line_token(self):
        # type: () -> Optional[Deb822ValueContinuationToken]
        return self._continuation_line_token

    @property
    def newline_token(self):
        # type: () -> Optional[Deb822WhitespaceToken]
        return self._newline_token

    def add_newline_if_missing(self) -> bool:
        if self._newline_token is None:
            self._newline_token = Deb822NewlineAfterValueToken()
            self._newline_token.parent_element = self
            self._full_size_cache = None
            return True
        return False

    def _iter_content_parts(self):
        # type: () -> Iterable[TokenOrElement]
        if self._leading_whitespace_token:
            yield self._leading_whitespace_token
        yield from self._value_tokens
        if self._trailing_whitespace_token:
            yield self._trailing_whitespace_token

    def _iter_content_tokens(self):
        # type: () -> Iterable[Deb822Token]
        for part in self._iter_content_parts():
            if isinstance(part, Deb822Element):
                yield from part.iter_tokens()
            else:
                yield part

    def convert_content_to_text(self) -> str:
        if (
            len(self._value_tokens) == 1
            and not self._leading_whitespace_token
            and not self._trailing_whitespace_token
            and isinstance(self._value_tokens[0], Deb822Token)
        ):
            # By default, we get a single value spanning the entire line
            # (minus continuation line and newline, but we are supposed to
            # exclude those)
            return self._value_tokens[0].text

        return "".join(t.text for t in self._iter_content_tokens())

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        if self._comment_element:
            yield self._comment_element
        if self._continuation_line_token:
            yield self._continuation_line_token
        yield from self._iter_content_parts()
        if self._newline_token:
            yield self._newline_token


class Deb822ValueElement(Deb822Element):
    __slots__ = ("_value_entry_elements",)

    def __init__(self, value_entry_elements):
        # type: (Sequence[Deb822ValueLineElement]) -> None
        super().__init__()
        # Split over two lines due to line length issues
        v = tuple(value_entry_elements)
        self._value_entry_elements = v  # type: Sequence[Deb822ValueLineElement]
        self._init_parent_of_parts()

    @property
    def value_lines(self):
        # type: () -> Sequence[Deb822ValueLineElement]
        """Read-only list of value entries"""
        return self._value_entry_elements

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        yield from self._value_entry_elements

    def add_final_newline_if_missing(self) -> bool:
        if self._value_entry_elements:
            changed = self._value_entry_elements[-1].add_newline_if_missing()
            if changed:
                self._full_size_cache = None
            return changed
        return False


class Deb822ParsedValueElement(Deb822Element):

    __slots__ = ("_text_cached", "_text_no_comments_cached", "_token_list")

    def __init__(self, tokens):
        # type: (List[Deb822Token]) -> None
        super().__init__()
        self._token_list = tokens
        self._init_parent_of_parts()
        if not isinstance(tokens[0], Deb822ValueToken) or not isinstance(
            tokens[-1], Deb822ValueToken
        ):
            raise ValueError(
                self.__class__.__name__ + " MUST start and end on a Deb822ValueToken"
            )
        if len(tokens) == 1:
            token = tokens[0]
            self._text_cached = token.text  # type: Optional[str]
            self._text_no_comments_cached = token.text  # type: Optional[str]
        else:
            self._text_cached = None
            self._text_no_comments_cached = None

    def convert_to_text(self) -> str:
        if self._text_no_comments_cached is None:
            self._text_no_comments_cached = super().convert_to_text()
        return self._text_no_comments_cached

    def convert_to_text_without_comments(self) -> str:
        if self._text_no_comments_cached is None:
            self._text_no_comments_cached = "".join(
                t.text for t in self.iter_tokens() if not t.is_comment
            )
        return self._text_no_comments_cached

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        yield from self._token_list


class Deb822CommentElement(Deb822Element):
    __slots__ = ("_comment_tokens",)

    def __init__(self, comment_tokens):
        # type: (Sequence[Deb822CommentToken]) -> None
        super().__init__()
        self._comment_tokens = tuple(
            comment_tokens
        )  # type: Sequence[Deb822CommentToken]
        if not comment_tokens:  # pragma: no cover
            raise ValueError("Comment elements must have at least one comment token")
        self._init_parent_of_parts()

    @property
    def is_comment(self) -> bool:
        return True

    def __len__(self) -> int:
        return len(self._comment_tokens)

    def __getitem__(self, item):
        # type: (int) -> Deb822CommentToken
        return self._comment_tokens[item]

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        yield from self._comment_tokens


class Deb822KeyValuePairElement(Deb822Element):
    __slots__ = (
        "_comment_element",
        "_field_token",
        "_separator_token",
        "_value_element",
    )

    def __init__(
        self,
        comment_element,  # type: Optional[Deb822CommentElement]
        field_token,  # type: Deb822FieldNameToken
        separator_token,  # type: Deb822FieldSeparatorToken
        value_element,  # type: Deb822ValueElement
    ):
        # type: (...) -> None
        super().__init__()
        self._comment_element = comment_element  # type: Optional[Deb822CommentElement]
        self._field_token = field_token  # type: Deb822FieldNameToken
        self._separator_token = separator_token  # type: Deb822FieldSeparatorToken
        self._value_element = value_element  # type: Deb822ValueElement
        self._init_parent_of_parts()

    @property
    def field_name(self):
        # type: () -> _strI
        return self.field_token.text

    @property
    def field_token(self):
        # type: () -> Deb822FieldNameToken
        return self._field_token

    @property
    def value_element(self):
        # type: () -> Deb822ValueElement
        return self._value_element

    @value_element.setter
    def value_element(self, new_value):
        # type: (Deb822ValueElement) -> None
        self._full_size_cache = None
        self._value_element.clear_parent_if_parent(self)
        self._value_element = new_value
        new_value.parent_element = self

    def interpret_as(
        self,
        interpreter,  # type: Interpretation[T]
        discard_comments_on_read=True,  # type: bool
    ):
        # type: (...) -> T
        return interpreter.interpret(
            self, discard_comments_on_read=discard_comments_on_read
        )

    @property
    def comment_element(self):
        # type: () -> Optional[Deb822CommentElement]
        return self._comment_element

    @comment_element.setter
    def comment_element(self, value):
        # type: (Optional[Deb822CommentElement]) -> None
        self._full_size_cache = None
        if value is not None:
            if not value[-1].text.endswith("\n"):
                raise ValueError("Field comments must end with a newline")
        if self._comment_element:
            self._comment_element.clear_parent_if_parent(self)
        if value is not None:
            value.parent_element = self
        self._comment_element = value

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        if self._comment_element:
            yield self._comment_element
        yield self._field_token
        yield self._separator_token
        yield self._value_element


def _format_comment(c):
    # type: (str) -> str
    if c == "":
        # Special-case: Empty strings are mapped to an empty comment line
        return "#\n"
    if "\n" in c[:-1]:
        raise ValueError("Comment lines must not have embedded newlines")
    if not c.endswith("\n"):
        c = c.rstrip() + "\n"
    if not c.startswith("#"):
        c = "# " + c.lstrip()
    return c


def _unpack_key(
    item,  # type: ParagraphKey
    raise_if_indexed=False,  # type: bool
):
    # type: (...) -> Tuple[_strI, Optional[int], Optional[Deb822FieldNameToken]]
    index = None  # type: Optional[int]
    name_token = None  # type: Optional[Deb822FieldNameToken]
    if isinstance(item, tuple):
        key, index = item
        if raise_if_indexed:
            # Fudge "(key, 0)" into a "key" callers to defensively support
            # both paragraph styles with the same key.
            if index != 0:
                msg = 'Cannot resolve key "{key}" with index {index}. The key is not indexed'
                raise KeyError(msg.format(key=key, index=index))
            index = None
        key = _strI(key)
    else:
        index = None
        if isinstance(item, Deb822FieldNameToken):
            name_token = item
            key = name_token.text
        else:
            key = _strI(item)

    return key, index, name_token


def _convert_value_lines_to_lines(
    value_lines,  # type: Iterable[Deb822ValueLineElement]
    strip_comments,  # type: bool
):
    # type: (...) -> Iterable[str]
    if not strip_comments:
        yield from (v.convert_to_text() for v in value_lines)
    else:
        for element in value_lines:
            yield "".join(x.text for x in element.iter_tokens() if not x.is_comment)


if sys.version_info >= (3, 9) or TYPE_CHECKING:
    _ParagraphMapping_Base = collections.abc.Mapping[ParagraphKey, T]
else:
    # Python 3.5 - 3.8 compat - we are not allowed to subscript the abc.Iterator
    # - use this little hack to work around it
    class _ParagraphMapping_Base(collections.abc.Mapping, Generic[T], ABC):
        pass


# Deb822ParagraphElement uses this Mixin (by having `_paragraph` return self).
# Therefore, the Mixin needs to call the "proper" methods on the paragraph to
# avoid doing infinite recursion.
class AutoResolvingMixin(Generic[T], _ParagraphMapping_Base[T]):

    @property
    def _auto_resolve_ambiguous_fields(self) -> bool:
        return True

    @property
    def _paragraph(self):
        # type: () -> Deb822ParagraphElement
        raise NotImplementedError  # pragma: no cover

    def __len__(self) -> int:
        return self._paragraph.kvpair_count

    def __contains__(self, item):
        # type: (object) -> bool
        return self._paragraph.contains_kvpair_element(item)

    def __iter__(self):
        # type: () -> Iterator[ParagraphKey]
        return iter(self._paragraph.iter_keys())

    def __getitem__(self, item):
        # type: (ParagraphKey) -> T
        if self._auto_resolve_ambiguous_fields and isinstance(item, str):
            v = self._paragraph.get_kvpair_element((item, 0))
        else:
            v = self._paragraph.get_kvpair_element(item)
        assert v is not None
        return self._interpret_value(item, v)

    def __delitem__(self, item):
        # type: (ParagraphKey) -> None
        self._paragraph.remove_kvpair_element(item)

    def _interpret_value(self, key, value):
        # type: (ParagraphKey, Deb822KeyValuePairElement) -> T
        raise NotImplementedError  # pragma: no cover


# Deb822ParagraphElement uses this Mixin (by having `_paragraph` return self).
# Therefore, the Mixin needs to call the "proper" methods on the paragraph to
# avoid doing infinite recursion.
class Deb822ParagraphToStrWrapperMixin(AutoResolvingMixin[str], ABC):

    @property
    def _auto_map_initial_line_whitespace(self) -> bool:
        return True

    @property
    def _discard_comments_on_read(self) -> bool:
        return True

    @property
    def _auto_map_final_newline_in_multiline_values(self) -> bool:
        return True

    @property
    def _preserve_field_comments_on_field_updates(self) -> bool:
        return True

    def _convert_value_to_str(self, kvpair_element):
        # type: (Deb822KeyValuePairElement) -> str
        value_element = kvpair_element.value_element
        value_entries = value_element.value_lines
        if len(value_entries) == 1:
            # Special case single line entry (e.g. "Package: foo") as they never
            # have comments and we can do some parts more efficient.
            value_entry = value_entries[0]
            t = value_entry.convert_to_text()
            if self._auto_map_initial_line_whitespace:
                t = t.strip()
            return t

        if self._auto_map_initial_line_whitespace or self._discard_comments_on_read:
            converter = _convert_value_lines_to_lines(
                value_entries,
                self._discard_comments_on_read,
            )

            auto_map_space = self._auto_map_initial_line_whitespace

            # Because we know there are more than one line, we can unconditionally inject
            # the newline after the first line
            as_text = "".join(
                line.strip() + "\n" if auto_map_space and i == 1 else line
                for i, line in enumerate(converter, start=1)
            )
        else:
            # No rewrite necessary.
            as_text = value_element.convert_to_text()

        if self._auto_map_final_newline_in_multiline_values and as_text[-1] == "\n":
            as_text = as_text[:-1]
        return as_text

    def __setitem__(self, item, value):
        # type: (ParagraphKey, str) -> None
        keep_comments = (
            self._preserve_field_comments_on_field_updates
        )  # type: Optional[bool]
        comment = None
        if keep_comments and self._auto_resolve_ambiguous_fields:
            # For ambiguous fields, we have to resolve the original field as
            # the set_field_* methods do not cope with ambiguous fields.  This
            # means we might as well clear the keep_comments flag as we have
            # resolved the comment.
            keep_comments = None
            key_lookup = item
            if isinstance(item, str):
                key_lookup = (item, 0)
            orig_kvpair = self._paragraph.get_kvpair_element(key_lookup, use_get=True)
            if orig_kvpair is not None:
                comment = orig_kvpair.comment_element

        if self._auto_map_initial_line_whitespace:
            try:
                idx = value.index("\n")
            except ValueError:
                idx = -1
            if idx == -1 or idx == len(value):
                self._paragraph.set_field_to_simple_value(
                    item,
                    value.strip(),
                    preserve_original_field_comment=keep_comments,
                    field_comment=comment,
                )
                return
            # Regenerate the first line with normalized whitespace if necessary
            first_line, rest = value.split("\n", 1)
            if first_line and first_line[:1] not in ("\t", " "):
                value = "".join((" ", first_line.strip(), "\n", rest))
            else:
                value = "".join((first_line, "\n", rest))
        if not value.endswith("\n"):
            if not self._auto_map_final_newline_in_multiline_values:
                raise ValueError(
                    "Values must end with a newline (or be single line"
                    " values and use the auto whitespace mapping feature)"
                )
            value += "\n"
        self._paragraph.set_field_from_raw_string(
            item,
            value,
            preserve_original_field_comment=keep_comments,
            field_comment=comment,
        )

    def _interpret_value(self, key, value):
        # type: (ParagraphKey, Deb822KeyValuePairElement) -> str
        # mypy is a bit dense and cannot see that T == str
        return self._convert_value_to_str(value)


class AbstractDeb822ParagraphWrapper(AutoResolvingMixin[T], ABC):

    def __init__(
        self,
        paragraph,  # type: Deb822ParagraphElement
        *,
        auto_resolve_ambiguous_fields=False,  # type: bool
        discard_comments_on_read=True,  # type: bool
    ):
        # type: (...) -> None
        self.__paragraph = paragraph
        self.__auto_resolve_ambiguous_fields = auto_resolve_ambiguous_fields
        self.__discard_comments_on_read = discard_comments_on_read

    @property
    def _paragraph(self):
        # type: () -> Deb822ParagraphElement
        return self.__paragraph

    @property
    def _discard_comments_on_read(self) -> bool:
        return self.__discard_comments_on_read

    @property
    def _auto_resolve_ambiguous_fields(self) -> bool:
        return self.__auto_resolve_ambiguous_fields


class Deb822InterpretingParagraphWrapper(AbstractDeb822ParagraphWrapper[T]):

    def __init__(
        self,
        paragraph,  # type: Deb822ParagraphElement
        interpretation,  # type: Interpretation[T]
        *,
        auto_resolve_ambiguous_fields=False,  # type: bool
        discard_comments_on_read=True,  # type: bool
    ):
        # type: (...) -> None
        super().__init__(
            paragraph,
            auto_resolve_ambiguous_fields=auto_resolve_ambiguous_fields,
            discard_comments_on_read=discard_comments_on_read,
        )
        self._interpretation = interpretation

    def _interpret_value(self, key, value):
        # type: (ParagraphKey, Deb822KeyValuePairElement) -> T
        return self._interpretation.interpret(value)


class Deb822DictishParagraphWrapper(
    AbstractDeb822ParagraphWrapper[str], Deb822ParagraphToStrWrapperMixin
):

    def __init__(
        self,
        paragraph,  # type: Deb822ParagraphElement
        *,
        discard_comments_on_read=True,  # type: bool
        auto_map_initial_line_whitespace=True,  # type: bool
        auto_resolve_ambiguous_fields=False,  # type: bool
        preserve_field_comments_on_field_updates=True,  # type: bool
        auto_map_final_newline_in_multiline_values=True,  # type: bool
    ):
        # type: (...) -> None
        super().__init__(
            paragraph,
            auto_resolve_ambiguous_fields=auto_resolve_ambiguous_fields,
            discard_comments_on_read=discard_comments_on_read,
        )
        self.__auto_map_initial_line_whitespace = auto_map_initial_line_whitespace
        self.__preserve_field_comments_on_field_updates = (
            preserve_field_comments_on_field_updates
        )
        self.__auto_map_final_newline_in_multiline_values = (
            auto_map_final_newline_in_multiline_values
        )

    @property
    def _auto_map_initial_line_whitespace(self) -> bool:
        return self.__auto_map_initial_line_whitespace

    @property
    def _preserve_field_comments_on_field_updates(self) -> bool:
        return self.__preserve_field_comments_on_field_updates

    @property
    def _auto_map_final_newline_in_multiline_values(self) -> bool:
        return self.__auto_map_final_newline_in_multiline_values


class Deb822ParagraphElement(Deb822Element, Deb822ParagraphToStrWrapperMixin, ABC):

    @classmethod
    def new_empty_paragraph(cls):
        # type: () -> Deb822ParagraphElement
        return Deb822NoDuplicateFieldsParagraphElement([], OrderedSet())

    @classmethod
    def from_dict(cls, mapping):
        # type: (Mapping[str, str]) -> Deb822ParagraphElement
        paragraph = cls.new_empty_paragraph()
        for k, v in mapping.items():
            paragraph[k] = v
        return paragraph

    @classmethod
    def from_kvpairs(cls, kvpair_elements):
        # type: (List[Deb822KeyValuePairElement]) -> Deb822ParagraphElement
        if not kvpair_elements:
            raise ValueError(
                "A paragraph must consist of at least one field/value pair"
            )
        kvpair_order = OrderedSet(kv.field_name for kv in kvpair_elements)
        if len(kvpair_order) == len(kvpair_elements):
            # Each field occurs at most once, which is good because that
            # means it is a valid paragraph and we can use the optimized
            # implementation.
            return Deb822NoDuplicateFieldsParagraphElement(
                kvpair_elements, kvpair_order
            )
        # Fallback implementation, that can cope with the repeated field names
        # at the cost of complexity.
        return Deb822DuplicateFieldsParagraphElement(kvpair_elements)

    @property
    def has_duplicate_fields(self) -> bool:
        """Tell whether this paragraph has duplicate fields"""
        return False

    def as_interpreted_dict_view(
        self,
        interpretation,  # type: Interpretation[T]
        *,
        auto_resolve_ambiguous_fields=True,  # type: bool
    ):
        # type: (...) -> Deb822InterpretingParagraphWrapper[T]
        r"""Provide a Dict-like view of the paragraph

        This method returns a dict-like object representing this paragraph and
        is useful for accessing fields in a given interpretation. It is possible
        to use multiple versions of this dict-like view with different interpretations
        on the same paragraph at the same time (for different fields).

            >>> example_deb822_paragraph = '''
            ... Package: foo
            ... # Field comment (because it becomes just before a field)
            ... Architecture: amd64
            ... # Inline comment (associated with the next line)
            ...               i386
            ... # We also support arm
            ...               arm64
            ...               armel
            ... '''
            >>> dfile = parse_deb822_file(example_deb822_paragraph.splitlines())
            >>> paragraph = next(iter(dfile))
            >>> list_view = paragraph.as_interpreted_dict_view(LIST_SPACE_SEPARATED_INTERPRETATION)
            >>> # With the defaults, you only deal with the semantic values
            >>> # - no leading or trailing whitespace on the first part of the value
            >>> list(list_view["Package"])
            ['foo']
            >>> with list_view["Architecture"] as arch_list:
            ...     orig_arch_list = list(arch_list)
            ...     arch_list.replace('i386', 'kfreebsd-amd64')
            >>> orig_arch_list
            ['amd64', 'i386', 'arm64', 'armel']
            >>> list(list_view["Architecture"])
            ['amd64', 'kfreebsd-amd64', 'arm64', 'armel']
            >>> print(paragraph.dump(), end='')
            Package: foo
            # Field comment (because it becomes just before a field)
            Architecture: amd64
            # Inline comment (associated with the next line)
                          kfreebsd-amd64
            # We also support arm
                          arm64
                          armel
            >>> # Format preserved and architecture replaced
            >>> with list_view["Architecture"] as arch_list:
            ...     # Prettify the result as sorting will cause awkward whitespace
            ...     arch_list.reformat_when_finished()
            ...     arch_list.sort()
            >>> print(paragraph.dump(), end='')
            Package: foo
            # Field comment (because it becomes just before a field)
            Architecture: amd64
            # We also support arm
                          arm64
                          armel
            # Inline comment (associated with the next line)
                          kfreebsd-amd64
            >>> list(list_view["Architecture"])
            ['amd64', 'arm64', 'armel', 'kfreebsd-amd64']
            >>> # Format preserved and architecture values sorted

        :param interpretation: Decides how the field values are interpreted.  As an example,
          use LIST_SPACE_SEPARATED_INTERPRETATION for fields such as Architecture in the
          debian/control file.
        :param auto_resolve_ambiguous_fields: This parameter is only relevant for paragraphs
          that contain the same field multiple times (these are generally invalid).  If the
          caller requests an ambiguous field from an invalid paragraph via a plain field name,
          the return dict-like object will refuse to resolve the field (not knowing which
          version to pick).  This parameter (if set to True) instead changes the error into
          assuming the caller wants the *first* variant.
        """
        return Deb822InterpretingParagraphWrapper(
            self,
            interpretation,
            auto_resolve_ambiguous_fields=auto_resolve_ambiguous_fields,
        )

    def configured_view(
        self,
        *,
        discard_comments_on_read=True,  # type: bool
        auto_map_initial_line_whitespace=True,  # type: bool
        auto_resolve_ambiguous_fields=True,  # type: bool
        preserve_field_comments_on_field_updates=True,  # type: bool
        auto_map_final_newline_in_multiline_values=True,  # type: bool
    ):
        # type: (...) -> Deb822DictishParagraphWrapper
        r"""Provide a Dict[str, str]-like view of this paragraph with non-standard parameters

        This method returns a dict-like object representing this paragraph that is
        optionally configured differently from the default view.

            >>> example_deb822_paragraph = '''
            ... Package: foo
            ... # Field comment (because it becomes just before a field)
            ... Depends: libfoo,
            ... # Inline comment (associated with the next line)
            ...          libbar,
            ... '''
            >>> dfile = parse_deb822_file(example_deb822_paragraph.splitlines())
            >>> paragraph = next(iter(dfile))
            >>> # With the defaults, you only deal with the semantic values
            >>> # - no leading or trailing whitespace on the first part of the value
            >>> paragraph["Package"]
            'foo'
            >>> # - no inline comments in multiline values (but whitespace will be present
            >>> #   subsequent lines.)
            >>> print(paragraph["Depends"])
            libfoo,
                     libbar,
            >>> paragraph['Foo'] = 'bar'
            >>> paragraph.get('Foo')
            'bar'
            >>> paragraph.get('Unknown-Field') is None
            True
            >>> # But you get asymmetric behaviour with set vs. get
            >>> paragraph['Foo'] = ' bar\n'
            >>> paragraph['Foo']
            'bar'
            >>> paragraph['Bar'] = '     bar\n#Comment\n another value\n'
            >>> # Note that the whitespace on the first line has been normalized.
            >>> print("Bar: " + paragraph['Bar'])
            Bar: bar
             another value
            >>> # The comment is present (in case you where wondering)
            >>> print(paragraph.get_kvpair_element('Bar').convert_to_text(), end='')
            Bar:     bar
            #Comment
             another value
            >>> # On the other hand, you can choose to see the values as they are
            >>> # - We will just reset the paragraph as a "nothing up my sleeve"
            >>> dfile = parse_deb822_file(example_deb822_paragraph.splitlines())
            >>> paragraph = next(iter(dfile))
            >>> nonstd_dictview = paragraph.configured_view(
            ...     discard_comments_on_read=False,
            ...     auto_map_initial_line_whitespace=False,
            ...     # For paragraphs with duplicate fields, you can choose to get an error
            ...     # rather than the dict picking the first value available.
            ...     auto_resolve_ambiguous_fields=False,
            ...     auto_map_final_newline_in_multiline_values=False,
            ... )
            >>> # Because we have reset the state, Foo and Bar are no longer there.
            >>> 'Bar' not in paragraph and 'Foo' not in paragraph
            True
            >>> # We can now see the comments (discard_comments_on_read=False)
            >>> # (The leading whitespace in front of "libfoo" is due to
            >>> #  auto_map_initial_line_whitespace=False)
            >>> print(nonstd_dictview["Depends"], end='')
             libfoo,
            # Inline comment (associated with the next line)
                     libbar,
            >>> # And all the optional whitespace on the first value line
            >>> # (auto_map_initial_line_whitespace=False)
            >>> nonstd_dictview["Package"] == ' foo\n'
            True
            >>> # ... which will give you symmetric behaviour with set vs. get
            >>> nonstd_dictview['Foo'] = '  bar \n'
            >>> nonstd_dictview['Foo']
            '  bar \n'
            >>> nonstd_dictview['Bar'] = '  bar \n#Comment\n another value\n'
            >>> nonstd_dictview['Bar']
            '  bar \n#Comment\n another value\n'
            >>> # But then you get no help either.
            >>> try:
            ...     nonstd_dictview["Baz"] = "foo"
            ... except ValueError:
            ...     print("Rejected")
            Rejected
            >>> # With auto_map_initial_line_whitespace=False, you have to include minimum a newline
            >>> nonstd_dictview["Baz"] = "foo\n"
            >>> # The absence of leading whitespace gives you the terse variant at the expensive
            >>> # readability
            >>> paragraph.get_kvpair_element('Baz').convert_to_text()
            'Baz:foo\n'
            >>> # But because they are views, changes performed via one view is visible in the other
            >>> paragraph['Foo']
            'bar'
            >>> # The views show the values according to their own rules. Therefore, there is an
            >>> # asymmetric between paragraph['Foo'] and nonstd_dictview['Foo']
            >>> # Nevertheless, you can read or write the fields via either - enabling you to use
            >>> # the view that best suit your use-case for the given field.
            >>> 'Baz' in paragraph and nonstd_dictview.get('Baz') is not None
            True
            >>> # Deletion via the view also works
            >>> del nonstd_dictview['Baz']
            >>> 'Baz' not in paragraph and nonstd_dictview.get('Baz') is None
            True


        :param discard_comments_on_read: When getting a field value from the dict,
          this parameter decides how in-line comments are handled.  When setting
          the value, inline comments are still allowed and will be retained.
          However, keep in mind that this option makes getter and setter asymmetric
          as a "get" following a "set" with inline comments will omit the comments
          even if they are there (see the code example).
        :param auto_map_initial_line_whitespace: Special-case the first value line
          by trimming unnecessary whitespace leaving only the value. For single-line
          values, all space including newline is pruned. For multi-line values, the
          newline is preserved / needed to distinguish the first line from the
          following lines.  When setting a value, this option normalizes the
          whitespace of the initial line of the value field.
          When this option is set to True makes the dictionary behave more like the
          original Deb822 module.
        :param preserve_field_comments_on_field_updates: Whether to preserve the field
          comments when mutating the field.
        :param auto_resolve_ambiguous_fields: This parameter is only relevant for paragraphs
          that contain the same field multiple times (these are generally invalid).  If the
          caller requests an ambiguous field from an invalid paragraph via a plain field name,
          the return dict-like object will refuse to resolve the field (not knowing which
          version to pick).  This parameter (if set to True) instead changes the error into
          assuming the caller wants the *first* variant.
        :param auto_map_final_newline_in_multiline_values: This parameter controls whether
          a multiline field with have / need a trailing newline. If True, the trailing
          newline is hidden on get and automatically added in set (if missing).
          When this option is set to True makes the dictionary behave more like the
          original Deb822 module.
        """
        return Deb822DictishParagraphWrapper(
            self,
            discard_comments_on_read=discard_comments_on_read,
            auto_map_initial_line_whitespace=auto_map_initial_line_whitespace,
            auto_resolve_ambiguous_fields=auto_resolve_ambiguous_fields,
            preserve_field_comments_on_field_updates=preserve_field_comments_on_field_updates,
            auto_map_final_newline_in_multiline_values=auto_map_final_newline_in_multiline_values,
        )

    @property
    def _paragraph(self):
        # type: () -> Deb822ParagraphElement
        return self

    def order_last(self, field):
        # type: (ParagraphKey) -> None
        """Re-order the given field so it is "last" in the paragraph"""
        raise NotImplementedError  # pragma: no cover

    def order_first(self, field):
        # type: (ParagraphKey) -> None
        """Re-order the given field so it is "first" in the paragraph"""
        raise NotImplementedError  # pragma: no cover

    def order_before(self, field, reference_field):
        # type: (ParagraphKey, ParagraphKey) -> None
        """Re-order the given field so appears directly after the reference field in the paragraph

        The reference field must be present."""
        raise NotImplementedError  # pragma: no cover

    def order_after(self, field, reference_field):
        # type: (ParagraphKey, ParagraphKey) -> None
        """Re-order the given field so appears directly before the reference field in the paragraph

        The reference field must be present.
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def kvpair_count(self) -> int:
        raise NotImplementedError  # pragma: no cover

    def iter_keys(self):
        # type: () -> Iterable[ParagraphKey]
        raise NotImplementedError  # pragma: no cover

    def contains_kvpair_element(self, item):
        # type: (object) -> bool
        raise NotImplementedError  # pragma: no cover

    def get_kvpair_element(
        self,
        item,  # type: ParagraphKey
        use_get=False,  # type: bool
    ):
        # type: (...) -> Optional[Deb822KeyValuePairElement]
        raise NotImplementedError  # pragma: no cover

    def set_kvpair_element(self, key, value):
        # type: (ParagraphKey, Deb822KeyValuePairElement) -> None
        raise NotImplementedError  # pragma: no cover

    def remove_kvpair_element(self, key):
        # type: (ParagraphKey) -> None
        raise NotImplementedError  # pragma: no cover

    def sort_fields(
        self, key=None  # type: Optional[Callable[[str], Any]]
    ):
        # type: (...) -> None
        """Re-order all fields

        :param key: Provide a key function (same semantics as for sorted).  Keep in mind that
          the module preserve the cases for field names - in generally, callers are recommended
          to use "lower()" to normalize the case.
        """
        raise NotImplementedError  # pragma: no cover

    def set_field_to_simple_value(
        self,
        item,  # type: ParagraphKey
        simple_value,  # type: str
        *,
        preserve_original_field_comment=None,  # type: Optional[bool]
        field_comment=None,  # type: Optional[Commentish]
    ):
        # type: (...) -> None
        r"""Sets a field in this paragraph to a simple "word" or "phrase"

        In many cases, it is better for callers to just use the paragraph as
        if it was a dictionary.  However, this method does enable to you choose
        the field comment (if any), which can be a reason for using it.

        This is suitable for "simple" fields like "Package".  Example:

            >>> example_deb822_paragraph = '''
            ... Package: foo
            ... '''
            >>> dfile = parse_deb822_file(example_deb822_paragraph.splitlines())
            >>> p = next(iter(dfile))
            >>> p.set_field_to_simple_value("Package", "mscgen")
            >>> p.set_field_to_simple_value("Architecture", "linux-any kfreebsd-any",
            ...                             field_comment=['Only ported to linux and kfreebsd'])
            >>> p.set_field_to_simple_value("Priority", "optional")
            >>> print(p.dump(), end='')
            Package: mscgen
            # Only ported to linux and kfreebsd
            Architecture: linux-any kfreebsd-any
            Priority: optional
            >>> # Values are formatted nicely by default, but it does not work with
            >>> # multi-line values
            >>> p.set_field_to_simple_value("Foo", "bar\nbin\n")
            Traceback (most recent call last):
                ...
            ValueError: Cannot use set_field_to_simple_value for values with newlines

        :param item: Name of the field to set.  If the paragraph already
          contains the field, then it will be replaced.  If the field exists,
          then it will preserve its order in the paragraph.  Otherwise, it is
          added to the end of the paragraph.
          Note this can be a "paragraph key", which enables you to control
          *which* instance of a field is being replaced (in case of duplicate
          fields).
        :param simple_value: The text to use as the value.  The value must not
          contain newlines.  Leading and trailing will be stripped but space
          within the value is preserved.  The value cannot contain comments
          (i.e. if the "#" token appears in the value, then it is considered
          a value rather than "start of a comment)
        :param preserve_original_field_comment: See the description for the
          parameter with the same name in the set_field_from_raw_string method.
        :param field_comment: See the description for the parameter with the same
          name in the set_field_from_raw_string method.
        """
        if "\n" in simple_value:
            raise ValueError(
                "Cannot use set_field_to_simple_value for values with newlines"
            )

        # Reformat it with a leading space and trailing newline. The latter because it is
        # necessary if there are any fields after it and the former because it looks nicer so
        # have single space after the field separator
        stripped = simple_value.strip()
        if stripped:
            raw_value = " " + stripped + "\n"
        else:
            # Special-case for empty values
            raw_value = "\n"
        self.set_field_from_raw_string(
            item,
            raw_value,
            preserve_original_field_comment=preserve_original_field_comment,
            field_comment=field_comment,
        )

    def set_field_from_raw_string(
        self,
        item,  # type: ParagraphKey
        raw_string_value,  # type: str
        *,
        preserve_original_field_comment=None,  # type: Optional[bool]
        field_comment=None,  # type: Optional[Commentish]
    ):
        # type: (...) -> None
        """Sets a field in this paragraph to a given text value

        In many cases, it is better for callers to just use the paragraph as
        if it was a dictionary.  However, this method does enable to you choose
        the field comment (if any) and lets to have a higher degree of control
        over whitespace (on the first line), which can be a reason for using it.

        Example usage:

            >>> example_deb822_paragraph = '''
            ... Package: foo
            ... '''
            >>> dfile = parse_deb822_file(example_deb822_paragraph.splitlines())
            >>> p = next(iter(dfile))
            >>> raw_value = '''
            ... Build-Depends: debhelper-compat (= 12),
            ...                some-other-bd,
            ... # Comment
            ...                another-bd,
            ... '''.lstrip()  # Remove leading newline, but *not* the trailing newline
            >>> fname, new_value = raw_value.split(':', 1)
            >>> p.set_field_from_raw_string(fname, new_value)
            >>> print(p.dump(), end='')
            Package: foo
            Build-Depends: debhelper-compat (= 12),
                           some-other-bd,
            # Comment
                           another-bd,
            >>> # Format preserved

        :param item: Name of the field to set.  If the paragraph already
          contains the field, then it will be replaced.  Otherwise, it is
          added to the end of the paragraph.
          Note this can be a "paragraph key", which enables you to control
          *which* instance of a field is being replaced (in case of duplicate
          fields).
        :param raw_string_value: The text to use as the value.  The text must
          be valid deb822 syntax and is used *exactly* as it is given.
          Accordingly, multi-line values must include mandatory leading space
          on continuation lines, newlines after the value, etc. On the
          flip-side, any optional space or comments will be included.

          Note that the first line will *never* be read as a comment (if the
          first line of the value starts with a "#" then it will result
          in "Field-Name:#..." which is parsed as a value starting with "#"
          rather than a comment).
        :param preserve_original_field_comment: If True, then if there is an
          existing field and that has a comment, then the comment will remain
          after this operation.  This is the default is the `field_comment`
          parameter is omitted.
          Note that if the parameter is True and the item is ambiguous, this
          will raise an AmbiguousDeb822FieldKeyError.  When the parameter is
          omitted, the ambiguity is resolved automatically and if the resolved
          field has a comment then that will be preserved (assuming
          field_comment is None).
        :param field_comment: If not None, add or replace the comment for
          the field.  Each string in the list will become one comment
          line (inserted directly before the field name). Will appear in the
          same order as they do in the list.

          If you want complete control over the formatting of the comments,
          then ensure that each line start with "#" and end with "\\n" before
          the call.  Otherwise, leading/trailing whitespace is normalized
          and the missing "#"/"\\n" character is inserted.
        """

        new_content = []  # type: List[str]
        if preserve_original_field_comment is not None:
            if field_comment is not None:
                raise ValueError(
                    'The "preserve_original_field_comment" conflicts with'
                    ' "field_comment" parameter'
                )
        elif field_comment is not None:
            if not isinstance(field_comment, Deb822CommentElement):
                new_content.extend(_format_comment(x) for x in field_comment)
                field_comment = None
            preserve_original_field_comment = False

        field_name, _, _ = _unpack_key(item)

        cased_field_name = field_name
        try:
            original = self.get_kvpair_element(item, use_get=True)
        except AmbiguousDeb822FieldKeyError:
            if preserve_original_field_comment:
                # If we were asked to preserve the original comment, then we
                # require a strict lookup
                raise
            original = self.get_kvpair_element((field_name, 0), use_get=True)

        if preserve_original_field_comment is None:
            # We simplify preserve_original_field_comment after the lookup of the field.
            # Otherwise, we can get ambiguous key errors when updating an ambiguous field
            # when the caller did not explicitly ask for that behaviour.
            preserve_original_field_comment = True

        if original:
            # If we already have the field, then preserve the original case
            cased_field_name = original.field_name
        raw = ":".join((cased_field_name, raw_string_value))
        raw_lines = raw.splitlines(keepends=True)
        for i, line in enumerate(raw_lines, start=1):
            if not line.endswith("\n"):
                raise ValueError(
                    "Line {i} in new value was missing trailing newline".format(i=i)
                )
            if i != 1 and line[0] not in (" ", "\t", "#"):
                msg = (
                    "Line {i} in new value was invalid.  It must either start"
                    ' with " " space (continuation line) or "#" (comment line).'
                    ' The line started with "{line}"'
                )
                raise ValueError(msg.format(i=i, line=line[0]))
        if len(raw_lines) > 1 and raw_lines[-1].startswith("#"):
            raise ValueError("The last line in a value field cannot be a comment")
        new_content.extend(raw_lines)
        # As absurd as it might seem, it is easier to just use the parser to
        # construct the AST correctly
        deb822_file = parse_deb822_file(iter(new_content))
        error_token = deb822_file.find_first_error_element()
        if error_token:
            raise ValueError("Syntax error in new field value for " + field_name)
        paragraph = next(iter(deb822_file))
        assert isinstance(paragraph, Deb822NoDuplicateFieldsParagraphElement)
        value = paragraph.get_kvpair_element(field_name)
        assert value is not None
        if preserve_original_field_comment:
            if original:
                value.comment_element = original.comment_element
                original.comment_element = None
        elif field_comment is not None:
            value.comment_element = field_comment
        self.set_kvpair_element(item, value)

    @overload
    def dump(
        self, fd  # type: IO[bytes]
    ):
        # type: (...) -> None
        pass

    @overload
    def dump(self) -> str:
        pass

    def dump(
        self, fd=None  # type: Optional[IO[bytes]]
    ):
        # type: (...) -> Optional[str]
        if fd is None:
            return "".join(t.text for t in self.iter_tokens())
        for token in self.iter_tokens():
            fd.write(token.text.encode("utf-8"))
        return None


class Deb822NoDuplicateFieldsParagraphElement(Deb822ParagraphElement):
    """Paragraph implementation optimized for valid deb822 files

    When there are no duplicated fields, we can use simpler and faster
    datastructures for common operations.
    """

    def __init__(
        self,
        kvpair_elements,  # type: List[Deb822KeyValuePairElement]
        kvpair_order,  # type: OrderedSet
    ):
        # type: (...) -> None
        super().__init__()
        self._kvpair_elements = {kv.field_name: kv for kv in kvpair_elements}
        self._kvpair_order = kvpair_order
        self._init_parent_of_parts()

    @property
    def kvpair_count(self) -> int:
        return len(self._kvpair_elements)

    def order_last(self, field):
        # type: (ParagraphKey) -> None
        """Re-order the given field so it is "last" in the paragraph"""
        unpacked_field, _, _ = _unpack_key(field, raise_if_indexed=True)
        self._kvpair_order.order_last(unpacked_field)

    def order_first(self, field):
        # type: (ParagraphKey) -> None
        """Re-order the given field so it is "first" in the paragraph"""
        unpacked_field, _, _ = _unpack_key(field, raise_if_indexed=True)
        self._kvpair_order.order_first(unpacked_field)

    def order_before(self, field, reference_field):
        # type: (ParagraphKey, ParagraphKey) -> None
        """Re-order the given field so appears directly after the reference field in the paragraph

        The reference field must be present."""
        unpacked_field, _, _ = _unpack_key(field, raise_if_indexed=True)
        unpacked_ref_field, _, _ = _unpack_key(reference_field, raise_if_indexed=True)
        self._kvpair_order.order_before(unpacked_field, unpacked_ref_field)

    def order_after(self, field, reference_field):
        # type: (ParagraphKey, ParagraphKey) -> None
        """Re-order the given field so appears directly before the reference field in the paragraph

        The reference field must be present.
        """
        unpacked_field, _, _ = _unpack_key(field, raise_if_indexed=True)
        unpacked_ref_field, _, _ = _unpack_key(reference_field, raise_if_indexed=True)
        self._kvpair_order.order_after(unpacked_field, unpacked_ref_field)

    # Overload to narrow the type to just str.
    def __iter__(self):
        # type: () -> Iterator[str]
        return iter(str(k) for k in self._kvpair_order)

    def iter_keys(self):
        # type: () -> Iterable[str]
        yield from (str(k) for k in self._kvpair_order)

    def remove_kvpair_element(self, key):
        # type: (ParagraphKey) -> None
        self._full_size_cache = None
        key, _, _ = _unpack_key(key, raise_if_indexed=True)
        del self._kvpair_elements[key]
        self._kvpair_order.remove(key)

    def contains_kvpair_element(self, item):
        # type: (object) -> bool
        if not isinstance(item, (str, tuple, Deb822FieldNameToken)):
            return False
        item = cast("ParagraphKey", item)
        key, _, _ = _unpack_key(item, raise_if_indexed=True)
        return key in self._kvpair_elements

    def get_kvpair_element(
        self,
        item,  # type: ParagraphKey
        use_get=False,  # type: bool
    ):
        # type: (...) -> Optional[Deb822KeyValuePairElement]
        item, _, _ = _unpack_key(item, raise_if_indexed=True)
        if use_get:
            return self._kvpair_elements.get(item)
        return self._kvpair_elements[item]

    def set_kvpair_element(self, key, value):
        # type: (ParagraphKey, Deb822KeyValuePairElement) -> None
        key, _, _ = _unpack_key(key, raise_if_indexed=True)
        if isinstance(key, Deb822FieldNameToken):
            if key is not value.field_token:
                raise ValueError(
                    "Key is a Deb822FieldNameToken, but not *the* Deb822FieldNameToken"
                    " for the value"
                )
            key = value.field_name
        else:
            if key != value.field_name:
                raise ValueError(
                    "Cannot insert value under a different field value than field name"
                    " from its Deb822FieldNameToken implies"
                )
            # Use the string from the Deb822FieldNameToken as we need to keep that in memory either
            # way
            key = value.field_name
        original_value = self._kvpair_elements.get(key)
        self._full_size_cache = None
        self._kvpair_elements[key] = value
        self._kvpair_order.append(key)
        if original_value is not None:
            original_value.parent_element = None
        value.parent_element = self

    def sort_fields(self, key=None):
        # type: (Optional[Callable[[str], Any]]) -> None
        """Re-order all fields

        :param key: Provide a key function (same semantics as for sorted).  Keep in mind that
          the module preserve the cases for field names - in generally, callers are recommended
          to use "lower()" to normalize the case.
        """
        for last_field_name in reversed(self._kvpair_order):
            last_kvpair = self._kvpair_elements[cast("_strI", last_field_name)]
            if last_kvpair.value_element.add_final_newline_if_missing():
                self._full_size_cache = None
            break

        if key is None:
            key = default_field_sort_key

        self._kvpair_order = OrderedSet(sorted(self._kvpair_order, key=key))

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        yield from (
            self._kvpair_elements[x]
            for x in cast("Iterable[_strI]", self._kvpair_order)
        )


class Deb822DuplicateFieldsParagraphElement(Deb822ParagraphElement):

    def __init__(self, kvpair_elements):
        # type: (List[Deb822KeyValuePairElement]) -> None
        super().__init__()
        self._kvpair_order = LinkedList()  # type: LinkedList[Deb822KeyValuePairElement]
        self._kvpair_elements = {}  # type: Dict[_strI, List[KVPNode]]
        self._init_kvpair_fields(kvpair_elements)
        self._init_parent_of_parts()

    @property
    def has_duplicate_fields(self) -> bool:
        # Most likely, the answer is "True" but if the caller "fixes" the problem
        # then this can return "False"
        return len(self._kvpair_order) > len(self._kvpair_elements)

    def _init_kvpair_fields(self, kvpairs):
        # type: (Iterable[Deb822KeyValuePairElement]) -> None
        assert not self._kvpair_order
        assert not self._kvpair_elements
        for kv in kvpairs:
            field_name = kv.field_name
            node = self._kvpair_order.append(kv)
            if field_name not in self._kvpair_elements:
                self._kvpair_elements[field_name] = [node]
            else:
                self._kvpair_elements[field_name].append(node)

    def _nodes_being_relocated(self, field):
        # type: (ParagraphKey) -> Tuple[List[KVPNode], List[KVPNode]]
        key, index, name_token = _unpack_key(field)
        nodes = self._kvpair_elements[key]
        nodes_being_relocated = []

        if name_token is not None or index is not None:
            single_node = self._resolve_to_single_node(nodes, key, index, name_token)
            assert single_node is not None
            nodes_being_relocated.append(single_node)
        else:
            nodes_being_relocated = nodes
        return nodes, nodes_being_relocated

    def order_last(self, field):
        # type: (ParagraphKey) -> None
        """Re-order the given field so it is "last" in the paragraph"""
        nodes, nodes_being_relocated = self._nodes_being_relocated(field)
        assert len(nodes_being_relocated) == 1 or len(nodes) == len(
            nodes_being_relocated
        )

        kvpair_order = self._kvpair_order
        for node in nodes_being_relocated:
            if kvpair_order.tail_node is node:
                # Special case for relocating a single node that happens to be the last.
                continue
            kvpair_order.remove_node(node)
            # assertion for mypy
            assert kvpair_order.tail_node is not None
            kvpair_order.insert_node_after(node, kvpair_order.tail_node)

        if (
            len(nodes_being_relocated) == 1
            and nodes_being_relocated[0] is not nodes[-1]
        ):
            single_node = nodes_being_relocated[0]
            nodes.remove(single_node)
            nodes.append(single_node)

    def order_first(self, field):
        # type: (ParagraphKey) -> None
        """Re-order the given field so it is "first" in the paragraph"""
        nodes, nodes_being_relocated = self._nodes_being_relocated(field)
        assert len(nodes_being_relocated) == 1 or len(nodes) == len(
            nodes_being_relocated
        )

        kvpair_order = self._kvpair_order
        for node in nodes_being_relocated:
            if kvpair_order.head_node is node:
                # Special case for relocating a single node that happens to be the first.
                continue
            kvpair_order.remove_node(node)
            # assertion for mypy
            assert kvpair_order.head_node is not None
            kvpair_order.insert_node_before(node, kvpair_order.head_node)

        if len(nodes_being_relocated) == 1 and nodes_being_relocated[0] is not nodes[0]:
            single_node = nodes_being_relocated[0]
            nodes.remove(single_node)
            nodes.insert(0, single_node)

    def order_before(self, field, reference_field):
        # type: (ParagraphKey, ParagraphKey) -> None
        """Re-order the given field so appears directly after the reference field in the paragraph

        The reference field must be present."""
        nodes, nodes_being_relocated = self._nodes_being_relocated(field)
        assert len(nodes_being_relocated) == 1 or len(nodes) == len(
            nodes_being_relocated
        )
        # For "before" we always use the "first" variant as reference in case of doubt
        _, reference_nodes = self._nodes_being_relocated(reference_field)
        reference_node = reference_nodes[0]
        if reference_node in nodes_being_relocated:
            raise ValueError("Cannot re-order a field relative to itself")

        kvpair_order = self._kvpair_order
        for node in nodes_being_relocated:
            kvpair_order.remove_node(node)
            kvpair_order.insert_node_before(node, reference_node)

        if len(nodes_being_relocated) == 1 and len(nodes) > 1:
            # Regenerate the (new) relative field order.
            field_name = nodes_being_relocated[0].value.field_name
            self._regenerate_relative_kvapir_order(field_name)

    def order_after(self, field, reference_field):
        # type: (ParagraphKey, ParagraphKey) -> None
        """Re-order the given field so appears directly before the reference field in the paragraph

        The reference field must be present.
        """
        nodes, nodes_being_relocated = self._nodes_being_relocated(field)
        assert len(nodes_being_relocated) == 1 or len(nodes) == len(
            nodes_being_relocated
        )
        _, reference_nodes = self._nodes_being_relocated(reference_field)
        # For "after" we always use the "last" variant as reference in case of doubt
        reference_node = reference_nodes[-1]
        if reference_node in nodes_being_relocated:
            raise ValueError("Cannot re-order a field relative to itself")

        kvpair_order = self._kvpair_order
        # Use "reversed" to preserve the relative order of the nodes assuming a bulk reorder
        for node in reversed(nodes_being_relocated):
            kvpair_order.remove_node(node)
            kvpair_order.insert_node_after(node, reference_node)

        if len(nodes_being_relocated) == 1 and len(nodes) > 1:
            # Regenerate the (new) relative field order.
            field_name = nodes_being_relocated[0].value.field_name
            self._regenerate_relative_kvapir_order(field_name)

    def _regenerate_relative_kvapir_order(self, field_name):
        # type: (_strI) -> None
        nodes = []
        for node in self._kvpair_order.iter_nodes():
            if node.value.field_name == field_name:
                nodes.append(node)
        self._kvpair_elements[field_name] = nodes

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        yield from self._kvpair_order

    @property
    def kvpair_count(self) -> int:
        return len(self._kvpair_order)

    def iter_keys(self):
        # type: () -> Iterable[ParagraphKey]
        yield from (kv.field_name for kv in self._kvpair_order)

    def _resolve_to_single_node(
        self,
        nodes,  # type: List[KVPNode]
        key,  # type: str
        index,  # type: Optional[int]
        name_token,  # type: Optional[Deb822FieldNameToken]
        use_get=False,  # type: bool
    ):
        # type: (...) -> Optional[KVPNode]
        if index is None:
            if len(nodes) != 1:
                if name_token is not None:
                    node = self._find_node_via_name_token(name_token, nodes)
                    if node is not None:
                        return node
                msg = (
                    "Ambiguous key {key} - the field appears {res_len} times. Use"
                    " ({key}, index) to denote which instance of the field you want.  (Index"
                    " can be 0..{res_len_1} or e.g. -1 to denote the last field)"
                )
                raise AmbiguousDeb822FieldKeyError(
                    msg.format(key=key, res_len=len(nodes), res_len_1=len(nodes) - 1)
                )
            index = 0
        try:
            return nodes[index]
        except IndexError:
            if use_get:
                return None
            msg = 'Field "{key}" was present but the index "{index}" was invalid.'
            raise KeyError(msg.format(key=key, index=index))

    def get_kvpair_element(
        self,
        item,  # type: ParagraphKey
        use_get=False,  # type: bool
    ):
        # type: (...) -> Optional[Deb822KeyValuePairElement]
        key, index, name_token = _unpack_key(item)
        if use_get:
            nodes = self._kvpair_elements.get(key)
            if nodes is None:
                return None
        else:
            nodes = self._kvpair_elements[key]
        node = self._resolve_to_single_node(
            nodes, key, index, name_token, use_get=use_get
        )
        if node is not None:
            return node.value
        return None

    @staticmethod
    def _find_node_via_name_token(
        name_token,  # type: Deb822FieldNameToken
        elements,  # type: Iterable[KVPNode]
    ):
        # type: (...) -> Optional[KVPNode]
        # if we are given a name token, then it is non-ambiguous if we have exactly
        # that name token in our list of nodes.  It will be an O(n) lookup but we
        # probably do not have that many duplicate fields (and even if do, it is not
        # exactly a valid file, so there little reason to optimize for it)
        for node in elements:
            if name_token is node.value.field_token:
                return node
        return None

    def contains_kvpair_element(self, item):
        # type: (object) -> bool
        if not isinstance(item, (str, tuple, Deb822FieldNameToken)):
            return False
        item = cast("ParagraphKey", item)
        try:
            return self.get_kvpair_element(item, use_get=True) is not None
        except AmbiguousDeb822FieldKeyError:
            return True

    def set_kvpair_element(self, key, value):
        # type: (ParagraphKey, Deb822KeyValuePairElement) -> None
        key, index, name_token = _unpack_key(key)
        if name_token:
            if name_token is not value.field_token:
                original_nodes = self._kvpair_elements.get(value.field_name)
                original_node = None
                if original_nodes is not None:
                    original_node = self._find_node_via_name_token(
                        name_token, original_nodes
                    )

                if original_node is None:
                    raise ValueError(
                        "Key is a Deb822FieldNameToken, but not *the*"
                        " Deb822FieldNameToken for the value nor the"
                        " Deb822FieldNameToken for an existing field in the paragraph"
                    )
                # Primarily for mypy's sake
                assert original_nodes is not None
                # Rely on the index-based code below to handle update.
                index = original_nodes.index(original_node)
            key = value.field_name
        else:
            if key != value.field_name:
                raise ValueError(
                    "Cannot insert value under a different field value than field name"
                    " from its Deb822FieldNameToken implies"
                )
            # Use the string from the Deb822FieldNameToken as it is a _strI and has the same value
            # (memory optimization)
            key = value.field_name
        self._full_size_cache = None
        original_nodes = self._kvpair_elements.get(key)
        if original_nodes is None or not original_nodes:
            if index is not None and index != 0:
                msg = (
                    "Cannot replace field ({key}, {index}) as the field does not exist"
                    " in the first place.  Please index-less key or ({key}, 0) if you"
                    " want to add the field."
                )
                raise KeyError(msg.format(key=key, index=index))
            node = self._kvpair_order.append(value)
            if key not in self._kvpair_elements:
                self._kvpair_elements[key] = [node]
            else:
                self._kvpair_elements[key].append(node)
            return

        replace_all = False
        if index is None:
            replace_all = True
            node = original_nodes[0]
            if len(original_nodes) != 1:
                self._kvpair_elements[key] = [node]
        else:
            # We insist on there being an original node, which as a side effect ensures
            # you cannot add additional copies of the field.  This means that you cannot
            # make the problem worse.
            node = original_nodes[index]

        # Replace the value of the existing node plus do a little dance
        # for the parent element part.
        node.value.parent_element = None
        value.parent_element = self
        node.value = value

        if replace_all and len(original_nodes) != 1:
            # If we were in a replace-all mode, discard any remaining nodes
            for n in original_nodes[1:]:
                n.value.parent_element = None
                self._kvpair_order.remove_node(n)

    def remove_kvpair_element(self, key):
        # type: (ParagraphKey) -> None
        key, idx, name_token = _unpack_key(key)
        field_list = self._kvpair_elements[key]

        if name_token is None and idx is None:
            self._full_size_cache = None
            # Remove all case
            for node in field_list:
                node.value.parent_element = None
                self._kvpair_order.remove_node(node)
            del self._kvpair_elements[key]
            return

        if name_token is not None:
            # Indirection between original_node and node for mypy's sake
            original_node = self._find_node_via_name_token(name_token, field_list)
            if original_node is None:
                msg = 'The field "{key}" is present but key used to access it is not.'
                raise KeyError(msg.format(key=key))
            node = original_node
        else:
            assert idx is not None
            try:
                node = field_list[idx]
            except KeyError:
                msg = 'The field "{key}" is present, but the index "{idx}" was invalid.'
                raise KeyError(msg.format(key=key, idx=idx))

        self._full_size_cache = None
        if len(field_list) == 1:
            del self._kvpair_elements[key]
        else:
            field_list.remove(node)
        node.value.parent_element = None
        self._kvpair_order.remove_node(node)

    def sort_fields(self, key=None):
        # type: (Optional[Callable[[str], Any]]) -> None
        """Re-order all fields

        :param key: Provide a key function (same semantics as for sorted).   Keep in mind that
          the module preserve the cases for field names - in generally, callers are recommended
          to use "lower()" to normalize the case.
        """

        if key is None:
            key = default_field_sort_key

        # Work around mypy that cannot seem to shred the Optional notion
        # without this little indirection
        key_impl = key

        def _actual_key(kvpair):
            # type: (Deb822KeyValuePairElement) -> Any
            return key_impl(kvpair.field_name)

        for last_kvpair in reversed(self._kvpair_order):
            if last_kvpair.value_element.add_final_newline_if_missing():
                self._full_size_cache = None
            break

        sorted_kvpair_list = sorted(self._kvpair_order, key=_actual_key)
        self._kvpair_order = LinkedList()
        self._kvpair_elements = {}
        self._init_kvpair_fields(sorted_kvpair_list)


class Deb822FileElement(Deb822Element):
    """Represents the entire deb822 file"""

    def __init__(self, token_and_elements):
        # type: (LinkedList[TokenOrElement]) -> None
        super().__init__()
        self._token_and_elements = token_and_elements
        self._init_parent_of_parts()

    @classmethod
    def new_empty_file(cls):
        # type: () -> Deb822FileElement
        """Creates a new Deb822FileElement with no contents

        Note that a deb822 file must be non-empty to be considered valid
        """
        return cls(LinkedList())

    @property
    def is_valid_file(self) -> bool:
        """Returns true if the file is valid

        Invalid elements include error elements (Deb822ErrorElement) but also
        issues such as paragraphs with duplicate fields or "empty" files
        (a valid deb822 file contains at least one paragraph).
        """
        had_paragraph = False
        for paragraph in self:
            had_paragraph = True
            if not paragraph or paragraph.has_duplicate_fields:
                return False

        if not had_paragraph:
            return False

        return self.find_first_error_element() is None

    def find_first_error_element(self):
        # type: () -> Optional[Deb822ErrorElement]
        """Returns the first Deb822ErrorElement (or None) in the file"""
        return next(
            iter(self.iter_recurse(only_element_or_token_type=Deb822ErrorElement)), None
        )

    def __iter__(self):
        # type: () -> Iterator[Deb822ParagraphElement]
        return iter(self.iter_parts_of_type(Deb822ParagraphElement))

    def iter_parts(self):
        # type: () -> Iterable[TokenOrElement]
        yield from self._token_and_elements

    def insert(self, idx, para):
        # type: (int, Deb822ParagraphElement) -> None
        """Inserts a paragraph into the file at the given "index" of paragraphs

        Note that if the index is between two paragraphs containing a "free
        floating" comment (e.g. paragraph/start-of-file, empty line, comment,
        empty line, paragraph) then it is unspecified which "side" of the
        comment the new paragraph will appear and this may change between
        versions of python-debian.


        >>> original = '''
        ... Package: libfoo-dev
        ... Depends: libfoo1 (= ${binary:Version}), ${shlib:Depends}, ${misc:Depends}
        ... '''.lstrip()
        >>> deb822_file = parse_deb822_file(original.splitlines())
        >>> para1 = Deb822ParagraphElement.new_empty_paragraph()
        >>> para1["Source"] = "foo"
        >>> para1["Build-Depends"] = "debhelper-compat (= 13)"
        >>> para2 = Deb822ParagraphElement.new_empty_paragraph()
        >>> para2["Package"] = "libfoo1"
        >>> para2["Depends"] = "${shlib:Depends}, ${misc:Depends}"
        >>> deb822_file.insert(0, para1)
        >>> deb822_file.insert(1, para2)
        >>> expected = '''
        ... Source: foo
        ... Build-Depends: debhelper-compat (= 13)
        ...
        ... Package: libfoo1
        ... Depends: ${shlib:Depends}, ${misc:Depends}
        ...
        ... Package: libfoo-dev
        ... Depends: libfoo1 (= ${binary:Version}), ${shlib:Depends}, ${misc:Depends}
        ... '''.lstrip()
        >>> deb822_file.dump() == expected
        True
        """

        anchor_node = None
        needs_newline = True
        self._full_size_cache = None
        if idx == 0:
            # Special-case, if idx is 0, then we insert it before everything else.
            # This is mostly a cosmetic choice for corner cases involving free-floating
            # comments in the file.
            if not self._token_and_elements:
                self.append(para)
                return
            anchor_node = self._token_and_elements.head_node
            needs_newline = bool(self._token_and_elements)
        else:
            i = 0
            for node in self._token_and_elements.iter_nodes():
                entry = node.value
                if isinstance(entry, Deb822ParagraphElement):
                    i += 1
                if idx == i - 1:
                    anchor_node = node
                    break

        if anchor_node is None:
            # Empty list or idx after the last paragraph both degenerate into append
            self.append(para)
        else:
            if needs_newline:
                # Remember to inject the "separating" newline between two paragraphs
                nl_token = self._set_parent(Deb822WhitespaceToken("\n"))
                anchor_node = self._token_and_elements.insert_before(
                    nl_token, anchor_node
                )
            self._token_and_elements.insert_before(self._set_parent(para), anchor_node)

    def append(self, paragraph):
        # type: (Deb822ParagraphElement) -> None
        """Appends a paragraph to the file

        >>> deb822_file = Deb822FileElement.new_empty_file()
        >>> para1 = Deb822ParagraphElement.new_empty_paragraph()
        >>> para1["Source"] = "foo"
        >>> para1["Build-Depends"] = "debhelper-compat (= 13)"
        >>> para2 = Deb822ParagraphElement.new_empty_paragraph()
        >>> para2["Package"] = "foo"
        >>> para2["Depends"] = "${shlib:Depends}, ${misc:Depends}"
        >>> deb822_file.append(para1)
        >>> deb822_file.append(para2)
        >>> expected = '''
        ... Source: foo
        ... Build-Depends: debhelper-compat (= 13)
        ...
        ... Package: foo
        ... Depends: ${shlib:Depends}, ${misc:Depends}
        ... '''.lstrip()
        >>> deb822_file.dump() == expected
        True
        """
        tail_element = self._token_and_elements.tail
        if paragraph.parent_element is not None:
            if paragraph.parent_element is self:
                raise ValueError("Paragraph is already a part of this file")
            raise ValueError("Paragraph is already part of another Deb822File")

        self._full_size_cache = None
        # We need a separating newline if there is not a whitespace token at the end of the file.
        # Note the special case where the file ends on a comment; here we insert a whitespace too
        # to be sure.  Otherwise, we would have to check that there is an empty line before that
        # comment and that is too much effort.
        if tail_element and not isinstance(tail_element, Deb822WhitespaceToken):
            self._token_and_elements.append(
                self._set_parent(Deb822WhitespaceToken("\n"))
            )
        self._token_and_elements.append(self._set_parent(paragraph))

    def remove(self, paragraph):
        # type: (Deb822ParagraphElement) -> None
        if paragraph.parent_element is not self:
            raise ValueError("Paragraph is part of a different file")
        node = None
        for node in self._token_and_elements.iter_nodes():
            if node.value is paragraph:
                break
        if node is None:
            raise RuntimeError("unable to find paragraph")
        self._full_size_cache = None
        previous_node = node.previous_node
        next_node = node.next_node
        self._token_and_elements.remove_node(node)
        if next_node is None:
            if previous_node and isinstance(previous_node.value, Deb822WhitespaceToken):
                self._token_and_elements.remove_node(previous_node)
        else:
            if isinstance(next_node.value, Deb822WhitespaceToken):
                self._token_and_elements.remove_node(next_node)
        paragraph.parent_element = None

    def _set_parent(self, t):
        # type: (TE) -> TE
        t.parent_element = self
        return t

    def position_in_parent(self) -> Position:
        # Recursive base-case
        return START_POSITION

    def position_in_file(self) -> Position:
        # By definition
        return START_POSITION

    @overload
    def dump(
        self, fd  # type: IO[bytes]
    ):
        # type: (...) -> None
        pass

    @overload
    def dump(self) -> str:
        pass

    def dump(
        self, fd=None  # type: Optional[IO[bytes]]
    ):
        # type: (...) -> Optional[str]
        if fd is None:
            return "".join(t.text for t in self.iter_tokens())
        for token in self.iter_tokens():
            fd.write(token.text.encode("utf-8"))
        return None


_combine_error_tokens_into_elements = combine_into_replacement(
    Deb822ErrorToken, Deb822ErrorElement
)
_combine_comment_tokens_into_elements = combine_into_replacement(
    Deb822CommentToken, Deb822CommentElement
)
_combine_vl_elements_into_value_elements = combine_into_replacement(
    Deb822ValueLineElement, Deb822ValueElement
)
_combine_kvp_elements_into_paragraphs = combine_into_replacement(
    Deb822KeyValuePairElement,
    Deb822ParagraphElement,
    constructor=Deb822ParagraphElement.from_kvpairs,
)


def _parsed_value_render_factory(discard_comments):
    # type: (bool) -> Callable[[Deb822ParsedValueElement], str]
    return (
        Deb822ParsedValueElement.convert_to_text_without_comments
        if discard_comments
        else Deb822ParsedValueElement.convert_to_text
    )


LIST_SPACE_SEPARATED_INTERPRETATION = ListInterpretation(
    whitespace_split_tokenizer,
    _parse_whitespace_list_value,
    Deb822ParsedValueElement,
    Deb822SemanticallySignificantWhiteSpace,
    lambda: Deb822SpaceSeparatorToken(" "),
    _parsed_value_render_factory,
)
LIST_COMMA_SEPARATED_INTERPRETATION = ListInterpretation(
    comma_split_tokenizer,
    _parse_comma_list_value,
    Deb822ParsedValueElement,
    Deb822CommaToken,
    Deb822CommaToken,
    _parsed_value_render_factory,
)
LIST_UPLOADERS_INTERPRETATION = ListInterpretation(
    comma_split_tokenizer,
    _parse_uploaders_list_value,
    Deb822ParsedValueElement,
    Deb822CommaToken,
    Deb822CommaToken,
    _parsed_value_render_factory,
)


def _non_end_of_line_token(v):
    # type: (TokenOrElement) -> bool
    # Consume tokens until the newline
    return not isinstance(v, Deb822WhitespaceToken) or v.text != "\n"


def _build_value_line(
    token_stream,  # type: Iterable[Union[TokenOrElement, Deb822CommentElement]]
):
    # type: (...) -> Iterable[Union[TokenOrElement, Deb822ValueLineElement]]
    """Parser helper - consumes tokens part of a Deb822ValueEntryElement and turns them into one"""
    buffered_stream = BufferingIterator(token_stream)

    # Deb822ValueLineElement is a bit tricky because of how we handle whitespace
    # and comments.
    #
    # In relation to comments, then only continuation lines can have comments.
    # If there is a comment before a "K: V" line, then the comment is associated
    # with the field rather than the value.
    #
    # On the whitespace front, then we separate syntactical mandatory whitespace
    # from optional whitespace.  As an example:
    #
    # """
    # # some comment associated with the Depends field
    # Depends:_foo_$
    # # some comment associated with the line containing "bar"
    # !________bar_$
    # """
    #
    # Where "$" and "!" represents mandatory whitespace (the newline and the first
    # space are required for the file to be parsed correctly), where as "_" is
    # "optional" whitespace (from a syntactical point of view).
    #
    # This distinction enable us to facilitate APIs for easy removal/normalization
    # of redundant whitespaces without having programmers worry about trashing
    # the file.
    #
    #

    comment_element = None
    continuation_line_token = None
    token = None  # type: Optional[TokenOrElement]

    for token in buffered_stream:
        start_of_value_entry = False
        if isinstance(token, Deb822ValueContinuationToken):
            continuation_line_token = token
            start_of_value_entry = True
            token = None
        elif isinstance(token, Deb822FieldSeparatorToken):
            start_of_value_entry = True
        elif isinstance(token, Deb822CommentElement):
            next_token = buffered_stream.peek()
            # If the next token is a continuation line token, then this comment
            # belong to a value and we might as well just start the value
            # parsing now.
            #
            # Note that we rely on this behaviour to avoid emitting the comment
            # token (failing to do so would cause the comment to appear twice
            # in the file).
            if isinstance(next_token, Deb822ValueContinuationToken):
                start_of_value_entry = True
                comment_element = token
                token = None
                # Use next with None to avoid raising StopIteration inside a generator
                # It won't happen, but pylint cannot see that, so we do this instead.
                continuation_line_token = cast(
                    "Deb822ValueContinuationToken", next(buffered_stream, None)
                )
                assert continuation_line_token is not None

        if token is not None:
            yield token
        if start_of_value_entry:
            tokens_in_value = list(buffered_stream.takewhile(_non_end_of_line_token))
            eol_token = cast("Deb822WhitespaceToken", next(buffered_stream, None))
            assert eol_token is None or eol_token.text == "\n"
            leading_whitespace = None
            trailing_whitespace = None
            # "Depends:\n foo" would cause tokens_in_value to be empty for the
            # first "value line" (the empty part between ":" and "\n")
            if tokens_in_value:
                # Another special-case, "Depends: \n foo" (i.e. space after colon)
                # should not introduce an IndexError
                if isinstance(tokens_in_value[-1], Deb822WhitespaceToken):
                    trailing_whitespace = cast(
                        "Deb822WhitespaceToken", tokens_in_value.pop()
                    )
                if tokens_in_value and isinstance(
                    tokens_in_value[-1], Deb822WhitespaceToken
                ):
                    leading_whitespace = cast(
                        "Deb822WhitespaceToken", tokens_in_value[0]
                    )
                    tokens_in_value = tokens_in_value[1:]
            yield Deb822ValueLineElement(
                comment_element,
                continuation_line_token,
                leading_whitespace,
                tokens_in_value,
                trailing_whitespace,
                eol_token,
            )
            comment_element = None
            continuation_line_token = None


def _build_field_with_value(
    token_stream,  # type: Iterable[Union[TokenOrElement, Deb822ValueElement]]
):
    # type: (...) -> Iterable[Union[TokenOrElement, Deb822KeyValuePairElement]]
    buffered_stream = BufferingIterator(token_stream)
    for token_or_element in buffered_stream:
        start_of_field = False
        comment_element = None
        if isinstance(token_or_element, Deb822FieldNameToken):
            start_of_field = True
        elif isinstance(token_or_element, Deb822CommentElement):
            comment_element = token_or_element
            next_token = buffered_stream.peek()
            start_of_field = isinstance(next_token, Deb822FieldNameToken)
            if start_of_field:
                # Remember to consume the field token
                try:
                    token_or_element = next(buffered_stream)
                except StopIteration:  # pragma: no cover
                    raise AssertionError

        if start_of_field:
            field_name = token_or_element
            separator = next(buffered_stream, None)
            value_element = next(buffered_stream, None)
            if separator is None or value_element is None:
                # Early EOF - should not be possible with how the tokenizer works
                # right now, but now it is future-proof.
                if comment_element:
                    yield comment_element
                error_elements = [field_name]
                if separator is not None:
                    error_elements.append(separator)
                yield Deb822ErrorElement(error_elements)
                return

            if isinstance(separator, Deb822FieldSeparatorToken) and isinstance(
                value_element, Deb822ValueElement
            ):
                yield Deb822KeyValuePairElement(
                    comment_element,
                    cast("Deb822FieldNameToken", field_name),
                    separator,
                    value_element,
                )
            else:
                # We had a parse error, consume until the newline.
                error_tokens = [token_or_element]  # type: List[TokenOrElement]
                error_tokens.extend(buffered_stream.takewhile(_non_end_of_line_token))
                nl = buffered_stream.peek()
                # Take the newline as well if present
                if nl and isinstance(nl, Deb822NewlineAfterValueToken):
                    next(buffered_stream, None)
                    error_tokens.append(nl)
                yield Deb822ErrorElement(error_tokens)
        else:
            # Token is not part of a field, emit it as-is
            yield token_or_element


def _abort_on_error_tokens(sequence):
    # type: (Iterable[TokenOrElement]) -> Iterable[TokenOrElement]
    line_no = 1
    for token in sequence:
        # We are always called while the sequence consists entirely of tokens
        if token.is_error:
            error_as_text = token.convert_to_text().replace("\n", "\\n")
            raise SyntaxOrParseError(
                'Syntax or Parse error on or near line {line_no}: "{error_as_text}"'.format(
                    error_as_text=error_as_text, line_no=line_no
                )
            )
        line_no += token.convert_to_text().count("\n")
        yield token


def parse_deb822_file(
    sequence,  # type: Union[Iterable[Union[str, bytes]], str]
    *,
    accept_files_with_error_tokens=False,  # type: bool
    accept_files_with_duplicated_fields=False,  # type: bool
    encoding="utf-8",  # type: str
):
    # type: (...) -> Deb822FileElement
    """

    :param sequence: An iterable over lines of str or bytes (an open file for
      reading will do).  If line endings are provided in the input, then they
      must be present on every line (except the last) will be preserved as-is.
      If omitted and the content is at least 2 lines, then parser will assume
      implicit newlines.
    :param accept_files_with_error_tokens: If True, files with critical syntax
      or parse errors will be returned as "successfully" parsed. Usually,
      working on files with this kind of errors are not desirable as it is
      hard to make sense of such files (and they might in fact not be a deb822
      file at all).  When set to False (the default) a ValueError is raised if
      there is a critical syntax or parse error.
      Note that duplicated fields in a paragraph is not considered a critical
      parse error by this parser as the implementation can gracefully cope
      with these. Use accept_files_with_duplicated_fields to determine if
      such files should be accepted.
    :param accept_files_with_duplicated_fields: If True, then
      files containing paragraphs with duplicated fields will be returned as
      "successfully" parsed even though they are invalid according to the
      specification.  The paragraphs will prefer the first appearance of the
      field unless caller explicitly requests otherwise (e.g., via
      Deb822ParagraphElement.configured_view).  If False, then this method
      will raise a ValueError if any duplicated fields are seen inside any
      paragraph.
    :param encoding: The encoding to use (this is here to support Deb822-like
       APIs, new code should not use this parameter).
    """

    if isinstance(sequence, (str, bytes)):
        # Match the deb822 API.
        sequence = sequence.splitlines(True)

    # The order of operations are important here.  As an example,
    # _build_value_line assumes that all comment tokens have been merged
    # into comment elements.  Likewise, _build_field_and_value assumes
    # that value tokens (along with their comments) have been combined
    # into elements.
    tokens = tokenize_deb822_file(
        sequence, encoding=encoding
    )  # type: Iterable[TokenOrElement]
    if not accept_files_with_error_tokens:
        tokens = _abort_on_error_tokens(tokens)
    tokens = _combine_comment_tokens_into_elements(tokens)
    tokens = _build_value_line(tokens)
    tokens = _combine_vl_elements_into_value_elements(tokens)
    tokens = _build_field_with_value(tokens)
    tokens = _combine_kvp_elements_into_paragraphs(tokens)
    # Combine any free-floating error tokens into error elements.  We do
    # this last as it enables other parts of the parser to include error
    # tokens in their error elements if they discover something is wrong.
    tokens = _combine_error_tokens_into_elements(tokens)

    deb822_file = Deb822FileElement(LinkedList(tokens))

    if not accept_files_with_duplicated_fields:
        for no, paragraph in enumerate(deb822_file):
            if isinstance(paragraph, Deb822DuplicateFieldsParagraphElement):
                field_names = set()
                dup_field = None
                for field in paragraph.keys():
                    field_name, _, _ = _unpack_key(field)
                    # assert for mypy
                    assert isinstance(field_name, str)
                    if field_name in field_names:
                        dup_field = field_name
                        break
                    field_names.add(field_name)
                if dup_field is not None:
                    msg = 'Duplicate field "{dup_field}" in paragraph number {no}'
                    raise ValueError(msg.format(dup_field=dup_field, no=no))

    return deb822_file


if __name__ == "__main__":  # pragma: no cover
    import doctest

    doctest.testmod()
