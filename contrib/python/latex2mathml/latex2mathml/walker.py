from typing import Any, Iterator, NamedTuple, Optional

from latex2mathml import commands
from latex2mathml.exceptions import (
    DenominatorNotFoundError,
    DoubleSubscriptsError,
    DoubleSuperscriptsError,
    ExtraLeftOrMissingRightError,
    InvalidAlignmentError,
    InvalidStyleForGenfracError,
    InvalidWidthError,
    LimitsMustFollowMathOperatorError,
    MissingEndError,
    MissingSuperScriptOrSubscriptError,
    NoAvailableTokensError,
    NumeratorNotFoundError,
)
from latex2mathml.symbols_parser import convert_symbol
from latex2mathml.tokenizer import tokenize


class Node(NamedTuple):
    token: str
    children: Optional[tuple[Any, ...]] = None
    delimiter: Optional[str] = None
    alignment: Optional[str] = None
    text: Optional[str] = None
    attributes: Optional[dict[str, str]] = None
    modifier: Optional[str] = None


def walk(data: str, display: str = "inline") -> list[Node]:
    tokens = tokenize(data)
    block = display == "block"
    return _walk(tokens, block=block)


def _walk(tokens: Iterator[str], terminator: Optional[str] = None, limit: int = 0, block: bool = False) -> list[Node]:
    group: list[Node] = []
    token: str
    has_available_tokens = False
    for token in tokens:
        has_available_tokens = True
        if token == terminator:
            delimiter = None
            if terminator == commands.RIGHT:
                delimiter = next(tokens)
            group.append(Node(token=token, delimiter=delimiter))
            break
        elif (token == commands.RIGHT != terminator) or (token == commands.MIDDLE and terminator != commands.RIGHT):
            raise ExtraLeftOrMissingRightError
        elif token == commands.LEFT:
            delimiter = next(tokens)
            children = tuple(_walk(tokens, terminator=commands.RIGHT))  # make \right as a child of \left
            if len(children) == 0 or children[-1].token != commands.RIGHT:
                raise ExtraLeftOrMissingRightError
            node = Node(token=token, children=children if len(children) else None, delimiter=delimiter)
        elif token == commands.OPENING_BRACE:
            children = tuple(_walk(tokens, terminator=commands.CLOSING_BRACE))
            if len(children) and children[-1].token == commands.CLOSING_BRACE:
                children = children[:-1]
            node = Node(token=commands.BRACES, children=children)
        elif token in (commands.SUBSCRIPT, commands.SUPERSCRIPT):
            try:
                previous = group.pop()
            except IndexError:
                previous = Node(token="")  # left operand can be empty if not present

            if token == previous.token == commands.SUBSCRIPT:
                raise DoubleSubscriptsError
            if (token == previous.token == commands.SUPERSCRIPT) and (
                previous.children is not None
                and len(previous.children) >= 2
                and previous.children[1].token != commands.PRIME
            ):
                raise DoubleSuperscriptsError

            modifier = None
            if previous.token == commands.LIMITS:
                modifier = commands.LIMITS
                try:
                    previous = group.pop()
                    if not previous.token.startswith("\\"):  # TODO: Complete list of operators
                        raise LimitsMustFollowMathOperatorError
                except IndexError:
                    raise LimitsMustFollowMathOperatorError
            elif block and previous.token in (commands.SUMMATION, commands.PRODUCT):
                # block summation and product should result in limited sub/sup
                modifier = commands.LIMITS

            if token == commands.SUBSCRIPT and previous.token == commands.SUPERSCRIPT and previous.children is not None:
                children = tuple(_walk(tokens, terminator=terminator, limit=1))
                node = Node(
                    token=commands.SUBSUP,
                    children=(previous.children[0], *children, previous.children[1]),
                    modifier=previous.modifier,
                )
            elif (
                token == commands.SUPERSCRIPT and previous.token == commands.SUBSCRIPT and previous.children is not None
            ):
                children = tuple(_walk(tokens, terminator=terminator, limit=1))
                node = Node(token=commands.SUBSUP, children=(*previous.children, *children), modifier=previous.modifier)
            elif (
                token == commands.SUPERSCRIPT
                and previous.token == commands.SUPERSCRIPT
                and previous.children is not None
                and previous.children[1].token == commands.PRIME
            ):
                children = tuple(_walk(tokens, terminator=terminator, limit=1))

                node = Node(
                    token=commands.SUPERSCRIPT,
                    children=(
                        previous.children[0],
                        Node(token=commands.BRACES, children=(previous.children[1], *children)),
                    ),
                    modifier=previous.modifier,
                )
            else:
                try:
                    children = tuple(_walk(tokens, terminator=terminator, limit=1))
                except NoAvailableTokensError:
                    raise MissingSuperScriptOrSubscriptError
                if previous.token in (commands.OVERBRACE, commands.UNDERBRACE):
                    modifier = previous.token
                node = Node(token=token, children=(previous, *children), modifier=modifier)
        elif token == commands.APOSTROPHE:
            try:
                previous = group.pop()
            except IndexError:
                previous = Node(token="")  # left operand can be empty if not present

            if (
                previous.token == commands.SUPERSCRIPT
                and previous.children is not None
                and len(previous.children) >= 2
                and previous.children[1].token != commands.PRIME
            ):
                raise DoubleSuperscriptsError

            if (
                previous.token == commands.SUPERSCRIPT
                and previous.children is not None
                and len(previous.children) >= 2
                and previous.children[1].token == commands.PRIME
            ):
                node = Node(token=commands.SUPERSCRIPT, children=(previous.children[0], Node(token=commands.DPRIME)))
            elif previous.token == commands.SUBSCRIPT and previous.children is not None:
                node = Node(
                    token=commands.SUBSUP,
                    children=(*previous.children, Node(token=commands.PRIME)),
                    modifier=previous.modifier,
                )
            else:
                node = Node(token=commands.SUPERSCRIPT, children=(previous, Node(token=commands.PRIME)))
        elif token in commands.COMMANDS_WITH_TWO_PARAMETERS:
            attributes = None
            children = tuple(_walk(tokens, terminator=terminator, limit=2))
            if token in (commands.OVERSET, commands.UNDERSET):
                children = children[::-1]
            node = Node(token=token, children=children, attributes=attributes)
        elif token in commands.COMMANDS_WITH_ONE_PARAMETER or token.startswith(commands.MATH):
            children = tuple(_walk(tokens, terminator=terminator, limit=1))
            node = Node(token=token, children=children)
        elif token == commands.NOT:
            try:
                next_node = tuple(_walk(tokens, terminator=terminator, limit=1))[0]
                if next_node.token.startswith("\\"):
                    negated_symbol = r"\n" + next_node.token[1:]
                    symbol = convert_symbol(negated_symbol)
                    if symbol:
                        node = Node(token=negated_symbol)
                        group.append(node)
                        continue
                node = Node(token=token)
                group.extend((node, next_node))
                continue
            except NoAvailableTokensError:
                node = Node(token=token)
        elif token in (commands.XLEFTARROW, commands.XRIGHTARROW):
            children = tuple(_walk(tokens, terminator=terminator, limit=1))
            if children[0].token == commands.OPENING_BRACKET:
                children = (
                    Node(
                        token=commands.BRACES, children=tuple(_walk(tokens, terminator=commands.CLOSING_BRACKET))[:-1]
                    ),
                    *tuple(_walk(tokens, terminator=terminator, limit=1)),
                )
            node = Node(token=token, children=children)
        elif token in (commands.HSKIP, commands.HSPACE, commands.KERN, commands.MKERN, commands.MSKIP, commands.MSPACE):
            children = tuple(_walk(tokens, terminator=terminator, limit=1))
            if children[0].token == commands.BRACES and children[0].children is not None:
                children = children[0].children
            node = Node(token=token, attributes={"width": children[0].token})
        elif token == commands.COLOR:
            attributes = {"mathcolor": next(tokens)}
            children = tuple(_walk(tokens, terminator=terminator))
            sibling = None
            if len(children) and children[-1].token == terminator:
                children, sibling = children[:-1], children[-1]
            group.append(Node(token=token, children=children, attributes=attributes))
            if sibling:
                group.append(sibling)
            break
        elif token == commands.STYLE:
            attributes = {"style": next(tokens)}
            next_node = tuple(_walk(tokens, terminator=terminator, limit=1))[0]
            node = next_node._replace(attributes=attributes)
        elif token in (
            *commands.BIG.keys(),
            *commands.BIG_OPEN_CLOSE.keys(),
            commands.FBOX,
            commands.HBOX,
            commands.MBOX,
            commands.MIDDLE,
            commands.TEXT,
            commands.TEXTBF,
            commands.TEXTIT,
            commands.TEXTRM,
            commands.TEXTSF,
            commands.TEXTTT,
        ):
            node = Node(token=token, text=next(tokens))
        elif token == commands.HREF:
            attributes = {"href": next(tokens)}
            children = tuple(_walk(tokens, terminator=terminator, limit=1))
            node = Node(token=token, children=children, attributes=attributes)
        elif token in (
            commands.ABOVE,
            commands.ATOP,
            commands.ABOVEWITHDELIMS,
            commands.ATOPWITHDELIMS,
            commands.BRACE,
            commands.BRACK,
            commands.CHOOSE,
            commands.OVER,
        ):
            attributes = None
            delimiter = None

            if token == commands.ABOVEWITHDELIMS:
                delimiter = next(tokens).lstrip("\\") + next(tokens).lstrip("\\")
            elif token == commands.ATOPWITHDELIMS:
                attributes = {"linethickness": "0"}
                delimiter = next(tokens).lstrip("\\") + next(tokens).lstrip("\\")
            elif token == commands.BRACE:
                delimiter = "{}"
            elif token == commands.BRACK:
                delimiter = "[]"
            elif token == commands.CHOOSE:
                delimiter = "()"

            if token in (commands.ABOVE, commands.ABOVEWITHDELIMS):
                dimension_node = tuple(_walk(tokens, terminator=terminator, limit=1))[0]
                dimension = _get_dimension(dimension_node)
                attributes = {"linethickness": dimension}
            elif token in (commands.ATOP, commands.BRACE, commands.BRACK, commands.CHOOSE):
                attributes = {"linethickness": "0"}

            denominator = tuple(_walk(tokens, terminator=terminator))

            sibling = None
            if len(denominator) and denominator[-1].token == terminator:
                denominator, sibling = denominator[:-1], denominator[-1]

            if len(denominator) == 0:
                if token in (commands.BRACE, commands.BRACK):
                    denominator = (Node(token=commands.BRACES, children=()),)
                else:
                    raise DenominatorNotFoundError
            if len(group) == 0:
                if token in (commands.BRACE, commands.BRACK):
                    group = [Node(token=commands.BRACES, children=())]
                else:
                    raise NumeratorNotFoundError
            if len(denominator) > 1:
                denominator = (Node(token=commands.BRACES, children=denominator),)

            if len(group) == 1:
                children = (*group, *denominator)
            else:
                children = (Node(token=commands.BRACES, children=tuple(group)), *denominator)
            group = [Node(token=commands.FRAC, children=children, attributes=attributes, delimiter=delimiter)]
            if sibling is not None:
                group.append(sibling)
            break
        elif token == commands.SQRT:
            root_nodes = None
            next_node = tuple(_walk(tokens, limit=1))[0]
            if next_node.token == commands.OPENING_BRACKET:
                root_nodes = tuple(_walk(tokens, terminator=commands.CLOSING_BRACKET))[:-1]
                next_node = tuple(_walk(tokens, limit=1))[0]
                if len(root_nodes) > 1:
                    root_nodes = (Node(token=commands.BRACES, children=root_nodes),)

            if root_nodes:
                node = Node(token=commands.ROOT, children=(next_node, *root_nodes))
            else:
                node = Node(token=token, children=(next_node,))
        elif token == commands.ROOT:
            root_nodes = tuple(_walk(tokens, terminator=r"\of"))[:-1]
            next_node = tuple(_walk(tokens, limit=1))[0]
            if len(root_nodes) > 1:
                root_nodes = (Node(token=commands.BRACES, children=root_nodes),)
            if root_nodes:
                node = Node(token=token, children=(next_node, *root_nodes))
            else:
                node = Node(token=token, children=(next_node, Node(token=commands.BRACES, children=())))
        elif token in commands.MATRICES:
            children = tuple(_walk(tokens, terminator=terminator))
            sibling = None
            if len(children) and children[-1].token == terminator:
                children, sibling = children[:-1], children[-1]
            if len(children) == 1 and children[0].token == commands.BRACES and children[0].children:
                children = children[0].children
            if sibling is not None:
                group.extend([Node(token=token, children=children, alignment=""), sibling])
                break
            else:
                node = Node(token=token, children=children, alignment="")
        elif token == commands.GENFRAC:
            delimiter = next(tokens).lstrip("\\") + next(tokens).lstrip("\\")
            dimension_node, style_node = tuple(_walk(tokens, terminator=terminator, limit=2))
            dimension = _get_dimension(dimension_node)
            style = _get_style(style_node)
            attributes = {"linethickness": dimension}
            children = tuple(_walk(tokens, terminator=terminator, limit=2))
            group.extend(
                [Node(token=style), Node(token=token, children=children, delimiter=delimiter, attributes=attributes)]
            )
            break
        elif token == commands.SIDESET:
            left, right, operator = tuple(_walk(tokens, terminator=terminator, limit=3))
            left_token, left_children = _make_subsup(left)
            right_token, right_children = _make_subsup(right)
            attributes = {"movablelimits": "false"}
            node = Node(
                token=token,
                children=(
                    Node(
                        token=left_token,
                        children=(
                            Node(
                                token=commands.VPHANTOM,
                                children=(
                                    Node(token=operator.token, children=operator.children, attributes=attributes),
                                ),
                            ),
                            *left_children,
                        ),
                    ),
                    Node(
                        token=right_token,
                        children=(
                            Node(token=operator.token, children=operator.children, attributes=attributes),
                            *right_children,
                        ),
                    ),
                ),
            )
        elif token == commands.SKEW:
            width_node, child = tuple(_walk(tokens, terminator=terminator, limit=2))
            width = width_node.token
            if width == commands.BRACES:
                if width_node.children is None or len(width_node.children) == 0:
                    raise InvalidWidthError
                width = width_node.children[0].token
            if not width.isdigit():
                raise InvalidWidthError
            node = Node(token=token, children=(child,), attributes={"width": f"{0.0555 * int(width):.3f}em"})
        elif token.startswith(commands.BEGIN):
            node = _get_environment_node(token, tokens)
        else:
            node = Node(token=token)

        group.append(node)

        if limit and len(group) >= limit:
            break
    if not has_available_tokens:
        raise NoAvailableTokensError
    return group


def _make_subsup(node: Node) -> tuple[str, tuple[Node, ...]]:
    # TODO: raise error instead of assertion
    assert node.token == commands.BRACES
    try:
        assert (
            node.children is not None
            and 2 <= len(node.children[0].children) <= 3
            and node.children[0].token
            in (
                commands.SUBSUP,
                commands.SUBSCRIPT,
                commands.SUPERSCRIPT,
            )
        )
        token = node.children[0].token
        children = node.children[0].children[1:]
        return token, children
    except IndexError:
        return "", ()


def _get_dimension(node: Node) -> str:
    dimension = node.token
    if node.token == commands.BRACES and node.children is not None:
        dimension = node.children[0].token
    return dimension


def _get_style(node: Node) -> str:
    style = node.token
    if node.token == commands.BRACES and node.children is not None:
        style = node.children[0].token
    if style == "0":
        return commands.DISPLAYSTYLE
    if style == "1":
        return commands.TEXTSTYLE
    if style == "2":
        return commands.SCRIPTSTYLE
    if style == "3":
        return commands.SCRIPTSCRIPTSTYLE
    raise InvalidStyleForGenfracError


def _get_environment_node(token: str, tokens: Iterator[str]) -> Node:
    # TODO: support non-matrix environments
    start_index = token.index("{") + 1
    environment = token[start_index:-1]
    terminator = rf"{commands.END}{{{environment}}}"
    children = tuple(_walk(tokens, terminator=terminator))
    if len(children) and children[-1].token != terminator:
        raise MissingEndError
    children = children[:-1]
    alignment = ""

    if len(children) and children[0].token == commands.OPENING_BRACKET:
        children_iter = iter(children)
        next(children_iter)  # remove BRACKET
        for c in children_iter:
            if c.token == commands.CLOSING_BRACKET:
                break
            elif c.token not in "lcr|":
                raise InvalidAlignmentError
            alignment += c.token
        children = tuple(children_iter)
    elif (
        len(children)
        and children[0].children is not None
        and (
            children[0].token == commands.BRACES
            or (environment.endswith("*") and children[0].token == commands.BRACKETS)
        )
        and all(c.token in "lcr|" for c in children[0].children)
    ):
        alignment = "".join(c.token for c in children[0].children)
        children = children[1:]

    return Node(token=rf"\{environment}", children=children, alignment=alignment)
