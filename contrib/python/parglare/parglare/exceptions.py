from typing import Optional, Tuple

from parglare.common import Location
from parglare.termui import s_attention as err
from parglare.termui import s_header as _


class ParglareError(Exception):
    def __init__(
        self,
        location: Location,
        message: str,
        context_message: Optional[str] = None,
        error_type: str = err("error"),
        input: Optional[str] = None,
        hint: Optional[str] = None,
    ):
        self.location = location
        self.hint = hint
        self.message = message
        self.context_message = context_message
        self.error_type = error_type

        context = (
            get_context(input, location, context_message) if context_message else None
        )
        hint = _(f"  hint: {hint}") if hint else None

        self.full_message = "\n".join(
            filter(None, [f"{error_type}: {message}", context, hint])
        )

    def __str__(self):
        return f"{self.location}: {self.full_message}"


def get_line_col_at_position(
    text: str, pos: int
) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    lines = text.splitlines(keepends=True)

    if pos > len(text):
        # Position out of range
        return None, None, None, None

    # Special handling of EOF
    if pos == len(text):
        prev_line = lines[-2].rstrip("\n\r") if len(lines) > 1 else None
        return (
            len(lines) - 1,
            len(lines[-1]),
            lines[-1].rstrip("\n\r"),
            prev_line,
        )

    current_pos = 0
    for lineidx, line in enumerate(lines):
        if current_pos <= pos < current_pos + len(line):
            prev_line = lines[lineidx - 1].rstrip("\n\r") if lineidx > 0 else None
            return lineidx, pos - current_pos, line.rstrip("\n\r"), prev_line
        current_pos += len(line)
    return None, None, None, None


def get_indented_message(
    message: str,
    indent: int,
    prefix: Optional[str] = None,
    marker: Optional[str] = None,
) -> str:
    """
    Returns message where all lines are indented by `indent`.

    If optional `prefix` is given it is prepended to every line.
    """
    indent_str = (_(prefix) if prefix is not None else "") + " " * indent
    first_indent_str = (
        (indent_str[: -len(marker) + 1] + err(marker)) if marker is not None else None
    )
    return "\n".join(
        [
            f"{first_indent_str}{line}"
            if marker is not None and lineidx == 0
            else f"{indent_str}{line}"
            for lineidx, line in enumerate(message.splitlines())
        ]
    )


def get_context(input, location: Location, message: str) -> Optional[str]:
    context = None
    if input is not None and location.start_position is not None:
        if type(input) is str:
            lineidx, colidx, line, prev_line = get_line_col_at_position(
                input, location.start_position
            )
        else:
            start = max(location.start_position - 10, 0)
            lineidx = 0
            colidx = len(str(input[start : location.start_position])) + 1
            line = str(input[start : location.start_position + 10])
            prev_line = None

        if lineidx is not None and colidx is not None:
            prev_line_context = (
                _(f"{lineidx:>5} | ") + f"{prev_line}\n" if prev_line else ""
            )
            context = (
                prev_line_context
                + _(f"{lineidx + 1:>5} | ")
                + f"{line}\n"
                + get_indented_message(message, colidx + 4, "      |", "^^^ ")
            )

    return context


class GrammarError(ParglareError):
    def __init__(self, location, message):
        super().__init__(location, message, error_type=err("grammar error"))


class SyntaxError(ParglareError):
    def __init__(
        self,
        location: Location,
        input,
        symbols_expected,
        tokens_ahead=None,
        symbols_before=None,
        last_heads=None,
        grammar=None,
        hint=None,
    ):
        """
        Args:
        location(Location): The :class:`Location` of the error.
        symbols_expected(list): A list of :class:`GrammarSymbol` expected at
            the location
        tokens_ahead(list): A list of :class:`Token` recognized at the current
            location.
        symbols_before(list): A list of :class:`GrammarSymbol` recognized just
            before the current position
        last_heads(list): A list of :class:`GSSNode` GLR heads before the
            error.
        grammar(Grammar): An instance of :class:`Grammar` being used for
            parsing.
        """
        self.symbols_expected = symbols_expected
        self.tokens_ahead = tokens_ahead if tokens_ahead else []
        self.symbols_before = symbols_before if symbols_before else []
        self.last_heads = last_heads
        self.grammar = grammar
        token_str = "tokens" if len(self.tokens_ahead) > 1 else "token"
        if not location.is_eof():
            message = f"unexpected {token_str} " + ", ".join(
                sorted([str(t) for t in self.tokens_ahead])
            )
        else:
            message = "unexpected end of file"
        context_message = _("expected: ") + " ".join(
            sorted([s.name for s in symbols_expected])
        )
        super().__init__(
            location,
            message,
            context_message=context_message,
            input=input,
            error_type=err("syntax error"),
            hint=hint,
        )


def expected_symbols_str(symbols):
    return " ".join(sorted([s.name for s in symbols]))


def disambiguation_error(tokens):
    return "Can't disambiguate between: {}".format(
        _(" ").join(sorted([str(t) for t in tokens]))
    )


class ParserInitError(Exception):
    pass


class DisambiguationError(ParglareError):
    def __init__(self, location, tokens):
        self.tokens = tokens
        message = disambiguation_error(tokens)
        super().__init__(location, message)


class DynamicDisambiguationConflict(Exception):
    def __init__(self, context, actions):
        self.state = state = context.state
        self.token = token = context.token
        self.actions = actions

        from parglare.parser import SHIFT

        message = (
            f"{str(state)}\nIn state {state.state_id}:{state.symbol} "
            f"and input symbol '{token}' after calling"
            " dynamic disambiguation still can't decide "
        )
        if actions[0].action == SHIFT:
            prod_str = " or ".join([f"'{str(a.prod)}'" for a in actions[1:]])
            message += f"whether to shift or reduce by production(s) {prod_str}."
        else:
            prod_str = " or ".join([f"'{str(a.prod)}'" for a in actions])
            message += f"which reduction to perform: {prod_str}"

        self.message = message

    def __str__(self):
        return self.message


class LRConflict:
    def __init__(self, state, term, productions):
        self.state = state
        self.term = term
        self.productions = productions

    @property
    def dynamic(self):
        return self.term in self.state.dynamic


class SRConflict(LRConflict):
    def __init__(self, state, term, productions):
        super().__init__(state, term, productions)

    def __str__(self):
        prod_str = " or ".join([f"'{str(p)}'" for p in self.productions])
        message = (
            "{}\nIn state {}:{} and input symbol '{}' can't "
            "decide whether to shift or reduce by production(s) {}.{}".format(
                str(self.state),
                self.state.state_id,
                self.state.symbol,
                self.term,
                prod_str,
                " Dynamic disambiguation strategy will be called."
                if self.dynamic
                else "",
            )
        )

        return message


class RRConflict(LRConflict):
    def __init__(self, state, term, productions):
        super().__init__(state, term, productions)

    def __str__(self):
        prod_str = " or ".join([f"'{str(p)}'" for p in self.productions])
        message = (
            "{}\nIn state {}:{} and input symbol '{}' can't "
            "decide which reduction to perform: {}.{}".format(
                str(self.state),
                self.state.state_id,
                self.state.symbol,
                self.term,
                prod_str,
                " Dynamic disambiguation strategy will be called."
                if self.dynamic
                else "",
            )
        )
        return message


class LRConflicts(Exception):
    def __init__(self, conflicts):
        self.conflicts = conflicts
        message = (
            f"\n{self.kind} conflicts in following states: "
            f"{set([c.state.state_id for c in conflicts])}"
        )
        super().__init__(message)


class SRConflicts(LRConflicts):
    kind = "Shift/Reduce"


class RRConflicts(LRConflicts):
    kind = "Reduce/Reduce"


class LoopError(Exception):
    pass
