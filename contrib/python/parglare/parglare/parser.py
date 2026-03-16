import ast
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union

from parglare import termui
from parglare.actions import pass_none
from parglare.common import (
    ErrorContext,
    Location,
    pos_to_line_col,
    position_context,
)
from parglare.exceptions import (
    DisambiguationError,
    DynamicDisambiguationConflict,
    ParserInitError,
    RRConflicts,
    SRConflicts,
    SyntaxError,
    expected_symbols_str,
)

if TYPE_CHECKING:
    from parglare.glr import GLRParser
from parglare.grammar import EMPTY, STOP, Grammar
from parglare.tables import ACCEPT, LALR, REDUCE, SHIFT, SLR
from parglare.termui import a_print, h_print, prints
from parglare.trees import NodeNonTerm, NodeTerm

logger = logging.getLogger(__name__)


def hint_key(state: int, tokens_ahead: Union[List["Token"], None]) -> Tuple[Any, ...]:
    if tokens_ahead is not None:
        lookaheads = sorted([t.symbol.name for t in tokens_ahead])
    else:
        lookaheads = []
    return (state,) + tuple(lookaheads)


class Parser:
    """Parser works like a DFA driven by LR tables. For a given grammar LR table
    will be created and cached or loaded from cache if cache is found.
    """

    def __init__(
        self,
        grammar: Grammar,
        in_layout=False,
        actions=None,
        layout_actions=None,
        debug=False,
        debug_trace=False,
        debug_colors=False,
        debug_layout=False,
        ws="\n\r\t ",
        consume_input=True,
        build_tree=False,
        call_actions_during_tree_build=False,
        tables=LALR,
        return_position=False,
        prefer_shifts=None,
        prefer_shifts_over_empty=None,
        error_recovery=False,
        dynamic_filter=None,
        custom_token_recognition=None,
        lexical_disambiguation=True,
        force_load_table=False,
        table=None,
    ):
        self.grammar = grammar
        self.in_layout = in_layout

        EMPTY.action = pass_none
        if actions:
            self.grammar._resolve_actions(
                action_overrides=actions, fail_on_no_resolve=True
            )

        self.layout_parser = None
        if self.in_layout:
            start_production = grammar.get_production_id("LAYOUT")
        else:
            start_production = 1
            layout_symbol = grammar.get_symbol("LAYOUT")
            if layout_symbol:
                self.layout_parser = Parser(
                    grammar,
                    in_layout=True,
                    consume_input=False,
                    actions=layout_actions,
                    ws=None,
                    return_position=True,
                    prefer_shifts=True,
                    prefer_shifts_over_empty=True,
                    debug=debug_layout,
                )

        self.ws = ws
        self.return_position = return_position
        self.debug = debug
        self.debug_trace = debug_trace
        self.debug_colors = debug_colors
        termui.colors = debug_colors
        self.debug_layout = debug_layout

        self.consume_input = consume_input
        self.build_tree = build_tree
        self.call_actions_during_tree_build = call_actions_during_tree_build

        self.error_recovery = error_recovery
        self.dynamic_filter = dynamic_filter
        self.custom_token_recognition = custom_token_recognition
        self.lexical_disambiguation = lexical_disambiguation

        # should we clear transient state after parsing.
        self.clear_transient = True

        if table is None:
            from .closure import LR_0, LR_1
            from .tables import create_load_table

            itemset_type = LR_0 if tables == SLR else LR_1

            if prefer_shifts is None:
                prefer_shifts = True
            if prefer_shifts_over_empty is None:
                prefer_shifts_over_empty = True

            self.table = create_load_table(
                grammar,
                itemset_type=itemset_type,
                start_production=start_production,
                prefer_shifts=prefer_shifts,
                prefer_shifts_over_empty=prefer_shifts_over_empty,
                lexical_disambiguation=lexical_disambiguation,
                force_load=force_load_table,
                in_layout=self.in_layout,
                debug=debug,
            )
        else:
            self.table = table

            # warn about overriden parameters
            for name, value, default in [
                ("tables", tables, LALR),
                ("prefer_shifts", prefer_shifts, None),
                ("prefer_shifts_over_empty", prefer_shifts_over_empty, None),
                ("force_load_table", force_load_table, False),
            ]:
                if value is not default:
                    logger.warning(
                        "Precomputed table overrides value of parameter %s",
                        name,
                    )

        self._check_parser()
        if not self.in_layout:
            self.error_hints = self._custom_error_hints()

        if debug:
            self.print_debug()

    def _check_parser(self):
        if self.table.sr_conflicts:
            self.print_debug()
            if self.dynamic_filter:
                unhandled_conflicts = []
                for src in self.table.sr_conflicts:
                    if not src.dynamic:
                        unhandled_conflicts.append(src)
            else:
                unhandled_conflicts = self.table.sr_conflicts

            if unhandled_conflicts:
                raise SRConflicts(unhandled_conflicts)

        # Reduce/Reduce conflicts are fatal for LR parsing
        if self.table.rr_conflicts:
            self.print_debug()
            if self.dynamic_filter:
                unhandled_conflicts = []
                for rrc in self.table.rr_conflicts:
                    if not rrc.dynamic:
                        unhandled_conflicts.append(rrc)
            else:
                unhandled_conflicts = self.table.rr_conflicts

            if unhandled_conflicts:
                raise RRConflicts(unhandled_conflicts)

    def _custom_error_hints(self) -> Union[Dict[Tuple, str], None]:
        """If custom error hints file exists check if it needs compiling and if
        so perform compilation.

        """
        if self.grammar.file_path is None:
            return None

        def compile_errors(hints_file: Path) -> Dict[Tuple, str]:
            # Parse hints file
            examples = []
            with open(hints_file) as f:
                example_src: List[str] = []
                hint: List[str] = []
                in_example = True
                lookahead = False

                def new_example():
                    nonlocal example_src, hint, lookahead, in_example
                    examples.append(
                        {
                            "example": "".join(example_src),
                            "hint": "\n".join(hint),
                            "lookahead": lookahead,
                        }
                    )
                    example_src = []
                    hint = []
                    in_example = True

                for line in f:
                    if not in_example and line.strip() == "":
                        continue
                    if line.startswith("====="):
                        new_example()
                        continue

                    if line.startswith(":::"):
                        lookahead = line[3] == "+"
                        in_example = False
                        continue

                    if in_example:
                        example_src.append(line)
                    else:
                        hint.append(line.strip())

                new_example()

            compiled_examples = {}
            self.clear_transient = False
            for example in examples:
                try:
                    self.parse(example["example"])
                except SyntaxError as e:
                    del example["example"]
                    states = []
                    try:
                        states = [self.parse_stack[-1].state.state_id]
                    except AttributeError:
                        # We are using GLR
                        if TYPE_CHECKING:
                            assert isinstance(self, GLRParser)
                        states = self._active_heads.keys()
                    lookahead = example.pop("lookahead")
                    lookaheads = e.tokens_ahead if lookahead else None
                    for state in states:
                        key = hint_key(state, lookaheads)
                        compiled_examples[key] = example["hint"]
            self.clear_transient = True

            return compiled_examples

        self._in_error_hints = True
        grammar_file = Path(self.grammar.file_path)
        hints_file = grammar_file.with_suffix(".pge")
        compiled_hints = None
        if hints_file.exists():
            hints_file_compiled = hints_file.with_suffix(".pgec")
            if (
                not hints_file_compiled.exists()
                or grammar_file.stat().st_mtime > hints_file_compiled.stat().st_mtime
                or hints_file.stat().st_mtime > hints_file_compiled.stat().st_mtime
            ):
                # Compilation is needed
                compiled_hints = compile_errors(hints_file)
                with open(hints_file_compiled, "w") as f:
                    serializable = {str(k): v for k, v in compiled_hints.items()}
                    json.dump(serializable, f)
            else:
                with open(hints_file_compiled) as f:
                    loaded = json.load(f)
                    compiled_hints = {ast.literal_eval(k): v for k, v in loaded.items()}

        del self._in_error_hints
        return compiled_hints

    def print_debug(self):
        if self.in_layout and self.debug_layout:
            a_print("*** LAYOUT parser ***", new_line=True)
        self.table.print_debug()

    def parse_file(self, file_name, **kwargs):
        """
        Parses content from the given file.
        Args:
            file_name(str): A file name.
        """
        with open(file_name, encoding="utf-8") as f:
            content = f.read()
        return self.parse(content, file_name=file_name, **kwargs)

    def parse(self, input_str, position=0, file_name=None, extra=None):
        """
        Parses the given input string.
        Args:
            input_str(str): A string to parse.
            position(int): Position to start from.
            file_name(str): File name if applicable. Used in error reporting.
            extra: An object that keeps custom parsing state. If not given
                initialized to dict.
        """

        if self.debug:
            a_print("*** PARSING STARTED", new_line=True)

        extra = {} if extra is None else extra

        self.errors = []
        self.in_error_recovery = False

        next_token = self._next_token
        debug = self.debug

        accepted_head = None
        start_head = LRStackNode(
            file_name, input_str, self.table.states[0], 0, position, extra
        )
        self._init_dynamic_disambiguation(start_head)
        self.parse_stack = parse_stack = [start_head]

        while True:
            head = parse_stack[-1]
            cur_state = head.state
            if debug:
                a_print("Current state:", str(cur_state.state_id), new_line=True)

            if head.token_ahead is None:
                if not self.in_layout:
                    self._skipws(head, input_str)
                    if self.debug:
                        h_print(
                            "Layout content:",
                            f"'{head.layout_content}'",
                            level=1,
                        )

                head.token_ahead = next_token(head)

            if debug:
                h_print(
                    "Context:",
                    position_context(head.input_str, head.position),
                    level=1,
                )
                h_print(
                    "Tokens expected:",
                    expected_symbols_str(cur_state.actions.keys()),
                    level=1,
                )
                h_print("Token ahead:", head.token_ahead, level=1)

            actions = None
            if head.token_ahead is not None:
                actions = cur_state.actions.get(head.token_ahead.symbol)
            if not actions and not self.consume_input:
                # If we don't have any action for the current token ahead
                # see if we can finish without consuming the whole input.
                actions = cur_state.actions.get(STOP)

            if not actions:
                symbols_expected = list(cur_state.actions.keys())
                tokens_ahead = self._get_all_possible_tokens_ahead(head)
                self.errors.append(
                    self._create_error(
                        input_str,
                        head,
                        symbols_expected,
                        tokens_ahead,
                        symbols_before=[cur_state.symbol],
                    )
                )

                if self.error_recovery:
                    if self.debug:
                        a_print("*** STARTING ERROR RECOVERY.", new_line=True)
                    if self._do_recovery():
                        # Error recovery succeeded
                        if self.debug:
                            a_print(
                                "*** ERROR RECOVERY SUCCEEDED. CONTINUING.",
                                new_line=True,
                            )
                        continue
                    else:
                        break
                else:
                    break

            # Dynamic disambiguation
            if self.dynamic_filter:
                actions = self._dynamic_disambiguation(head, actions)

                # If after dynamic disambiguation we still have at least one
                # shift and non-empty reduction or multiple non-empty
                # reductions raise exception.
                if (
                    len(
                        [
                            a
                            for a in actions
                            if (a.action is SHIFT)
                            or ((a.action is REDUCE) and len(a.prod.rhs))
                        ]
                    )
                    > 1
                ):
                    raise DynamicDisambiguationConflict(head, actions)

            # If dynamic disambiguation is disabled either globaly by not
            # giving disambiguation function or localy by not marking
            # any production dynamic for this state take the first action.
            # First action is either SHIFT while there might be empty
            # reductions, or it is the only reduction.
            # Otherwise, parser construction should raise an error.
            act = actions[0]

            if act.action is SHIFT:
                cur_state = act.state

                if debug:
                    a_print(
                        "Shift:",
                        f'{cur_state.state_id} "{head.token_ahead.value}"'
                        + " at position "
                        + str(pos_to_line_col(input_str, head.position)),
                        level=1,
                    )

                new_position = head.position + len(head.token_ahead)
                new_head = LRStackNode(
                    file_name,
                    input_str,
                    state=act.state,
                    frontier=head.frontier + 1,
                    token=head.token_ahead,
                    extra=head.extra,
                    layout_content=head.layout_content_ahead,
                    position=new_position,
                    start_position=head.position,
                    end_position=new_position,
                )
                new_head.results = self._call_shift_action(new_head)
                parse_stack.append(new_head)

                self.in_error_recovery = False

            elif act.action is REDUCE:
                # if this is EMPTY reduction try to take another if
                # exists.
                if len(act.prod.rhs) == 0 and len(actions) > 1:
                    act = actions[1]
                production = act.prod

                if debug:
                    a_print("Reducing", f"by prod '{production}'.", level=1)

                r_length = len(production.rhs)
                if r_length:
                    start_reduction_head = parse_stack[-r_length]
                    results = [x.results for x in parse_stack[-r_length:]]
                    del parse_stack[-r_length:]
                    next_state = parse_stack[-1].state.gotos[production.symbol]
                    new_head = LRStackNode(
                        file_name,
                        input_str,
                        state=next_state,
                        frontier=head.frontier,
                        position=head.position,
                        extra=head.extra,
                        production=production,
                        start_position=start_reduction_head.start_position,
                        end_position=head.end_position,
                        token_ahead=head.token_ahead,
                        layout_content=start_reduction_head.layout_content,
                        layout_content_ahead=head.layout_content_ahead,
                    )
                else:
                    # Empty reduction
                    results = []
                    next_state = cur_state.gotos[production.symbol]
                    new_head = LRStackNode(
                        file_name,
                        input_str,
                        state=next_state,
                        frontier=head.frontier,
                        position=head.position,
                        extra=head.extra,
                        production=production,
                        start_position=head.end_position,
                        end_position=head.end_position,
                        token_ahead=head.token_ahead,
                        layout_content="",
                        layout_content_ahead=head.layout_content_ahead,
                    )

                # Calling reduce action
                new_head.results = self._call_reduce_action(new_head, results)
                parse_stack.append(new_head)

            elif act.action is ACCEPT:
                accepted_head = head
                break

        if accepted_head:
            if debug:
                a_print("SUCCESS!!!")
            if self.return_position:
                return parse_stack[1].results, parse_stack[1].position
            else:
                return parse_stack[1].results
        else:
            error = self.errors[-1]
            del self.errors
            raise error

    def call_actions(self, node):
        """
        Calls semantic actions for the given tree node.
        """

        def inner_call_actions(node):
            sem_action = node.symbol.action
            if node.is_term():
                if sem_action:
                    try:
                        result = sem_action(
                            node.context, node.value, *node.additional_data
                        )
                    except TypeError as e:
                        raise TypeError(
                            "{}: terminal={} action={} params={}".format(
                                str(e),
                                node.symbol.name,
                                repr(sem_action),
                                (
                                    node.context,
                                    node.value,
                                    node.additional_data,
                                ),
                            )
                        ) from e
                else:
                    result = node.value
            else:
                subresults = []
                # Recursive right to left, bottom up. Simulate LR
                # reductions.
                for n in reversed(node):
                    subresults.append(inner_call_actions(n))
                subresults.reverse()

                if sem_action:
                    assignments = node.production.assignments
                    if assignments:
                        assgn_results = {}
                        for a in assignments.values():
                            if a.op == "=":
                                assgn_results[a.name] = subresults[a.index]
                            else:
                                assgn_results[a.name] = bool(subresults[a.index])
                    if isinstance(sem_action, list):
                        if assignments:
                            result = sem_action[node.production.prod_symbol_id](
                                node, subresults, **assgn_results
                            )
                        else:
                            result = sem_action[node.production.prod_symbol_id](
                                node.context, subresults
                            )
                    else:
                        if assignments:
                            result = sem_action(node.context, subresults, **assgn_results)
                        else:
                            result = sem_action(node.context, subresults)
                else:
                    result = subresults[0] if len(subresults) == 1 else subresults

            return result

        return inner_call_actions(node)

    def _skipws(self, head, input_str):
        in_len = len(input_str)
        layout_content_ahead = ""

        if self.layout_parser:
            _, pos = self.layout_parser.parse(input_str, head.position)
            if pos > head.position:
                layout_content_ahead = input_str[head.position : pos]
                head.position = pos
        elif self.ws:
            old_pos = head.position
            try:
                while head.position < in_len and input_str[head.position] in self.ws:
                    head.position += 1
            except TypeError as ex:
                raise ParserInitError(
                    "For parsing non-textual content please set `ws` to `None`."
                ) from ex
            layout_content_ahead = input_str[old_pos : head.position]

        if self.debug:
            content = layout_content_ahead
            if isinstance(layout_content_ahead, str):
                content = content.replace("\n", "\\n")
            h_print("Skipping whitespaces:", f"'{content}'")
            h_print("New position:", pos_to_line_col(input_str, head.position))
        head.layout_content_ahead = layout_content_ahead

    def _next_token(self, head):
        tokens = self._next_tokens(head)
        if not tokens:
            return None
        elif len(tokens) == 1:
            return tokens[0]
        else:
            raise DisambiguationError(Location(head), tokens)

    def _next_tokens(self, head):
        """
        For the current position in the input stream and actions in the current
        state find next tokens. This function must return only tokens that
        are relevant to specified context - ie it mustn't return a token
        if it's not expected by any action in given state.
        """
        state = head.state
        input_str = head.input_str
        position = head.position
        actions = state.actions
        in_len = len(input_str)
        tokens = []

        # add special STOP token if they are applicable
        if STOP in actions and (
            not self.consume_input or (self.consume_input and position == in_len)
        ):
            tokens.append(STOP_token)

        if position < in_len:
            # Get tokens by trying recognizers - but only if we are not at
            # the end, because token cannot be empty
            if self.custom_token_recognition:

                def get_tokens():
                    return self._token_recognition(head)

                custom_tokens = self.custom_token_recognition(
                    head,
                    get_tokens,
                )
                if custom_tokens is not None:
                    tokens.extend(custom_tokens)
            else:
                tokens.extend(self._token_recognition(head))

        # do lexical disambiguation if it is enabled
        if self.lexical_disambiguation:
            tokens = self._lexical_disambiguation(tokens)

        return tokens

    def _token_recognition(self, head):
        input_str = head.input_str
        actions = head.state.actions
        position = head.position
        finish_flags = head.state.finish_flags

        tokens = []
        last_prior = -1
        for idx, symbol in enumerate(actions):
            if symbol.prior < last_prior and tokens:
                break
            last_prior = symbol.prior
            try:
                tok = symbol.recognizer(input_str, position)
            except TypeError:
                try:
                    tok = symbol.recognizer(head, input_str, position)
                except TypeError as e:
                    raise TypeError(f'In recognizer for "{symbol}": {e}') from e

            additional_data = ()
            if type(tok) is tuple:
                tok, *additional_data = tok
            if tok:
                tokens.append(Token(symbol, tok, position, additional_data))
                if finish_flags[idx]:
                    break
        return tokens

    def _get_all_possible_tokens_ahead(self, context):
        """
        Check what is ahead no matter the current state.
        Just check with all recognizers available.
        """
        tokens = []
        if context.position < len(context.input_str):
            for terminal in self.grammar.terminals.values():
                if (
                    terminal.user_meta is not None
                    and terminal.user_meta.get("unexpected", True) is False
                ):
                    continue
                if terminal.name == "KEYWORD":
                    continue
                try:
                    tok = terminal.recognizer(context.input_str, context.position)
                except TypeError:
                    tok = terminal.recognizer(
                        context, context.input_str, context.position
                    )
                additional_data = ()
                if type(tok) is tuple:
                    tok, *additional_data = tok
                if tok:
                    tokens.append(Token(terminal, tok, context.position, additional_data))
        return tokens

    def _init_dynamic_disambiguation(self, context):
        if self.dynamic_filter:
            if self.debug:
                prints("\tInitializing dynamic disambiguation.")
            self.dynamic_filter(context, None, None, None, None, None)

    def _dynamic_disambiguation(self, context, actions):
        dyn_actions = []
        for a in actions:
            if a.action is SHIFT:
                if self._call_dynamic_filter(context, context.state, a.state, SHIFT):
                    dyn_actions.append(a)
            elif a.action is REDUCE:
                r_len = len(a.prod.rhs)
                results = [x.results for x in self.parse_stack[-r_len:]] if r_len else []
                context.production = a.prod
                if self._call_dynamic_filter(
                    context, context.state, a.state, REDUCE, a.prod, results
                ):
                    dyn_actions.append(a)
            else:
                dyn_actions.append(a)
        return dyn_actions

    def _call_dynamic_filter(
        self,
        context,
        from_state,
        to_state,
        action,
        production=None,
        subresults=None,
    ):
        token = context.token
        if context.token is None:
            context.token = context.token_ahead
        if (action is SHIFT and not to_state.symbol.dynamic) or (
            action is REDUCE and not production.dynamic
        ):
            return True

        if self.debug:
            if action is SHIFT:
                act_str = "SHIFT"
                token = context.token
                production = ""
                subresults = ""
            else:
                act_str = "REDUCE"
                token = context.token_ahead
                production = f", prod={context.production}"
                subresults = f", subresults={subresults}"

            h_print(
                "Calling filter for action:",
                f" {act_str}, token={token}{production}{subresults}",
                level=2,
            )

        accepted = self.dynamic_filter(
            context, from_state, to_state, action, production, subresults
        )
        if self.debug:
            if accepted:
                a_print("Action accepted.", level=2)
            else:
                a_print("Action rejected.", level=2)

        return accepted

    def _call_shift_action(self, context):
        """
        Calls registered shift action for the given grammar symbol.
        """
        debug = self.debug
        token = context.token
        sem_action = token.symbol.action

        if self.build_tree:
            # call action for building tree node if tree building is enabled
            if debug:
                h_print("Building terminal node", f"'{token.symbol.name}'.", level=2)

            # If both build_tree and call_actions_during_build are set to
            # True, semantic actions will be call but their result will be
            # discarded. For more info check following issue:
            # https://github.com/igordejanovic/parglare/issues/44
            if self.call_actions_during_tree_build and sem_action:
                sem_action(context, token.value, *token.additional_data)

            return NodeTerm(context, token)

        if sem_action:
            result = sem_action(context, token.value, *token.additional_data)

        else:
            if debug:
                h_print(
                    "No action defined",
                    f"for '{token.symbol.name}'. Result is matched string.",
                    level=1,
                )
            result = token.value

        if debug:
            h_print(
                "Action result = ",
                f"type:{type(result)} value:{repr(result)}",
                level=1,
            )

        return result

    def _call_reduce_action(self, context, subresults):
        """
        Calls registered reduce action for the given grammar symbol.
        """
        debug = self.debug
        result = None
        bt_result = None
        production = context.production

        if self.build_tree:
            # call action for building tree node if enabled.
            if debug:
                h_print(
                    "Building non-terminal node",
                    f"'{production.symbol.name}'.",
                    level=2,
                )

            bt_result = NodeNonTerm(context, children=subresults, production=production)
            context.node = bt_result
            if not self.call_actions_during_tree_build:
                return bt_result

        sem_action = production.symbol.action
        if sem_action:
            assignments = production.assignments
            if assignments:
                assgn_results = {}
                for a in assignments.values():
                    if a.op == "=":
                        assgn_results[a.name] = subresults[a.index]
                    else:
                        assgn_results[a.name] = bool(subresults[a.index])

            if isinstance(sem_action, list):
                if assignments:
                    result = sem_action[production.prod_symbol_id](
                        context, subresults, **assgn_results
                    )
                else:
                    result = sem_action[production.prod_symbol_id](context, subresults)
            else:
                if assignments:
                    result = sem_action(context, subresults, **assgn_results)
                else:
                    result = sem_action(context, subresults)

        else:
            if debug:
                h_print(
                    "No action defined",
                    f" for '{production.symbol.name}'.",
                    level=1,
                )
            if len(subresults) == 1:
                if debug:
                    h_print("Unpacking a single subresult.", level=1)
                result = subresults[0]
            else:
                if debug:
                    h_print("Result is a list of subresults.", level=1)
                result = subresults

        if debug:
            h_print(
                "Action result =",
                f"type:{type(result)} value:{repr(result)}",
                level=1,
            )

        # If build_tree is set to True, discard the result of the semantic
        # action, and return the result of treebuild_reduce_action.
        return bt_result if bt_result is not None else result

    def _lexical_disambiguation(self, tokens):
        """
        For the given list of matched tokens apply disambiguation strategy.

        Args:
        tokens (list of Token)
        """

        if self.debug:
            h_print(
                "Lexical disambiguation.",
                f" Tokens: {[x for x in tokens]}",
                level=1,
            )

        if len(tokens) <= 1:
            return tokens

        # Longest-match strategy.
        max_len = max(len(x.value) for x in tokens)
        tokens = [x for x in tokens if len(x.value) == max_len]
        if self.debug:
            h_print(
                "Disambiguation by longest-match strategy.",
                f"Tokens: {[x for x in tokens]}",
                level=1,
            )
        if len(tokens) == 1:
            return tokens

        # try to find preferred token.
        pref_tokens = [x for x in tokens if x.symbol.prefer]
        if pref_tokens:
            if self.debug:
                h_print(f"Preferring tokens {pref_tokens}.", level=1)
            return pref_tokens

        return tokens

    def _do_recovery(self):
        debug = self.debug
        if debug:
            a_print("**Recovery initiated.**")

        head = self.parse_stack[-1]
        error = self.errors[-1]

        if isinstance(self.error_recovery, bool):
            # Default recovery
            if debug:
                prints("\tDoing default error recovery.")
            successful = self.default_error_recovery(head)
        else:
            # Custom recovery provided during parser construction
            if debug:
                prints("\tDoing custom error recovery.")
            successful = self.error_recovery(head, error, self.default_error_recovery)

        # The recovery may either decide to skip erroneous part of
        # the input and resume at the place that can continue or it
        # might decide to fill in missing tokens.
        if successful:
            if debug:
                h_print("Recovery ")
            error.location.end_position = head.position
            if debug:
                a_print(
                    "New position is ",
                    pos_to_line_col(head.input_str, head.position),
                    level=1,
                )
                a_print("New lookahead token is ", head.token_ahead, level=1)
        return successful

    def default_error_recovery(self, head):
        """
        The default recovery strategy is to search from the current location
        for expected terminals.

        Returns True if successful, False otherwise.
        """

        while head.position < len(head.input_str):
            head.position += 1
            token = self._next_token(head)
            if token:
                head.token_ahead = token
                return True
        return False

    def _create_error(
        self,
        input,
        context,
        symbols_expected,
        tokens_ahead=None,
        symbols_before=None,
        last_heads=None,
    ):
        hint = None
        if (
            not self.in_layout
            and not hasattr(self, "_in_error_hints")
            and self.error_hints is not None
        ):
            # Check if a specific version with tokens ahead is available
            hint = self.error_hints.get(
                hint_key(context.state.state_id, tokens_ahead), None
            )
            if hint is None:
                # Check if more generic version without tokens ahead is availabe
                hint = self.error_hints.get(hint_key(context.state.state_id, None), None)

        error = SyntaxError(
            Location(context=ErrorContext(context)),
            input,
            symbols_expected,
            tokens_ahead,
            symbols_before=symbols_before,
            last_heads=last_heads,
            grammar=self.grammar,
            hint=hint,
        )

        if self.debug:
            a_print("Error: ", error, level=1)

        return error


class LRStackNode:
    """
    An element of the LR parsing stack. Also the parsing context.
    """

    __slots__ = [
        "file_name",
        "input_str",
        "state",
        "frontier",
        "position",
        "extra",
        "results",
        "start_position",
        "end_position",
        "token_ahead",
        "token",
        "production",
        "layout_content",
        "layout_content_ahead",
        "node",
    ]

    def __init__(
        self,
        file_name,
        input_str,
        state,
        frontier,
        position,
        extra,
        results=None,
        start_position=None,
        end_position=None,
        token=None,
        token_ahead=None,
        production=None,
        layout_content="",
        layout_content_ahead="",
    ):
        self.file_name = file_name
        self.input_str = input_str
        self.state = state
        self.frontier = frontier
        self.position = position
        self.extra = extra

        self.results = results

        self.start_position = start_position
        self.end_position = end_position

        self.token_ahead = token_ahead

        # For shift nodes
        self.token = token

        # For reduced nodes
        self.production = production

        self.layout_content = layout_content
        self.layout_content_ahead = layout_content_ahead

        # Parse tree node used if parse tree is produced
        self.node = None

    def __repr__(self):
        return "<LRStackNode({}:{}{})>".format(
            self.state.state_id,
            self.state.symbol,
            f", pos=({self.start_position}-{self.end_position})"
            if self.start_position is not None
            else "",
        )

    @property
    def symbol(self):
        return self.state.symbol


class Token:
    """
    Token or lexeme matched from the input.
    """

    __slots__ = ["symbol", "value", "additional_data", "length", "position"]

    def __init__(self, symbol, value, position, additional_data=(), length=None):
        self.symbol = symbol
        self.value = value
        self.additional_data = additional_data
        self.length = length if length is not None else len(value)
        self.position = position

    def __repr__(self):
        if str(self.symbol) != self.value:
            return f"{str(self.symbol)}({str(self.value)})"
        else:
            return self.value

    def __len__(self):
        return self.length

    @property
    def end_position(self):
        return self.position + self.length

    def __bool__(self):
        return True


STOP_token = Token(STOP, "", None)
