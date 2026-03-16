from functools import reduce
from itertools import takewhile
from typing import Dict

from parglare import Parser
from parglare import termui as t
from parglare.common import dot_escape, position_context
from parglare.common import replace_newlines as _
from parglare.parser import REDUCE, SHIFT, Token, pos_to_line_col
from parglare.tables import LRState
from parglare.termui import a_print, h_print, prints
from parglare.trees import (
    Forest,
    NodeNonTerm,
    NodeTerm,
    to_dot,
    to_str,
    visitor,
)


def no_colors(f):
    """
    Decorator for trace methods to prevent ANSI COLOR codes appearing in
    the trace dot output.
    """

    def nc_f(*args, **kwargs):
        self = args[0]
        t.colors = False
        r = f(*args, **kwargs)
        t.colors = self.debug_colors
        return r

    return nc_f


class GLRParser(Parser):
    """
    A Tomita-style GLR parser.
    """

    def __init__(self, *args, **kwargs):
        table = kwargs.get("table")
        lexical_disambiguation = kwargs.get("lexical_disambiguation")
        if table is None:
            # The default for GLR is not to use any strategy preferring shifts
            # over reduce thus investigating all possibilities.
            # These settings are only applicable if parse table is not computed
            # yet. If it is, then leave None values to avoid
            # "parameter overriden" warnings.
            prefer_shifts = kwargs.get("prefer_shifts")
            prefer_shifts_over_empty = kwargs.get("prefer_shifts_over_empty")

            prefer_shifts = False if prefer_shifts is None else prefer_shifts
            prefer_shifts_over_empty = (
                False if prefer_shifts_over_empty is None else prefer_shifts_over_empty
            )
            if lexical_disambiguation is None:
                lexical_disambiguation = False

            kwargs["prefer_shifts"] = prefer_shifts
            kwargs["prefer_shifts_over_empty"] = prefer_shifts_over_empty

        kwargs["lexical_disambiguation"] = lexical_disambiguation
        self.debug_trace_frontiers = kwargs.pop("debug_trace_frontiers", False)

        super().__init__(*args, **kwargs)

    def _check_parser(self):
        """
        Conflicts in table are allowed with GLR.
        """
        pass

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
            a_print("*** PARSING STARTED\n")
            self.debug_frontier = 0
            self.debug_step = 0
            if self.debug_trace:
                self._dot_trace = ""
                self._dot_trace_ranks = ""
                self._trace_frontier_heads = []
                self._trace_frontier_steps = []

        self.file_name = file_name
        extra = {} if extra is None else extra

        # Error reporting and recovery
        self.errors = []
        self._in_error_reporting = False
        self._expected = set()
        self._tokens_ahead = []
        self._last_shifted_heads = []
        self._for_shifter = []

        # We start with a single parser head in state 0.
        start_head = GSSNode(
            file_name,
            input_str,
            self.table.states[0],
            position,
            0,
            extra,
            ambiguity=1,
            debug=self.debug,
        )
        self._init_dynamic_disambiguation(start_head)

        # Accepted (finished) heads
        self._accepted_heads = []

        if self.debug and self.debug_trace:
            self._trace_head(start_head)

        # The main loop
        self._active_heads = {0: start_head}
        while self._active_heads or self._in_error_reporting:
            if self.debug:
                a_print(
                    f"** REDUCING - frontier {self.debug_frontier}",
                    new_line=True,
                )
                self._debug__active_heads(self._active_heads.values())
            if not self._in_error_reporting:
                self._last_shifted_heads = list(self._active_heads.values())
                self._find_lookaheads()
            while self._active_heads_per_symbol:
                _, self._active_heads = self._active_heads_per_symbol.popitem()
                self._for_actor = list(self._active_heads.values())
                # Used to optimize revisiting only heads that will
                # traverse newly added paths.
                # state_id -> set(state_id)
                self._states_traversed = {}
                while self._for_actor:
                    head = self._for_actor.pop()
                    self._actor(head)
            if self._in_error_reporting:
                self._finish_error_reporting(input_str)
                if self.error_recovery:
                    self._do_error_recovery()
                    self._for_shifter = []
                    continue
                break
            self._do_shifts()

            if not self._active_heads and not self._accepted_heads:
                if self.debug:
                    a_print("*** ENTERING ERROR REPORTING MODE.", new_line=True)
                self._enter_error_reporting()

        if self.debug and self.debug_trace:
            self._trace_finish()
            self._export__dot_trace()

        if self._accepted_heads:
            # Return results
            forest = Forest(self)
            if self.debug:
                a_print(f"*** {forest.solutions} successful parse(s).")

            if self.clear_transient:
                self._remove_transient_state()
            return forest
        else:
            # Report error
            if self.clear_transient:
                self._remove_transient_state()
            error = self.errors[-1]
            del self.errors
            raise error

    def _find_lookaheads(self):
        debug = self.debug
        # Make sub-frontiers per symbol of the token ahead thus handling lexical
        # ambiguity by the same GLR mechanics
        self._active_heads_per_symbol = {}
        while self._active_heads:
            _, head = self._active_heads.popitem()
            if head.token_ahead is not None:
                # May happen after error recovery
                self._active_heads_per_symbol.setdefault(head.token_ahead.symbol, {})[
                    head.state.state_id
                ] = head
                continue
            if debug:
                h_print(f"Finding lookaheads for head {head}", new_line=True)
            self._skipws(head, head.input_str)

            tokens = self._next_tokens(head)

            if debug:
                head._debug_context(
                    expected_symbols=head.state.actions.keys(),
                )

            if tokens:
                while tokens:
                    token = tokens.pop()
                    head = head.for_token(token)
                    self._active_heads_per_symbol.setdefault(token.symbol, {})[
                        head.state.state_id
                    ] = head
            else:
                # Can't find lookahead. This head can't progress
                if debug:
                    h_print("No lookaheads found. Killing head.")

    def _actor(self, head):
        debug = self.debug
        for action in head.state.actions.get(head.token_ahead.symbol, []):
            if action.action == SHIFT:
                self._for_shifter.append((head, action.state))
            elif action.action == REDUCE:
                self._do_reductions(head, action.prod)
            else:
                if not self._in_error_reporting:
                    self._accepted_heads.append(head)
                    if debug:
                        a_print("**ACCEPTING HEAD: ", str(head))
                        if self.debug_trace:
                            self._trace_step_finish(head)

    def _do_reductions(self, head, production, update_parent=None):
        """
        Reduce the given head by the given production. If update_parent is given
        this is update/limited reduction so just traverse the given parent instead of
        all parents of the parent's head.
        """
        debug = self.debug
        if debug:
            h_print(f"\tFinding reduction paths for head: {head}")
            h_print(f"\tand production: {production}")
            if update_parent:
                h_print("\tLimited/update reduction due to new path addition.")

        states_traversed = self._states_traversed
        prod_len = len(production.rhs)
        if prod_len == 0:
            # Special case, empty reduction
            self._reduce(
                head,
                head,
                production,
                NodeNonTerm(None, [], production=production),
                head.position,
                head.position,
            )
        else:
            # Find roots of possible reductions by going backwards for
            # prod_len steps following all possible paths. Collect
            # subresults along the way to be used with semantic actions
            to_process = [(head, [], prod_len, None, update_parent is None)]
            if debug:
                h_print(f"Calculate reduction paths of length {prod_len}:", level=1)
                h_print(f"start node= {head}", level=2)
            while to_process:
                (node, results, length, last_parent, traversed) = to_process.pop()
                length = length - 1
                if debug:
                    h_print(f"node = {node}", level=2, new_line=True)
                    h_print(
                        "backpath length = {}{}".format(
                            prod_len - length, " - ROOT" if not length else ""
                        ),
                        level=2,
                    )

                if node.frontier == head.frontier:
                    # Cache traversed states for revisit optimization
                    states_traversed.setdefault(node.state.state_id, set()).add(
                        head.state.state_id
                    )

                for parent in (
                    [update_parent]
                    if update_parent and update_parent.head == node
                    else list(node.parents.values())
                ):
                    if debug:
                        h_print("", str(parent.head), level=3)

                    new_results = [parent] + results

                    if last_parent is None:
                        last_parent = parent

                    traversed = traversed or (
                        update_parent and update_parent.head == node
                    )

                    if length:
                        to_process.append(
                            (
                                parent.root,
                                new_results,
                                length,
                                last_parent,
                                traversed,
                            )
                        )
                    elif traversed:
                        self._reduce(
                            head,
                            parent.root,
                            production,
                            NodeNonTerm(None, new_results, production=production),
                            parent.start_position,
                            last_parent.end_position,
                        )

    def _reduce(
        self,
        head,
        root_head,
        production,
        node_nonterm,
        start_position,
        end_position,
    ):
        """
        Executes the given reduction.
        """
        if start_position is None:
            start_position = end_position = root_head.position
        state = root_head.state.gotos[production.symbol]

        if self.debug:
            self.debug_step += 1
            a_print(
                f"{self._debug_step_str()} REDUCING head ",
                str(head),
                new_line=True,
            )
            a_print("by prod ", production, level=1)
            a_print(f"to state {state.state_id}:{state.symbol}", level=1)
            a_print("root is ", root_head, level=1)
            a_print(f"Position span: {start_position} - {end_position}", level=1)

        new_head = GSSNode(
            head.file_name,
            head.input_str,
            state,
            head.position,
            head.frontier,
            head.extra,
            token_ahead=head.token_ahead,
            layout_content=root_head.layout_content,
            layout_content_ahead=head.layout_content_ahead,
            debug=self.debug,
        )
        parent = Parent(
            new_head,
            root_head,
            start_position,
            end_position,
            production=production,
            possibilities=[node_nonterm],
        )

        if self.dynamic_filter and not self._call_dynamic_filter(
            parent, head.state, state, REDUCE, production, list(node_nonterm)
        ):
            # Action rejected by dynamic filter
            return

        active_head = self._active_heads.get(state.state_id, None)
        if active_head:
            created = active_head.create_link(parent)
            if self.debug and self.debug_trace:
                self._trace_step(head, parent)

            # Calculate heads to revisit with the new path. Only those heads that
            # are already processed (not in _for_actor) and are traversing this
            # new head state on the current frontier should be considered.
            if created and state.state_id in self._states_traversed:
                to_revisit = self._states_traversed[state.state_id].intersection(
                    self._active_heads.keys()
                ) - set(h.state.state_id for h in self._for_actor)
                if to_revisit:
                    if self.debug:
                        h_print(
                            "Revisiting reductions for processed "
                            f"active heads in states {to_revisit}",
                            level=1,
                        )
                    for r_head_state in to_revisit:
                        r_head = self._active_heads[r_head_state]
                        for action in [
                            a
                            for a in r_head.state.actions.get(head.token_ahead.symbol, [])
                            if a.action == REDUCE
                        ]:
                            self._do_reductions(r_head, action.prod, parent)
        else:
            # No cycles. Do the reduction.
            new_head.create_link(parent)
            if self.debug and self.debug_trace:
                self._trace_step(head, parent)
            self._for_actor.append(new_head)
            self._active_heads[new_head.state.state_id] = new_head

            if self.debug:
                a_print("New head: ", new_head, level=1, new_line=True)
                if self.debug_trace:
                    self._trace_head(new_head)

    def _do_shifts(self):
        debug = self.debug
        if debug:
            self.debug_frontier += 1
            self.debug_step = 0
            a_print(f"** SHIFTING - frontier {self.debug_frontier}", new_line=True)
            self._debug__active_heads(self._active_heads.values())
            if self.debug_trace:
                self._trace_frontier()

        self._active_heads = {}

        # Due to lexical ambiguity heads might be at different positions.
        # We must order heads by position before shift to process them in
        # the right order. Only shift heads with minimal position during
        # a single frontier processing.
        self._for_shifter.sort(key=lambda x: x[0].token_ahead.end_position, reverse=True)
        end_position = None
        while self._for_shifter:
            head, to_state = self._for_shifter.pop()
            if end_position is not None and head.token_ahead.end_position > end_position:
                self._for_shifter.append((head, to_state))
                break
            end_position = head.token_ahead.end_position
            if debug:
                self.debug_step += 1
                a_print(
                    f"{self._debug_step_str()}. SHIFTING head: ",
                    head,
                    new_line=True,
                )
            shifted_head = self._active_heads.get(to_state.state_id, None)
            if shifted_head:
                # If this token has already been shifted connect shifted head to
                # this head.
                parent = next(iter(shifted_head.parents.values())).clone_with_root(head)
                if self.dynamic_filter and not self._call_dynamic_filter(
                    parent, head.state, to_state, SHIFT
                ):
                    continue
            else:
                # We need to create new shifted head
                if debug:
                    head._debug_context(
                        expected_symbols=None,
                    )

                end_position = head.position + len(head.token_ahead)
                shifted_head = GSSNode(
                    head.file_name,
                    head.input_str,
                    to_state,
                    end_position,
                    head.frontier + 1,
                    head.extra,
                    ambiguity=1,
                    layout_content=head.layout_content_ahead,
                    debug=self.debug,
                )
                parent = Parent(
                    shifted_head,
                    head,
                    head.position,
                    end_position,
                    token=head.token_ahead,
                )

                if self.dynamic_filter and not self._call_dynamic_filter(
                    parent, head.state, to_state, SHIFT
                ):
                    continue

                if self.debug:
                    a_print("New shifted head ", shifted_head, level=1)
                    if self.debug_trace:
                        self._trace_head(shifted_head)

                self._active_heads[to_state.state_id] = shifted_head

            shifted_head.create_link(parent)
            if self.debug and self.debug_trace:
                self._trace_step(head, parent)

    def _enter_error_reporting(self):
        """
        To correctly report what is found ahead and what is expected we shall:

            - execute all grammar recognizers at the farther position reached
              in the input by the active heads.  This will be part of the error
              report (what is found ahead if anything can be recognized).

            - for all last reducing heads, simulate parsing for each of
              possible lookaheads in the head's state until either SHIFT or
              ACCEPT is successfuly executed.  Collect each possible lookahead
              where this is achieved for reporting.  This will be another part
              of the error report (what is expected).

        """

        self._in_error_reporting = True

        # Start with the last shifted heads sorted by position.
        self._last_shifted_heads.sort(key=lambda h: h.position, reverse=True)
        last_head = self._last_shifted_heads[0]
        farthest_heads = takewhile(
            lambda h: h.position == last_head.position, self._last_shifted_heads
        )

        self._tokens_ahead = self._get_all_possible_tokens_ahead(last_head)

        self._active_heads_per_symbol = {}
        for head in farthest_heads:
            for possible_lookahead in head.state.actions:
                h = head.for_token(Token(possible_lookahead, [], position=head.position))
                self._active_heads_per_symbol.setdefault(possible_lookahead, {})[
                    h.state.state_id
                ] = h

    def _finish_error_reporting(self, input_str):
        # Expected symbols are only those that can cause active heads
        # to shift.
        self._expected = set(h.token_ahead.symbol for h, _ in self._for_shifter)
        if self.debug:
            a_print("*** LEAVING ERROR REPORTING MODE.", new_line=True)
            h_print(
                "Tokens expected:",
                ", ".join([t.name for t in self._expected]),
                level=1,
            )
            h_print("Tokens found:", self._tokens_ahead, level=1)

        # After leaving error reporting mode, register error and try
        # recovery if enabled
        context = self._last_shifted_heads[0]
        self.errors.append(
            self._create_error(
                input_str,
                context,
                self._expected,
                tokens_ahead=self._tokens_ahead,
                symbols_before=list({h.state.symbol for h in self._last_shifted_heads}),
                last_heads=self._last_shifted_heads,
            )
        )

        self.for_shifter = []
        self._in_error_reporting = False

    def _do_error_recovery(self):
        """
        If recovery is enabled, does error recovery for the heads in
        _last_shifted_heads.

        """
        if self.debug:
            a_print("*** STARTING ERROR RECOVERY.", new_line=True)
        error = self.errors[-1]
        debug = self.debug
        self._active_heads = {}
        for head in self._last_shifted_heads:
            if debug:
                input_str = head.input_str
                symbols = head.state.actions.keys()
                h_print(
                    f"Recovery initiated for head {head}.",
                    level=1,
                    new_line=True,
                )
                h_print("Symbols expected: ", [s.name for s in symbols], level=1)
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

            if successful:
                error.location.end_position = head.position
                if debug:
                    a_print(
                        "New position is ",
                        pos_to_line_col(input_str, head.position),
                        level=1,
                    )
                    a_print("New lookahead token is ", head.token_ahead, level=1)
                self._active_heads[head.state.state_id] = head
                if self.debug:
                    a_print(
                        "*** ERROR RECOVERY SUCCEEDED. CONTINUING.",
                        new_line=True,
                    )
            else:
                if debug:
                    a_print("Killing head: ", head, level=1)
                    if self.debug_trace:
                        self._trace_step_kill(head)

    def _remove_transient_state(self):
        """
        Delete references to transient parser objects to lower memory
        consumption.
        """
        del self._for_actor
        del self._for_shifter
        del self._last_shifted_heads
        del self._accepted_heads
        del self._active_heads
        del self._states_traversed
        del self._expected
        del self._tokens_ahead
        if self.debug_trace:
            del self._dot_trace
            del self._dot_trace_ranks
            del self._trace_frontier_heads
            del self._trace_frontier_steps

    def _debug_step_str(self):
        return f"{self.debug_frontier}.{self.debug_step}"

    def _debug__active_heads(self, heads):
        if not heads:
            h_print("No active heads.")
        else:
            h_print("Active heads = ", len(heads))
            for head in heads:
                prints(f"\t{head}")
            h_print(f"Number of trees = {sum([len(h.parents) for h in heads])}")

    @no_colors
    def _trace_head(self, head):
        self._trace_frontier_heads.append(head)

    @no_colors
    def _trace_step(self, from_head, parent):
        self._trace_frontier_steps.append((from_head, parent))

    @no_colors
    def _trace_step_finish(self, from_head):
        self._dot_trace += f"\n{from_head.key} -> ACCEPT;\n"

    @no_colors
    def _trace_frontier(self):
        parents_processed = set()

        for head in self._trace_frontier_heads:
            self._dot_trace += (
                f'{head.key} [label="{head.frontier}. '
                f'{head.state.state_id}:{dot_escape(head.state.symbol.name)}"];\n'
            )

        for step_no, step in enumerate(self._trace_frontier_steps):
            step_no += 1
            from_head, parent = step
            if parent not in parents_processed:
                self._dot_trace += (
                    f"{parent.head.key} -> {parent.root.key} "
                    f'[label="{parent.ambiguity}"];\n'
                )
                parents_processed.add(parent)
            if parent.production:
                # Reduce step
                label = f"R:{dot_escape(parent.production)}"
            else:
                # Shift step
                label = (
                    f"S:{dot_escape(parent.token.symbol.name)}"
                    f"({dot_escape(parent.token.value)})"
                )
            self._dot_trace += (
                f"{from_head.key} -> {parent.head.key} "
                f'[label="{parent.head.frontier}.{step_no} '
                f'{label}" {TRACE_DOT_STEP_STYLE}];\n'
            )

        self._dot_trace_ranks += "{{rank=same; {}; {}}}\n".format(
            self.debug_frontier - 1,
            "".join([f" {x.key};" for x in self._trace_frontier_heads]),
        )
        self._trace_frontier_heads = []
        self._trace_frontier_steps = []

    @no_colors
    def _trace_step_kill(self, from_head):
        self._dot_trace += (
            f'{from_head.key}_killed [shape="diamond" fillcolor="red" label="killed"];\n'
        )
        self._dot_trace += (
            f"{from_head.key} -> {from_head.key}_killed "
            f'[label="{self._debug_step_str()}." {TRACE_DOT_STEP_STYLE}];\n'
        )

    @no_colors
    def _trace_step_drop(self, from_head, to_head):
        self._dot_trace += (
            f"{from_head.key} -> {to_head.key} "
            f'[label="drop empty" {TRACE_DOT_DROP_STYLE}];\n'
        )

    @no_colors
    def _trace_finish(self):
        if self.debug_trace and self.debug_trace_frontiers:
            self._dot_trace += '\nnode [shape=none, style=""]\n'
            self._dot_trace += self._dot_trace_ranks
            self._dot_trace += "->".join(str(i) for i in range(self.debug_frontier))
            self._dot_trace += "[arrowhead=none];\n"

    def _export__dot_trace(self):
        file_name = (
            f"{self.file_name}_trace.dot" if self.file_name else "parglare_trace.dot"
        )
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(DOT_HEADER)
            f.write(self._dot_trace)
            f.write("}\n")

        prints(f"Generated file {file_name}.")
        prints("You can use dot viewer or generate pdf with the following command:")
        h_print(f"dot -Tpdf -O {file_name}")


class Parent:
    """
    Represent a backlink in the GSS stack with all possibilities in
    case of ambiguity.
    """

    __slots__ = [
        "head",
        "root",
        "start_position",
        "end_position",
        "possibilities",
        "_solutions",
        "_ambiguities",
        "production",
        "token",
    ]

    def __init__(
        self,
        head,
        root,
        start_position,
        end_position=None,
        possibilities=None,
        production=None,
        token=None,
    ):
        self.root = root
        self.head = head
        self.start_position = start_position
        self.end_position = end_position if end_position is not None else start_position

        self.production = production
        self.token = token
        self._solutions = None
        self._ambiguities = None

        # A list of NodeNonTerm or NodeTerm objects representing alternative
        # interpretations of what is seen between root and head GSS nodes.
        self.possibilities = []
        if possibilities:
            self.possibilities = possibilities
            for p in possibilities:
                p.context = self
        elif token:
            self.possibilities.append(NodeTerm(self, token))

    def merge(self, other):
        self.possibilities.extend(other.possibilities)
        self._solutions = None

    def clone_with_root(self, root):
        return Parent(
            self.head,
            root,
            self.start_position,
            self.end_position,
            list(self.possibilities),
            token=self.token,
        )

    @property
    def ambiguity(self):
        return len(self.possibilities)

    @property
    def ambiguities(self):
        """
        Total ambiguities in the sub-tree.
        Keep cache of visited nodes to prevent double counting.
        """
        if self._ambiguities is None:
            visited = set()

            def iterator(node):
                def iter_non_visited(n, collection):
                    for i in collection:
                        if id(i) not in visited:
                            visited.add(id(i))
                            yield i

                if isinstance(node, Parent):
                    return iter_non_visited(node, node.possibilities)
                elif isinstance(node, NodeNonTerm):
                    return iter_non_visited(node, node.children)
                else:
                    return iter([])

            def calculate(node, subresults, _):
                amb = 0
                if isinstance(node, Parent) and len(node.possibilities) > 1:
                    amb = 1
                return sum(subresults) + amb

            self._ambiguities = visitor(
                self, iterator, calculate, memoize=True, check_cycle=True
            )
            del visited

        return self._ambiguities

    @property
    def solutions(self):
        "Total number of trees/solutions."
        if self._solutions is None:

            def iterator(node):
                if isinstance(node, Parent):
                    return iter(node.possibilities)
                elif isinstance(node, NodeNonTerm):
                    return iter(node.children)
                else:
                    return iter([])

            def calculate(node, subresults, _):
                if isinstance(node, Parent):
                    return sum(subresults)
                else:
                    return reduce(lambda x, y: x * y, subresults, 1)

            self._solutions = visitor(
                self, iterator, calculate, memoize=True, check_cycle=True
            )

        return self._solutions

    @property
    def id(self):
        return f"{self.head.id}->{self.root.id}"

    @property
    def layout_content(self):
        return self.head.layout_content

    @property
    def layout_content_ahead(self):
        return self.head.layout_content_ahead

    @property
    def token_ahead(self):
        return self.head.token_ahead

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return (
            f"{self.root.id}({self.root.symbol})<-{self.head.id}"
            f"({self.head.symbol}) [{self.ambiguity}]"
        )

    def __repr__(self):
        return str(self)

    def __getattr__(self, attr):
        return getattr(self.head, attr)

    def __iter__(self):
        return iter(self.possibilities)

    def to_str(self):
        if len(self.possibilities) == 1:
            return to_str(self.possibilities[0])
        else:
            return to_str(self)

    def to_dot(self, positions=True):
        if len(self.possibilities) == 1:
            return to_dot(self.possibilities[0], positions)
        else:
            return to_dot(self, positions)


class GSSNode:
    """
    Graph Structured Stack node.

    A node in the Graph Structured Stack (GSS) used by the GLR parser to
    handle non-determinism. Multiple parse paths can share common prefixes
    through this structure, enabling efficient handling of ambiguous grammars.

    Attributes:
        file_name (str): Name of the file being parsed, used for error reporting.
        input_str (str): The input string being parsed.
        state (LRState): The LR automaton state this node represents.
        position (int): Current position in the input string.
        frontier (int): The frontier (shift level) when this node was created.
        extra: User-defined object for maintaining custom parsing state.
        parents (dict): Mapping of root node IDs to Parent objects, representing
            multiple paths the parser took to reach this state.
        id (str): Unique node identifier, created from frontier and state ID.
        token_ahead (Token): The lookahead token for this head, if determined.
        layout_content (str): Layout (whitespace/comments) content before this node.
        layout_content_ahead (str): Layout content after current position.
    """

    __slots__ = [
        "file_name",
        "input_str",
        "id",
        "state",
        "extra",
        "position",
        "frontier",
        "parents",
        "_ambiguity",
        "token_ahead",
        "layout_content",
        "layout_content_ahead",
        "debug",
        "_hash",
    ]

    def __init__(
        self,
        file_name: str,
        input_str,
        state: LRState,
        position: int,
        frontier: int,
        extra,
        ambiguity=None,
        token_ahead=None,
        layout_content="",
        layout_content_ahead="",
        debug=False,
    ):
        self.state = state
        self.position = position
        self.frontier = frontier
        self.input_str = input_str
        self.file_name = file_name
        self.extra = extra
        self.id = f"{frontier}_{state.state_id}"

        self._ambiguity = ambiguity

        self.token_ahead = token_ahead
        self.layout_content = layout_content
        self.layout_content_ahead = layout_content_ahead
        self.debug = debug

        # Parents keyed by root node id
        self.parents: Dict[int, Parent] = {}

    def create_link(self, parent):
        parent.head = self
        existing_parent = self.parents.get(parent.root.id)
        created = False
        if existing_parent:
            existing_parent.merge(parent)
            if self.debug:
                h_print("Extending possibilities \tof head:", self, level=1)
                h_print("  parent head:", parent.root, level=3)
        else:
            self.parents[parent.root.id] = parent
            created = True
            if self.debug:
                h_print("Creating link \tfrom head:", self, level=1)
                h_print("  to head:", parent.root, level=3)

        return created

    @property
    def ambiguity(self):
        return self._ambiguity or sum(p.ambiguity for p in self.parents.values())

    def for_token(self, token):
        """
        Create head for the given token either by returning this head if the
        token is appropriate or making a clone.

        This is used to support lexical ambiguity. Multiple tokens might be
        matched at the same state and position. In this case parser should
        fork and this is done by cloning stack head.
        """
        if self.token_ahead is None:
            self.token_ahead = token
            return self
        elif self.token_ahead == token:
            return self
        else:
            new_head = GSSNode(
                self.file_name,
                self.input_str,
                self.state,
                self.position,
                self.frontier,
                self.extra,
                token_ahead=token,
                layout_content=self.layout_content,
                debug=self.debug,
            )
            new_head.parents = dict(self.parents)
            return new_head

    def __eq__(self, other):
        """
        Stack nodes are equal if they are on the same position in the same
        state for the same lookahead token.
        """
        return self.id == other.id and self.token_ahead == other.token_ahead

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return _(
            "<{}:{}, id={}{}, position={}, ambiguity={}>".format(
                self.state.state_id,
                self.state.symbol,
                self.id,
                f", token ahead={self.token_ahead}"
                if self.token_ahead is not None
                else "",
                self.position,
                self.ambiguity,
            )
        )

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash((self.id, self.token_ahead.symbol))

    @property
    def key(self):
        """Head unique identifier used for dot trace."""
        return f"head_{self.id}"

    @property
    def symbol(self):
        return self.state.symbol

    def _debug_context(
        self,
        expected_symbols=None,
    ):
        h_print("Position:", pos_to_line_col(self.input_str, self.position))
        h_print("Context:", _(position_context(self.input_str, self.position)))
        if self.layout_content:
            h_print("Layout: ", f"'{_(self.layout_content)}'", level=1)
        if expected_symbols:
            h_print("Symbols expected: ", [s.name for s in expected_symbols])
        if self.token_ahead:
            h_print("Token(s) ahead:", _(str(self.token_ahead)))


DOT_HEADER = """
    digraph parglare_trace {
    rankdir=LR
    fontname = "Bitstream Vera Sans"
    fontsize = 8
    node[
        style=filled,
        fillcolor=aliceblue
    ]
    nodesep = 0.3
    edge[dir=black,arrowtail=empty]

"""

TRACE_DOT_STEP_STYLE = 'color="red" style="dashed"'
TRACE_DOT_DROP_STYLE = 'color="orange" style="dotted"'
