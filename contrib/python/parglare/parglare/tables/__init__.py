import contextlib
import logging
import os
from collections import OrderedDict
from itertools import chain

from parglare.closure import LR_1, closure
from parglare.exceptions import GrammarError, RRConflict, SRConflict
from parglare.grammar import (
    ASSOC_LEFT,
    ASSOC_RIGHT,
    AUGSYMBOL,
    DEFAULT_PRIORITY,
    EMPTY,
    STOP,
    Grammar,
    NonTerminal,
    ProductionRHS,
    RegExRecognizer,
    StringRecognizer,
)
from parglare.tables.persist import load_table, save_table
from parglare.termui import a_print, h_print, prints, s_emph, s_header

logger = logging.getLogger(__name__)


SHIFT = 0
REDUCE = 1
ACCEPT = 2

# Tables construction algorithms
SLR = 0
LALR = 1


def create_load_table(
    grammar,
    itemset_type=LR_1,
    start_production=1,
    prefer_shifts=False,
    prefer_shifts_over_empty=True,
    force_create=False,
    force_load=False,
    in_layout=False,
    debug=False,
    **kwargs,
):
    """
    Construct table by loading from file if present and newer than the grammar.
    If table file is older than the grammar or non-existent calculate the table
    and save to file.

    Arguments:
    see create_table

    force_create(bool): If set to True table will be created even if table file
        exists.
    force_load(bool): If set to True table will be loaded if exists even if
        it's not newer than the grammar, i.e. modification time will not be
        checked.

    """

    if in_layout:
        # For layout grammars always calculate table.
        # Those are usually very small grammars so there is no point in
        # using cached tables.
        if debug:
            a_print(
                "** Calculating LR table for the layout parser...",
                new_line=True,
            )
        return create_table(
            grammar,
            itemset_type,
            start_production,
            prefer_shifts,
            prefer_shifts_over_empty,
        )
    else:
        if debug:
            a_print("** Calculating LR table...", new_line=True)

    table_file_name = None
    if grammar.file_path:
        file_basename, _ = os.path.splitext(grammar.file_path)
        table_file_name = f"{file_basename}.pgc"

    create_table_file = True

    if not force_create and not force_load and grammar.file_path:
        file_basename, _ = os.path.splitext(grammar.file_path)
        table_file_name = f"{file_basename}.pgc"

        if os.path.exists(table_file_name):
            create_table_file = False
            table_mtime = os.path.getmtime(table_file_name)
            # Check if older than any of the grammar files
            for g_file_name in grammar.imported_files:
                if os.path.getmtime(g_file_name) > table_mtime:
                    create_table_file = True
                    break

    if (create_table_file or force_create) and not force_load:
        table = create_table(
            grammar,
            itemset_type,
            start_production,
            prefer_shifts,
            prefer_shifts_over_empty,
            debug=debug,
            **kwargs,
        )
        if table_file_name:
            with contextlib.suppress(PermissionError):
                save_table(table_file_name, table)
    else:
        if debug:
            h_print(f"Loading LR table from '{table_file_name}'")
        table = load_table(table_file_name, grammar)

    return table


def create_table(
    grammar,
    itemset_type=LR_1,
    start_production=1,
    prefer_shifts=False,
    prefer_shifts_over_empty=True,
    debug=False,
    **kwargs,
):
    """
    Arguments:
    grammar (Grammar):
    itemset_type(int) - SRL=0 LR_1=1. By default LR_1.
    start_production(int) - The production which defines start state.
        By default 1 - first production from the grammar.
    prefer_shifts(bool) - Conflict resolution strategy which favours SHIFT over
        REDUCE (gready). By default False.
    prefer_shifts_over_empty(bool) - Conflict resolution strategy which favours
        SHIFT over REDUCE of EMPTY. By default False. If prefer_shifts is
        `True` this param is ignored.
    """

    first_sets = first(grammar)

    # Check for states with GOTO links but without SHIFT links.
    # This is invalid as the GOTO link will never be traversed.
    for nt, firsts in first_sets.items():
        if nt.name != "S'" and not firsts:
            raise GrammarError(
                location=nt.location,
                message=f'First set empty for grammar symbol "{nt}". '
                "An infinite recursion on the "
                "grammar symbol.",
            )

    follow_sets = follow(grammar, first_sets)

    _old_start_production_rhs = grammar.productions[0].rhs
    start_prod_symbol = grammar.productions[start_production].symbol
    grammar.productions[0].rhs = ProductionRHS([start_prod_symbol, STOP])

    # Create a state for the first production (augmented)
    s = LRState(grammar, 0, AUGSYMBOL, [LRItem(grammar.productions[0], 0, set())])

    state_queue = [s]
    state_id = 1

    states = []

    if debug:
        h_print("Constructing LR automaton states...")
    while state_queue:
        state = state_queue.pop(0)

        # For each state calculate its closure first, i.e. starting from a so
        # called "kernel items" expand collection with non-kernel items. We will
        # also calculate GOTO and ACTIONS dicts for each state. These dicts will
        # be keyed by a grammar symbol.
        closure(state, itemset_type, first_sets)
        states.append(state)

        # To find out other states we examine following grammar symbols in the
        # current state (symbols following current position/"dot") and group all
        # items by a grammar symbol.
        per_next_symbol = OrderedDict()

        # Each production has a priority. But since productions are grouped by
        # grammar symbol that is ahead we take the maximal priority given for
        # all productions for the given grammar symbol.
        state._max_prior_per_symbol = {}

        for item in state.items:
            symbol = item.symbol_at_position
            if symbol:
                per_next_symbol.setdefault(symbol, []).append(item)

                # Here we calculate max priorities for each grammar symbol to
                # use it for SHIFT/REDUCE conflict resolution
                prod_prior = item.production.prior
                old_prior = state._max_prior_per_symbol.setdefault(symbol, prod_prior)
                state._max_prior_per_symbol[symbol] = max(prod_prior, old_prior)

        # For each group symbol we create new state and form its kernel
        # items from the group items with positions moved one step ahead.
        for symbol, items in per_next_symbol.items():
            if symbol is STOP:
                state.actions[symbol] = [Action(ACCEPT)]
                continue
            inc_items = [item.get_pos_inc() for item in items]
            maybe_new_state = LRState(grammar, state_id, symbol, inc_items)
            target_state = maybe_new_state
            try:
                idx = states.index(maybe_new_state)
                target_state = states[idx]
            except ValueError:
                try:
                    idx = state_queue.index(maybe_new_state)
                    target_state = state_queue[idx]
                except ValueError:
                    pass

            if target_state is maybe_new_state:
                # We've found a new state. Register it for later processing.
                state_queue.append(target_state)
                state_id += 1
            else:
                # A state with this kernel items already exists.
                # LALR: Try to merge states, i.e. update items follow sets.
                if itemset_type is LR_1 and not merge_states(
                    target_state, maybe_new_state
                ):
                    target_state = maybe_new_state
                    state_queue.append(target_state)
                    state_id += 1

            # Create entries in GOTO and ACTION tables
            if isinstance(symbol, NonTerminal):
                # For each non-terminal symbol we create an entry in GOTO
                # table.
                state.gotos[symbol] = target_state

            else:
                # For each terminal symbol we create SHIFT action in the
                # ACTION table.
                state.actions[symbol] = [Action(SHIFT, state=target_state)]

    if debug:
        h_print(f"{len(states)} LR automata states constructed")
        h_print("Finishing LALR calculation...")

    # For LR(1) itemsets refresh/propagate item's follows as the LALR
    # merging might change item's follow in previous states
    if itemset_type is LR_1:
        # Propagate updates as long as there were items propagated in the last
        # loop run.
        update = True
        while update:
            update = False

            for state in states:
                # First refresh state's follows
                closure(state, LR_1, first_sets)

            for state in states:
                # Propagate follows to next states. GOTOs/ACTIONs keep
                # information about states created from this state
                inc_items = [i.get_pos_inc() for i in state.items]
                for target_state in chain(
                    state.gotos.values(),
                    [
                        a.state
                        for i in state.actions.values()
                        for a in i
                        if a.action is SHIFT
                    ],
                ):
                    for next_item in target_state.kernel_items:
                        this_item = inc_items[inc_items.index(next_item)]
                        if this_item.follow.difference(next_item.follow):
                            update = True
                            next_item.follow.update(this_item.follow)

    if debug:
        h_print(
            "Calculate REDUCTION entries in ACTION tables and resolve possible conflicts."
        )

    # Calculate REDUCTION entries in ACTION tables and resolve possible
    # conflicts.
    for state in states:
        actions = state.actions

        for item in state.items:
            if item.is_at_end:
                # If the position is at the end then this item
                # would call for reduction but only for terminals
                # from the FOLLOW set of item (LR(1)) or the production LHS
                # non-terminal (LR(0)).
                if itemset_type is LR_1:
                    follow_set = item.follow
                else:
                    follow_set = follow_sets[item.production.symbol]

                prod = item.production
                new_reduce = Action(REDUCE, prod=prod)

                for terminal in follow_set:
                    if terminal not in actions:
                        actions[terminal] = [new_reduce]
                    else:
                        # Conflict! Try to resolve
                        t_acts = actions[terminal]
                        should_reduce = True

                        # Only one SHIFT or ACCEPT might exists for a single
                        # terminal.
                        shifts = [x for x in t_acts if x.action in (SHIFT, ACCEPT)]
                        assert len(shifts) <= 1
                        t_shift = shifts[0] if shifts else None

                        # But many REDUCEs might exist
                        t_reduces = [x for x in t_acts if x.action is REDUCE]

                        # We should try to resolve using standard
                        # disambiguation rules between current reduction and
                        # all previous actions.

                        if t_shift:
                            # SHIFT/REDUCE conflict. Use assoc and priority to
                            # resolve
                            # For disambiguation treat ACCEPT action the same
                            # as SHIFT.
                            if t_shift.action is ACCEPT:
                                sh_prior = DEFAULT_PRIORITY
                            else:
                                sh_prior = state._max_prior_per_symbol[
                                    t_shift.state.symbol
                                ]
                            if prod.prior == sh_prior:
                                if prod.assoc == ASSOC_LEFT:
                                    # Override SHIFT with this REDUCE
                                    actions[terminal].remove(t_shift)
                                elif prod.assoc == ASSOC_RIGHT:
                                    # If associativity is right leave SHIFT
                                    # action as "stronger" and don't consider
                                    # this reduction any more. Right
                                    # associative reductions can't be in the
                                    # same set of actions together with SHIFTs.
                                    should_reduce = False
                                else:
                                    # If priorities are the same and no
                                    # associativity defined use preferred
                                    # strategy.
                                    is_empty = len(prod.rhs) == 0
                                    prod_pse = (
                                        is_empty
                                        and prefer_shifts_over_empty
                                        and not prod.nopse
                                    )
                                    prod_ps = (
                                        not is_empty and prefer_shifts and not prod.nops
                                    )
                                    should_reduce = not (prod_pse or prod_ps)
                            elif prod.prior > sh_prior:
                                # This item operation priority is higher =>
                                # override with reduce
                                actions[terminal].remove(t_shift)
                            else:
                                # If priority of existing SHIFT action is
                                # higher then leave it instead
                                should_reduce = False

                        if should_reduce:
                            if not t_reduces:
                                actions[terminal].append(new_reduce)
                            else:
                                # REDUCE/REDUCE conflicts
                                # Try to resolve using priorities
                                if prod.prior == t_reduces[0].prod.prior:
                                    actions[terminal].append(new_reduce)
                                elif prod.prior > t_reduces[0].prod.prior:
                                    # If this production priority is higher
                                    # it should override all other reductions.
                                    actions[terminal][:] = [
                                        x
                                        for x in actions[terminal]
                                        if x.action is not REDUCE
                                    ]
                                    actions[terminal].append(new_reduce)

    grammar.productions[0].rhs = _old_start_production_rhs
    table = LRTable(states, **kwargs)
    return table


def merge_states(old_state, new_state):
    """Try to merge new_state to old_state if possible (LALR). If not possible
    return False.

    If old state has no R/R conflicts additional check is made and merging is
    not done if it would add R/R conflict.

    """

    # If states are not equal (i.e. have the same kernel items) no merge is
    # possible
    if old_state != new_state:
        return False

    item_pairs = []
    for old_item in (s for s in old_state.kernel_items if s.is_at_end):
        new_item = new_state.get_item(old_item)
        item_pairs.append((old_item, new_item))

    # Check if merging would result in additional R/R conflict by investigating
    # if after merging there could be a lookahead token that would call for
    # different reductions. If that is the case we shall not merge states.
    for old, new in item_pairs:
        for s in (s for s in old_state.kernel_items if s.is_at_end and s is not old):
            if s.follow.intersection(new.follow.difference(old.follow)):
                return False

    # Do the merge by updating old items follow sets.
    for old, new in item_pairs:
        old.follow.update(new.follow)
    return True


class LRTable:
    def __init__(
        self,
        states,
        calc_finish_flags=True,
        # lexical_disambiguation defaults to True, when
        # calc_finish_flags is set
        lexical_disambiguation=None,
        debug=False,
    ):
        self.states = states
        if calc_finish_flags:
            if lexical_disambiguation is None:
                lexical_disambiguation = True
            self.sort_state_actions()
            if lexical_disambiguation:
                self.calc_finish_flags()
            else:
                for state in self.states:
                    state.finish_flags = [False] * len(state.actions)
        else:
            if lexical_disambiguation is not None:
                logger.warning(
                    "lexical_disambiguation flag ignored because "
                    "calc_finish_flags is not set"
                )
        self.calc_conflicts_and_dynamic_terminals(debug)

    def sort_state_actions(self):
        """
        State actions need to be sorted in order to utilize scanning
        optimization based on explicit or implicit disambiguation.
        Also, by sorting actions table save file is made deterministic.
        """

        def act_order(act_item):
            """Priority is the strongest property. After that honor string
            recognizer over other types of recognizers.

            """
            symbol, act = act_item
            cmp_str = "{:010d}{:500s}".format(
                symbol.prior * 1000
                + (
                    500
                    + (
                        len(symbol.recognizer.value)
                        if type(symbol.recognizer) is StringRecognizer
                        else 0
                    )
                    +
                    # Account for `\b` at the beginning and end of keyword regex
                    (
                        (len(symbol.recognizer._regex) - 4)
                        if type(symbol.recognizer) is RegExRecognizer and symbol.keyword
                        else 0
                    )
                ),
                symbol.fqn,
            )
            return cmp_str

        for state in self.states:
            state.actions = OrderedDict(
                sorted(state.actions.items(), key=act_order, reverse=True)
            )

    def calc_finish_flags(self):
        """
        Scanning optimization. Preorder actions based on terminal priority
        and specificity. Set _finish flags.
        """
        for state in self.states:
            finish_flags = []
            prior = None
            for symbol, _act in reversed(list(state.actions.items())):
                if symbol.finish is not None:
                    finish_flags.append(symbol.finish)
                else:
                    finish_flags.append(
                        (symbol.prior > prior if prior else False)
                        or type(symbol.recognizer) is StringRecognizer
                        or symbol.keyword
                    )
                prior = symbol.prior

            finish_flags.reverse()
            state.finish_flags = finish_flags

    def calc_conflicts_and_dynamic_terminals(self, debug=False):
        """
        Determine S/R and R/R conflicts and states dynamic terminals.
        """
        self.sr_conflicts = []
        self.rr_conflicts = []

        if debug:
            h_print("Calculating conflicts and dynamic terminals...")

        for state in self.states:
            for term, actions in state.actions.items():
                # Mark state for dynamic disambiguation
                if term.dynamic:
                    state.dynamic.add(term)

                if len(actions) > 1:
                    if actions[0].action in [SHIFT, ACCEPT]:
                        # Create SR conflicts for each S-R pair of actions
                        # except EMPTY reduction as SHIFT will always be
                        # preferred in LR parsing and GLR has a special
                        # handling of EMPTY reduce in order to avoid infinite
                        # looping.
                        for r_act in actions[1:]:
                            # Mark state for dynamic disambiguation
                            if r_act.prod.dynamic:
                                state.dynamic.add(term)

                            self.sr_conflicts.append(
                                SRConflict(state, term, [x.prod for x in actions[1:]])
                            )
                    else:
                        prods = [x.prod for x in actions if len(x.prod.rhs)]

                        # Mark state for dynamic disambiguation
                        if any([p.dynamic for p in prods]):
                            state.dynamic.add(term)

                        empty_prods = [x.prod for x in actions if not len(x.prod.rhs)]
                        # Multiple empty reductions possible
                        if len(empty_prods) > 1:
                            self.rr_conflicts.append(RRConflict(state, term, empty_prods))
                        # Multiple non-empty reductions possible
                        if len(prods) > 1:
                            self.rr_conflicts.append(RRConflict(state, term, prods))

    def print_debug(self):
        a_print("*** STATES ***", new_line=True)
        for state in self.states:
            state.print_debug()

            if state.gotos:
                h_print("GOTO:", level=1, new_line=True)
                prints(
                    "\t"
                    + ", ".join(
                        [
                            ("%s" + s_emph("->") + "%d") % (k, v.state_id)
                            for k, v in state.gotos.items()
                        ]
                    )
                )
            h_print("ACTIONS:", level=1, new_line=True)
            prints(
                "\t"
                + ", ".join(
                    [
                        ("%s" + s_emph("->") + "%s")
                        % (
                            k,
                            str(v[0])
                            if len(v) == 1
                            else "[{}]".format(",".join([str(x) for x in v])),
                        )
                        for k, v in state.actions.items()
                    ]
                )
            )

        if self.sr_conflicts:
            a_print("*** S/R conflicts ***", new_line=True)
            if len(self.sr_conflicts) == 1:
                message = "There is {} S/R conflict."
            else:
                message = "There are {} S/R conflicts."
            h_print(message.format(len(self.sr_conflicts)))
            for src in self.sr_conflicts:
                print(src)

        if self.rr_conflicts:
            a_print("*** R/R conflicts ***", new_line=True)
            if len(self.rr_conflicts) == 1:
                message = "There is {} R/R conflict."
            else:
                message = "There are {} R/R conflicts."
            h_print(message.format(len(self.rr_conflicts)))
            for rrc in self.rr_conflicts:
                print(rrc)


class Action:
    __slots__ = ["action", "state", "prod"]

    def __init__(self, action, state=None, prod=None):
        self.action = action
        self.state = state
        self.prod = prod

    def __str__(self):
        ac = {SHIFT: "SHIFT", REDUCE: "REDUCE", ACCEPT: "ACCEPT"}.get(self.action)
        if self.action == SHIFT:
            p = self.state.state_id
        elif self.action == REDUCE:
            p = self.prod.prod_id
        else:
            p = ""
        return "{}{}".format(ac, f":{p}" if p else "")

    def __repr__(self):
        return str(self)

    @property
    def dynamic(self):
        if self.action is SHIFT:
            return self.state.symbol.dynamic
        elif self.action is REDUCE:
            return self.prod.dynamic
        else:
            return False


class LRItem:
    """
    Represents an item in the items set. Item is defined by a production and a
    position inside production (the dot). If the item is of LR_1 type follow
    set is also defined. Follow set is a set of terminals that can follow
    non-terminal at given position in the given production.
    """

    __slots__ = ("production", "position", "follow")

    def __init__(self, production, position, follow=None):
        self.production = production
        self.position = position
        self.follow = follow if follow else set()

    def __eq__(self, other):
        return (
            other
            and self.production == other.production
            and self.position == other.position
        )

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return str(self)

    def __str__(self):
        s = []
        for idx, r in enumerate(self.production.rhs):
            if idx == self.position:
                s.append(".")
            s.append(str(r))
        if len(self.production.rhs) == self.position:
            s.append(".")
        s = " ".join(s)

        follow = (
            (s_emph("{{") + "{}" + s_emph("}}")).format(
                ", ".join([str(t) for t in self.follow])
            )
            if self.follow
            else "{}"
        )

        return (s_header("%d:") + " %s " + s_emph("=") + " %s   %s") % (
            self.production.prod_id,
            self.production.symbol,
            s,
            follow,
        )

    @property
    def is_kernel(self):
        """
        Kernel items are items whose position is not at the beginning.
        The only exception to this rule is start symbol of the augmented
        grammar.
        """
        return self.position > 0 or self.production.symbol is AUGSYMBOL

    def get_pos_inc(self):
        """
        Returns new LRItem with incremented position or None if position
        cannot be incremented (e.g. it is already at the end of the production)
        """

        if self.position < len(self.production.rhs):
            return LRItem(self.production, self.position + 1, self.follow)

    @property
    def symbol_at_position(self):
        """
        Returns symbol from production RHS at the position of this item.
        """
        return self.production.rhs[self.position]

    @property
    def is_at_end(self):
        """
        Is the position at the end? If so, it is a candidate for reduction.
        """
        return self.position == len(self.production.rhs)


class LRState:
    """LR State is a set of LR items and a dict of LR automata actions and
    gotos.

    Attributes:
    grammar(Grammar):
    state_id(int):
    symbol(GrammarSymbol):
    items(list of LRItem):
    actions(OrderedDict): Keys are grammar terminal symbols, values are
        lists of Action instances.
    gotos(OrderedDict): Keys are grammar non-terminal symbols, values are
        instances of LRState.
    dynamic(set of terminal symbols): If terminal symbol is in set dynamic
        ambiguity strategy callable is called for the terminal symbol
        lookahead.
    finish_flags:

    """

    __slots__ = [
        "grammar",
        "state_id",
        "symbol",
        "items",
        "actions",
        "gotos",
        "dynamic",
        "finish_flags",
        "_max_prior_per_symbol",
    ]

    def __init__(self, grammar, state_id, symbol, items=None):
        self.grammar = grammar
        self.state_id = state_id
        self.symbol = symbol
        self.items = items if items else []

        self.actions = OrderedDict()
        self.gotos = OrderedDict()
        self.dynamic = set()

    def __eq__(self, other):
        """Two states are equal if their kernel items are equal."""
        this_kernel = [x for x in self.items if x.is_kernel]
        other_kernel = [x for x in other.items if x.is_kernel]
        if len(this_kernel) != len(other_kernel):
            return False
        return all(item in other_kernel for item in this_kernel)

    def __ne__(self, other):
        return not self == other

    @property
    def kernel_items(self):
        """
        Returns kernel items of this state.
        """
        return [i for i in self.items if i.is_kernel]

    @property
    def nonkernel_items(self):
        """
        Returns nonkernel items of this state.
        """
        return [i for i in self.items if not i.is_kernel]

    def get_item(self, other_item):
        """
        Get this state item that is equal to the given other_item.
        """
        return self.items[self.items.index(other_item)]

    def __str__(self):
        s = s_header(f"\n\nState {self.state_id}:{self.symbol}\n")
        return s + "\n".join([f"\t{i}" for i in self.items])

    def __unicode__(self):
        return str(self)

    def __repr__(self):
        return f"LRState({self.state_id}:{self.symbol.name})"

    def print_debug(self):
        prints(str(self))


def first(grammar):
    """Calculates the sets of terminals that can start the sentence derived from
    all grammar symbols.

    The Dragon book p. 221.

    Returns:
    dict of sets of Terminal keyed by GrammarSymbol.
    """
    assert isinstance(grammar, Grammar), "grammar parameter should be Grammar instance."

    if hasattr(grammar, "_first_sets"):
        # If first sets is already calculated return it
        return grammar._first_sets

    first_sets = {}
    for t in grammar.terminals.values():
        first_sets[t] = set([t])
    for nt in grammar.nonterminals.values():
        first_sets[nt] = set()

    additions = True
    while additions:
        additions = False

        for p in grammar.productions:
            nonterm = p.symbol
            for rhs_symbol in p.rhs:
                rhs_symbol_first = set(first_sets[rhs_symbol])
                rhs_symbol_first.discard(EMPTY)
                if rhs_symbol_first.difference(first_sets[nonterm]):
                    first_sets[nonterm].update(first_sets[rhs_symbol])
                    additions = True
                # If current RHS symbol can't derive EMPTY
                # this production can't add any more members of
                # the first set for LHS nonterminal.
                if EMPTY not in first_sets[rhs_symbol]:
                    break
            else:
                # If we reached the end of the RHS and each
                # symbol along the way could derive EMPTY than
                # we must add EMPTY to the first set of LHS symbol.
                if EMPTY not in first_sets[nonterm]:
                    first_sets[nonterm].add(EMPTY)
                    additions = True

    grammar._first_sets = first_sets
    return first_sets


def follow(grammar, first_sets=None):
    """Calculates the sets of terminals that can follow some non-terminal for the
    given grammar.

    Args:
    grammar (Grammar): An initialized grammar.
    first_sets (dict): A sets of FIRST terminals keyed by a grammar symbol.
    """

    if first_sets is None:
        first_sets = first(grammar)

    follow_sets = {}
    for symbol in grammar.nonterminals.values():
        follow_sets[symbol] = set()

    additions = True
    while additions:
        additions = False
        for symbol in grammar.nonterminals.values():
            for p in grammar.productions:
                for idx, s in enumerate(p.rhs):
                    if s == symbol:
                        prod_follow = set()
                        for rsymbol in p.rhs[idx + 1 :]:
                            sfollow = first_sets[rsymbol]
                            prod_follow.update(sfollow)
                            if EMPTY not in sfollow:
                                break
                        else:
                            prod_follow.update(follow_sets[p.symbol])
                        prod_follow.discard(EMPTY)
                        if prod_follow.difference(follow_sets[symbol]):
                            additions = True
                            follow_sets[symbol].update(prod_follow)
    return follow_sets
