import copy
import itertools
import re
from collections import Counter
from dataclasses import dataclass, field
from os import path
from typing import Callable, Dict, List, Optional

from parglare import termui
from parglare.actions import collect, collect_sep, pass_none, pass_single
from parglare.common import Location, load_python_module
from parglare.exceptions import GrammarError, ParserInitError
from parglare.termui import a_print, h_print, prints, s_emph, s_header
from parglare.trees import visitor

# Associativity
ASSOC_NONE = 0
ASSOC_LEFT = 1
ASSOC_RIGHT = 2

# Priority
DEFAULT_PRIORITY = 10

# Multiplicity
MULT_ONE = "1"
MULT_OPTIONAL = "0..1"
MULT_ONE_OR_MORE = "1..*"
MULT_ZERO_OR_MORE = "0..*"

RESERVED_SYMBOL_NAMES = ["STOP", "EMPTY"]
SPECIAL_SYMBOL_NAMES = ["KEYWORD", "LAYOUT"]


def escape(instr):
    return instr.replace("\n", r"\n").replace("\t", r"\t")


class GrammarSymbol:
    """
    Represents an abstract grammar symbol.

    Attributes:
    name(str): The name of this grammar symbol.
    location(Location): The location where symbol is defined.
    action_name(string): Name of common/user action given in the grammar.
    action(callable): Resolved action given by the user. Overrides grammar
        action if provided. If not provided by the user defaults to
        grammar_action.
    grammar_action(callable): Resolved action given in the grammar.
    imported_with (PGFileImport): PGFileImport where this symbol is first time
        imported from. Used for FQN calculation.
    user_meta(dict): User meta-data.
    """

    def __init__(self, name, location=None, imported_with=None, user_meta=None):
        self.name = escape(name)
        self.location = location
        self.action_name = None
        self.action = None
        self.grammar_action = None
        self.imported_with = imported_with
        self.user_meta = user_meta

        # A Python class for AST nodes. Can be defined by the user during
        # grammar construction or is created on the fly by parglare.
        self.cls = None

        self._hash = hash(self.fqn)

    @property
    def fqn(self):
        if self.imported_with:
            return f"{self.imported_with.fqn}.{self.name}"
        return self.name

    @property
    def action_fqn(self):
        if self.action_name:
            if self.imported_with:
                return f"{self.imported_with.fqn}.{self.action_name}"
            return self.action_name

    def add_user_meta_data(self, name, value):
        if self.user_meta is None:
            self.user_meta = {}
        self.user_meta[name] = value

    def __getattr__(self, name):
        if self.user_meta is not None:
            attr = self.user_meta.get(name)
            if attr:
                return attr
        raise AttributeError

    def __unicode__(self):
        return str(self)

    def __str__(self):
        return self.fqn

    def __repr__(self):
        return f"{type(self).__name__}({str(self)})"

    def __hash__(self):
        return self._hash


class NonTerminal(GrammarSymbol):
    """Represents a non-termial symbol of the grammar.

    Attributes:
    productions(list of Production): A list of alternative productions for
        this NonTerminal.
    """

    def __init__(
        self,
        name,
        productions=None,
        location=None,
        imported_with=None,
        user_meta=None,
    ):
        super().__init__(name, location, imported_with, user_meta)
        self.productions = productions if productions is not None else []


class Terminal(GrammarSymbol):
    """Represent a terminal symbol of the grammar.

    Attributes:
    prior(int): Priority used for lexical disambiguation.
    dynamic(bool): Should dynamic disambiguation be called to resolve conflict
        involving this terminal.
    finish(bool): Used for scanning optimization. If this terminal is `finish`
        no other recognizers will be checked if this succeeds. If not provided
        in the grammar implicit rules will be used during table construction.
    prefer(bool): Prefer this recognizer in case of multiple recognizers match
        at the same place and implicit disambiguation doesn't resolve.
    keyword(bool): `True` if this Terminal represents keyword. `False` by
        default.

    recognizer(callable): Called with input list of objects and position in the
        stream. Should return a sublist of recognized objects. The sublist
        should be rooted at the given position.
    """

    def __init__(self, name, recognizer=None, location=None, imported_with=None):
        self.prior = DEFAULT_PRIORITY
        self._recognizer = None
        self.recognizer = recognizer if recognizer else StringRecognizer(name)
        self.finish = None
        self.prefer = False
        self.dynamic = False
        self.keyword = False
        super().__init__(name, location, imported_with, user_meta=None)

    @property
    def recognizer(self):
        return self._recognizer

    @recognizer.setter
    def recognizer(self, value):
        self._recognizer = value


class Reference:
    """
    A name reference to a GrammarSymbol used for cross-resolving during
    grammar construction.
    Attributes:
        name (str): The FQN name of the referred symbol. This is the name of
            the original desuggared symbol without taking into account
            multiplicity and separator.
        location (Location): Location object of this reference.
        multiplicty(str): Multiplicity of the RHS reference (used for regex
            operators ?, *, +). See MULT_* constants above. By default
            multiplicity is MULT_ONE.
        greedy(bool): If the multiplicity was greedy (e.g. ?!, *! or +!).
        separator (symbol or Reference): A reference to the separator symbol or
            the separator symbol itself if resolved.
    """

    def __init__(self, location: Location, name: str, imported_with: "PGFileImport"):
        self.name = name
        self.location = location
        self.imported_with = imported_with
        self.multiplicity = MULT_ONE
        self.greedy = False
        self.separator = None

    @property
    def multiplicity_fqn(self):
        """
        Returns the name of the symbol that should be used if
        multiplicity/separator is used.
        """
        return make_multiplicity_fqn(
            self.fqn,
            self.multiplicity,
            self.separator.name if self.separator else None,
        )

    @property
    def fqn(self):
        if self.imported_with:
            return f"{self.imported_with.fqn}.{self.name}"
        return self.name

    def clone(self):
        new_ref = Reference(self.location, self.name, self.imported_with)
        new_ref.multiplicity = self.multiplicity
        new_ref.separator = self.separator
        return new_ref

    def __repr__(self):
        return self.name


class Recognizer:
    """
    Recognizers are callables capable of recognizing low-level patterns
    (a.k.a tokens) in the input.
    """

    def __init__(self, name, location=None):
        self.name = name
        self.location = location


class StringRecognizer(Recognizer):
    def __init__(self, value, ignore_case=False, **kwargs):
        super().__init__(value, **kwargs)
        self.value = value
        self.ignore_case = ignore_case
        self.value_cmp = value.lower() if ignore_case else value

    def __call__(self, in_str, pos):
        if self.ignore_case:
            if in_str[pos : pos + len(self.value)].lower() == self.value_cmp:
                return self.value
        else:
            if in_str[pos : pos + len(self.value)] == self.value_cmp:
                return self.value


def esc_control_characters(regex):
    """
    Escape control characters in regular expressions.
    """
    unescapes = [
        ("\a", r"\a"),
        ("\b", r"\b"),
        ("\f", r"\f"),
        ("\n", r"\n"),
        ("\r", r"\r"),
        ("\t", r"\t"),
        ("\v", r"\v"),
    ]
    for val, text in unescapes:
        regex = regex.replace(val, text)
    return regex


class RegExRecognizer(Recognizer):
    def __init__(
        self,
        regex,
        name=None,
        re_flags=re.MULTILINE,
        ignore_case=False,
        **kwargs,
    ):
        if name is None:
            name = regex
        super().__init__(name, kwargs)
        self._regex = regex
        self.ignore_case = ignore_case
        if ignore_case:
            re_flags |= re.IGNORECASE
        re_flags |= re.VERBOSE
        self.re_flags = re_flags
        try:
            self.regex = re.compile(self._regex, re_flags)
        except re.error as ex:
            regex = esc_control_characters(self._regex)
            message = 'Regex compile error in /{}/ (report: "{}")'
            raise GrammarError(None, message.format(regex, str(ex))) from ex

    def __call__(self, in_str, pos):
        m = self.regex.match(in_str, pos)
        if m and m.group():
            return m.group()


def EMPTY_recognizer(input, pos):
    pass


def STOP_recognizer(input, pos):
    pass


# These two terminals are special terminals used internally.
AUGSYMBOL = NonTerminal("S'")
STOP = Terminal("STOP", STOP_recognizer)

# EMPTY is a special terminal used in the grammars.
# It will match nothing and always succeed.
EMPTY = Terminal("EMPTY", EMPTY_recognizer)
EMPTY.grammar_action = pass_none


class Production:
    """Represent production from the grammar.

    Attributes:
    symbol (GrammarSymbol):
    rhs (ProductionRHS):
    assignments(dict): Assignment instances keyed by name.
    assoc (int): Associativity. Used for ambiguity (shift/reduce) resolution.
    prior (int): Priority. Used for ambiguity (shift/reduce) resolution.
    dynamic (bool): Is dynamic disambiguation used for this production.
    nops (bool): Disable prefer_shifts strategy for this production.
        Only makes sense for GLR parser.
    nopse (bool): Disable prefer_shifts_over_empty strategy for this
        production. Only makes sense for GLR parser.
    user_meta(dict): User meta-data.
    prod_id (int): Ordinal number of the production.
    prod_symbol_id (int): A zero-based ordinal of alternative choice for this
        production grammar symbol.
    """

    def __init__(
        self,
        symbol,
        rhs,
        assignments=None,
        assoc=ASSOC_NONE,
        prior=DEFAULT_PRIORITY,
        dynamic=False,
        nops=False,
        nopse=False,
        user_meta=None,
    ):
        """
        Args:
        symbol (GrammarSymbol): A grammar symbol on the LHS of the production.
        rhs (list of GrammarSymbols):
        """
        self.symbol = symbol
        self.rhs = rhs if rhs else ProductionRHS()
        self.assignments = None
        if assignments:
            self.assignments = {}
            for assignment in assignments:
                if assignment.name:
                    self.assignments[assignment.name] = assignment
        self.assoc = assoc
        self.prior = prior
        self.dynamic = dynamic
        self.nops = nops
        self.nopse = nopse
        self.user_meta = user_meta

    def __str__(self):
        if hasattr(self, "prod_id"):
            return (s_header("%d:") + " %s " + s_emph("=") + " %s") % (
                self.prod_id,
                self.symbol,
                self.rhs,
            )
        return ("%s " + s_emph("=") + " %s") % (self.symbol, self.rhs)

    def __repr__(self):
        return f"Production({str(self)})"

    def __getattr__(self, name):
        if self.user_meta is not None:
            attr = self.user_meta.get(name)
            if attr:
                return attr
        raise AttributeError


class ProductionRHS(list):
    def __getitem__(self, idx):
        try:
            while True:
                symbol = super().__getitem__(idx)
                if symbol is not EMPTY:
                    break
                idx += 1
            return symbol
        except IndexError:
            return None

    def __len__(self):
        return super().__len__() - self.count(EMPTY)

    def __str__(self):
        return " ".join([str(x) for x in self])

    def __repr__(self):
        return "ProductionRHS([{}])".format(", ".join([str(x) for x in self]))


class Assignment:
    """
    General assignment (`=` or `?=`, a.k.a. `named matches`) in productions.
    Used also for references as LHS and assignment operator are optional.
    """

    def __init__(self, name, op, symbol):
        """
        Attributes:
            name(str): The name on the LHS of assignment.
            op(str): Either a `=` or `?=`.
            symbol(Reference or GrammarSymbol): A grammar symbol on the RHS.
            symbol_name(str): A de-sugarred grammar symbol name on the
                RHS, i.e. referenced symbol without regex operators.
            multiplicty(str): Multiplicity of the RHS reference (used for regex
                operators ?, *, +). See MULT_* constants above. By default
                multiplicity is MULT_ONE.
            index(int): Index in the production RHS
        """
        self.name = name
        self.op = op
        self.symbol = symbol
        self.symbol_name = symbol.name
        self.multiplicity = (
            symbol.multiplicity if isinstance(symbol, Reference) else MULT_ONE
        )
        self.index = None


class PGAttribute:
    """
    PGAttribute definition created by named matches.

    Attributes:
        name(str): The name of the attribute.
        multiplicity(str): Multiplicity of the attribute. See MULT_* constants.
        type_name(str): The type name of the attribute value(s). It is also the
            name of the referring grammar rule.
    """

    def __init__(self, name, multiplicity, type_name):
        self.name = name
        self.multiplicity = multiplicity
        self.type_name = type_name


@dataclass
class GrammarContext:
    """
    Context used to collect grammar information and provide info to actions
    during grammar parsing.

    """

    classes: Dict = field(default_factory=dict)
    debug: bool = False
    debug_colors: bool = False
    re_flags: re.RegexFlag = re.MULTILINE
    groups: List = field(default_factory=list)
    groups_counter: Counter = field(default_factory=Counter)
    ignore_case: bool = False
    imported_with: Optional["PGFileImport"] = None
    inline_terminals: Dict = field(default_factory=dict)


class PGFile:
    """Objects of this class represent parglare grammar files.

    Grammar files can be imported using `import` keyword. Rules referenced from
    the imported grammar must be fully qualified by the grammar module name. By
    default the name of the target .pg file is the name of the module. `as`
    keyword can be used to override the default.

    Example:
    ```
    import `some/path/mygrammar.pg` as target
    ```

    Rules from file `mygrammar.pg` will be available under `target` namespace:

    ```
    MyRule: target.someRule+;
    ```

    Actions are by default loaded from the file named `<grammar>_actions.py`
    where `grammar` is basename of grammar file. Recognizers are loaded from
    `<grammar>_recognizers.py`. Actions and recognizers given this way are both
    optional. Furthermore, both actions and recognizers can be overridden by
    supplying actions and/or recognizers dict during grammar/parser
    instantiation.

    Attributes:

    productions (list of Production): Local productions defined in this file.
    terminals (dict of Terminal):
    classes (dict of ParglareClass): Dynamically created classes. Used by
        obj action.
    imports (dict): Mapping imported module/file local name to PGFile object.
    file_path (str): A full canonic path to the .pg file.
    grammar (Grammar): A root/grammar file.
    recognizers (dict of callables): A dict of Python callables used as a
        terminal recognizers.
    """

    def __init__(
        self,
        productions: List[Production],
        terminals: Optional[List[Terminal]] = None,
        classes=None,
        imports=None,
        file_path=None,
        grammar: Optional["Grammar"] = None,
        recognizers=None,
        imported_with=None,
    ):
        self.productions = productions
        self.terminals = terminals
        self.classes = classes if classes else {}
        self.grammar: Optional[Grammar]
        if grammar is not None:
            assert isinstance(grammar, Grammar)
            self.grammar = grammar
        else:
            self.grammar = None

        self.file_path = path.realpath(file_path) if file_path else None
        self.imported_with = imported_with
        self.recognizers = recognizers
        self.actions: Dict[str, Callable] = {}

        self._make_symbols_resolution_map()

        if self.file_path and self.grammar:
            self.grammar.imported_files[self.file_path] = self

        if imports:
            self.imports = {i.module_name: i for i in imports}
            for i in imports:
                i.grammar = self.grammar
                try:
                    i.load_pgfile()
                except OSError as ex:
                    raise GrammarError(
                        location=Location(file_name=self.file_path),
                        message=f'Can\'t import file "{i.file_path}".',
                    ) from ex
        else:
            self.imports = {}

        self._check_overrides()
        self._load_actions()
        self._load_recognizers()

    def _make_symbols_resolution_map(self):
        """
        Collect non-terminals and terminals and make dicts for resolving
        by name.
        """
        nonterminals_by_name = {}
        terminals_by_name = {}
        terminals_by_str_rec = {}

        # Check terminal uniqueness in both name and string recognition
        # and collect all terminals from explicit definitions.
        for terminal in self.terminals:
            if terminal.name in terminals_by_name:
                raise GrammarError(
                    location=terminal.location,
                    message=f'Multiple definitions of terminal rule "{terminal.name}"',
                )
            if isinstance(terminal.recognizer, StringRecognizer):
                rec = terminal.recognizer
                if rec.value in terminals_by_str_rec:
                    raise GrammarError(
                        location=terminal.location,
                        message=f'Terminals "{terminal.name}" and '
                        f'"{terminals_by_str_rec[rec.value].name}" match '
                        "the same string.",
                    )
                terminals_by_str_rec[rec.value] = terminal
            terminals_by_name[terminal.name] = terminal

        self.terminals = terminals_by_name

        # Collect non-terminals
        for production in self.productions:
            symbol = production.symbol
            symbol.imported_with = self.imported_with
            # Check that there is no terminal defined by the same name.
            if symbol.name in self.terminals:
                raise GrammarError(
                    location=symbol.location,
                    message=f'Rule "{symbol.name}" already defined as terminal',
                )
            # Unify all non-terminal objects
            if symbol.name in nonterminals_by_name:
                old_symbol = symbol
                new_symbol = nonterminals_by_name[symbol.name]
                production.symbol = new_symbol
            else:
                nonterminals_by_name[symbol.name] = symbol
                old_symbol = new_symbol = symbol
            new_symbol.productions.append(production)
            new_symbol.cls = self.grammar.classes.get(new_symbol.fqn, None)

            # Check grammar actions for rules/symbols.
            if (
                new_symbol.action_name
                and new_symbol.action_name != old_symbol.action_name
            ):
                raise GrammarError(
                    location=new_symbol.location,
                    message="Multiple different grammar actions "
                    f'for rule "{new_symbol.name}".',
                )

        self.nonterminals = nonterminals_by_name
        self.symbols_by_name = dict(nonterminals_by_name)
        self.symbols_by_name.update(self.terminals)

        # Add special terminals
        self.symbols_by_name["EMPTY"] = EMPTY
        self.symbols_by_name["STOP"] = STOP

    def _check_overrides(self):
        """
        Check that all overrides defined in the current file are
        valid FQNs. Just to be sure that typos don't go unnoticed.
        """
        for symbol_fqn, symbol in self.symbols_by_name.items():
            # Must resolve first level without resolve_symbol_by_name
            # as otherwise the override rule itself would be found.
            if "." in symbol_fqn:
                import_module_name, name = symbol_fqn.split(".", 1)
                try:
                    imported_pg_file = self.imports[import_module_name]
                    if not imported_pg_file.resolve_symbol_by_name(name):
                        raise GrammarError(
                            location=symbol.location,
                            message=f"Unexisting name for symbol override {symbol_fqn}.",
                        )
                except KeyError as ex_inner:
                    raise GrammarError(
                        location=symbol.location,
                        message=f'Unexisting module "{import_module_name}"'
                        f' in reference "{symbol_fqn}"',
                    ) from ex_inner

    def _load_actions(self):
        """
        Loads actions from <grammar_name>_actions.py if the file exists.
        Actions must be collected with action decorator and the decorator must
        be called `action`.
        """
        actions_file = None
        if self.file_path:
            actions_file = path.join(
                path.dirname(self.file_path),
                f"{path.splitext(path.basename(self.file_path))[0]}_actions.py",
            )
            if path.exists(actions_file):
                mod_name = "{}actions".format(
                    self.imported_with.fqn if self.imported_with is not None else ""
                )
                actions_module = load_python_module(mod_name, actions_file)
                if not hasattr(actions_module, "action"):
                    raise GrammarError(
                        Location(file_name=actions_file),
                        message=f'Actions file "{actions_file}" must have "action" '
                        "decorator defined.",
                    )
                self.actions = actions_module.action.all

    def _load_recognizers(self):
        """
        Load recognizers from <grammar_name>_recognizers.py. Override
        with provided recognizers.
        """
        if self.file_path:
            recognizers_file = path.join(
                path.dirname(self.file_path),
                f"{path.splitext(path.basename(self.file_path))[0]}_recognizers.py",
            )

            if path.exists(recognizers_file):
                mod_name = "{}recognizers".format(
                    self.imported_with.fqn if self.imported_with is not None else ""
                )
                mod_recognizers = load_python_module(mod_name, recognizers_file)
                recognizers = mod_recognizers.recognizer.all

                for recognizer_name, recognizer in recognizers.items():
                    symbol = self.resolve_symbol_by_name(
                        recognizer_name,
                        location=Location(file_name=recognizers_file),
                    )
                    if symbol is None:
                        raise GrammarError(
                            location=Location(file_name=recognizers_file),
                            message="Recognizer given for unknown "
                            f'terminal "{recognizer_name}".',
                        )
                    if not isinstance(symbol, Terminal):
                        raise GrammarError(
                            location=Location(file_name=recognizers_file),
                            message="Recognizer given for "
                            f'non-terminal "{recognizer_name}".',
                        )
                    symbol.recognizer = recognizer

    def resolve_symbol_by_name(
        self, symbol_fqn: str, location: Optional[Location] = None
    ) -> Optional[GrammarSymbol]:
        """
        Resolve symbol by FQN. Respect overrides.
        """
        try:
            # Try to get local symbol by FQN in order to override symbols from
            # imported grammars.
            return self.symbols_by_name[symbol_fqn]
        except KeyError:
            if "." in symbol_fqn:
                import_module_name, name = symbol_fqn.split(".", 1)
                try:
                    imported_pg_file = self.imports[import_module_name]
                except KeyError as ex_inner:
                    raise GrammarError(
                        location=location,
                        message=f'Unexisting module "{import_module_name}"'
                        f' in reference "{symbol_fqn}"',
                    ) from ex_inner
                return imported_pg_file.resolve_symbol_by_name(name, location)
        return None

    def resolve_action_by_name(self, action_name: str) -> Optional[Callable]:
        """
        Return registered action for the given action's FQN.
        """
        if action_name in self.actions:
            return self.actions[action_name]
        if "." in action_name:
            import_module_name, name = action_name.split(".", 1)
            if import_module_name in self.imports:
                imported_pg_file = self.imports[import_module_name]
                return imported_pg_file.resolve_action_by_name(name)
        return None


class Grammar(PGFile):
    """
    Grammar is a collection of production rules, nonterminals and terminals.
    First production is reserved for the augmented production (S' -> S).

    Attributes:
    start_symbol (GrammarSymbol or str): start/root symbol of the grammar or
        its name.
    nonterminals (set of NonTerminal):
    terminals(set of Terminal):
    imported_files(dict): Global registry of all imported files.

    """

    def __init__(
        self,
        productions=None,
        terminals=None,
        classes=None,
        imports=None,
        file_path=None,
        recognizers=None,
        start_symbol=None,
        _no_check_recognizers=False,
    ):
        """
        Grammar constructor is not meant to be called directly by the user.
        See `from_str` and `from_file` static methods instead.

        Arguments:
        see Grammar attributes.
        _no_check_recognizers (bool, internal): Used by pglr tool to circumvent
             errors for empty recognizers that will be provided in user code.
        """

        self.imported_files = {}

        super().__init__(
            productions=productions,
            terminals=terminals,
            classes=classes,
            imports=imports,
            file_path=file_path,
            grammar=self,
            recognizers=recognizers,
        )

        self._no_check_recognizers = _no_check_recognizers

        # Determine start symbol. If name is provided search for it. If name is
        # not given use the first production LHS symbol as the start symbol.
        if start_symbol:
            if isinstance(start_symbol, str):
                for p in self.productions:
                    if p.symbol.name == start_symbol:
                        self.start_symbol = p.symbol
            else:
                self.start_symbol = start_symbol
        else:
            # By default, first production symbol is the start symbol.
            self.start_symbol = self.productions[0].symbol

        self._init_grammar()

    def _init_grammar(self):
        """
        Extracts all grammar symbol (nonterminal and terminal) from the
        grammar, resolves and check references in productions, unify all
        grammar symbol objects and enumerate productions.
        """
        # Reserve 0 production. It is used for augmented prod. in LR
        # automata calculation.
        self.productions.insert(
            0, Production(AUGSYMBOL, ProductionRHS([self.start_symbol, STOP]))
        )

        self._add_resolve_all_production_symbols()
        self._enumerate_productions()
        self._fix_keyword_terminals()
        self._resolve_actions()

        # Connect recognizers, override grammar provided
        if not self._no_check_recognizers:
            self._connect_override_recognizers()

    def _add_resolve_all_production_symbols(self):
        """
        Registers all grammar symbols and resolve RHS of each production.
        """

        self.nonterminals = {}
        for prod in self.productions:
            self.nonterminals[prod.symbol.fqn] = prod.symbol
        self.terminals.update([(s.name, s) for s in (EMPTY, STOP)])

        def add_productions(productions):
            for production in productions:
                symbol = production.symbol
                if symbol.fqn not in self.nonterminals:
                    self.nonterminals[symbol.fqn] = symbol
                for idx, rhs_elem in enumerate(production.rhs):
                    if isinstance(rhs_elem, Reference):
                        rhs_elem = production.rhs[idx] = self._resolve_ref(rhs_elem)
                    if isinstance(rhs_elem, Terminal):
                        if rhs_elem.fqn not in self.terminals:
                            self.terminals[rhs_elem.fqn] = rhs_elem
                        else:
                            # Unify terminals
                            production.rhs[idx] = self.terminals[rhs_elem.fqn]
                    elif isinstance(rhs_elem, NonTerminal):
                        if rhs_elem.fqn not in self.nonterminals:
                            # This may happen for RHS refs that create new
                            # productions (e.g. syntactic sugar extensions - *,
                            # +...)
                            self.productions.extend(rhs_elem.productions)
                            add_productions(rhs_elem.productions)
                    else:
                        # This should never happen
                        raise AssertionError(
                            f"Invalid RHS element type '{type(rhs_elem)}'."
                        )

        add_productions(list(self.productions))

    def register_symbol(self, symbol):
        self.symbols_by_name[symbol.name] = symbol

    def _resolve_ref(self, symbol_ref):
        """Resolves given symbol reference.

        For local name search this file, for FQN use imports and delegate to
        imported file.

        If this is first pass do not fail on unexisting reference as there
        might be new symbols created during resolving (e.g. multiplicity
        symbols).

        """
        if isinstance(symbol_ref.separator, Reference):
            symbol_ref.separator = self._resolve_ref(symbol_ref.separator)

        symbol_fqn = symbol_ref.fqn
        symbol = self.resolve_symbol_by_name(symbol_fqn, symbol_ref.location)
        if not symbol:
            raise GrammarError(
                location=symbol_ref.location,
                message=f'Unknown symbol "{symbol_fqn}"',
            )

        mult = symbol_ref.multiplicity
        if mult != MULT_ONE:
            # If multiplicity is used than we are referring to
            # sugared symbol
            separator = symbol_ref.separator if symbol_ref.separator else None

            base_symbol = symbol
            symbol_name = symbol_ref.multiplicity_fqn
            symbol = self.resolve_symbol_by_name(symbol_name, symbol_ref.location)
            if not symbol:
                # If there is no multiplicity version of the symbol we
                # will create one at this place
                symbol = self._make_multiplicity_symbol(
                    symbol_ref, base_symbol, separator, self.imported_with
                )

        return symbol

    def _make_multiplicity_symbol(
        self, symbol_ref, base_symbol, separator, imported_with
    ):
        """
        Creates new NonTerminal for symbol refs using multiplicity and
        separators.
        """
        mult = symbol_ref.multiplicity
        assoc = ASSOC_RIGHT if symbol_ref.greedy else ASSOC_NONE
        if mult in [MULT_ONE_OR_MORE, MULT_ZERO_OR_MORE]:
            symbol_name = make_multiplicity_fqn(
                symbol_ref.fqn,
                MULT_ONE_OR_MORE,
                separator.name if separator else None,
            )
            symbol = self.resolve_symbol_by_name(symbol_name)
            if not symbol:
                # noqa See: http://www.igordejanovic.net/parglare/grammar_language/#one-or-more_1
                productions = []
                symbol = NonTerminal(
                    symbol_name,
                    productions,
                    base_symbol.location,
                    imported_with=imported_with,
                )

                if separator:
                    productions.append(
                        Production(
                            symbol,
                            ProductionRHS([symbol, separator, base_symbol]),
                        )
                    )
                    symbol.action_name = "collect_sep"
                else:
                    productions.append(
                        Production(symbol, ProductionRHS([symbol, base_symbol]))
                    )
                    symbol.action_name = "collect"

                productions.append(Production(symbol, ProductionRHS([base_symbol])))

                self.register_symbol(symbol)

            if mult == MULT_ZERO_OR_MORE:
                productions = []
                symbol_one = symbol
                symbol_name = make_multiplicity_fqn(
                    symbol_ref.fqn, mult, separator.name if separator else None
                )
                symbol = NonTerminal(
                    symbol_name,
                    productions,
                    base_symbol.location,
                    imported_with=imported_with,
                )

                productions.extend(
                    [
                        Production(
                            symbol,
                            ProductionRHS([symbol_one]),
                            assoc=assoc,
                            nops=True,
                        ),
                        Production(symbol, ProductionRHS([EMPTY]), assoc=assoc),
                    ]
                )

                def action(_, nodes):
                    if nodes:
                        return nodes[0]
                    return []

                symbol.grammar_action = action

                self.register_symbol(symbol)

            else:
                if symbol_ref.greedy:
                    productions = []
                    symbol_one = symbol
                    symbol = NonTerminal(
                        f"{symbol_name}_g",
                        productions,
                        base_symbol.location,
                        imported_with=imported_with,
                    )
                    productions.extend(
                        [
                            Production(
                                symbol,
                                ProductionRHS([symbol_one]),
                                assoc=ASSOC_RIGHT,
                            )
                        ]
                    )
                    symbol.action_name = "pass_single"
                    self.register_symbol(symbol)

        else:
            # MULT_OPTIONAL
            if separator:
                raise GrammarError(
                    location=symbol_ref.location,
                    message="Repetition modifier not allowed for "
                    f'optional (?) for symbol "{symbol_ref.name}".',
                )
            productions = []
            symbol_name = make_multiplicity_fqn(symbol_ref.fqn, mult)
            symbol = NonTerminal(
                symbol_name,
                productions,
                base_symbol.location,
                imported_with=imported_with,
            )
            productions.extend(
                [
                    Production(symbol, ProductionRHS([base_symbol])),
                    Production(symbol, ProductionRHS([EMPTY]), assoc=assoc),
                ]
            )

            symbol.action_name = "optional"

            self.register_symbol(symbol)

        return symbol

    def _enumerate_productions(self):
        """
        Enumerates all productions (prod_id) and production per symbol
        (prod_symbol_id).
        """
        idx_per_symbol = {}
        for idx, prod in enumerate(self.productions):
            prod.prod_id = idx
            prod.prod_symbol_id = idx_per_symbol.get(prod.symbol, 0)
            idx_per_symbol[prod.symbol] = idx_per_symbol.get(prod.symbol, 0) + 1

    def _fix_keyword_terminals(self):
        """
        If KEYWORD terminal with regex match is given fix all matching string
        recognizers to match on a word boundary.
        """
        keyword_term = self.get_terminal("KEYWORD")
        if keyword_term is None:
            return

        # KEYWORD rule must have a regex recognizer
        keyword_rec = keyword_term.recognizer
        if not isinstance(keyword_rec, RegExRecognizer):
            raise GrammarError(
                location=keyword_term.location,
                message="KEYWORD rule must have a regex recognizer defined.",
            )

        # Change each string recognizer corresponding to the KEYWORD
        # regex by the regex recognizer that match on word boundaries.
        for term in self.terminals.values():
            if isinstance(term.recognizer, StringRecognizer):
                match = keyword_rec(term.recognizer.value, 0)
                if match == term.recognizer.value:
                    term.recognizer = RegExRecognizer(
                        rf"\b{match}\b", ignore_case=term.recognizer.ignore_case
                    )
                    term.keyword = True

    def _resolve_actions(self, action_overrides=None, fail_on_no_resolve=False):
        """
        Checks and resolves semantic actions given in the grammar and
        additional `*_actions.py` module.

        Args:
            action_overrides(dict): Dict of actions that take precendence. Used
                for actions supplied during parser construction.
        """
        import parglare.actions as actmodule

        for symbol in self:
            # Resolve trying from most specific to least specific
            action = None

            # 1. Resolve by fully qualified symbol name
            if "." in symbol.fqn:
                if action_overrides:
                    action = action_overrides.get(symbol.fqn, None)

                if action is None:
                    action = self.resolve_action_by_name(symbol.fqn)

            # 2. Fully qualified action name
            if (
                action is None
                and symbol.action_fqn is not None
                and "." in symbol.action_fqn
            ):
                if action_overrides:
                    action = action_overrides.get(symbol.action_fqn, None)

                if action is None:
                    action = self.resolve_action_by_name(symbol.action_fqn)

            # 3. Symbol name
            if action is None:
                if action_overrides:
                    action = action_overrides.get(symbol.name, None)

                if action is None:
                    action = self.resolve_action_by_name(symbol.name)

            # 4. Action name
            if action is None and symbol.action_name is not None:
                if action_overrides:
                    action = action_overrides.get(symbol.action_name, None)

                if action is None:
                    action = self.resolve_action_by_name(symbol.action_name)

                # 5. Try to find action in built-in actions module.
                if action is None:
                    action_name = symbol.action_name
                    if hasattr(actmodule, action_name):
                        action = getattr(actmodule, action_name)

            if symbol.action_name and action is None and fail_on_no_resolve:
                raise ParserInitError(
                    f'Action "{symbol.action_name}" given for rule "{symbol.name}" '
                    "doesn't exists in parglare common actions and "
                    'is not provided using "actions" parameter.'
                )

            if action is not None:
                symbol.action = action

                # Some sanity checks for actions
                if isinstance(symbol.action, list):
                    if isinstance(symbol, Terminal):
                        raise ParserInitError(
                            f'Cannot use a list of actions for terminal "{symbol.name}".'
                        )
                    else:
                        if len(symbol.action) != len(symbol.productions):
                            raise ParserInitError(
                                "Length of list of actions must match the "
                                "number of productions for non-terminal "
                                f'"{symbol.name}".'
                            )
            else:
                symbol.action = symbol.grammar_action

    def _connect_override_recognizers(self):
        for term in self.terminals.values():
            if self.recognizers and term.fqn in self.recognizers:
                term.recognizer = self.recognizers[term.fqn]
            else:
                if term.recognizer is None:
                    if not self.recognizers:
                        raise GrammarError(
                            location=term.location,
                            message=f'Terminal "{term.fqn}" has no recognizer defined '
                            "and no recognizers are given during grammar "
                            "construction.",
                        )
                    else:
                        if term.fqn not in self.recognizers:
                            raise GrammarError(
                                location=term.location,
                                message=f'Terminal "{term.fqn}" '
                                "has no recognizer defined.",
                            )

    def get_terminal(self, name):
        "Returns terminal with the given fully qualified name or name."
        return self.terminals.get(name)

    def get_nonterminal(self, name):
        "Returns non-terminal with the given fully qualified name or name."
        return self.nonterminals.get(name)

    def get_productions(self, name):
        "Returns production for the given symbol"
        return [p for p in self.productions if p.symbol.fqn == name]

    def get_symbol(self, name):
        "Returns grammar symbol with the given name."
        s = self.get_terminal(name)
        if not s:
            s = self.get_nonterminal(name)
        return s

    def __iter__(self):
        return (
            s
            for s in itertools.chain(self.nonterminals.values(), self.terminals.values())
            if s not in [AUGSYMBOL, STOP]
        )

    def get_production_id(self, name):
        "Returns first production id for the given symbol name"
        for p in self.productions:
            if p.symbol.fqn == name:
                return p.prod_id

    @staticmethod
    def from_struct(productions, start_symbol=None):
        """Used internally to bootstrap grammar file parser."""
        productions, terminals = create_productions_terminals(productions)
        return Grammar(productions, terminals=terminals, start_symbol=start_symbol)

    @staticmethod
    def _parse(
        parse_fun_name,
        what_to_parse,
        recognizers=None,
        ignore_case=False,
        re_flags=re.MULTILINE,
        debug=False,
        debug_parse=False,
        debug_colors=False,
        _no_check_recognizers=False,
    ):
        extra = GrammarContext(
            debug=debug,
            debug_colors=debug_colors,
            ignore_case=ignore_case,
            re_flags=re_flags,
        )
        grammar_parser = get_grammar_parser(debug_parse, debug_colors)
        imports, productions, terminals, classes = getattr(
            grammar_parser, parse_fun_name
        )(what_to_parse, extra=extra)
        g = Grammar(
            productions=productions,
            terminals=terminals,
            classes=classes,
            imports=imports,
            recognizers=recognizers,
            file_path=what_to_parse if parse_fun_name == "parse_file" else None,
            _no_check_recognizers=_no_check_recognizers,
        )
        termui.colors = debug_colors
        if debug:
            g.print_debug()

        return g

    @staticmethod
    def from_string(grammar_str, **kwargs):
        return Grammar._parse("parse", grammar_str, **kwargs)

    @staticmethod
    def from_file(file_name, **kwargs):
        file_name = path.realpath(file_name)
        return Grammar._parse("parse_file", file_name, **kwargs)

    def print_debug(self):
        a_print("*** GRAMMAR ***", new_line=True)
        h_print("Terminals:")
        prints(" ".join([str(t) for t in self.terminals]))
        h_print("NonTerminals:")
        prints(" ".join([str(n) for n in self.nonterminals]))

        h_print("Productions:")
        for p in self.productions:
            prints(str(p))


class PGFileImport:
    """
    Represents import of a grammar file.

    Attributes:
    module_name (str): Name of this import. By default is the name of grammar
        file without .pg extension.
    file_path (str): A canonical full path of the imported .pg file.
    context: grammar parsing context state.
    imported_with (PGFileImport | None): First import this import is
        imported from. Used for FQN calculation.
    grammar (Grammar | None): Grammar object under construction.
    pgfile (PGFile instance or None):

    """

    def __init__(self, module_name: str, file_path: str, context: GrammarContext):
        self.module_name = module_name
        self.file_path: str = file_path
        self.context = context
        self.imported_with: Optional[PGFileImport] = context.imported_with
        self.grammar: Optional[Grammar] = None
        self.pgfile: Optional[PGFile] = None

    @property
    def fqn(self):
        "A fully qualified name of the import following the first import path."
        if self.imported_with:
            return f"{self.imported_with.fqn}.{self.module_name}"
        return self.module_name

    def load_pgfile(self):
        if self.pgfile is None:
            # First search the global registry of imported files.
            if self.file_path in self.grammar.imported_files:
                self.pgfile = self.grammar.imported_files[self.file_path]
            else:
                # If not found construct new PGFile
                context = copy.copy(self.context)
                context.file_name = self.file_path
                context.inline_terminals = {}
                context.imported_with = self
                imports, productions, terminals, classes = get_grammar_parser(
                    self.context.debug, self.context.debug_colors
                ).parse_file(self.file_path, extra=context)
                self.pgfile = PGFile(
                    productions=productions,
                    terminals=terminals,
                    classes=classes,
                    imports=imports,
                    grammar=self.grammar,
                    imported_with=self,
                    file_path=self.file_path,
                )

    def resolve_symbol_by_name(self, symbol_name, location=None):
        "Resolves symbol from the imported file."

        return self.pgfile.resolve_symbol_by_name(symbol_name, location)

    def resolve_action_by_name(self, action_name):
        "Resolves action from the imported file."

        return self.pgfile.resolve_action_by_name(action_name)


def create_productions_terminals(productions):
    """Creates Production instances from the list of productions given in
    the form:
    [LHS, RHS, optional ASSOC, optional PRIOR].
    Where LHS is grammar symbol and RHS is a list or tuple of grammar
    symbols from the right-hand side of the production.
    """
    gp = []
    inline_terminals = {}
    for p in productions:
        assoc = ASSOC_NONE
        prior = DEFAULT_PRIORITY
        symbol = p[0]
        if not isinstance(symbol, NonTerminal):
            raise GrammarError(
                location=None,
                message=f"Invalid production symbol '{symbol}' for production '{str(p)}'",
            )
        rhs = ProductionRHS(p[1])
        if len(p) > 2:
            assoc = p[2]
        if len(p) > 3:
            prior = p[3]

        # Convert strings to string recognizers
        for idx, t in enumerate(rhs):
            if isinstance(t, str):
                if t not in inline_terminals:
                    inline_terminals[t] = Terminal(recognizer=StringRecognizer(t), name=t)
                rhs[idx] = Reference(
                    location=None, name=t, imported_with=symbol.imported_with
                )
            elif isinstance(t, Terminal):
                if t.name not in inline_terminals:
                    inline_terminals[t.name] = t
                rhs[idx] = Reference(
                    location=None,
                    name=t.name,
                    imported_with=symbol.imported_with,
                )

        gp.append(Production(symbol, rhs, assoc=assoc, prior=prior))

    return gp, list(inline_terminals.values())


def make_multiplicity_fqn(symbol_name, multiplicity=None, separator_name=None):
    if multiplicity is None or multiplicity == MULT_ONE:
        return symbol_name
    name_by_mult = {
        MULT_ZERO_OR_MORE: "0",
        MULT_ONE_OR_MORE: "1",
        MULT_OPTIONAL: "opt",
    }
    if multiplicity:
        return "{}_{}{}".format(
            symbol_name,
            name_by_mult[multiplicity],
            f"_{separator_name}" if separator_name else "",
        )


def check_name(context, name):
    """
    Used in actions to check for reserved names usage.
    """

    if name in RESERVED_SYMBOL_NAMES:
        raise GrammarError(
            location=Location(context),
            message=f'Rule name "{name}" is reserved.',
        )


# Grammar for grammars

(
    PGFILE,
    IMPORTS,
    IMPORT,
    PRODUCTION_RULES,
    PRODUCTION_RULE,
    PRODUCTION_RULE_WITH_ACTION,
    PRODUCTION_RULE_RHS,
    PRODUCTION,
    PRODUCTION_GROUP,
    TERMINAL_RULES,
    TERMINAL_RULE,
    TERMINAL_RULE_WITH_ACTION,
    PROD_META_DATA,
    PROD_META_DATAS,
    TERM_META_DATA,
    TERM_META_DATAS,
    USER_META_DATA,
    CONST,
    ASSIGNMENT,
    ASSIGNMENTS,
    PLAIN_ASSIGNMENT,
    BOOL_ASSIGNMENT,
    GSYMBOL_REFERENCE,
    OPT_REP_OPERATOR,
    REP_OPERATOR,
    OPT_REP_MODIFIERS_EXP,
    OPT_REP_MODIFIERS,
    OPT_REP_MODIFIER,
    GSYMBOL,
    RECOGNIZER,
    LAYOUT,
    LAYOUT_ITEM,
    COMMENT,
    CORNC,
    CORNCS,
) = (
    NonTerminal(name)
    for name in [
        "PGFile",
        "Imports",
        "Import",
        "ProductionRules",
        "ProductionRule",
        "ProductionRuleWithAction",
        "ProductionRuleRHS",
        "Production",
        "ProductionGroup",
        "TerminalRules",
        "TerminalRule",
        "TerminalRuleWithAction",
        "ProductionMetaData",
        "ProductionMetaDatas",
        "TerminalMetaData",
        "TerminalMetaDatas",
        "UserMetaData",
        "Const",
        "Assignment",
        "Assignments",
        "PlainAssignment",
        "BoolAssignment",
        "GrammarSymbolReference",
        "OptRepeatOperator",
        "RepeatOperator",
        "OptionalRepeatModifiersExpression",
        "OptionalRepeatModifiers",
        "OptionalRepeatModifier",
        "GrammarSymbol",
        "Recognizer",
        "LAYOUT",
        "LAYOUT_ITEM",
        "Comment",
        "CORNC",
        "CORNCS",
    ]
)

pg_terminals = (
    NAME,
    REGEX_TERM,
    INT_CONST,
    FLOAT_CONST,
    BOOL_CONST,
    STR_CONST,
    ACTION,
    WS,
    COMMENTLINE,
    NOTCOMMENT,
) = [
    Terminal(name, RegExRecognizer(regex))
    for name, regex in [
        ("Name", r"[a-zA-Z_][a-zA-Z0-9_\.]*"),
        ("RegExTerm", r"\/(\\.|[^\/\\])*\/"),
        ("IntConst", r"\d+"),
        (
            "FloatConst",
            r"""[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?(?<=[\w\.])(?![\w\.])""",
        ),  # noqa
        ("BoolConst", r"true|false"),
        (
            "StrConst",
            r"""(?s)('[^'\\]*(?:\\.[^'\\]*)*')|"""
            r"""("[^"\\]*(?:\\.[^"\\]*)*")""",
        ),
        ("Action", r"@[a-zA-Z0-9_]+"),
        ("WS", r"\s+"),
        ("CommentLine", r"\/\/.*"),
        ("NotComment", r"((\*[^\/])|[^\s*\/]|\/[^\*])+"),
    ]
]

pg_productions = [
    [PGFILE, [PRODUCTION_RULES]],
    [PGFILE, [IMPORTS, PRODUCTION_RULES]],
    [PGFILE, [PRODUCTION_RULES, "terminals", TERMINAL_RULES]],
    [PGFILE, [IMPORTS, PRODUCTION_RULES, "terminals", TERMINAL_RULES]],
    [PGFILE, ["terminals", TERMINAL_RULES]],
    [IMPORTS, [IMPORTS, IMPORT]],
    [IMPORTS, [IMPORT]],
    [IMPORT, ["import", STR_CONST, ";"]],
    [IMPORT, ["import", STR_CONST, "as", NAME, ";"]],
    [PRODUCTION_RULES, [PRODUCTION_RULES, PRODUCTION_RULE_WITH_ACTION]],
    [PRODUCTION_RULES, [PRODUCTION_RULE_WITH_ACTION]],
    [PRODUCTION_RULE_WITH_ACTION, [ACTION, PRODUCTION_RULE]],
    [PRODUCTION_RULE_WITH_ACTION, [PRODUCTION_RULE]],
    [PRODUCTION_RULE, [NAME, ":", PRODUCTION_RULE_RHS, ";"]],
    [
        PRODUCTION_RULE,
        [NAME, "{", PROD_META_DATAS, "}", ":", PRODUCTION_RULE_RHS, ";"],
    ],
    [
        PRODUCTION_RULE_RHS,
        [PRODUCTION_RULE_RHS, "|", PRODUCTION],
        ASSOC_LEFT,
        5,
    ],
    [PRODUCTION_RULE_RHS, [PRODUCTION], ASSOC_LEFT, 5],
    [PRODUCTION, [ASSIGNMENTS]],
    [PRODUCTION, [ASSIGNMENTS, "{", PROD_META_DATAS, "}"]],
    [TERMINAL_RULES, [TERMINAL_RULES, TERMINAL_RULE_WITH_ACTION]],
    [TERMINAL_RULES, [TERMINAL_RULE_WITH_ACTION]],
    [TERMINAL_RULE_WITH_ACTION, [ACTION, TERMINAL_RULE]],
    [TERMINAL_RULE_WITH_ACTION, [TERMINAL_RULE]],
    [TERMINAL_RULE, [NAME, ":", RECOGNIZER, ";"], ASSOC_LEFT, 15],
    [TERMINAL_RULE, [NAME, ":", ";"], ASSOC_LEFT, 15],
    [
        TERMINAL_RULE,
        [NAME, ":", RECOGNIZER, "{", TERM_META_DATAS, "}", ";"],
        ASSOC_LEFT,
        15,
    ],
    [
        TERMINAL_RULE,
        [NAME, ":", "{", TERM_META_DATAS, "}", ";"],
        ASSOC_LEFT,
        15,
    ],
    [PROD_META_DATA, ["left"]],
    [PROD_META_DATA, ["reduce"]],
    [PROD_META_DATA, ["right"]],
    [PROD_META_DATA, ["shift"]],
    [PROD_META_DATA, ["dynamic"]],
    [PROD_META_DATA, ["nops"]],  # no prefer shifts
    [PROD_META_DATA, ["nopse"]],  # no prefer shifts over empty
    [PROD_META_DATA, [INT_CONST]],  # priority
    [PROD_META_DATA, [USER_META_DATA]],
    [PROD_META_DATAS, [PROD_META_DATAS, ",", PROD_META_DATA], ASSOC_LEFT],
    [PROD_META_DATAS, [PROD_META_DATA]],
    [TERM_META_DATA, ["prefer"]],
    [TERM_META_DATA, ["finish"]],
    [TERM_META_DATA, ["nofinish"]],
    [TERM_META_DATA, ["dynamic"]],
    [TERM_META_DATA, [INT_CONST]],  # priority
    [TERM_META_DATA, [USER_META_DATA]],
    [TERM_META_DATAS, [TERM_META_DATAS, ",", TERM_META_DATA]],
    [TERM_META_DATAS, [TERM_META_DATA]],
    # User custom meta-data
    [USER_META_DATA, [NAME, ":", CONST]],
    [CONST, [INT_CONST]],
    [CONST, [FLOAT_CONST]],
    [CONST, [BOOL_CONST]],
    [CONST, [STR_CONST]],
    # Assignments
    [ASSIGNMENT, [PLAIN_ASSIGNMENT]],
    [ASSIGNMENT, [BOOL_ASSIGNMENT]],
    [ASSIGNMENT, [GSYMBOL_REFERENCE]],
    [ASSIGNMENTS, [ASSIGNMENTS, ASSIGNMENT]],
    [ASSIGNMENTS, [ASSIGNMENT]],
    [PLAIN_ASSIGNMENT, [NAME, "=", GSYMBOL_REFERENCE]],
    [BOOL_ASSIGNMENT, [NAME, "?=", GSYMBOL_REFERENCE]],
    # Groups
    [PRODUCTION_GROUP, ["(", PRODUCTION_RULE_RHS, ")"]],
    # Regex-like repeat operators
    [GSYMBOL_REFERENCE, [GSYMBOL, OPT_REP_OPERATOR]],
    [GSYMBOL_REFERENCE, [PRODUCTION_GROUP, OPT_REP_OPERATOR]],
    [OPT_REP_OPERATOR, [REP_OPERATOR]],
    [OPT_REP_OPERATOR, [EMPTY]],
    [REP_OPERATOR, ["*", OPT_REP_MODIFIERS_EXP]],
    [REP_OPERATOR, ["*!", OPT_REP_MODIFIERS_EXP]],
    [REP_OPERATOR, ["+", OPT_REP_MODIFIERS_EXP]],
    [REP_OPERATOR, ["+!", OPT_REP_MODIFIERS_EXP]],
    [REP_OPERATOR, ["?", OPT_REP_MODIFIERS_EXP]],
    [REP_OPERATOR, ["?!", OPT_REP_MODIFIERS_EXP]],
    [OPT_REP_MODIFIERS_EXP, ["[", OPT_REP_MODIFIERS, "]"]],
    [OPT_REP_MODIFIERS_EXP, [EMPTY]],
    [OPT_REP_MODIFIERS, [OPT_REP_MODIFIERS, ",", OPT_REP_MODIFIER]],
    [OPT_REP_MODIFIERS, [OPT_REP_MODIFIER]],
    [OPT_REP_MODIFIER, [NAME]],
    [GSYMBOL, [NAME]],
    [GSYMBOL, [STR_CONST]],
    [RECOGNIZER, [STR_CONST]],
    [RECOGNIZER, [REGEX_TERM]],
    # Support for comments,
    [LAYOUT, [LAYOUT_ITEM]],
    [LAYOUT, [LAYOUT, LAYOUT_ITEM]],
    [LAYOUT, [EMPTY]],
    [LAYOUT_ITEM, [WS]],
    [LAYOUT_ITEM, [COMMENT]],
    [COMMENT, ["/*", CORNCS, "*/"]],
    [COMMENT, [COMMENTLINE]],
    [CORNCS, [CORNC]],
    [CORNCS, [CORNCS, CORNC]],
    [CORNCS, [EMPTY]],
    [CORNC, [COMMENT]],
    [CORNC, [NOTCOMMENT]],
    [CORNC, [WS]],
]


grammar_parser = None


def get_grammar_parser(debug, debug_colors):
    global grammar_parser
    if not grammar_parser:
        from parglare import Parser

        grammar_parser = Parser(
            Grammar.from_struct(pg_productions, PGFILE),
            actions=pg_actions,
            debug=debug,
            debug_colors=debug_colors,
        )
    EMPTY.action = pass_none
    return grammar_parser


def act_pgfile(context, nodes):
    imports, productions, terminals = [], [], []
    while nodes:
        first = nodes.pop(0)
        if first and isinstance(first, list):
            if isinstance(first[0], PGFileImport):
                imports = first
            elif isinstance(first[0], Production):
                productions = first
            elif isinstance(first[0], Terminal):
                terminals = first

    for terminal in context.extra.inline_terminals.values():
        terminals.append(terminal)

    return [imports, productions, terminals, context.extra.classes]


def act_import(context, nodes):
    if not context.file_name:
        raise GrammarError(
            location=Location(context),
            message="Import can be used only for grammars defined in files.",
        )
    import_path = nodes[1]
    module_name = nodes[3] if len(nodes) > 3 else None
    if module_name is None:
        module_name = path.splitext(path.basename(import_path))[0]
    if not path.isabs(import_path):
        import_path = path.realpath(
            path.join(path.dirname(context.file_name), import_path)
        )
    else:
        import_path = path.realpath(import_path)

    return PGFileImport(module_name, import_path, context.extra)


def act_production_rules(_, nodes):
    e1, e2 = nodes
    e1.extend(e2)
    return e1


def act_production_rule_with_action(_, nodes):
    productions, group_productions = nodes[-1]
    if len(nodes) > 1:
        action_name = nodes[0]
        # Strip @ char
        action_name = action_name[1:]
        for p in productions:
            p.symbol.action_name = action_name
    productions.extend(group_productions)
    return productions


def act_production_rule(context, nodes):
    if len(nodes) == 4:
        # No meta-data
        name, _, rhs_prods, __ = nodes
        rule_meta_datas = {}
    else:
        name, rule_meta_datas, rhs_prods = nodes[0], nodes[2], nodes[5]
        rule_meta_datas = get_production_rule_meta_datas(rule_meta_datas)

    check_name(context, name)

    prods = _create_prods(context, rhs_prods, name, rule_meta_datas)
    group_prods = []
    if context.extra.groups:
        counter = context.extra.groups_counter
        while context.extra.groups:
            ref, gprods = context.extra.groups.pop()
            gname = f"{name}_g{counter[name] + 1}"
            ref.name = gname
            counter[name] += 1
            group_prods.extend(_create_prods(context, gprods, gname, rule_meta_datas))

    return prods, group_prods


def _create_prods(context, rhs_prods, name, rule_meta_datas):
    symbol = NonTerminal(
        name,
        location=Location(context),
        imported_with=context.extra.imported_with,
        user_meta=rule_meta_datas.get("user_meta", None),
    )

    # Collect all productions for this rule
    prods = []
    attrs = {}
    for prod in rhs_prods:
        assignments, meta_datas = prod
        # Here we know the indexes of assignments
        for idx, a in enumerate(assignments):
            if a.name:
                a.index = idx
        gsymbols = (a.symbol for a in assignments)
        assoc = meta_datas.get("assoc", rule_meta_datas.get("assoc", ASSOC_NONE))
        prior = meta_datas.get(
            "priority", rule_meta_datas.get("priority", DEFAULT_PRIORITY)
        )
        dynamic = meta_datas.get("dynamic", rule_meta_datas.get("dynamic", False))
        nops = meta_datas.get("nops", rule_meta_datas.get("nops", False))
        nopse = meta_datas.get("nopse", rule_meta_datas.get("nopse", False))

        # User meta-data if formed by rule-level user meta-data with overrides
        # from production-level user meta-data.
        user_meta = dict(rule_meta_datas.get("user_meta", {}))
        user_meta.update(meta_datas.get("user_meta", {}))
        prods.append(
            Production(
                symbol,
                ProductionRHS(gsymbols),
                assignments=assignments,
                assoc=assoc,
                prior=prior,
                dynamic=dynamic,
                nops=nops,
                nopse=nopse,
                user_meta=user_meta,
            )
        )

        for a in assignments:
            if a.name:
                attrs[a.name] = PGAttribute(a.name, a.multiplicity, a.symbol_name)
            # TODO: check/handle multiple assignments to the same attribute
            #       If a single production have multiple assignment of the
            #       same attribute, multiplicity must be set to many.

    # If named matches are used create Python class that will be used
    # for object instantiation.
    if attrs:

        class ParglareClass(metaclass=ParglareMetaClass):
            """Dynamically created class. Each parglare rule that uses named
            matches by default uses this action that will create Python object
            of this class.

            Attributes:
                _pg_attrs(dict): A dict of meta-attributes keyed by name.
                    Used by common rules.
                _pg_start_position(int): A position in the input string where
                    this class is defined.
                _pg_end_position(int): A position in the input string where
                    this class ends.
                _pg_children(list): A list of child nodes.
                _pg_children_names(list): A list of child node names
                    (i.e. LHS of assignments)
                _pg_extras(object): An arbitrary user-defined object.

            """

            __slots__ = list(attrs) + [
                "_pg_start_position",
                "_pg_end_position",
                "_pg_children",
                "_pg_children_names",
                "_pg_extras",
            ]

            _pg_attrs = attrs

            def __init__(self, **attrs):
                self._pg_children = list(attrs.values())
                self._pg_children_names = list(attrs.keys())
                for attr_name, attr_value in attrs.items():
                    setattr(self, attr_name, attr_value)

            def __repr__(self):
                if hasattr(self, "name"):
                    return f"<{name}:{self.name}>"
                else:
                    return f"<parglare:{name} instance at {hex(id(self))}>"

            def to_str(self):
                def visit(n, subresults, depth):
                    indent = "  " * (depth + 1)
                    if hasattr(n, "_pg_children"):
                        s = "{} [{}->{}]\n{}".format(
                            n.__class__.__name__,
                            n._pg_start_position,
                            n._pg_end_position,
                            "\n".join(
                                [
                                    f"{indent}{n._pg_children_names[i]}={subresult}"
                                    for (i, subresult) in enumerate(subresults)
                                ]
                            ),
                        )
                    elif isinstance(n, list):
                        s = "{}[\n{}\n{}]".format(
                            indent,
                            "\n".join([f"{indent}{el}" for el in subresults]),
                            indent,
                        )
                    else:
                        s = repr(n)
                    return s

                return visitor(self, ast_tree_iterator, visit)

        ParglareClass.__name__ = str(symbol.fqn)
        if symbol.fqn in context.extra.classes:
            # If rule has multiple definition merge attributes.
            context.extra.classes[symbol.fqn]._pg_attrs.update(attrs)
        else:
            context.extra.classes[symbol.fqn] = ParglareClass

        symbol.action_name = "obj"

    return prods


def get_production_rule_meta_datas(raw_meta_datas):
    meta_datas = {}
    for meta_data in raw_meta_datas:
        if meta_data in ["left", "reduce"]:
            meta_datas["assoc"] = ASSOC_LEFT
        elif meta_data in ["right", "shift"]:
            meta_datas["assoc"] = ASSOC_RIGHT
        elif meta_data == "dynamic":
            meta_datas["dynamic"] = True
        elif meta_data == "nops":
            meta_datas["nops"] = True
        elif meta_data == "nopse":
            meta_datas["nopse"] = True
        elif isinstance(meta_data, int):
            meta_datas["priority"] = meta_data
        else:
            # User meta-data
            assert isinstance(meta_data, list)
            name, _, value = meta_data
            meta_datas.setdefault("user_meta", {})[name] = value
    return meta_datas


def act_production(_, nodes):
    assignments = nodes[0]
    meta_datas = {}
    if len(nodes) > 1:
        meta_datas = get_production_rule_meta_datas(nodes[2])

    return (assignments, meta_datas)


def act_production_group(context, nodes):
    # Group name will be known when the grammar rule is
    # reduced so store these production for later.
    productions = nodes[1]
    reference = Reference(Location(context), "resolving", context.extra.imported_with)
    context.extra.groups.append((reference, productions))
    return reference


def _set_term_props(term, props):
    for t in props:
        if isinstance(t, int):
            term.prior = t
        elif isinstance(t, list):
            # User meta-data
            name, _, value = t
            term.add_user_meta_data(name, value)
        elif t == "finish":
            term.finish = True
        elif t == "nofinish":
            term.finish = False
        elif t == "prefer":
            term.prefer = True
        elif t == "dynamic":
            term.dynamic = True
        else:
            print(t)
            raise AssertionError()


def act_term_rule(context, nodes):
    name = nodes[0]
    recognizer = nodes[2]

    check_name(context, name)
    term = Terminal(
        name,
        recognizer,
        location=Location(context),
        imported_with=context.extra.imported_with,
    )
    if len(nodes) > 4:
        _set_term_props(term, nodes[4])
    return term


def act_term_rule_empty_body(context, nodes):
    name = nodes[0]

    check_name(context, name)
    term = Terminal(
        name,
        location=Location(context),
        imported_with=context.extra.imported_with,
    )
    term.recognizer = None
    if len(nodes) > 3:
        _set_term_props(term, nodes[3])
    return term


def act_term_rule_with_action(context, nodes):
    if len(nodes) > 1:
        action_name, term = nodes
        # Strip @ char
        action_name = action_name[1:]
        term.action_name = action_name
    else:
        term = nodes[0]

    return term


def act_gsymbol_reference(context, nodes):
    """Repetition operators (`*`, `+`, `?`) will create additional productions in
    the grammar with name generated from original symbol name and suffixes:
    - `_0` - for `*`
    - `_1` - for `+`
    - `_opt` - for `?`

    Zero or more produces `one or more` productions and additional productions
    of the form:

    ```
    somerule_0: somerule_1 | EMPTY;
    ```

    In addition if separator is used another suffix is added which is the name
    of the separator rule, for example:

    ```
    spam*[comma] --> spam_0_comma and spam_1_comma
    spam+[comma] --> spam_1_comma
    spam* --> spam_0 and spam_1
    spam? --> spam_opt
    ```

    """
    symbol_ref, rep_op = nodes
    if rep_op:
        if len(rep_op) > 1:
            rep_op, modifiers = rep_op
        else:
            rep_op = rep_op[0]
            modifiers = None

        sep_ref = None
        if modifiers:
            sep_ref = modifiers[1]
            sep_ref = Reference(Location(context), sep_ref, context.extra.imported_with)
            symbol_ref.separator = sep_ref

        if rep_op.startswith("*"):
            symbol_ref.multiplicity = MULT_ZERO_OR_MORE
        elif rep_op.startswith("+"):
            symbol_ref.multiplicity = MULT_ONE_OR_MORE
        else:
            symbol_ref.multiplicity = MULT_OPTIONAL

        if rep_op.endswith("!"):
            symbol_ref.greedy = True

    return symbol_ref


def act_gsymbol_string_recognizer(context, nodes):
    recognizer = act_recognizer_str(context, nodes)

    terminal_ref = Reference(
        Location(context), recognizer.name, context.extra.imported_with
    )

    if terminal_ref.name not in context.extra.inline_terminals:
        check_name(context, terminal_ref.name)
        context.extra.inline_terminals[terminal_ref.name] = Terminal(
            terminal_ref.name, recognizer, location=Location(context)
        )

    return terminal_ref


def act_assignment(_, nodes):
    gsymbol_reference = nodes[0]
    if isinstance(gsymbol_reference, list):
        # Named match
        name, op, gsymbol_reference = gsymbol_reference
    else:
        name, op = None, None

    return Assignment(name, op, gsymbol_reference)


def act_recognizer_str(context, nodes):
    value = nodes[0]
    value = (
        value.replace(r"\"", '"')
        .replace(r"\'", "'")
        .replace(r"\\", "\\")
        .replace(r"\n", "\n")
        .replace(r"\t", "\t")
    )
    return StringRecognizer(value, ignore_case=context.extra.ignore_case)


def act_recognizer_regex(context, nodes):
    value = nodes[0]
    return RegExRecognizer(
        value,
        re_flags=context.extra.re_flags,
        ignore_case=context.extra.ignore_case,
    )


def act_str_term(context, value):
    value = value[1:-1]
    value = value.replace(r"\\", "\\")
    value = value.replace(r"\'", "'")
    return value


def act_regex_term(context, value):
    return value[1:-1]


pg_actions = {
    "PGFile": act_pgfile,
    "Imports": collect,
    "Import": act_import,
    "ProductionRules": [act_production_rules, pass_single],
    "ProductionRule": act_production_rule,
    "ProductionRuleWithAction": act_production_rule_with_action,
    "ProductionRuleRHS": collect_sep,
    "Production": act_production,
    "ProductionGroup": act_production_group,
    "TerminalRules": collect,
    "TerminalRule": [
        act_term_rule,
        act_term_rule_empty_body,
        act_term_rule,
        act_term_rule_empty_body,
    ],
    "TerminalRuleWithAction": act_term_rule_with_action,
    "ProductionMetaDatas": collect_sep,
    "TerminalMetaDatas": collect_sep,
    "Assignment": act_assignment,
    "Assignments": collect,
    "GrammarSymbolReference": act_gsymbol_reference,
    "GrammarSymbol": [
        lambda context, nodes: Reference(
            Location(context), nodes[0], context.extra.imported_with
        ),
        act_gsymbol_string_recognizer,
    ],
    "Recognizer": [act_recognizer_str, act_recognizer_regex],
    "StrConst": act_str_term,
    "RegExTerm": act_regex_term,
    # Constants
    "IntConst": lambda _, value: int(value),
    "FloatConst": lambda _, value: float(value),
    "BoolConst": lambda _, value: value and value.lower() == "true",
}


class ParglareMetaClass(type):
    def __repr__(cls):
        return f"<parglare:{cls.__name__} class at {id(cls)}>"


def ast_tree_iterator(root):
    if hasattr(root, "_pg_children"):
        return iter(root._pg_children)
    if isinstance(root, list):
        return iter(root)
    return iter([])
