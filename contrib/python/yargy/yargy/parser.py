
from collections import defaultdict

from .record import Record
from .check import assert_type

from .token import get_tokens_span
from .span import resolve_spans
from .tree import (
    Node,
    Leaf,
    Tree
)
from .tokenizer import (
    Tokenizer,
    MorphTokenizer
)
from .tagger import (
    Tagger,
    PassTagger
)
from .rule.bnf import is_rule


class Chart(object):
    def __init__(self, tokens):
        self.tokens = list(tokens)

        self.columns = [Column(0, None)]
        for index, token in enumerate(self.tokens, 1):
            self.columns.append(Column(index, token))

    def matches(self, rule):
        for column in self.columns:
            for state in column.matches(rule):
                yield state

    def __iter__(self):
        size = len(self.columns)
        for index in range(size):
            column = self.columns[index]
            next_column = None
            if index + 1 < size:
                next_column = self.columns[index + 1]
            yield column, next_column

    @property
    def last_column(self):
        return self.columns[len(self.columns) - 1]

    def __getitem__(self, index):
        return self.columns[index]

    def __repr__(self):
        return 'Chart({columns!r}, ...)'.format(
            columns=self.columns
        )

    @property
    def source(self):
        for column in self.columns:
            for line in column.source:
                yield line
            yield ''

    def _repr_pretty_(self, printer, cycle):
        for line in self.source:
            printer.text(line)
            printer.break_()


class Column(object):
    def __init__(self, index, token):
        self.index = index
        self.token = token
        self.states = []
        self.hashes = set()
        self.states_index = defaultdict(list)

    def __iter__(self):
        return iter(self.states)

    def matches(self, rule):
        for state in self.states:
            if state.completed and id(state.rule) == id(rule):
                yield state

    def append(self, state):
        value = hash(state)
        if value not in self.hashes:
            self.hashes.add(value)
            self.states.append(state)
            self.update_index(state)

    def update_index(self, state):
        if not state.completed:
            next_term = state.next_term
            if is_rule(next_term):
                self.states_index[id(next_term)].append(state)

    def __repr__(self):
        return 'Column({index!r}, {token!r}, ...)'.format(
            index=self.index,
            token=self.token
        )

    @property
    def first(self):
        return self.index == 0

    @property
    def source(self):
        yield '{index!r} {token!r}'.format(
            index=self.index,
            token=self.token
        )
        yield '----------------'
        for state in self.states:
            yield str(state)

    def _repr_pretty_(self, printer, cycle):
        for line in self.source:
            printer.text(line)
            printer.break_()


class State(object):
    def __init__(self, rule, production, dot_index,
                 start_column, stop_column,
                 node):
        self.rule = rule
        self.production = production
        self.dot_index = dot_index
        self.start_column = start_column
        self.stop_column = stop_column
        self.node = node

    def __hash__(self):
        return hash((
            id(self.rule), id(self.production), self.dot_index,
            self.start_column.index, self.stop_column.index,
            tuple(id(_) for _ in self.node.children)
        ))

    @property
    def completed(self):
        return self.dot_index >= len(self.production.terms)

    @property
    def next_term(self):
        return self.production.terms[self.dot_index]

    @property
    def parents(self):
        return self.start_column.states_index[id(self.rule)]

    @property
    def range(self):
        return self.start_column.index, self.stop_column.index

    def __str__(self):
        terms = self.production.terms
        production = ' '.join(
            [_.label for _ in terms[:self.dot_index]]
            + ['$']
            + [_.label for _ in terms[self.dot_index:]]
        )
        return '[{start}:{stop}] {name} -> {production}'.format(
            start=self.start_column.index,
            stop=self.stop_column.index,
            name=self.rule.label,
            production=production,
        )


class Match(Record):
    __attributes__ = ['tokens', 'span']

    def __init__(self, tree):
        self.tree = tree
        self.tokens = [_.token for _ in tree.walk(types=Leaf)]
        self.span = get_tokens_span(self.tokens)

    @property
    def rule(self):
        return self.tree.root.rule

    @property
    def fact(self):
        fact = self.tree.interpret()
        return fact.normalized


def prepare_trees(states):
    for state in states:
        yield Tree(
            state.node,
            state.range
        )


def prepare_match(tree):
    tree = tree.normalized
    relations = tree.relations
    if relations.validate():
        tree = tree.constrain(relations)
        return Match(tree)


def prepare_matches(states):
    for state in states:
        match = prepare_match(state)
        if match:
            yield match


def prepare_resolved_matches(trees):
    spans = []
    span_matches = {}
    for tree in trees:
        span = tree.range
        if span not in span_matches:
            match = prepare_match(tree)
            if match:
                spans.append(span)
                span_matches[span] = match

    for span in resolve_spans(spans):
        yield span_matches[span]


class Context(Record):
    __attributes__ = ['tokenizer', 'tagger']

    def __init__(self, tokenizer, tagger=None):
        self.tokenizer = tokenizer
        self.tagger = tagger


class Parser(object):
    def __init__(self, rule, tokenizer=None, tagger=None):
        if not tokenizer:
            tokenizer = MorphTokenizer()
        assert_type(tokenizer, Tokenizer)
        self.tokenizer = tokenizer

        if not tagger:
            tagger = PassTagger()
        assert_type(tagger, Tagger)
        self.tagger = tagger

        context = Context(tokenizer, tagger)
        rule = rule.activate(context)
        rule = rule.normalized
        self.rule = rule.as_bnf.start

    def chart(self, text, all=True):
        tokens = self.tokenizer(text)
        tokens = self.tagger(tokens)
        chart = Chart(tokens)
        for column, next_column in chart:
            if column.first or all:
                self.predict(column, next_column, self.rule)
            for state in column:
                if state.completed:
                    self.complete(column, state)
                else:
                    next_term = state.next_term
                    if is_rule(next_term):
                        self.predict(column, next_column, next_term)
                    elif next_column:
                        self.scan(next_column, next_term, state)
        return chart

    def matches(self, text, all=True):
        chart = self.chart(text, all=all)
        return (
            chart
            if all
            else chart.last_column
        ).matches(self.rule)

    def extract(self, text, all=True):
        states = self.matches(text, all=all)
        trees = prepare_trees(states)
        return prepare_matches(trees)

    def findall(self, text):
        states = self.matches(text)
        trees = prepare_trees(states)
        trees = sorted(trees)
        return prepare_resolved_matches(trees)

    def find(self, text):
        for match in self.findall(text):
            return match

    def match(self, text):
        states = self.matches(text, all=False)
        trees = prepare_trees(states)
        trees = sorted(trees)
        for match in prepare_matches(trees):
            return match

    def predict(self, column, next_column, rule):
        productions = (
            rule.predict(next_column.token)
            if next_column
            else rule.productions
        )
        for index, production in enumerate(productions):
            node = Node(
                rule, production,
                rank=index,
                children=[]
            )
            state = State(
                rule, production,
                dot_index=0,
                start_column=column,
                stop_column=column,
                node=node
            )
            column.append(state)

    def scan(self, column, predicate, state):
        token = column.token
        if predicate(token):
            leaf = Leaf(predicate, predicate.constrain(token))
            state = State(
                state.rule, state.production,
                dot_index=state.dot_index + 1,
                start_column=state.start_column,
                stop_column=column,
                node=state.node.attached(leaf)
            )
            column.append(state)

    def complete(self, column, completed):
        for state in completed.parents:
            state = State(
                state.rule, state.production,
                dot_index=state.dot_index + 1,
                start_column=state.start_column,
                stop_column=column,
                node=state.node.attached(completed.node)
            )
            column.append(state)
