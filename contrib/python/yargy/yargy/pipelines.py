
from collections import defaultdict

from .record import Record
from .check import assert_type
from .rule.constructors import Production
from .rule.bnf import BNFRule
from .token import is_morph_token
from .predicates.bank import (
    eq,
    caseless,
    DictionaryPredicate
)


class PipelineBNFRule(BNFRule):
    abbr = 'pipeline'

    def __init__(self, productions):
        productions = list(productions)
        super(PipelineBNFRule, self).__init__(productions)
        self.index = self.build_index(self.productions)

    def build_index(self, productions):
        index = defaultdict(list)
        for production in productions:
            term = production.terms[0]
            index[term.value].append(production)
        return dict(index)

    def predict(self, token):
        if token.value in self.index:
            for production in self.index[token.value]:
                yield production

    def __str__(self):
        return '{name} -> {abbr}'.format(
            name=self.label,
            abbr=self.abbr
        )


class CaselessPipelineBNFRule(PipelineBNFRule):
    abbr = 'caseless_pipeline'

    def predict(self, token):
        value = token.value.lower()
        if value in self.index:
            for production in self.index[value]:
                yield production


class MorphPipelineBNFRule(PipelineBNFRule):
    abbr = 'morph_pipeline'

    def build_index(self, productions):
        index = defaultdict(list)
        for production in productions:
            term = production.terms[0]
            for value in term.value:
                index[value].append(production)
        return dict(index)

    def predict(self, token):
        if is_morph_token(token):
            normalized = {_.normalized for _ in token.forms}
            for value in normalized:
                if value in self.index:
                    for production in self.index[value]:
                        yield production
        else:
            value = token.normalized
            if value in self.index:
                for production in self.index[value]:
                    yield production


class Key(Record):
    __attributes__ = ['value', 'terms']

    def __init__(self, value, terms):
        self.value = value
        self.terms = terms


class PipelineProduction(Production):
    __attributes__ = ['value', 'terms']

    def __init__(self, value, terms):
        self.value = value
        super(PipelineProduction, self).__init__(terms)


class Pipeline(Record):
    __attributes__ = ['keys']

    predicate = eq
    bnf = PipelineBNFRule

    def __init__(self, keys):
        self.keys = list(keys)

    def activate(self, _):
        return self

    @property
    def productions(self):
        for key in self.keys:
            yield PipelineProduction(
                key.value,
                [self.predicate(_) for _ in key.terms]
            )

    @property
    def as_bnf(self):
        return self.bnf(self.productions)

    @property
    def label(self):
        return self.__class__.__name__


class CaselessPipeline(Pipeline):
    predicate = caseless
    bnf = CaselessPipelineBNFRule


class MorphPipeline(Pipeline):
    predicate = DictionaryPredicate
    bnf = MorphPipelineBNFRule


class PipelineScheme(Pipeline):
    __attributes__ = ['lines']

    pipeline = Pipeline
    label = '[pipeline]'

    def __init__(self, lines):
        lines = list(lines)
        for line in lines:
            assert_type(line, str)
        self.lines = lines

    def get_key(self, line, tokenizer):
        return Key(
            line,
            tokenizer.split(line)
        )

    def activate(self, context):
        return self.pipeline(
            self.get_key(_, context.tokenizer)
            for _ in self.lines
        )


class CaselessPipelineScheme(PipelineScheme):
    pipeline = CaselessPipeline
    label = '[caseless_pipeline]'


class MorphPipelineScheme(PipelineScheme):
    pipeline = MorphPipeline
    label = '[morph_pipeline]'

    def get_key(self, line, tokenizer):
        parts = tokenizer.split(line)
        return Key(
            line,
            [tokenizer.morph.normalized(_) for _ in parts]
        )


def pipeline(lines):
    from .rule.constructors import PipelineRule
    return PipelineRule(PipelineScheme(lines))


def caseless_pipeline(lines):
    from .rule.constructors import PipelineRule
    return PipelineRule(CaselessPipelineScheme(lines))


def morph_pipeline(lines):
    from .rule.constructors import PipelineRule
    return PipelineRule(MorphPipelineScheme(lines))
