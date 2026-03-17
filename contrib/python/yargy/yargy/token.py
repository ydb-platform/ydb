
from .record import Record
from .span import Span


class Token(Record):
    __attributes__ = ['value', 'span', 'type']

    def __init__(self, value, span, type):
        self.value = value
        self.span = span
        self.type = type

    @property
    def normalized(self):
        return self.value.lower()

    def morphed(self, forms):
        return MorphToken(
            self.value, self.span, self.type,
            forms
        )

    def tagged(self, tag):
        return TagToken(
            self.value, self.span, self.type,
            tag
        )


def is_token(item):
    return isinstance(item, Token)


class MorphToken(Token):
    __attributes__ = ['value', 'span', 'type', 'forms']

    def __init__(self, value, span, type, forms):
        Token.__init__(self, value, span, type)
        self.forms = forms

    @property
    def normalized(self):
        form = self.forms[0]
        return form.normalized

    def tagged(self, tag):
        return MorphTagToken(
            self.value, self.span, self.type,
            tag, self.forms
        )

    def constrained(self, forms):
        return MorphToken(
            self.value, self.span, self.type,
            forms
        )


def is_morph_token(item):
    return isinstance(item, MorphToken)


class TagToken(Token):
    __attributes__ = ['value', 'span', 'type', 'tag']

    def __init__(self, value, span, type, tag):
        Token.__init__(self, value, span, type)
        self.tag = tag


def is_tag_token(item):
    return isinstance(item, TagToken)


class MorphTagToken(MorphToken, TagToken):
    __attributes__ = ['value', 'span', 'type', 'tag', 'forms']

    def __init__(self, value, span, type, tag, forms):
        Token.__init__(self, value, span, type)
        self.tag = tag
        self.forms = forms

    def constrained(self, forms):
        return MorphTagToken(
            self.value, self.span, self.type,
            self.tag, forms
        )


def format_tokens(tokens):
    previous = None
    for token in tokens:
        if previous:
            _, stop = previous.span
            start, _ = token.span
            if start - stop > 0:
                yield ' '
        previous = token
        yield token.value


def join_tokens(tokens):
    return ''.join(format_tokens(tokens))


def normalize_token(token):
    return Token(
        token.normalized,
        token.span,
        token.type
    )


def join_normalized_tokens(tokens):
    return join_tokens(
        normalize_token(_)
        for _ in tokens
    )


def inflect_token(token, grams):
    if is_morph_token(token):
        form = token.forms[0]
        value = form.inflect(grams)
    else:
        value = token.normalized
    return Token(
        value,
        token.span,
        token.type
    )


def join_inflected_tokens(tokens, grams):
    return join_tokens(
        inflect_token(_, grams)
        for _ in tokens
    )


def get_tokens_span(tokens):
    head, tail = tokens[0], tokens[-1]
    return Span(head.span.start, tail.span.stop)
