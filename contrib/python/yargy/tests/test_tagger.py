
from yargy import Parser
from yargy.tagger import Tagger
from yargy.predicates import tag


INSIDE = 'I'
OUTSIDE = 'O'


class MyTagger(Tagger):
    tags = {INSIDE, OUTSIDE}

    def __call__(self, tokens):
        for index, token in enumerate(tokens):
            yield token.tagged(
                OUTSIDE
                if index % 3 == 0
                else INSIDE
            )


def test_tagger():
    text = 'a b c d e f g'
    A = tag('I').repeatable()
    parser = Parser(A, tagger=MyTagger())

    matches = parser.findall(text)
    spans = [_.span for _ in matches]
    substrings = [
        text[start:stop]
        for start, stop in spans
    ]
    assert substrings == ['b c', 'e f']
