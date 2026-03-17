

class Tagger(object):
    tags = []

    def __call__(self, tokens):
        raise NotImplementedError

    def check_tag(self, tag):
        return tag in self.tags


class PassTagger(Tagger):
    def __call__(self, tokens):
        for token in tokens:
            yield token


class TaggersComposition(Tagger):
    def __init__(self, taggers):
        self.taggers = taggers

    def __call__(self, tokens):
        for tagger in self.taggers:
            tokens = tagger(tokens)
        return tokens

    def check_tag(self, tag):
        return any(
            _.check_tag(tag)
            for _ in self.taggers
        )
