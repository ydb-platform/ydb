from pymorphy3.units.base import BaseAnalyzerUnit


class UnknAnalyzer(BaseAnalyzerUnit):
    """
    Add an UNKN parse if other analyzers returned nothing.
    This allows to always have at least one parse result.
    """
    def init(self, morph):
        super().init(morph)
        self.morph.TagClass.add_grammemes_to_known('UNKN', 'НЕИЗВ')
        self._tag = self.morph.TagClass('UNKN')

    def parse(self, word, word_lower, seen_parses):
        if seen_parses:
            return []

        methods = ((self, word),)
        return [(word_lower, self._tag, word_lower, 1.0, methods)]

    def tag(self, word, word_lower, seen_tags):
        if seen_tags:
            return []
        return [self._tag]

    def get_lexeme(self, form):
        return [form]

    def normalized(self, form):
        return form
