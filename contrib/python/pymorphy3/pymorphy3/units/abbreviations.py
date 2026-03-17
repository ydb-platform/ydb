"""
Analyzer units for abbreviated words
------------------------------------
"""
from pymorphy3.units.base import BaseAnalyzerUnit


class _InitialsAnalyzer(BaseAnalyzerUnit):
    def __init__(self, letters, tag_pattern=None, score=0.1):
        if tag_pattern is None:
            if hasattr(self, 'TAG_PATTERN'):
                tag_pattern = self.TAG_PATTERN
            else:
                raise ValueError("Please provide tag_pattern.")
        self.tag_pattern = tag_pattern
        self.score = score
        self.letters = letters
        self._letters_set = set(letters)

    def init(self, morph):
        super().init(morph)
        self._init_grammemes(self.morph.TagClass)
        self._tags = self._get_gender_case_tags(self.tag_pattern)

    def _init_grammemes(self, tag_cls):
        tag_cls.add_grammemes_to_known('Init', 'иниц', overwrite=False)

    def _get_gender_case_tags(self, pattern):
        return [
            self.morph.TagClass(pattern % {'gender': gender, 'case': case})
            for gender in ['masc', 'femn']
            for case in ['nomn', 'gent', 'datv', 'accs', 'ablt', 'loct']
        ]

    def parse(self, word, word_lower, seen_parses):
        if word not in self._letters_set:
            return []
        return [
            (word_lower, tag, word_lower, self.score, ((self, word),))
            for tag in self._tags
        ]

    def tag(self, word, word_lower, seen_tags):
        if word not in self._letters_set:
            return []
        return self._tags[:]


class AbbreviatedFirstNameAnalyzer(_InitialsAnalyzer):
    TAG_PATTERN = 'NOUN,anim,%(gender)s,Sgtm,Name,Fixd,Abbr,Init sing,%(case)s'

    def init(self, morph):
        super().init(morph)
        self._tags_masc = [tag for tag in self._tags if 'masc' in tag]
        self._tags_femn = [tag for tag in self._tags if 'femn' in tag]
        assert self._tags_masc + self._tags_femn == self._tags

    def _init_grammemes(self, tag_cls):
        super()._init_grammemes(tag_cls)
        self.morph.TagClass.add_grammemes_to_known('Name', 'имя', overwrite=False)

    def get_lexeme(self, form):
        # 2 lexemes: masc and femn
        fixed_word, form_tag, normal_form, score, methods_stack = form
        tags = self._tags_masc if 'masc' in form_tag else self._tags_femn
        return [
            (fixed_word, tag, normal_form, score, methods_stack)
            for tag in tags
        ]

    def normalized(self, form):
        # don't normalize female names to male names
        fixed_word, form_tag, normal_form, score, methods_stack = form
        tags = self._tags_masc if 'masc' in form_tag else self._tags_femn
        return fixed_word, tags[0], normal_form, score, methods_stack


class AbbreviatedPatronymicAnalyzer(_InitialsAnalyzer):
    TAG_PATTERN = 'NOUN,anim,%(gender)s,Sgtm,Patr,Fixd,Abbr,Init sing,%(case)s'

    def _init_grammemes(self, tag_cls):
        super()._init_grammemes(tag_cls)
        self.morph.TagClass.add_grammemes_to_known('Patr', 'отч', overwrite=False)

    def get_lexeme(self, form):
        fixed_word, _, normal_form, score, methods_stack = form
        return [
            (fixed_word, tag, normal_form, score, methods_stack)
            for tag in self._tags
        ]

    def normalized(self, form):
        fixed_word, _, normal_form, score, methods_stack = form
        return fixed_word, self._tags[0], normal_form, score, methods_stack
