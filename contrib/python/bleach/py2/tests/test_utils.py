from collections import OrderedDict

from bleach.utils import alphabetize_attributes


class TestAlphabeticalAttributes:
    def test_empty_cases(self):
        assert alphabetize_attributes(None) is None

        assert alphabetize_attributes({}) == {}

    def test_ordering(self):
        assert alphabetize_attributes({(None, "a"): 1, (None, "b"): 2}) == OrderedDict(
            [((None, "a"), 1), ((None, "b"), 2)]
        )
        assert alphabetize_attributes({(None, "b"): 1, (None, "a"): 2}) == OrderedDict(
            [((None, "a"), 2), ((None, "b"), 1)]
        )

    def test_different_namespaces(self):
        assert alphabetize_attributes(
            {("xlink", "href"): "abc", (None, "alt"): "123"}
        ) == OrderedDict([((None, "alt"), "123"), (("xlink", "href"), "abc")])
