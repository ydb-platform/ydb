from __future__ import annotations

import sys
from array import array

import pytest

from rapidfuzz import fuzz_cpp, fuzz_py, utils_cpp, utils_py
from rapidfuzz.distance import ScoreAlignment

from tests.common import symmetric_scorer_tester


class fuzz:
    @staticmethod
    def ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def partial_ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.partial_ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.partial_ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def partial_ratio_alignment(*args, **kwargs):
        dist1 = fuzz_cpp.partial_ratio_alignment(*args, **kwargs)
        dist2 = fuzz_py.partial_ratio_alignment(*args, **kwargs)
        if dist1 is None or dist2 is None:
            assert dist1 == dist2
        else:
            assert pytest.approx(dist1[0]) == dist2[0]
            assert list(dist1)[1:] == list(dist2)[1:]
        return dist1

    @staticmethod
    def token_sort_ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.token_sort_ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.token_sort_ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def token_set_ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.token_set_ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.token_set_ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def token_ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.token_ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.token_ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def partial_token_sort_ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.partial_token_sort_ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.partial_token_sort_ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def partial_token_set_ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.partial_token_set_ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.partial_token_set_ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def partial_token_ratio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.partial_token_ratio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.partial_token_ratio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def WRatio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.WRatio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.WRatio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1

    @staticmethod
    def QRatio(*args, **kwargs):
        dist1 = symmetric_scorer_tester(fuzz_cpp.QRatio, *args, **kwargs)
        dist2 = symmetric_scorer_tester(fuzz_py.QRatio, *args, **kwargs)
        assert pytest.approx(dist1) == dist2
        return dist1


scorers = [
    fuzz.ratio,
    fuzz.partial_ratio,
    fuzz.token_sort_ratio,
    fuzz.token_set_ratio,
    fuzz.token_ratio,
    fuzz.partial_token_sort_ratio,
    fuzz.partial_token_set_ratio,
    fuzz.partial_token_ratio,
    fuzz.WRatio,
    fuzz.QRatio,
]


def test_no_processor():
    assert fuzz.ratio("new york mets", "new york mets") == 100
    assert fuzz.ratio("new york mets", "new YORK mets") != 100


def test_partial_ratio():
    assert fuzz.partial_ratio("new york mets", "the wonderful new york mets") == 100


def test_token_sort_ratio():
    assert fuzz.token_sort_ratio("new york mets", "new york mets") == 100


def testPartialTokenSortRatio():
    assert fuzz.partial_token_sort_ratio("new york mets", "new york mets") == 100
    assert fuzz.partial_token_sort_ratio("new york mets vs atlanta braves", "atlanta braves vs new york mets") == 100


def testTokenSetRatio():
    assert fuzz.token_set_ratio("new york mets vs atlanta braves", "atlanta braves vs new york mets") == 100
    assert fuzz.token_set_ratio("js", "vue js") == 100


def testPartialTokenSetRatio():
    assert fuzz.partial_token_set_ratio("new york mets vs atlanta braves", "atlanta braves vs new york mets") == 100


def testQuickRatioEqual():
    assert fuzz.QRatio("new york mets", "new york mets", processor=utils_cpp.default_process) == 100
    assert fuzz.QRatio("new york mets", "new york mets", processor=utils_py.default_process) == 100


def testQuickRatioCaseInsensitive():
    assert fuzz.QRatio("new york mets", "new YORK mets", processor=utils_cpp.default_process) == 100
    assert fuzz.QRatio("new york mets", "new YORK mets", processor=utils_py.default_process) == 100


def testQuickRatioNotEqual():
    assert fuzz.QRatio("new york mets", "the wonderful new york mets") != 100


def testWRatioEqual():
    assert fuzz.WRatio("new york mets", "new york mets", processor=utils_cpp.default_process) == 100
    assert fuzz.WRatio("new york mets", "new york mets", processor=utils_py.default_process) == 100


def testWRatioCaseInsensitive():
    assert fuzz.WRatio("new york mets", "new YORK mets", processor=utils_cpp.default_process) == 100
    assert fuzz.WRatio("new york mets", "new YORK mets", processor=utils_py.default_process) == 100


def testWRatioPartialMatch():
    # a partial match is scaled by .9
    assert fuzz.WRatio("new york mets", "the wonderful new york mets") == 90


def testWRatioMisorderedMatch():
    # misordered full matches are scaled by .95
    assert fuzz.WRatio("new york mets vs atlanta braves", "atlanta braves vs new york mets") == 95


def testWRatioUnicode():
    assert fuzz.WRatio("new york mets", "new york mets") == 100


def test_issue452():
    assert pytest.approx(fuzz.WRatio("hello", "hello" + "abcde" * 7)) == 90


def testQRatioUnicode():
    assert fuzz.WRatio("new york mets", "new york mets") == 100


def test_issue76():
    assert pytest.approx(fuzz.partial_ratio("physics 2 vid", "study physics physics 2")) == 81.81818
    assert fuzz.partial_ratio("physics 2 vid", "study physics physics 2 video") == 100


def test_issue90():
    assert pytest.approx(fuzz.partial_ratio("ax b", "a b a c b")) == 85.71428


def test_issue138():
    str1 = "a" * 65
    str2 = "a" + chr(256) + "a" * 63
    assert pytest.approx(fuzz.partial_ratio(str1, str2)) == 99.22481


def test_partial_ratio_alignment():
    a = "a certain string"
    s = "certain"
    assert fuzz.partial_ratio_alignment(s, a) == ScoreAlignment(100, 0, len(s), 2, 2 + len(s))
    assert fuzz.partial_ratio_alignment(a, s) == ScoreAlignment(100, 2, 2 + len(s), 0, len(s))
    assert fuzz.partial_ratio_alignment(None, "test") is None
    assert fuzz.partial_ratio_alignment("test", None) is None

    assert fuzz.partial_ratio_alignment("test", "tesx", score_cutoff=90) is None


def test_issue196():
    """
    fuzz.WRatio did not work correctly with score_cutoffs
    """
    assert pytest.approx(fuzz.WRatio("South Korea", "North Korea")) == 81.81818
    assert fuzz.WRatio("South Korea", "North Korea", score_cutoff=85.4) == 0.0
    assert fuzz.WRatio("South Korea", "North Korea", score_cutoff=85.5) == 0.0


def test_issue231():
    str1 = "er merkantilismus förderte handle und verkehr mit teils marktkonformen, teils dirigistischen maßnahmen."
    str2 = "ils marktkonformen, teils dirigistischen maßnahmen. an der schwelle zum 19. jahrhundert entstand ein neu"

    alignment = fuzz.partial_ratio_alignment(str1, str2)
    assert alignment.src_start == 0
    assert alignment.src_end == 103
    assert alignment.dest_start == 0
    assert alignment.dest_end == 51


def test_empty_string():
    """
    when both strings are empty this is either a perfect match or no match
    See https://github.com/rapidfuzz/RapidFuzz/issues/110
    """
    # perfect match
    assert fuzz.ratio("", "") == 100
    assert fuzz.partial_ratio("", "") == 100
    assert fuzz.token_sort_ratio("", "") == 100
    assert fuzz.partial_token_sort_ratio("", "") == 100
    assert fuzz.token_ratio("", "") == 100
    assert fuzz.partial_token_ratio("", "") == 100

    # no match
    assert fuzz.WRatio("", "") == 0
    assert fuzz.QRatio("", "") == 0
    assert fuzz.token_set_ratio("", "") == 0
    assert fuzz.partial_token_set_ratio("", "") == 0

    # perfect match when no words
    assert fuzz.token_set_ratio("    ", "    ") == 0
    assert fuzz.partial_token_set_ratio("    ", "    ") == 0


@pytest.mark.parametrize("scorer", scorers)
def test_invalid_input(scorer):
    """
    when invalid types are passed to a scorer an exception should be thrown
    """
    with pytest.raises(TypeError):
        scorer(1, 1, catch_exceptions=True)


def unicodeArray(text):
    if sys.version_info >= (3, 13, 0):
        return array("w", list(text))
    return array("u", text)


@pytest.mark.parametrize("scorer", scorers)
def test_array(scorer):
    """
    arrays should be supported and treated in a compatible way to strings
    """
    # todo add support in pure python implementation
    assert scorer(
        unicodeArray("the wonderful new york mets"),
        unicodeArray("the wonderful new york mets"),
    )
    assert scorer("the wonderful new york mets", unicodeArray("the wonderful new york mets"))
    assert scorer(unicodeArray("the wonderful new york mets"), "the wonderful new york mets")


@pytest.mark.parametrize("scorer", scorers)
def test_bytes(scorer):
    """
    bytes should be supported and treated in a compatible way to strings
    """
    assert scorer(b"the wonderful new york mets", b"the wonderful new york mets") == 100
    assert scorer("the wonderful new york mets", b"the wonderful new york mets") == 100
    assert scorer(b"the wonderful new york mets", "the wonderful new york mets") == 100


@pytest.mark.parametrize("scorer", scorers)
def test_none_string(scorer):
    """
    when None is passed to a scorer the result should always be 0
    """
    assert scorer("test", None) == 0
    assert scorer(None, "test") == 0


@pytest.mark.parametrize("scorer", scorers)
def test_nan_string(scorer):
    """
    when float("nan") is passed to a scorer the result should always be 0
    """
    assert scorer("test", float("nan")) == 0
    assert scorer(float("nan"), "test") == 0


@pytest.mark.parametrize("scorer", scorers)
def test_simple_unicode_tests(scorer):
    """
    some very simple tests using unicode with scorers
    to catch relatively obvious implementation errors
    """
    s1 = "ÁÄ"
    s2 = "ABCD"
    assert scorer(s1, s2) == 0
    assert scorer(s1, s1) == 100


@pytest.mark.parametrize(
    "processor",
    [
        utils_cpp.default_process,
        lambda s: utils_cpp.default_process(s),
        utils_py.default_process,
        lambda s: utils_py.default_process(s),
    ],
)
@pytest.mark.parametrize("scorer", scorers)
def test_scorer_case_insensitive(processor, scorer):
    """
    each scorer should be able to preprocess strings properly
    """
    assert scorer("new york mets", "new YORK mets", processor=processor) == 100


@pytest.mark.parametrize("processor", [None, lambda s: s])
def test_ratio_case_censitive(processor):
    assert fuzz.ratio("new york mets", "new YORK mets", processor=processor) != 100


@pytest.mark.parametrize("scorer", scorers)
def test_custom_processor(scorer):
    """
    Any scorer should accept any type as s1 and s2, as long as it is a string
    after preprocessing.
    """
    s1 = ["chicago cubs vs new york mets", "CitiField", "2011-05-11", "8pm"]
    s2 = ["chicago cubs vs new york mets", "CitiFields", "2012-05-11", "9pm"]
    s3 = ["different string", "CitiFields", "2012-05-11", "9pm"]
    assert scorer(s1, s2, processor=lambda event: event[0]) == 100
    assert scorer(s2, s3, processor=lambda event: event[0]) != 100


@pytest.mark.parametrize("scorer", scorers)
def testIssue206(scorer):
    """
    test correct behavior of score_cutoff
    """
    score1 = scorer("South Korea", "North Korea")
    score2 = scorer("South Korea", "North Korea", score_cutoff=score1 - 0.0001)
    assert score1 == score2


@pytest.mark.parametrize("scorer", scorers)
def test_help(scorer):
    """
    test that all help texts can be printed without throwing an exception,
    since they are implemented in C++ as well
    """
    help(scorer)


def testIssue257():
    s1 = "aaaaaaaaaaaaaaaaaaaaaaaabacaaaaaaaabaaabaaaaaaaababbbbbbbbbbabbcb"
    s2 = "aaaaaaaaaaaaaaaaaaaaaaaababaaaaaaaabaaabaaaaaaaababbbbbbbbbbabbcb"
    score = fuzz.partial_ratio(s1, s2)
    assert pytest.approx(score) == 98.46153846153847
    score = fuzz.partial_ratio(s2, s1)
    assert pytest.approx(score) == 98.46153846153847
