from __future__ import annotations

import pytest

from rapidfuzz import fuzz, process_cpp, process_py
from rapidfuzz.distance import DamerauLevenshtein, Levenshtein, Levenshtein_py
from rapidfuzz.utils import default_process


def wrapped(func):
    from functools import wraps

    @wraps(func)
    def decorator(*args, **kwargs):
        return 100

    return decorator


class process:
    @staticmethod
    def extract_iter(*args, **kwargs):
        res1 = process_cpp.extract_iter(*args, **kwargs)
        res2 = process_py.extract_iter(*args, **kwargs)

        for elem1, elem2 in zip(res1, res2):
            assert elem1 == elem2
            yield elem1

    @staticmethod
    def extractOne(*args, **kwargs):
        res1 = process_cpp.extractOne(*args, **kwargs)
        res2 = process_py.extractOne(*args, **kwargs)
        assert res1 == res2
        return res1

    @staticmethod
    def extract(*args, **kwargs):
        res1 = process_cpp.extract(*args, **kwargs)
        res2 = process_py.extract(*args, **kwargs)
        assert res1 == res2
        return res1

    @staticmethod
    def cdist(*args, **kwargs):
        import numpy as np

        res1 = process_cpp.cdist(*args, **kwargs)
        res2 = process_py.cdist(*args, **kwargs)
        assert res1.dtype == res2.dtype
        assert res1.shape == res2.shape
        if res1.size and res2.size:
            assert np.array_equal(res1, res2)
        return res1

    @staticmethod
    def cpdist(*args, **kwargs):
        import numpy as np

        res1 = process_cpp.cpdist(*args, **kwargs)
        res2 = process_py.cpdist(*args, **kwargs)
        assert res1.dtype == res2.dtype
        assert res1.shape == res2.shape
        if res1.size and res2.size:
            assert np.array_equal(res1, res2)
        return res1


baseball_strings = [
    "new york mets vs chicago cubs",
    "chicago cubs vs chicago white sox",
    "philladelphia phillies vs atlanta braves",
    "braves vs mets",
]


def test_extractOne_exceptions():
    with pytest.raises(TypeError):
        process_cpp.extractOne()
    with pytest.raises(TypeError):
        process_py.extractOne()
    with pytest.raises(TypeError):
        process_cpp.extractOne(1)
    with pytest.raises(TypeError):
        process_py.extractOne(1)
    with pytest.raises(TypeError):
        process_cpp.extractOne(1, [""])
    with pytest.raises(TypeError):
        process_py.extractOne(1, [""])
    with pytest.raises(TypeError):
        process_cpp.extractOne("", [1])
    with pytest.raises(TypeError):
        process_py.extractOne("", [1])
    with pytest.raises(TypeError):
        process_cpp.extractOne("", {1: 1})
    with pytest.raises(TypeError):
        process_py.extractOne("", {1: 1})


def test_extract_exceptions():
    with pytest.raises(TypeError):
        process_cpp.extract()
    with pytest.raises(TypeError):
        process_py.extract()
    with pytest.raises(TypeError):
        process_cpp.extract(1)
    with pytest.raises(TypeError):
        process_py.extract(1)
    with pytest.raises(TypeError):
        process_cpp.extract(1, [""])
    with pytest.raises(TypeError):
        process_py.extract(1, [""])
    with pytest.raises(TypeError):
        process_cpp.extract("", [1])
    with pytest.raises(TypeError):
        process_py.extract("", [1])
    with pytest.raises(TypeError):
        process_cpp.extract("", {1: 1})
    with pytest.raises(TypeError):
        process_py.extract("", {1: 1})


def test_extract_iter_exceptions():
    with pytest.raises(TypeError):
        process_cpp.extract_iter()
    with pytest.raises(TypeError):
        process_py.extract_iter()
    with pytest.raises(TypeError):
        process_cpp.extract_iter(1)
    with pytest.raises(TypeError):
        process_py.extract_iter(1)
    with pytest.raises(TypeError):
        next(process_cpp.extract_iter(1, [""]))
    with pytest.raises(TypeError):
        next(process_py.extract_iter(1, [""]))
    with pytest.raises(TypeError):
        next(process_cpp.extract_iter("", [1]))
    with pytest.raises(TypeError):
        next(process_py.extract_iter("", [1]))
    with pytest.raises(TypeError):
        next(process_cpp.extract_iter("", {1: 1}))
    with pytest.raises(TypeError):
        next(process_py.extract_iter("", {1: 1}))


def test_get_best_choice1():
    query = "new york mets at atlanta braves"
    best = process.extractOne(query, baseball_strings)
    assert best[0] == "braves vs mets"
    best = process.extractOne(query, set(baseball_strings))
    assert best[0] == "braves vs mets"

    best = process.extract(query, baseball_strings)[0]
    assert best[0] == "braves vs mets"
    best = process.extract(query, set(baseball_strings))[0]
    assert best[0] == "braves vs mets"


def test_get_best_choice2():
    query = "philadelphia phillies at atlanta braves"
    best = process.extractOne(query, baseball_strings)
    assert best[0] == baseball_strings[2]
    best = process.extractOne(query, set(baseball_strings))
    assert best[0] == baseball_strings[2]

    best = process.extract(query, baseball_strings)[0]
    assert best[0] == baseball_strings[2]
    best = process.extract(query, set(baseball_strings))[0]
    assert best[0] == baseball_strings[2]


def test_get_best_choice3():
    query = "atlanta braves at philadelphia phillies"
    best = process.extractOne(query, baseball_strings)
    assert best[0] == baseball_strings[2]
    best = process.extractOne(query, set(baseball_strings))
    assert best[0] == baseball_strings[2]

    best = process.extract(query, baseball_strings)[0]
    assert best[0] == baseball_strings[2]
    best = process.extract(query, set(baseball_strings))[0]
    assert best[0] == baseball_strings[2]


def test_get_best_choice4():
    query = "chicago cubs vs new york mets"
    best = process.extractOne(query, baseball_strings)
    assert best[0] == baseball_strings[0]
    best = process.extractOne(query, set(baseball_strings))
    assert best[0] == baseball_strings[0]


def test_with_processor():
    """
    extractOne should accept any type as long as it is a string
    after preprocessing
    """
    events = [
        ["chicago cubs vs new york mets", "CitiField", "2011-05-11", "8pm"],
        ["new york yankees vs boston red sox", "Fenway Park", "2011-05-11", "8pm"],
        ["atlanta braves vs pittsburgh pirates", "PNC Park", "2011-05-11", "8pm"],
    ]
    query = events[0]

    best = process.extractOne(query, events, processor=lambda event: event[0])
    assert best[0] == events[0]

    best = process.extract(query, events, processor=lambda event: event[0])[0]
    assert best[0] == events[0]

    eventsDict = dict(enumerate(events))
    best = process.extractOne(query, eventsDict, processor=lambda event: event[0])
    assert best[0] == events[0]

    best = process.extract(query, eventsDict, processor=lambda event: event[0])[0]
    assert best[0] == events[0]

    best = process.extractOne("new york mets", ["new YORK mets"])
    assert 72 < best[1] < 73

    best = process.extract("new york mets", ["new YORK mets"])[0]
    assert 72 < best[1] < 73

    best = process.extractOne("new york mets", ["new YORK mets"], processor=default_process)
    assert best[1] == 100

    best = process.extract("new york mets", ["new YORK mets"], processor=default_process)[0]
    assert best[1] == 100


def test_with_scorer():
    choices = [
        "new york mets vs chicago cubs",
        "chicago cubs at new york mets",
        "atlanta braves vs pittsbugh pirates",
        "new york yankees vs boston red sox",
    ]

    choices_mapping = {
        1: "new york mets vs chicago cubs",
        2: "chicago cubs at new york mets",
        3: "atlanta braves vs pittsbugh pirates",
        4: "new york yankees vs boston red sox",
    }

    # in this hypothetical example we care about ordering, so we use quick ratio
    query = "new york mets at chicago cubs"

    # first, as an example, the normal way would select the "more 'complete' match of choices[1]"
    best = process.extractOne(query, choices)
    assert best[0] == choices[1]
    best = process.extract(query, choices)[0]
    assert best[0] == choices[1]
    # dict
    best = process.extractOne(query, choices_mapping)
    assert best[0] == choices_mapping[2]
    best = process.extract(query, choices_mapping)[0]
    assert best[0] == choices_mapping[2]

    # now, use the custom scorer
    best = process.extractOne(query, choices, scorer=fuzz.QRatio)
    assert best[0] == choices[0]
    best = process.extract(query, choices, scorer=fuzz.QRatio)[0]
    assert best[0] == choices[0]
    # dict
    best = process.extractOne(query, choices_mapping, scorer=fuzz.QRatio)
    assert best[0] == choices_mapping[1]
    best = process.extract(query, choices_mapping, scorer=fuzz.QRatio)[0]
    assert best[0] == choices_mapping[1]


def test_with_cutoff():
    choices = [
        "new york mets vs chicago cubs",
        "chicago cubs at new york mets",
        "atlanta braves vs pittsbugh pirates",
        "new york yankees vs boston red sox",
    ]

    query = "los angeles dodgers vs san francisco giants"

    # in this situation, this is an event that does not exist in the list
    # we don't want to randomly match to something, so we use a reasonable cutoff
    best = process.extractOne(query, choices, score_cutoff=50)
    assert best is None

    # however if we had no cutoff, something would get returned
    best = process.extractOne(query, choices)
    assert best is not None


def test_with_cutoff_edge_cases():
    choices = [
        "new york mets vs chicago cubs",
        "chicago cubs at new york mets",
        "atlanta braves vs pittsbugh pirates",
        "new york yankees vs boston red sox",
    ]

    query = "new york mets vs chicago cubs"
    # Only find 100-score cases
    best = process.extractOne(query, choices, score_cutoff=100)
    assert best is not None
    assert best[0] == choices[0]

    # 0-score cases do not return None
    best = process.extractOne("", choices)
    assert best is not None
    assert best[1] == 0


def test_none_elements():
    """
    when a None element is used, it is skipped and the index is still correct
    """
    # no processor
    best = process.extractOne("test", [None, "tes"])
    assert best[2] == 1
    best = process.extractOne(None, [None, "tes"])
    assert best is None
    best = process.extractOne("test", {0: None, 1: "tes"})
    assert best[2] == 1
    best = process.extractOne(None, {0: None, 1: "tes"})
    assert best is None

    # C++ processor
    best = process.extractOne("test", [None, "tes"], processor=default_process)
    assert best[2] == 1
    best = process.extractOne(None, [None, "tes"], processor=default_process)
    assert best is None
    best = process.extractOne("test", {0: None, 1: "tes"}, processor=default_process)
    assert best[2] == 1
    best = process.extractOne(None, {0: None, 1: "tes"}, processor=default_process)
    assert best is None

    # python processor
    best = process.extractOne("test", [None, "tes"], processor=lambda s: s)
    assert best[2] == 1
    best = process.extractOne(None, [None, "tes"], processor=lambda s: s)
    assert best is None
    best = process.extractOne("test", {0: None, 1: "tes"}, processor=lambda s: s)
    assert best[2] == 1
    best = process.extractOne(None, {0: None, 1: "tes"}, processor=lambda s: s)
    assert best is None

    # no processor
    best = process.extract("test", [None, "tes"])
    assert best[0][2] == 1
    best = process.extract(None, [None, "tes"])
    assert best == []
    best = process.extract("test", {0: None, 1: "tes"})
    assert best[0][2] == 1
    best = process.extract(None, {0: None, 1: "tes"})
    assert best == []

    # C++ processor
    best = process.extract("test", [None, "tes"], processor=default_process)
    assert best[0][2] == 1
    best = process.extract(None, [None, "tes"], processor=default_process)
    assert best == []
    best = process.extract("test", {0: None, 1: "tes"}, processor=default_process)
    assert best[0][2] == 1
    best = process.extract(None, {0: None, 1: "tes"}, processor=default_process)
    assert best == []

    # python processor
    best = process.extract("test", [None, "tes"], processor=lambda s: s)
    assert best[0][2] == 1
    best = process.extract(None, [None, "tes"], processor=lambda s: s)
    assert best == []
    best = process.extract("test", {0: None, 1: "tes"}, processor=lambda s: s)
    assert best[0][2] == 1
    best = process.extract(None, {0: None, 1: "tes"}, processor=lambda s: s)
    assert best == []


def test_numpy_nan_elements():
    """
    when a np.nan element is used, it is skipped and the index is still correct
    """
    np = pytest.importorskip("numpy")
    # no processor
    best = process.extractOne("test", [np.nan, "tes"])
    assert best[2] == 1
    best = process.extractOne(np.nan, [np.nan, "tes"])
    assert best is None
    best = process.extractOne("test", {0: np.nan, 1: "tes"})
    assert best[2] == 1
    best = process.extractOne(np.nan, {0: np.nan, 1: "tes"})
    assert best is None

    # C++ processor
    best = process.extractOne("test", [np.nan, "tes"], processor=default_process)
    assert best[2] == 1
    best = process.extractOne(np.nan, [np.nan, "tes"], processor=default_process)
    assert best is None
    best = process.extractOne("test", {0: np.nan, 1: "tes"}, processor=default_process)
    assert best[2] == 1
    best = process.extractOne(np.nan, {0: np.nan, 1: "tes"}, processor=default_process)
    assert best is None

    # python processor
    best = process.extractOne("test", [np.nan, "tes"], processor=lambda s: s)
    assert best[2] == 1
    best = process.extractOne(np.nan, [np.nan, "tes"], processor=lambda s: s)
    assert best is None
    best = process.extractOne("test", {0: np.nan, 1: "tes"}, processor=lambda s: s)
    assert best[2] == 1
    best = process.extractOne(np.nan, {0: np.nan, 1: "tes"}, processor=lambda s: s)
    assert best is None

    # no processor
    best = process.extract("test", [np.nan, "tes"])
    assert best[0][2] == 1
    best = process.extract(np.nan, [np.nan, "tes"])
    assert best == []
    best = process.extract("test", {0: np.nan, 1: "tes"})
    assert best[0][2] == 1
    best = process.extract(np.nan, {0: np.nan, 1: "tes"})
    assert best == []

    # C++ processor
    best = process.extract("test", [np.nan, "tes"], processor=default_process)
    assert best[0][2] == 1
    best = process.extract(np.nan, [np.nan, "tes"], processor=default_process)
    assert best == []
    best = process.extract("test", {0: np.nan, 1: "tes"}, processor=default_process)
    assert best[0][2] == 1
    best = process.extract(np.nan, {0: np.nan, 1: "tes"}, processor=default_process)
    assert best == []

    # python processor
    best = process.extract("test", [np.nan, "tes"], processor=lambda s: s)
    assert best[0][2] == 1
    best = process.extract(np.nan, [np.nan, "tes"], processor=lambda s: s)
    assert best == []
    best = process.extract("test", {0: np.nan, 1: "tes"}, processor=lambda s: s)
    assert best[0][2] == 1
    best = process.extract(np.nan, {0: np.nan, 1: "tes"}, processor=lambda s: s)
    assert best == []


def test_pandas_nan_elements():
    """
    when a pd.NA element is used, it is skipped and the index is still correct
    """
    pd = pytest.importorskip("pandas")
    # no processor
    best = process.extractOne("test", [pd.NA, "tes"])
    assert best[2] == 1
    best = process.extractOne(pd.NA, [pd.NA, "tes"])
    assert best is None
    best = process.extractOne("test", {0: pd.NA, 1: "tes"})
    assert best[2] == 1
    best = process.extractOne(pd.NA, {0: pd.NA, 1: "tes"})
    assert best is None

    # C++ processor
    best = process.extractOne("test", [pd.NA, "tes"], processor=default_process)
    assert best[2] == 1
    best = process.extractOne(pd.NA, [pd.NA, "tes"], processor=default_process)
    assert best is None
    best = process.extractOne("test", {0: pd.NA, 1: "tes"}, processor=default_process)
    assert best[2] == 1
    best = process.extractOne(pd.NA, {0: pd.NA, 1: "tes"}, processor=default_process)
    assert best is None

    # python processor
    best = process.extractOne("test", [pd.NA, "tes"], processor=lambda s: s)
    assert best[2] == 1
    best = process.extractOne(pd.NA, [pd.NA, "tes"], processor=lambda s: s)
    assert best is None
    best = process.extractOne("test", {0: pd.NA, 1: "tes"}, processor=lambda s: s)
    assert best[2] == 1
    best = process.extractOne(pd.NA, {0: pd.NA, 1: "tes"}, processor=lambda s: s)
    assert best is None

    # no processor
    best = process.extract("test", [pd.NA, "tes"])
    assert best[0][2] == 1
    best = process.extract(pd.NA, [pd.NA, "tes"])
    assert best == []
    best = process.extract("test", {0: pd.NA, 1: "tes"})
    assert best[0][2] == 1
    best = process.extract(pd.NA, {0: pd.NA, 1: "tes"})
    assert best == []

    # C++ processor
    best = process.extract("test", [pd.NA, "tes"], processor=default_process)
    assert best[0][2] == 1
    best = process.extract(pd.NA, [pd.NA, "tes"], processor=default_process)
    assert best == []
    best = process.extract("test", {0: pd.NA, 1: "tes"}, processor=default_process)
    assert best[0][2] == 1
    best = process.extract(pd.NA, {0: pd.NA, 1: "tes"}, processor=default_process)
    assert best == []

    # python processor
    best = process.extract("test", [pd.NA, "tes"], processor=lambda s: s)
    assert best[0][2] == 1
    best = process.extract(pd.NA, [pd.NA, "tes"], processor=lambda s: s)
    assert best == []
    best = process.extract("test", {0: pd.NA, 1: "tes"}, processor=lambda s: s)
    assert best[0][2] == 1
    best = process.extract(pd.NA, {0: pd.NA, 1: "tes"}, processor=lambda s: s)
    assert best == []


def test_result_order():
    """
    when multiple elements have the same score, the first one should be returned
    """
    best = process.extractOne("test", ["tes", "tes"])
    assert best[2] == 0

    best = process.extract("test", ["tes", "tes"], limit=1)
    assert best[0][2] == 0


def test_extract_limits():
    """
    test process.extract with special limits
    """
    bests = process.extract("test", ["tes", "tes"], limit=1, score_cutoff=100)
    assert bests == []

    bests = process.extract("test", ["te", "test"], limit=None, scorer=Levenshtein.distance)
    assert bests == [("test", 0, 1), ("te", 2, 0)]


def test_empty_strings():
    choices = [
        "",
        "new york mets vs chicago cubs",
        "new york yankees vs boston red sox",
        "",
        "",
    ]

    query = "new york mets at chicago cubs"

    best = process.extractOne(query, choices)
    assert best[0] == choices[1]


def test_none_strings():
    choices = [
        None,
        "new york mets vs chicago cubs",
        "new york yankees vs boston red sox",
        None,
        None,
    ]

    query = "new york mets at chicago cubs"

    best = process.extractOne(query, choices)
    assert best[0] == choices[1]

    bests = process.extract(query, choices)
    assert bests[0][0] == choices[1]
    assert bests[1][0] == choices[2]

    bests = list(process.extract_iter(query, choices))
    assert bests[0][0] == choices[1]
    assert bests[1][0] == choices[2]

    try:
        import numpy as np
    except Exception:
        np = None

    if np is not None:
        scores = process.cdist([query], choices)
        assert scores[0, 0] == 0
        assert scores[0, 3] == 0
        assert scores[0, 4] == 0

        scores = process.cdist(["", None], ["", None], scorer=DamerauLevenshtein.normalized_similarity)
        assert scores[0, 0] == 1
        assert scores[0, 1] == 0
        assert scores[1, 0] == 0
        assert scores[1, 1] == 0

        scores = process.cpdist(choices, choices)
        assert scores[0] == 0
        assert scores[3] == 0
        assert scores[4] == 0


def test_issue81():
    # this mostly tests whether this segfaults due to incorrect ref counting
    pd = pytest.importorskip("pandas")
    choices = pd.Series(
        ["test color brightness", "test lemon", "test lavender"],
        index=[67478, 67479, 67480],
    )
    matches = process.extract("test", choices)
    assert matches == [
        ("test color brightness", 90.0, 67478),
        ("test lemon", 90.0, 67479),
        ("test lavender", 90.0, 67480),
    ]


def custom_scorer(s1, s2, score_cutoff=0):
    return fuzz.ratio(s1, s2, score_cutoff=score_cutoff)


@pytest.mark.parametrize("processor", [None, lambda s: s])
@pytest.mark.parametrize("scorer", [fuzz.ratio, custom_scorer])
def test_extractOne_case_sensitive(processor, scorer):
    assert (
        process.extractOne(
            "new york mets",
            ["new", "new YORK mets"],
            processor=processor,
            scorer=scorer,
        )[1]
        != 100
    )


@pytest.mark.parametrize("scorer", [fuzz.ratio, custom_scorer])
def test_extractOne_use_first_match(scorer):
    assert process.extractOne("new york mets", ["new york mets", "new york mets"], scorer=scorer)[2] == 0


@pytest.mark.parametrize("scorer", [fuzz.ratio, fuzz.WRatio, custom_scorer])
def test_cdist_empty_seq(scorer):
    pytest.importorskip("numpy")
    assert process.cdist([], ["a", "b"], scorer=scorer).shape == (0, 2)
    assert process.cdist(["a", "b"], [], scorer=scorer).shape == (2, 0)


@pytest.mark.parametrize("scorer", [fuzz.ratio, fuzz.WRatio, custom_scorer])
def test_cpdist_empty_seq(scorer):
    pytest.importorskip("numpy")
    assert process.cpdist([], [], scorer=scorer).shape == (0,)


@pytest.mark.parametrize("scorer", [fuzz.ratio])
def test_wrapped_function(scorer):
    pytest.importorskip("numpy")
    scorer = wrapped(scorer)
    assert process.cdist(["test"], [float("nan")], scorer=scorer)[0, 0] == 100
    assert process.cdist(["test"], [None], scorer=scorer)[0, 0] == 100
    assert process.cdist(["test"], ["tes"], scorer=scorer)[0, 0] == 100
    assert process.cpdist(["test"], [float("nan")], scorer=scorer)[0] == 100
    assert process.cpdist(["test"], [None], scorer=scorer)[0] == 100
    assert process.cpdist(["test"], ["tes"], scorer=scorer)[0] == 100

    try:
        import pandas as pd
    except Exception:
        pd = None

    if pd is not None:
        assert process.cdist(["test"], [pd.NA], scorer=scorer)[0, 0] == 100
        assert process.cpdist(["test"], [pd.NA], scorer=scorer)[0] == 100


def test_cdist_not_symmetric():
    np = pytest.importorskip("numpy")
    strings = ["test", "test2"]
    expected_res = np.array([[0, 1], [2, 0]])
    assert np.array_equal(
        process.cdist(strings, strings, scorer=Levenshtein.distance, scorer_kwargs={"weights": (1, 2, 1)}),
        expected_res,
    )


def test_cpdist_not_same_length():
    pytest.importorskip("numpy")
    with pytest.raises(ValueError, match="Length of queries and choices must be the same!"):
        process.cpdist(["a", "b"], [])
    with pytest.raises(ValueError, match="Length of queries and choices must be the same!"):
        process.cpdist(["a", "b"], ["f"])


def test_cpdist_multiplier():
    np = pytest.importorskip("numpy")
    string1 = ["test"]
    string2 = ["test2"]
    expected_res = np.array([204])
    assert np.array_equal(
        process.cpdist(
            string1, string2, scorer=Levenshtein.normalized_similarity, score_multiplier=255, dtype=np.uint8
        ),
        expected_res,
    )
    expected_res = np.array([51])
    assert np.array_equal(
        process.cpdist(string1, string2, scorer=Levenshtein.normalized_distance, score_multiplier=255, dtype=np.uint8),
        expected_res,
    )


def test_cdist_multiplier():
    np = pytest.importorskip("numpy")
    strings = ["test", "test2"]
    expected_res = np.array([[255, 204], [204, 255]])
    assert np.array_equal(
        process.cdist(strings, strings, scorer=Levenshtein.normalized_similarity, score_multiplier=255, dtype=np.uint8),
        expected_res,
    )
    expected_res = np.array([[0, 51], [51, 0]])
    assert np.array_equal(
        process.cdist(strings, strings, scorer=Levenshtein.normalized_distance, score_multiplier=255, dtype=np.uint8),
        expected_res,
    )
    # less useful, but test it is working
    expected_res = np.array([[8, 8], [8, 10]])
    assert np.array_equal(
        process.cdist(strings, strings, scorer=Levenshtein.similarity, score_multiplier=2),
        expected_res,
    )
    expected_res = np.array([[0, 2], [2, 0]])
    assert np.array_equal(
        process.cdist(strings, strings, scorer=Levenshtein.distance, score_multiplier=2),
        expected_res,
    )


def test_str_dtype():
    np = pytest.importorskip("numpy")
    assert process.cdist(["test"], ["test"], scorer=fuzz.ratio, dtype="uint8").dtype == np.uint8
    assert process.cpdist(["test"], ["test"], scorer=fuzz.ratio, dtype="uint8").dtype == np.uint8


def test_generators():
    """
    We should be able to use a generators as choices in process.extract
    as long as they are finite.
    """

    def generate_choices():
        choices = ["a", "Bb", "CcC"]
        yield from choices

    search = "aaa"
    # do not call process.extract, since the first call would consume the generator
    res1 = process_cpp.extract(search, generate_choices())
    res2 = process_py.extract(search, generate_choices())
    assert res1 == res2
    assert len(res1) > 0


def test_cdist_pure_python_dtype():
    np = pytest.importorskip("numpy")
    assert process.cdist(["test"], ["test"], scorer=Levenshtein_py.distance).dtype == np.uint32
    assert process.cdist(["test"], ["test"], scorer=Levenshtein_py.similarity).dtype == np.uint32
    assert process.cdist(["test"], ["test"], scorer=Levenshtein_py.normalized_distance).dtype == np.float32
    assert process.cdist(["test"], ["test"], scorer=Levenshtein_py.normalized_similarity).dtype == np.float32


def test_cpdist_pure_python_dtype():
    np = pytest.importorskip("numpy")
    assert process.cpdist(["test"], ["test"], scorer=Levenshtein_py.distance).dtype == np.uint32
    assert process.cpdist(["test"], ["test"], scorer=Levenshtein_py.similarity).dtype == np.uint32
    assert process.cpdist(["test"], ["test"], scorer=Levenshtein_py.normalized_distance).dtype == np.float32
    assert process.cpdist(["test"], ["test"], scorer=Levenshtein_py.normalized_similarity).dtype == np.float32


def test_cpdist_integral_dtype_rounding():
    np = pytest.importorskip("numpy")
    str1 = ["1 2 33 5"]
    str2 = ["1 22 33 55"]
    float_result = process.cpdist(str1, str2, dtype=np.float32)[0]
    int_result = process.cpdist(str1, str2, dtype=np.uint32)[0]
    # Check that the float result should be rounded up when in integer format
    assert float_result % 1 >= 0.5
    # Check if the rounded float result is equal to the integer result
    assert round(float_result) == int_result
