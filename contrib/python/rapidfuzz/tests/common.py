"""
common parts of the test suite for rapidfuzz
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isnan
from typing import Any

import pytest

from rapidfuzz import process_cpp, process_py

try:
    from pandas import NA as pandas_NA
except BaseException:
    pandas_NA = None


def _get_scorer_flags_py(scorer: Any, scorer_kwargs: dict[str, Any]) -> tuple[int, int]:
    params = getattr(scorer, "_RF_ScorerPy", None)
    if params is not None:
        flags = params["get_scorer_flags"](**scorer_kwargs)
        return (flags["worst_score"], flags["optimal_score"])
    return (0, 100)


def is_none(s):
    if s is None or s is pandas_NA:
        return True

    return isinstance(s, float) and isnan(s)


def call_and_maybe_catch(call, *args, catch_exceptions=False, **kwargs):
    if not catch_exceptions:
        return call(*args, **kwargs)

    try:
        return call(*args, **kwargs)
    except AssertionError as e:
        raise e
    except Exception as e:
        return e


def compare_exceptions(e1, e2):
    try:
        return str(e1) == str(e2)
    except Exception:
        return False


def scorer_tester(scorer, s1, s2, catch_exceptions=False, **kwargs):
    score1 = call_and_maybe_catch(scorer, s1, s2, **kwargs)
    exception = isinstance(score1, Exception)

    temp_kwargs = kwargs.copy()
    process_kwargs = {}

    if "processor" in kwargs:
        process_kwargs["processor"] = kwargs["processor"]
        del temp_kwargs["processor"]

    if "score_cutoff" in kwargs:
        process_kwargs["score_cutoff"] = kwargs["score_cutoff"]
        del temp_kwargs["score_cutoff"]

    if temp_kwargs:
        process_kwargs["scorer_kwargs"] = temp_kwargs

    extractOne_res1 = call_and_maybe_catch(
        process_cpp.extractOne, s1, [s2], catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
    )
    extractOne_res2 = call_and_maybe_catch(
        process_py.extractOne, s1, [s2], catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
    )
    extract_res1 = call_and_maybe_catch(
        process_cpp.extract, s1, [s2], catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
    )
    extract_res2 = call_and_maybe_catch(
        process_py.extract, s1, [s2], catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
    )
    extract_iter_res1 = call_and_maybe_catch(
        list, process_cpp.extract_iter(s1, [s2], scorer=scorer, **process_kwargs), catch_exceptions=catch_exceptions
    )
    extract_iter_res2 = call_and_maybe_catch(
        list, process_py.extract_iter(s1, [s2], scorer=scorer, **process_kwargs), catch_exceptions=catch_exceptions
    )

    if exception:
        assert compare_exceptions(extractOne_res1, score1)
        assert compare_exceptions(extractOne_res2, score1)
        assert compare_exceptions(extract_res1, score1)
        assert compare_exceptions(extract_res2, score1)
        assert compare_exceptions(extract_iter_res1, score1)
        assert compare_exceptions(extract_iter_res2, score1)
    elif is_none(s1) or is_none(s2):
        assert extractOne_res1 is None
        assert extractOne_res2 is None
        assert extract_res1 == []
        assert extract_res2 == []
        assert extract_iter_res1 == []
        assert extract_iter_res2 == []
    elif kwargs.get("score_cutoff") is not None:
        worst_score, optimal_score = _get_scorer_flags_py(scorer, process_kwargs.get("scorer_kwargs", {}))
        lowest_score_worst = optimal_score > worst_score
        is_filtered = score1 < kwargs["score_cutoff"] if lowest_score_worst else score1 > kwargs["score_cutoff"]

        if is_filtered:
            assert extractOne_res1 is None
            assert extractOne_res2 is None
            assert extract_res1 == []
            assert extract_res2 == []
            assert extract_iter_res1 == []
            assert extract_iter_res2 == []
        else:
            assert pytest.approx(score1) == extractOne_res1[1]
            assert pytest.approx(score1) == extractOne_res2[1]
            assert pytest.approx(score1) == extract_res1[0][1]
            assert pytest.approx(score1) == extract_res2[0][1]
            assert pytest.approx(score1) == extract_iter_res1[0][1]
            assert pytest.approx(score1) == extract_iter_res2[0][1]
    else:
        assert pytest.approx(score1) == extractOne_res1[1]
        assert pytest.approx(score1) == extractOne_res2[1]
        assert pytest.approx(score1) == extract_res1[0][1]
        assert pytest.approx(score1) == extract_res2[0][1]
        assert pytest.approx(score1) == extract_iter_res1[0][1]
        assert pytest.approx(score1) == extract_iter_res2[0][1]

    try:
        import numpy as np
    except Exception:
        np = None

    if np is not None:
        cdist_scores1 = call_and_maybe_catch(
            process_cpp.cdist, [s1], [s2], catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
        )
        cdist_scores2 = call_and_maybe_catch(
            process_py.cdist, [s1], [s2], catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
        )
        # probably trigger multi match / simd implementations
        cdist_scores3 = call_and_maybe_catch(
            process_cpp.cdist, [s1] * 2, [s2] * 4, catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
        )
        cdist_scores4 = call_and_maybe_catch(
            process_py.cdist, [s1] * 2, [s2] * 4, catch_exceptions=catch_exceptions, scorer=scorer, **process_kwargs
        )

        if exception:
            assert compare_exceptions(cdist_scores1, score1)
            assert compare_exceptions(cdist_scores2, score1)
            assert compare_exceptions(cdist_scores3, score1)
            assert compare_exceptions(cdist_scores4, score1)
        else:
            assert np.all(np.isclose(cdist_scores1, score1))
            assert np.all(np.isclose(cdist_scores2, score1))
            assert np.all(np.isclose(cdist_scores3, score1))
            assert np.all(np.isclose(cdist_scores4, score1))

    if exception:
        raise score1

    return score1


def symmetric_scorer_tester(scorer, s1, s2, catch_exceptions=False, **kwargs):
    score1 = call_and_maybe_catch(scorer_tester, scorer, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
    score2 = call_and_maybe_catch(scorer_tester, scorer, s2, s1, catch_exceptions=catch_exceptions, **kwargs)

    if isinstance(score1, Exception):
        assert compare_exceptions(score1, score2)
        raise score1

    assert pytest.approx(score1) == score2
    return score1


@dataclass
class Scorer:
    distance: Any
    similarity: Any
    normalized_distance: Any
    normalized_similarity: Any
    editops: Any
    opcodes: Any


class GenericScorer:
    def __init__(self, py_scorers, cpp_scorers, get_scorer_flags):
        self.py_scorers = py_scorers
        self.cpp_scorers = cpp_scorers
        self.scorers = self.py_scorers + self.cpp_scorers

        def validate_attrs(func1, func2):
            assert hasattr(func1, "_RF_ScorerPy")
            assert hasattr(func2, "_RF_ScorerPy")
            assert func1.__name__ == func2.__name__
            assert func1.__qualname__ == func2.__qualname__
            assert func1.__doc__ == func2.__doc__

        for scorer in self.scorers:
            validate_attrs(scorer.distance, self.scorers[0].distance)
            validate_attrs(scorer.similarity, self.scorers[0].similarity)
            validate_attrs(scorer.normalized_distance, self.scorers[0].normalized_distance)
            validate_attrs(scorer.normalized_similarity, self.scorers[0].normalized_similarity)

        for scorer in self.cpp_scorers:
            assert hasattr(scorer.distance, "_RF_Scorer")
            assert hasattr(scorer.similarity, "_RF_Scorer")
            assert hasattr(scorer.normalized_distance, "_RF_Scorer")
            assert hasattr(scorer.normalized_similarity, "_RF_Scorer")

        self.get_scorer_flags = get_scorer_flags

    def _editops(self, s1, s2, catch_exceptions=False, **kwargs):
        results = [
            call_and_maybe_catch(scorer.editops, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
            for scorer in self.scorers
        ]

        for result in results:
            assert compare_exceptions(result, results[0])

        if any(isinstance(result, Exception) for result in results):
            raise results[0]

        return results[0]

    def _opcodes(self, s1, s2, catch_exceptions=False, **kwargs):
        results = [
            call_and_maybe_catch(scorer.opcodes, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
            for scorer in self.scorers
        ]

        for result in results:
            assert compare_exceptions(result, results[0])

        if any(isinstance(result, Exception) for result in results):
            raise results[0]

        return results[0]

    def _distance(self, s1, s2, catch_exceptions=False, **kwargs):
        symmetric = self.get_scorer_flags(s1, s2, **kwargs)["symmetric"]
        tester = symmetric_scorer_tester if symmetric else scorer_tester

        scores = [
            call_and_maybe_catch(tester, scorer.distance, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
            for scorer in self.scorers
        ]

        if any(isinstance(score, Exception) for score in scores):
            for score in scores:
                assert compare_exceptions(score, scores[0])
            raise scores[0]

        scores = sorted(scores)
        assert pytest.approx(scores[0]) == scores[-1]
        return scores[0]

    def _similarity(self, s1, s2, catch_exceptions=False, **kwargs):
        symmetric = self.get_scorer_flags(s1, s2, **kwargs)["symmetric"]
        tester = symmetric_scorer_tester if symmetric else scorer_tester

        scores = [
            call_and_maybe_catch(tester, scorer.similarity, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
            for scorer in self.scorers
        ]

        if any(isinstance(score, Exception) for score in scores):
            for score in scores:
                assert compare_exceptions(score, scores[0])
            raise scores[0]

        scores = sorted(scores)
        assert pytest.approx(scores[0]) == scores[-1]
        return scores[0]

    def _normalized_distance(self, s1, s2, catch_exceptions=False, **kwargs):
        symmetric = self.get_scorer_flags(s1, s2, **kwargs)["symmetric"]
        tester = symmetric_scorer_tester if symmetric else scorer_tester

        scores = [
            call_and_maybe_catch(
                tester, scorer.normalized_distance, s1, s2, catch_exceptions=catch_exceptions, **kwargs
            )
            for scorer in self.scorers
        ]

        if any(isinstance(score, Exception) for score in scores):
            for score in scores:
                assert compare_exceptions(score, scores[0])
            raise scores[0]

        scores = sorted(scores)
        assert pytest.approx(scores[0]) == scores[-1]
        return scores[0]

    def _normalized_similarity(self, s1, s2, catch_exceptions=False, **kwargs):
        symmetric = self.get_scorer_flags(s1, s2, **kwargs)["symmetric"]
        tester = symmetric_scorer_tester if symmetric else scorer_tester

        scores = [
            call_and_maybe_catch(
                tester, scorer.normalized_similarity, s1, s2, catch_exceptions=catch_exceptions, **kwargs
            )
            for scorer in self.scorers
        ]

        if any(isinstance(score, Exception) for score in scores):
            for score in scores:
                assert compare_exceptions(score, scores[0])
            raise scores[0]

        scores = sorted(scores)
        assert pytest.approx(scores[0]) == scores[-1]
        return scores[0]

    def _validate(self, s1, s2, catch_exceptions=False, **kwargs):
        # todo requires more complex test handling
        # score_cutoff = kwargs.get("score_cutoff")
        kwargs = {k: v for k, v in kwargs.items() if k != "score_cutoff"}

        maximum = self.get_scorer_flags(s1, s2, **kwargs)["maximum"]

        dist = call_and_maybe_catch(self._distance, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        sim = call_and_maybe_catch(self._similarity, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        norm_dist = call_and_maybe_catch(self._normalized_distance, s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        norm_sim = call_and_maybe_catch(
            self._normalized_similarity, s1, s2, catch_exceptions=catch_exceptions, **kwargs
        )

        if isinstance(dist, Exception):
            assert compare_exceptions(dist, sim)
            assert compare_exceptions(dist, norm_dist)
            assert compare_exceptions(dist, norm_sim)
            raise dist

        assert pytest.approx(dist) == maximum - sim
        if maximum != 0:
            assert pytest.approx(dist / maximum) == norm_dist
            assert pytest.approx(sim / maximum) == norm_sim
        else:
            assert pytest.approx(0.0) == norm_dist
            assert pytest.approx(1.0) == norm_sim

        return dist, sim, norm_dist, norm_sim

    def distance(self, s1, s2, catch_exceptions=False, **kwargs):
        dist, _, _, _ = self._validate(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        if "score_cutoff" not in kwargs:
            return dist

        return self._distance(s1, s2, catch_exceptions=catch_exceptions, **kwargs)

    def similarity(self, s1, s2, catch_exceptions=False, **kwargs):
        _, sim, _, _ = self._validate(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        if "score_cutoff" not in kwargs:
            return sim

        return self._similarity(s1, s2, catch_exceptions=catch_exceptions, **kwargs)

    def normalized_distance(self, s1, s2, catch_exceptions=False, **kwargs):
        if not is_none(s1) and not is_none(s2):
            _, _, norm_dist, _ = self._validate(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
            # todo we should be able to handle this in a nicer way
            if "score_cutoff" not in kwargs:
                return norm_dist
        return self._normalized_distance(s1, s2, catch_exceptions=catch_exceptions, **kwargs)

    def normalized_similarity(self, s1, s2, catch_exceptions=False, **kwargs):
        if not is_none(s1) and not is_none(s2):
            _, _, _, norm_sim = self._validate(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
            if "score_cutoff" not in kwargs:
                return norm_sim
        return self._normalized_similarity(s1, s2, catch_exceptions=catch_exceptions, **kwargs)

    def editops(self, s1, s2, catch_exceptions=False, **kwargs):
        editops_ = self._editops(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        opcodes_ = self._opcodes(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        assert opcodes_.as_editops() == editops_
        assert opcodes_ == editops_.as_opcodes()
        return editops_

    def opcodes(self, s1, s2, catch_exceptions=False, **kwargs):
        editops_ = self._editops(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        opcodes_ = self._opcodes(s1, s2, catch_exceptions=catch_exceptions, **kwargs)
        assert opcodes_.as_editops() == editops_
        assert opcodes_ == editops_.as_opcodes()
        return opcodes_
