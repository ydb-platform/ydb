from __future__ import annotations

from rapidfuzz._feature_detector import AVX2, supports
from rapidfuzz.distance import metrics_cpp, metrics_py
from tests.common import GenericScorer, Scorer, is_none

cpp_scorer_modules = [metrics_cpp]
try:
    if supports(AVX2):
        from rapidfuzz.distance import metrics_cpp_avx2

        cpp_scorer_modules.append(metrics_cpp_avx2)
except Exception:
    pass


def create_generic_scorer(func_name, get_scorer_flags):
    py_scorers = [
        Scorer(
            distance=getattr(metrics_py, func_name + "_distance"),
            similarity=getattr(metrics_py, func_name + "_similarity"),
            normalized_distance=getattr(metrics_py, func_name + "_normalized_distance"),
            normalized_similarity=getattr(metrics_py, func_name + "_normalized_similarity"),
            editops=getattr(metrics_py, func_name + "_editops", None),
            opcodes=getattr(metrics_py, func_name + "_opcodes", None),
        )
    ]

    cpp_scorers = [
        Scorer(
            distance=getattr(mod, func_name + "_distance"),
            similarity=getattr(mod, func_name + "_similarity"),
            normalized_distance=getattr(mod, func_name + "_normalized_distance"),
            normalized_similarity=getattr(mod, func_name + "_normalized_similarity"),
            editops=getattr(metrics_cpp, func_name + "_editops", None),
            opcodes=getattr(metrics_cpp, func_name + "_opcodes", None),
        )
        for mod in cpp_scorer_modules
    ]

    return GenericScorer(py_scorers, cpp_scorers, get_scorer_flags)


def get_scorer_flags_damerau_levenshtein(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": max(len(s1), len(s2)), "symmetric": True}


DamerauLevenshtein = create_generic_scorer("damerau_levenshtein", get_scorer_flags_damerau_levenshtein)


def get_scorer_flags_hamming(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": max(len(s1), len(s2)), "symmetric": True}


Hamming = create_generic_scorer("hamming", get_scorer_flags_hamming)


def get_scorer_flags_indel(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": len(s1) + len(s2), "symmetric": True}


Indel = create_generic_scorer("indel", get_scorer_flags_indel)


def get_scorer_flags_jaro(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": 1.0, "symmetric": True}


Jaro = create_generic_scorer("jaro", get_scorer_flags_jaro)


def get_scorer_flags_jaro_winkler(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": 1.0, "symmetric": True}


JaroWinkler = create_generic_scorer("jaro_winkler", get_scorer_flags_jaro_winkler)


def get_scorer_flags_lcs_seq(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": max(len(s1), len(s2)), "symmetric": True}


LCSseq = create_generic_scorer("lcs_seq", get_scorer_flags_lcs_seq)


def get_scorer_flags_levenshtein(s1, s2, weights=(1, 1, 1), **kwargs):
    insert_cost, delete_cost, replace_cost = weights

    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": insert_cost == delete_cost}

    max_dist = len(s1) * delete_cost + len(s2) * insert_cost

    if len(s1) >= len(s2):
        max_dist = min(max_dist, len(s2) * replace_cost + (len(s1) - len(s2)) * delete_cost)
    else:
        max_dist = min(max_dist, len(s1) * replace_cost + (len(s2) - len(s1)) * insert_cost)

    return {"maximum": max_dist, "symmetric": insert_cost == delete_cost}


Levenshtein = create_generic_scorer("levenshtein", get_scorer_flags_levenshtein)


def get_scorer_flags_osa(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": max(len(s1), len(s2)), "symmetric": True}


OSA = create_generic_scorer("osa", get_scorer_flags_osa)


def get_scorer_flags_postfix(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": max(len(s1), len(s2)), "symmetric": True}


Postfix = create_generic_scorer("postfix", get_scorer_flags_postfix)


def get_scorer_flags_prefix(s1, s2, **kwargs):
    if is_none(s1) or is_none(s2):
        return {"maximum": None, "symmetric": True}
    return {"maximum": max(len(s1), len(s2)), "symmetric": True}


Prefix = create_generic_scorer("prefix", get_scorer_flags_prefix)

all_scorer_modules = [
    DamerauLevenshtein,
    Hamming,
    Indel,
    Jaro,
    JaroWinkler,
    LCSseq,
    Levenshtein,
    OSA,
    Postfix,
    Prefix,
]
