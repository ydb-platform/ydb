#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import defaultdict
from collections import namedtuple
from functools import partial
from itertools import groupby

from intbitset import intbitset

from licensedcode.tokenize import ngrams

"""
Approximate matching strategies using token sets and multisets.

This is used as a pre-filter to find candidates rules that have the highest
likeliness of matching a query and to filter rules that could not possibly yield
a valid match. The candidates rules are later for pair-wise matching with the
query. This way either less or no matching is needed.

We collect a subset of rules that could be matched by ranking them and keep the
top candidates. We also filter out rules based on minimum thresholds such as
matched token occurrences or an approximation of the length of a match.

The primary technique is token ids sets and multisets intersections. We compute
a resemblance and containment based on these set intersections that is used as
vector to rank. This is essentially a traditional IR inverted index approach.

But we also want to return every matches and not just probabilistic top-ranked
matches based on frequencies as is typically done in a search engine. Therefore
we compute the intersection of the query against every rules. This proves more
efficient than a traditional inverted intersection in part because the queries
are much larger (1000's of tokens) than a traditional search engine query.

Since we use integers to represent tokens, we reduce the problem to integer set
or multisets/bags/counters intersections. Furthermore, we have a finite and
limited number of tokens.

Two techniques are used in sequence: tokens sets and multisets.

Tokens occurrence sets
======================

A tokens occurrence set is represented as an array of bits (aka. a bitmap) where
each bit position corresponds to a token id. The length of each bit array is
therefore equal to the number of unique tokens across all rules. This forms a
term occurrence matrix stored compactly as bitmaps. With about 15K unique tokens
and about 6k rules, we store about 90 millions bits (15K x 6K) for about 10MB
of total storage for this matrix. Computing intersections of bitmaps is fast
even if it needs to be done thousand times for each query and query run.

The length of the intersection of a query and rule bitmap tells us the count of
shared tokens. We can skip rules based on thresholds and we then rank and keep
the top rules.


Tokens ids multisets aka. frequency counters aka. term vectors
==============================================================

A tokens frequency counter maps a token id to the number of times it shows up in
a text. This is also called a multiset or a bag or counter or a term vector.

Given the subset of ranked candidate rules from the token sets intersection
step, we intersect the query and rule token multisets. For each shared token we
collect the minimum count of a token present in both. We sum these to obtain an
approximation to the number of matchable tokens between the query and rule. This
is an approximation because it does not consider the relative positions of the
tokens so it may be bigger than what will eventually be matched using a sequence
alignment.

This sum is then used for the same filtering and ranking used for the token sets
step: skip if some threshold is not met and rank the candidates.

Finally we return the sorted top candidates.
"""

# Set to True for tracing
TRACE = False
TRACE_CANDIDATES = False
TRACE_CANDIDATES_SET = False
TRACE_CANDIDATES_MSET = False
TRACE_CANDIDATES_FILTER_DUPE = False


def logger_debug(*args): pass


if (TRACE or TRACE_CANDIDATES or
  TRACE_CANDIDATES_SET or TRACE_CANDIDATES_SET or
  TRACE_CANDIDATES_FILTER_DUPE):

    import logging
    import sys

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


def tids_sets_intersector(qset, iset):
    """
    Return the intersection of a query and index token ids sets.
    """
    return qset & iset


tids_set_counter = len


def multisets_intersector(qmset, imset):
    """
    Return the intersection of a query and index token ids multisets. For a
    token id present in both multisets, the intersection value is the smaller of
    the occurence count in the query and rule for this token.
    Optimized for defaultdicts.
    """
    # NOTE: Using a Counter is less efficient
    intersection = defaultdict(int)
    # iterate the smallest of the two sets
    if len(qmset) < len(imset):
        set1, set2 = qmset, imset
    else:
        set1, set2 = imset, qmset

    for key, s1count in set1.items():
        s2count = set2[key]
        intersection[key] = min(s1count, s2count)
    return {k: count for k, count in intersection.items() if count}


def multiset_counter(mset):
    """
    Return the sum of occurences of elements present in a token ids multiset,
    aka. the multiset cardinality.
    """
    return sum(mset.values())


def high_tids_set_subset(tids_set, len_legalese):
    """
    Return a subset of a set of token ids that are only legalese tokens.
    """
    return intbitset([i for i in tids_set if i < len_legalese])


def high_tids_multiset_subset(mset, len_legalese):
    """
    Return a subset of a multiset with items made only of legalese tokens.
    """
    return {tid: count for tid, count in mset.items()
            if tid < len_legalese}


def high_bigrams_multiset_subset(mset, len_legalese):
    """
    Return a subset of a multiset with items made only of legalese tokens.
    """
    return {bigram: count for bigram, count in mset.items()
            if bigram[0] < len_legalese or bigram[1] < len_legalese}


def high_multiset_subset(mset, len_legalese, _use_bigrams=False):
    """
    Return a subset of a multiset with items made only of legalese tokens.
    """
    if _use_bigrams:
        return high_bigrams_multiset_subset(mset, len_legalese)
    else:
        return high_tids_multiset_subset(mset, len_legalese)


# FIXME: this is NOT used at all BUT should be used and pe-indexed
def compute_high_set_and_mset(tids_set, mset, len_legalese, _use_bigrams=False):
    """
    Return a tuple of (high tids set, high tids multiset) given a
    tids_set and mset of all token tids and the `len_legalese`.
    """
    high_tids_set = high_tids_set_subset(tids_set, len_legalese)
    high_mset = high_multiset_subset(mset, len_legalese, _use_bigrams=_use_bigrams)
    return high_tids_set, high_mset


def build_set_and_tids_mset(token_ids):
    """
    Return a tuple of (tids set, multiset) given a `token_ids` tids
    sequence.
    """
    tids_mset = defaultdict(int)

    for tid in token_ids:
        # this skips already matched token ids that are -1
        if tid == -1:
            continue
        tids_mset[tid] += 1

    tids_set = intbitset(tids_mset.keys())

    return tids_set, tids_mset


def build_set_and_bigrams_mset(token_ids):
    """
    Return a tuple of (tids set, multiset) given a `token_ids` tids
    sequence.
    """
    tids_set = intbitset()
    bigrams_mset = defaultdict(int)

    for bigram in ngrams(token_ids, 2):
        # this skips already matched token ids that are -1
        if -1 in bigram:
            continue
        bigrams_mset[bigram] += 1
        tids_set.update(bigram)

    return tids_set, bigrams_mset


def build_set_and_mset(token_ids, _use_bigrams=False):
    """
    Return a tuple of (tids set, multiset) given a `token_ids` tids
    sequence.
    """
    if _use_bigrams:
        return build_set_and_bigrams_mset(token_ids)
    else:
        return build_set_and_tids_mset(token_ids)

# FIXME: we should consider more aggressively the thresholds and what a match
# filters would discard when we compute candidates to eventually discard many or
# all candidates: we compute too many candidates that may waste time in seq
# matching for no reason.


def compute_candidates(query_run, idx, matchable_rids, top=50,
                       high_resemblance=False, high_resemblance_threshold=0.8,
                       _use_bigrams=False):
    """
    Return a ranked list of rule candidates for further matching give a
    `query_run`. Use approximate matching based on token sets ignoring
    positions. Only consider rules that have an rid in a `matchable_rids` rids
    set if provided.

    The ranking is based on a combo of resemblance, containment, length and
    other measures.

    if `high_resemblance` is True, this return only candidates that have a a
    high resemblance above `high_resemblance_threshold`.
    """
    # collect query-side sets used for matching
    token_ids = query_run.matchable_tokens()
    qset, qmset = build_set_and_mset(token_ids, _use_bigrams=_use_bigrams)

    len_legalese = idx.len_legalese

    # perform two steps of ranking:
    # step one with tid sets and step two with tid multisets for refinement

    ############################################################################
    # step 1 is on token id sets:
    ############################################################################

    sortable_candidates = []
    sortable_candidates_append = sortable_candidates.append

    sets_by_rid = idx.sets_by_rid

    for rid, rule in enumerate(idx.rules_by_rid):
        if rid not in matchable_rids:
            continue

        scores_vectors, high_set_intersection = compare_token_sets(
            qset=qset,
            iset=sets_by_rid[rid],
            intersector=tids_sets_intersector,
            counter=tids_set_counter,
            high_intersection_filter=high_tids_set_subset,
            len_legalese=len_legalese,
            unique=True,
            rule=rule,
            filter_non_matching=True,
            high_resemblance_threshold=high_resemblance_threshold)

        if scores_vectors:
            svr, svf = scores_vectors
            if (not high_resemblance
            or (high_resemblance and svr.is_highly_resemblant and svf.is_highly_resemblant)):
                sortable_candidates_append((scores_vectors, rid, rule, high_set_intersection))

    if not sortable_candidates:
        return sortable_candidates

    sortable_candidates.sort(reverse=True)

    if TRACE_CANDIDATES_SET:
        logger_debug('\n\n\ncompute_candidates: sets: sortable_candidates:', len(sortable_candidates))
        print()
        for rank, x in enumerate(sortable_candidates[:20], 1):
            print(rank, x)
        print()

    ####################################################################
    # step 2 is on tids multisets
    ####################################################################
    # keep only the 10 x top candidates
    candidates = sortable_candidates[:top * 10]
    sortable_candidates = []
    sortable_candidates_append = sortable_candidates.append

    if _use_bigrams:
        filter_non_matching = False
    else:
        filter_non_matching = True

    msets_by_rid = idx.msets_by_rid

    high_intersection_filter = partial(high_multiset_subset, _use_bigrams=_use_bigrams)

    for _score_vectors, rid, rule, high_set_intersection in candidates:

        scores_vectors, _intersection = compare_token_sets(
            qset=qmset,
            iset=msets_by_rid[rid],
            intersector=multisets_intersector,
            counter=multiset_counter,
            high_intersection_filter=high_intersection_filter,
            len_legalese=len_legalese,
            unique=False,
            rule=rule,
            filter_non_matching=filter_non_matching,
            high_resemblance_threshold=high_resemblance_threshold)

        if scores_vectors:
            svr, svf = scores_vectors
            if (not high_resemblance
            or (high_resemblance and svr.is_highly_resemblant and svf.is_highly_resemblant)):
                # note: we keep the high_set_intersection of sets from step1,
                # not multisets from this step2
                sortable_candidates_append((scores_vectors, rid, rule, high_set_intersection))

    if not sortable_candidates:
        return sortable_candidates

    # rank candidates
    candidates = sorted(filter_dupes(sortable_candidates), reverse=True)[:top]

    if TRACE_CANDIDATES_MSET and candidates:
        logger_debug('\n\n\ncompute_candidates: FINAL: sortable_candidates:', len(candidates))
        # CSV-like printout
        print(','.join(
            ['rank', 'rule'] +
            [x + '_rounded' for x in ScoresVector._fields] +
            list(ScoresVector._fields)))

        for rank, ((score_vec1, score_vec2), rid, rule, high_set_intersection) in enumerate(candidates, 1):
            print(','.join(str(x) for x in ([rank, rule.identifier] + list(score_vec1) + list(score_vec2))))

    return candidates[:top]


def compare_token_sets(qset, iset,
        intersector, counter, high_intersection_filter,
        len_legalese, unique,
        rule,
        filter_non_matching=True,
        high_resemblance_threshold=0.8):
    """
    Compare a `qset` query set or multiset with a `iset` index rule set or
    multiset. Return a tuple of (ScoresVector tuple, intersection) from
    comparing the sets. The ScoresVector is designed to be used as a rank
    sorting key to rank multiple set intersections. Return (None, None) if there
    is no relevant intersection between sets.
    """
    intersection = intersector(qset, iset)
    if not intersection:
        return None, None

    high_intersection = high_intersection_filter(intersection, len_legalese)

    if filter_non_matching:
        if not high_intersection:
            return None, None

        high_matched_length = counter(high_intersection)
        min_high_matched_length = rule.get_min_high_matched_length(unique)

        # need some high match above min high
        if high_matched_length < min_high_matched_length:
            return None, None

    matched_length = counter(intersection)
    min_matched_length = rule.get_min_matched_length(unique)

    if filter_non_matching and matched_length < min_matched_length:
        return None, None

    # Compute resemblance and containment: note we are interested in the index-
    # side containment of a rule in the query and not how much of a query is
    # contained in a rule. In practice we have three main cases:
    # 1. A smaller notice contained in a larger code file or doc file
    # 2. A single whole license text
    # 3. A file that contains mostly licenses notices and texts and contain several of these
    # Containment captures best case 1.
    # Resemblance captures best case 2.
    # Containment first and resemblance second also helps with case 3. which is
    # mixed and gives the best rankings in practice, as we want to further
    # process first rules that are highly contained in the query.
    iset_len = rule.get_length(unique)
    qset_len = counter(qset)
    union_len = qset_len + iset_len - matched_length
    resemblance = matched_length / union_len
    containment = matched_length / iset_len
    # by squaring the resemblance that is otherwise between 0 and 1, we make
    # higher resemblance more important and lower ones less so. This is
    # capturing that resemblance matters when high (e.g. the sets are highly
    # similar) and that otherwise containment matters most. This is could be
    # seen as a form of "smoothing"
    amplified_resemblance = resemblance ** 2

    minimum_containment = rule._minimum_containment

    # FIXME: we should not recompute this /100 ... it should be cached in the index
    if filter_non_matching and minimum_containment and containment < minimum_containment:
        return None, None

    scores = (
        ScoresVector(
            is_highly_resemblant=round(resemblance, 1) >= high_resemblance_threshold,
            containment=round(containment, 1),
            resemblance=round(amplified_resemblance, 1),
            matched_length=round(matched_length / 20, 1),
        ),
        ScoresVector(
            is_highly_resemblant=resemblance >= high_resemblance_threshold,
            containment=containment,
            resemblance=amplified_resemblance,
            matched_length=matched_length,
        )
    )
    return scores, high_intersection


_scores_vector_fields = [
    'is_highly_resemblant',
    'containment',
    'resemblance',
    'matched_length']

ScoresVector = namedtuple('ScoresVector', _scores_vector_fields)


def filter_dupes(sortable_candidates):
    """
    Given a list of `sortable_candidates` as (score_vector, rid, rule, intersection)
    yield filtered candidates.
    """

    def group_key(item):
        (sv_round, _sv_full), _rid, rule, _inter = item
        return (
            rule.license_expression,
            sv_round.is_highly_resemblant,
            sv_round.containment,
            sv_round.resemblance,
            sv_round.matched_length
        )

    sortable_candidates = sorted(sortable_candidates, key=group_key)

    def rank_key(item):
        (_sv_round, sv_full), _rid, rule, _inter = item
        return sv_full, rule.identifier

    for group, duplicates in groupby(sortable_candidates, key=group_key):
        duplicates = sorted(duplicates, reverse=True, key=rank_key)

        if TRACE_CANDIDATES_FILTER_DUPE:
            print()
            logger_debug('compute_candidates: ', 'duplicates:', len(duplicates), repr(group))
            for dupe in duplicates:
                print(dupe)

            print()
            print('Keeping only:', duplicates[0])
            print()
            print()

        yield duplicates[0]
