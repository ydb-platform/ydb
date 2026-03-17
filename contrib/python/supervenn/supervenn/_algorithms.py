# -*- coding: utf-8 -*-
"""
    supervenn._algorithms
    This module implements all the algorithms used to prepare data for plotting.
    ~~~~~~~~~~~
    A semi-formal explanation of what is going on here
    ==================================================

    CHUNKS

    Consider a three-way Venn diagram, shown below using squares rather than the usual circles, for obvious reasons.
    It is easy to see that the three squares S1, S2, S3 break each other into seven elementary undivided parts, which we
    will name :chunks:. The 7 chunks are marked as ch1 - ch7.


              *-------S1-------*
              |                |
              |           ch1  |
        *-----|---------*      |
        |     |  ch4    |      |
        |     |         |      |
        |     |   *-----|------|-----*
        |     |   | ch7 | ch5  |     |
       S2     |   |     |      |     |
        |     *---|-----|------*     |
        |         |     |            |
        |  ch2    | ch6 |            S3
        *---------|-----*            |
                  |          ch3     |
                  |                  |
                  *------------------*

    The number 7 is easily derived by the following simple consideration. Each chunk is defined by a unique
    subset of {S1, S2, S3}. For example, chunk ch4 is defined by {S1, S2} - it consists of exactly the points that lie
    inside S1 and S2, but outside of S3. So the number of chunks is 2^3 - 1, 2^3 being the number of possible subsets of
    {S1, S2, S3}, and -1 being for the empty set which has no business being among our chunks.

    Note that, depending on how the three squares are positioned, there can be less chunks then 7. For instance, if the
    three squares are disjoint, the number of chunks will be 3, as each square will be a chunk in itself.

    What is important for us about the chunks, is that they are like the elementary building blocks of our
    configuration of squares. In other words, we can represent any of the squares as a disjoint union of some of the
    chunks. Also any combination of the squares w.r.t. the set-theoretical operations of intersection, union and
    difference can be represented in the same way.

    Now let's move from squares on the plane to abstract finite sets. The same line of reasoning leads us to
    the conclusion that for N sets S1, ..., SN, there exist at most 2^N - 1 chunks (sets), such that any of the sets
    (and any combination of the sets w.r.t union, intersection and difference) can be uniquely represented as the
    disjoint union of several of the chunks.

    COMPOSITION ARRAY

    Suppose we have N sets S1, ..., SN, and we have found the chunks ch1, ... chM. As we said above, each of the sets
    can be uniquely represented as a disjoint union of some of the chunks. Suppose we have found such decompositions for
    all of the sets.
    Now, represent the decompositions as a N x M array of zeros and ones, where the i,j-th item is 1 <=> set S_i
    contains chunk ch_j. We'll call this array the :composition array: of our sets.
    For example, for the squares above, the composition array will look like this:

       ch1  ch2  ch3  ch4  ch5  ch6  ch7
    S1  1    0    0    1    1    0    1  ->  Square 1 is made up of chunks ch1, ch4, ch5 and ch7
    S2  0    1    0    1    0    1    1
    S3  0    0    1    0    1    1    1

    Note that since chunks are something we've just made up by ourselves, they don't have to be ordered in any
    particular way. This means that we can reorder the chunks (= the columns if the composition array) if we
    need to.

    Most part of the present module is actually about finding a permutations of the columns, that will minimize the
    number of gaps between the 1's in all the rows of the array, so that the sets are visually broken into as few parts
    as possible.

    For the array above, we see that there are 5 total zero-filled gaps between ones: one in the first row, two in the
    second, one in the third. But if we find and apply the right permutation of columns, we'll have only one gap between
    the 1's in the whole array, highlighted by asterisks below:


       ch1  ch4  ch2  ch6  ch7  ch5  ch3
    S1  1    1   *0*   1    1    0    0
    S2  0    1    1    1    1    0    0
    S3  0    0    0    1    1    1    1

    When there are few chunks (say, no more than 8) the optimal permutations is easily found in a bruteforce manner by
    checking all the possible permutations. But for a greater number of chunks this would take too long. So instead an
    approximate greedy algorithm with randomization is used. It is implemented in the run_randomized_greedy_algorithm()
    function. The algorithm will be described below.

    REORDERING THE ROWS / SETS

    Unlike the order of chunks, the order of sets can be relevant in some cases. For example, our sets sets may relate
    to different moments in time, and we may deem it important to preserve this temporal structure in the plot. But in
    other cases when there is no inherent order to our sets, we might want to reorder the sets (rows of the composition
    array) so that similar sets are closer to each other.

    This is done by running the same algorithm on the transposed array. But there is an important distinction here: the
    chunks are of different size, and it makes more sense to minimize total gap width, instead of just gap count.
    To allow that, the run_randomized_greedy_algorithm() function accepts an additional argument named :row_weights:,
    and minimizes gap counts weighted with these coefficients. In other words, the minimization target becomes
    sum_i(gaps_count_in_row[i] * row_weights[i]) instead of just sum_i(gaps_count_in_row[i]).

    DESCRIPTION OF THE ALGORITHM

    Recall that our goal is to a find a permutation of columns of a matrix of zeros and ones, so that the row-weighted
    sum of gaps counts in rows is as low as possible.

    Define similarity of two columns as the sum of row weights for rows where the values are equal in the two columns.
    Precompute matrix of similarities of all columns (if arr has N columns, this matrix has shape N x N)
    Find two most similar columns C1, C2, initialize two lists [C1,] and [C2,]
    Among the remaining N-2 columns, find one that has largest similarity to either of C1 or C2. Append that column to
    the corresponding list. E.g. we now have lists [C1, C3] and [C2,]
    Among the remaining N-3 columns find one that has largest similarity to the last elemt of either of the lists (in
    our example, C3 and C2). Append it to the corresponding list.
    Continue until all columns are distributed between in two lists.
    Finally, one of the lists is reversed and the other is concatenated to it on the right, which gives the resulting
    permutation. This concludes the greedy algorithm.

    To mitigate the greediness, the similarity matrix is perturbed by means of adding a random matrix R with
    elements {0, noise_value} with a fixed  probability of non-zero, and the whole greedy procedure is repeated.
    This is done with :seeds: different random matrices, and so :seeds: + 1 permutations are obtained (including the one
    produced from the unperturbed similarities matrix). The permutation that yields the smallest value
    of count_runs_of_ones_in_rows(arr[:, permutation]) is returned.
"""
from collections import defaultdict
import datetime
from itertools import permutations
import warnings

import numpy as np

HUGE_NUMBER = 1e10 # can fail for weighted! FIXME
DEFAULT_MAX_BRUTEFORCE_SIZE = 8
BRUTEFORCE_SIZE_HARD_LIMIT = 12
DEFAULT_SEEDS = 10000
DEFAULT_NOISE_PROB = 0.0075
DEFAULT_MAX_NOISE = 1.1


def get_total_gaps_in_rows(arr, row_weights=None):
    """
    In a numpy.array arr, count how many gaps of zeros are there between contigous runs of non-zero values in each row.
    The counts in each row are multiplied by weights given by row_weights array and summed. By default, all row weights
    are equal to 1, so the returned value is just the total count of gaps in rows.
    :param arr: 2D numpy.array, all that matters is which elements are zero and which are not.
    :param row_weights: 1D numpy array with len equal to len(arr), provides weights for each row.
    :return: weighted sum of number of gaps in rows.
    """
    if row_weights is None:
        row_weights = np.ones(len(arr), dtype=int)
    if len(arr) != len(row_weights):
        raise ValueError('len(row_weights) == {} != {} == len(arr)'.format(len(row_weights), len(arr)))

    arr = arr.astype(bool)

    # Shift array to the left, dropping last column and appending column of zeros on the left.
    shifted_arr = np.concatenate([np.zeros(len(arr), dtype=bool).reshape((-1, 1)), arr[:, :-1]], 1)
    rowwise_runs_counts = (arr & (~shifted_arr)).sum(1)
    rowwise_gaps_counts = np.maximum(rowwise_runs_counts - 1, 0)

    return rowwise_gaps_counts.dot(row_weights)


def break_into_chunks(sets):
    """
    Let us have a collection {S_1, ..., S_n} of finite sets and U be the union of all these sets.
    For a given subset C = {i_1, ..., i_k} of indices {1, ..., n}, define the 'chunk', corresponding to C, as the set
    of elements of U, that belong to S_i_1, ..., S_i_k, but not to any of the other sets.
    For example, for a collection of two sets {S_1, S_2}, there can be max three chunks: S_1 - S_2, S_2 - S_1, S1 & S_2.
    For three sets, there can be max 7 chunks (imagine a generic three-way Venn diagram and count how many different
    area colors it can have).
    In general, the number of possible non-empty chunks for a collection of n sets is equal to min(|U|, 2^n - 1).
    Any chunk either lies completely inside any or completely outside any of the sets S_1, ... S_n.

    This function takes a list of sets as its only argument and returns a dict with frozensets of indices as keys and
    chunks as values.
    :param sets: list of sets
    :return: chunks_dict - dict with frozensets as keys and sets as values.
    """
    if not sets:
        raise ValueError('Sets list is empty.')

    all_items = set.union(*sets)

    if not all_items:
        raise ValueError('All sets are empty')

    # Each chunk is characterized by its occurrence pattern, which is a unique subset of indices of our sets.
    # E.g. chunk with signature {1, 2, 5} is exactly the set of items such that they belong to sets 1, 2, 5, and
    # don't belong to any of the other sets.
    # Build a dict with signatures as keys (as frozensets), and lists of items as values,
    chunks_dict = defaultdict(set)
    for item in all_items:
        occurrence_pattern = frozenset({i for i, set_ in enumerate(sets) if item in set_})
        chunks_dict[occurrence_pattern].add(item)
    return dict(chunks_dict)


def get_chunks_and_composition_array(sets):
    """
    Take
    - list of all chunks (each chunk is a set of items)
    - a numpy.array A of zeros and ones with len(sets) rows and len(chunks) columns,
    where A[i, j] == 1 <=> sets[i] includes chunks[j].
    :param sets: list of sets
    :return: chunks - list of sets, arr - numpy.array, chunks_dict - dict w
    """
    chunks_dict = break_into_chunks(sets)
    chunks_count = len(chunks_dict)
    chunks = []
    arr = np.zeros((len(sets), chunks_count), dtype=int)

    for idx, (sets_indices, items) in enumerate(chunks_dict.items()):
        chunks.append(items)
        arr[list(sets_indices), idx] = 1

    return chunks, arr


def find_best_columns_permutation_bruteforce(arr, row_weights=None):
    """
    Using exhaustive search, find permutation of columns of np.array arr that will provide minimum value of weighted sum
    of gaps counts in rows of arr. Will take unreasonably long time if arr has > 10 cols.
    :param arr: 2D numpy.array.  All that matters is which elements are zero and which are not
    :param row_weights: 1D numpy array with len equal to len(arr). Provides weights for each row.
    :return: optimal permutation as a list of column indices.
    """
    if arr.shape[1] > BRUTEFORCE_SIZE_HARD_LIMIT:
        raise ValueError('Bruteforce ordering method accepts max {} columns, got {} instead. It would take too long.'
                         .format(BRUTEFORCE_SIZE_HARD_LIMIT, arr.shape[1]))
    best_permutation = None
    best_total_gaps = HUGE_NUMBER
    for permutation in permutations(range(arr.shape[1])):
        total_gaps = get_total_gaps_in_rows(arr[:, permutation], row_weights=row_weights)
        if total_gaps < best_total_gaps:
            best_permutation = permutation
            best_total_gaps = total_gaps
    return list(best_permutation)


def columns_similarities_matrix(arr, row_weights=None):
    """
    Let A and B be two 1D arrays with zeros and ones, of same shape, and row_weigths a non-negative array of same shape.
    Define weighted similarity of A and B w.r.t. row weights as the sum of elements of row_weights in positions where
    A and B have equal values.
    So, given a M x N array arr, this function computes the N x N matrix of weighted similarities of its columns.
    All weights are equal to 1 by default, in which case the similarity of two columns is equal to the number of
    positions where values coincide in the two columns.
    :param arr: M x N array with zeros and ones
    :param row_weights: 1D numpy array with len equal to len(arr). Provides weights for each row.
    :return: N x N array. with weighted similarities of columns of arr as defined above.
    """

    if row_weights is None:
        row_weights = np.ones(len(arr), dtype=int)
    if len(arr) != len(row_weights):
        raise ValueError('len(row_weights) must be equal to number of rows of arr')

    return (arr.T * row_weights).dot(arr) + ((1 - arr).T * row_weights).dot(1 - arr)


def find_columns_permutation_greedily(similarities):
    """
    Given an array of columns similarities, use a greedy algorithm to try and find a permutation of columns that
    will lower the number of gaps in rows.
    :param similarities: numpy.array of column similarities.
    :return: a permutation (as a list of indices) of column indices, producing lower value of count_runs
    """

    if len(similarities) == 1:
        return [0]

    # fill diagonal with negative value so that same column is never paired with itself, even if all similarities
    # between different columns are zero.
    similarities = similarities.copy()
    np.fill_diagonal(similarities, -1)

    ncols = similarities.shape[0]

    # define placed_flags[i] == 1 <=> i-th column of arr has already been assinged a place in the permutation
    placed_flags = np.zeros(ncols, dtype=int)

    # find two most similar columns. Initialize two lists with tham.
    first_col_index, second_col_index = np.unravel_index(similarities.argmax(), similarities.shape)
    first_tail = [first_col_index]
    second_tail = [second_col_index]
    placed_flags[first_col_index] = 1
    placed_flags[second_col_index] = 1

    # the main greedy loop
    for _ in range(ncols - 2):
        # find column most similar to the last element of any of the two lists, append it to the corresponding list
        similarities_to_first = similarities[:, first_tail[-1]] - placed_flags * HUGE_NUMBER
        similarities_to_second = similarities[:, second_tail[-1]] - placed_flags * HUGE_NUMBER
        first_argmax = similarities_to_first.argmax()
        second_argmax = similarities_to_second.argmax()
        if similarities_to_first[first_argmax] >= similarities_to_second[second_argmax]:
            idx = first_argmax
            first_tail.append(idx)
        else:
            idx = second_argmax
            second_tail.append(idx)

        # mark the chosen column as placed
        placed_flags[idx] = 1

    permutation = second_tail[::-1] + first_tail

    return permutation


# todo rename to reflect what it does, not only how it does it
def run_greedy_algorithm_on_composition_array(arr, row_weights=None):
    """
    Given a composition array, use a greedy algorithm to find a permutation of columns that will try to minimize the
    total weighted number of gaps in the rows of the array.
    :param arr: numpy array with zeros and ones
    :param row_weights: 1D numpy array with len equal to len(arr). Provides weights for each row.
    :return:
    """
    similarities = columns_similarities_matrix(arr, row_weights=row_weights)
    return find_columns_permutation_greedily(similarities)


# todo rename to reflect what it does, not only how it does it
def run_randomized_greedy_algorithm(arr, row_weights=None, seeds=DEFAULT_SEEDS, noise_prob=DEFAULT_NOISE_PROB):
    """
    For a 2D np.array arr, find a permutation of columns that approximately minimizes the row-weighted number of gaps in
    the rows of the permuted array. An approximate randomized greedy algorithm is used.



    :param arr: np.array
    :param seeds: number of different random perturbations to figh sticking in local minima.
    :return: a list of indices representing an approximately optimal permutation of columns of arr
    """

    arr = arr.astype(int)

    # Compute similarities matrix
    similarities = columns_similarities_matrix(arr, row_weights=row_weights)

    best_found_permutation = None
    best_found_gaps_count = HUGE_NUMBER

    for seed in range(seeds + 1):
        # Perturb similarities matrix if seed != 0
        if seed == 0:
            noise = np.zeros_like(similarities)
        else:
            np.random.seed(seed)
            noise = (np.random.uniform(0, 1, size=similarities.shape) < noise_prob).astype(int)
            np.random.seed(datetime.datetime.now().microsecond)

        np.fill_diagonal(noise, 0)
        noisy_similarities = similarities + noise

        permutation = find_columns_permutation_greedily(noisy_similarities)

        # Compute the target value for the found permutation, compare with current best.
        gaps = get_total_gaps_in_rows(arr[:, permutation], row_weights=row_weights)
        if gaps < best_found_gaps_count:
            best_found_permutation = permutation
            best_found_gaps_count = gaps

    return best_found_permutation


def get_permutations(chunks, composition_array, chunks_ordering='minimize gaps', sets_ordering=None,
                     reverse_chunks_order=True, reverse_sets_order=True,
                     max_bruteforce_size=DEFAULT_MAX_BRUTEFORCE_SIZE,
                     seeds=DEFAULT_SEEDS, noise_prob=DEFAULT_NOISE_PROB):
    """
    Given chunks and composition array, get permutations which will order the chunks and the sets according to specified
    ordering methods.
    :param chunks, composition_array: as returned by get_chunks_and_composition_array
    For explanation of other params, see docstring to supervenn()
    :return: a dict of the form {'chunks_ordering': [3, 2, 5, 4, 1, 6, 0], 'sets_ordering': [2, 0, 1, 3]}
    """
    chunk_sizes = [len(chunk) for chunk in chunks]
    set_sizes = composition_array.dot(np.array(chunk_sizes))

    chunks_case = {
        'sizes': chunk_sizes,
        'param': 'chunks_ordering',
        'array': composition_array,
        'row_weights': None,
        'ordering': chunks_ordering,
        'allowed_orderings': ['size', 'occurrence', 'random', 'minimize gaps'] + ['occurence'],  # todo remove with typo
        'reverse': reverse_chunks_order
    }

    if chunks_ordering == 'occurence':
        warnings.warn('Please use chunks_ordering="occurrence" (with double "r") instead of "occurence" (spelling fixed'
                      'in 0.3.0). The incorrect variant is still supported, but will be removed in a future version')

    sets_case = {
        'sizes': set_sizes,
        'param': 'sets_ordering',
        'array': composition_array.T,
        'row_weights': chunk_sizes,
        'ordering': sets_ordering,
        'allowed_orderings': ['size', 'chunk count', 'random', 'minimize gaps', None],
        'reverse': reverse_sets_order
    }

    permutations_ = {}

    for case in chunks_case, sets_case:
        if case['ordering'] not in case['allowed_orderings']:
            raise ValueError('Unknown {}: {} (should be one of {})'
                             .format(case['param'], case['ordering'], case['allowed_orderings']))

        if case['ordering'] == 'size':
            permutation = np.argsort(case['sizes'])
        elif case['ordering'] in ['occurrence', 'chunk count'] + ['occurence']:
            permutation = np.argsort(case['array'].sum(0))
        elif case['ordering'] == 'random':
            permutation = np.array(range(len(case['sizes'])))
            np.random.shuffle(permutation)
        elif case['ordering'] is None:
            permutation = np.array(range(len(case['sizes'])))
        elif case['ordering'] == 'minimize gaps':
            if len(case['sizes']) <= min(max_bruteforce_size, BRUTEFORCE_SIZE_HARD_LIMIT):
                permutation = find_best_columns_permutation_bruteforce(case['array'], row_weights=case['row_weights'])
            else:
                permutation = run_randomized_greedy_algorithm(case['array'], seeds=seeds, noise_prob=noise_prob,
                                                              row_weights=case['row_weights'])
        else:
            raise ValueError(case['ordering'])

        if case['ordering'] is not None and case['reverse']:
            permutation = permutation[::-1]

        permutations_[case['param']] = permutation

    return permutations_


def _get_ordered_chunks_and_composition_array(sets, **kwargs):
    """
    Wrapper for get_permutations, used only for testing
    :param sets: list of sets
    :param kwargs: all arguments to get_permutations except for chunks and composition_array,
    :return:
    """
    chunks, composition_array = get_chunks_and_composition_array(sets)

    permutations_ = get_permutations(chunks, composition_array, **kwargs)

    ordered_chunks = [chunks[i] for i in permutations_['chunks_ordering']]
    ordered_composition_array = composition_array[:, permutations_['chunks_ordering']]
    ordered_composition_array = ordered_composition_array[permutations_['sets_ordering'], :]

    return ordered_chunks, ordered_composition_array
