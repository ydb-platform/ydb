# -*- coding: utf-8 -*-
"""
Tests for _algorithms module.
"""
import unittest
import numpy as np
from collections import Counter
from itertools import product


from supervenn._algorithms import (
    get_total_gaps_in_rows,
    get_chunks_and_composition_array,
    _get_ordered_chunks_and_composition_array,
    find_best_columns_permutation_bruteforce,
    run_greedy_algorithm_on_composition_array,
    run_randomized_greedy_algorithm
)


def freeze_sets(sets):
    """
    Convert an iterable of sets to a set of frozensets, and check that all sets are not repeated.
    """
    counter = Counter(frozenset(set_) for set_ in sets)
    assert max(counter.values()) == 1
    return set(counter)


def is_ascending(sequence):
    return all(sequence[i + 1] >= sequence[i] for i in range(len(sequence) - 1))


def array_is_binary(arr):
    """
    :param arr: np.array
    :return: True if the array only contains zeros and ones, False otherwise
    """
    return not set(arr.flatten()) - {0, 1}


def make_random_sets(min_sets_count=1):
    sets_count = np.random.randint(min_sets_count, 10)
    max_item = np.random.choice([2 ** i for i in range(8)]) + 1
    max_size = np.random.choice([2 ** i for i in range(8)]) + 1
    sets = [set(np.random.randint(1, max_item, size=np.random.randint(1, max_size)))
            for _ in range(sets_count)]

    # decimate sets to introduce some empty sets into the list
    for index in np.random.randint(0, len(sets), size=int(len(sets) / 10)):
        sets[index] = set()
    return sets


def is_permutation(sequence):
    n = len(sequence)
    if len(sequence) != n:
        return False
    if len(set(sequence)) != n:
        return False
    if set(sequence) != set(range(n)):
        return False
    return True


class TestGetTotalGaps(unittest.TestCase):

    def test_all_zeros(self):
        """
        Test that number of runs of a zero matrix is zero
        """
        arr = np.zeros((2, 3), dtype=bool)
        gaps_count = get_total_gaps_in_rows(arr)
        self.assertEqual(gaps_count, 0)

    def test_all_ones(self):
        """
        Test that the number of runs of a matrix of ones is equal to the number of rows.
        """
        arr = np.ones((7, 3), dtype=bool)
        gaps_count = get_total_gaps_in_rows(arr)
        self.assertEqual(gaps_count, 0)

    def test_one_col(self):
        """
        Test that the number of runs of a matrix with one col is equal to the number of ones
        """
        rows_count = 100
        arr = np.random.randint(0, 2, size=(rows_count, 1))
        gaps_count = get_total_gaps_in_rows(arr)
        self.assertEqual(gaps_count, 0)

    def test_nontrivial_one(self):
        arr = np.array([[1, 0, 1, 0],
                        [1, 0, 0, 1],
                        [0, 1, 0, 1]])
        gaps_count = get_total_gaps_in_rows(arr)
        self.assertEqual(gaps_count, 3)

    def test_nontrivial_two(self):
        arr = np.array([[1, 0, 0, 0],
                        [0, 0, 0, 1],
                        [0, 0, 0, 1]])
        gaps_count = get_total_gaps_in_rows(arr)
        self.assertEqual(gaps_count, 0)

    def test_nontrivial_three(self):
        arr = np.array([[1, 1, 0, 1],
                        [0, 1, 1, 0],
                        [1, 0, 1, 1]])
        gaps_count = get_total_gaps_in_rows(arr)
        self.assertEqual(gaps_count, 2)


class TestGetChunksAndCompositionArray(unittest.TestCase):
    def test_single_set(self):
        sets = [set(np.random.randint(1, 1000, size=100))]
        chunks, arr = get_chunks_and_composition_array(sets)
        self.assertEqual(chunks, sets)
        self.assertEqual(arr.shape, (1, 1))
        self.assertEqual(arr[0, 0], 1)

    def test_one_empty(self):
        set_ = set(np.random.randint(1, 1000, size=100))
        sets = [set_, set()]
        chunks, arr = get_chunks_and_composition_array(sets)
        self.assertEqual(chunks, [set_])
        self.assertEqual(arr.shape, (2, 1))
        self.assertEqual(list(arr[:, 0]), [1, 0])

    def test_disjoint_small(self):
        sets = [{1}, {2}]
        chunks, arr = get_chunks_and_composition_array(sets)
        self.assertEqual(freeze_sets(sets), freeze_sets(chunks))
        self.assertTrue(np.array_equal(arr, np.eye(2, dtype=int)))  # fixme can be not eye

    def test_disjoint_large(self):
        set_count = 4
        sets = [set(np.random.randint(1000 * i, 1000 * (i + 1), size=100 * (i + 1))) for i in range(set_count)]
        chunks, arr = get_chunks_and_composition_array(sets)
        self.assertEqual(freeze_sets(sets), freeze_sets(chunks))
        # Verify that array is made of zeros and ones
        self.assertTrue(array_is_binary(arr))
        # Verify that each row and each column of arr sum to 1
        self.assertEqual(set(arr.sum(0)), {1})
        self.assertEqual(set(arr.sum(1)), {1})

    def test_two_sets(self):
        sets = [{1, 2, 3}, {3, 4}]
        chunks, arr = get_chunks_and_composition_array(sets)
        self.assertEqual(freeze_sets(chunks), {frozenset([1, 2]), frozenset([3]), frozenset([4])})
        self.assertTrue(array_is_binary(arr))

    def test_chunks_for_random_sets(self):
        """
        For random sets, test that
        1) the union of sets is the disjoint union of chunks
        2) each chunk is either completely inside any set either completely outside
        :return:
        """
        for _ in range(100):
            sets = make_random_sets()
            chunks, arr = get_chunks_and_composition_array(sets)
            all_elements = set.union(*sets)
            self.assertEqual(set.union(*chunks), all_elements)
            self.assertEqual(sum(len(chunk) for chunk in chunks), len(all_elements))
            for chunk, set_ in product(chunks, sets):
                self.assertTrue(not chunk - set_ or chunk - set_ == chunk)

    def test_chunks_and_array(self):
        """
        For random sets, test that the you indeed can recreate every original set as the union of chunks indicated in
        the corresponding row of array.
        """
        for _ in range(100):
            sets = make_random_sets()
            chunks, arr = get_chunks_and_composition_array(sets)
            for (set_, row) in zip(sets, arr):
                chunks_in_this_set = [chunk for is_included, chunk in zip(row, chunks) if is_included]
                recreated_set = set.union(*chunks_in_this_set)
                self.assertEqual(set_, recreated_set)
                self.assertEqual(sum(len(chunk) for chunk in chunks_in_this_set), len(set_))


def make_test_for_algorithm(function):

    class TestPermutationFinder(unittest.TestCase):
        def test_single_set(self):
            arr = np.array([[1]])
            permutation = function(arr)
            self.assertEqual(permutation, [0])

        def test_single_chunk(self):
            arr = np.array([1, 0, 1, 0]).reshape((-1, 1))
            permutation = function(arr)
            self.assertEqual(permutation, [0])

        def test_two_cols(self):
            for sets_count in 1, 2, 10:
                col_1 = (np.random.uniform(0, 1, size=sets_count) > 0.6).astype(int)
                col_2 = (np.random.uniform(0, 1, size=sets_count) > 0.6).astype(int)
                col_1[0] = 1
                col_2[0] = 1
                arr = np.concatenate([col_1.reshape((-1, 1)), col_2.reshape((-1, 1))], 1)
                permutation = function(arr)
                self.assertIn(permutation, [[0, 1], [1, 0]])

        def test_is_permutation(self):
            for _ in range(100):
                ncols = np.random.randint(1, 5)
                nrows = np.random.randint(1, 5)
                arr = (np.random.uniform(0, 1, size=(nrows, ncols)) > 0.5).astype(int)
                arr[0][0] = 1
                permutation = function(arr)
                self.assertTrue(is_permutation(permutation))

        def test_obvious_one(self):
            arr = np.array([[1, 1, 1, 1],
                            [1, 0, 1, 0],
                            [1, 0, 1, 0]])
            permutation = function(arr)
            permutation_string = ''.join((str(i) for i in permutation))
            self.assertIn('02', permutation_string + ' ' + permutation_string[::-1])

        def test_obvious_two(self):
            arr = np.array([[0, 1, 0, 1, 0, 0],
                            [1, 0, 1, 1, 0, 1],
                            [1, 0, 0, 1, 1, 0]])
            permutation = function(arr)
            permutation_string = ''.join((str(i) for i in permutation))
            self.assertIn('03', permutation_string + ' ' + permutation_string[::-1])

        def test_obvious_three(self):
            arr = np.array([[0, 1, 0, 1, 1, 0],
                            [1, 0, 1, 1, 0, 1],
                            [1, 0, 0, 1, 0, 0],
                            [1, 1, 0, 1, 0, 0],
                            [1, 0, 0, 0, 1, 0]])
            permutation = function(arr)
            permutation_string = ''.join((str(i) for i in permutation))
            self.assertIn('03', permutation_string + ' ' + permutation_string[::-1])
            self.assertIn('13', permutation_string + ' ' + permutation_string[::-1])

    return TestPermutationFinder


TestFindBestColumnPermutationBruteforce = make_test_for_algorithm(find_best_columns_permutation_bruteforce)
TestRunGreedyAlgorithmOnCompositionArray = make_test_for_algorithm(run_greedy_algorithm_on_composition_array)
TestRunRandomizedGreedyAlgorithm = make_test_for_algorithm(run_randomized_greedy_algorithm)

# TODO: A test that randomized is not worse than non-randomized

class TestRandomizedAlgorithmQuality(unittest.TestCase):

    def test_quality(self):
        for _ in range(100):
            one_prob = np.random.uniform(0.1, 0.9)
            nrows = np.random.randint(1, 30)
            ncols = np.random.randint(1, 8)
            arr = np.random.uniform(0, 1, size=(nrows, ncols)) < one_prob
            permutation = run_randomized_greedy_algorithm(arr)
            target = get_total_gaps_in_rows(arr[:, permutation])
            best_permutation = find_best_columns_permutation_bruteforce(arr)
            best_target = get_total_gaps_in_rows(arr[:, best_permutation])
            self.assertLessEqual((target - best_target), 0.3 * best_target + 1)


class TestRandomizedAlgorithmReproducible(unittest.TestCase):

    def test_reproducible(self):
        arr = np.random.uniform(0, 1, size=(20, 20)) < 0.5
        representations_set = set()
        for _ in range(10):
            permutation = run_randomized_greedy_algorithm(arr)
            representations_set.add(str(permutation))
        self.assertEqual(len(representations_set), 1)


class TestGetOrderedChunksAndCompositionArray(unittest.TestCase):
    def test_order_chunks_size_descending(self):
        for _ in range(10):
            sets = make_random_sets()
            chunks, _ = _get_ordered_chunks_and_composition_array(sets, chunks_ordering='size')
            chunk_sizes_descending = is_ascending([len(chunk) for chunk in chunks][::-1])
            self.assertTrue(chunk_sizes_descending)

    def test_order_chunks_size_ascending(self):
        for _ in range(10):
            sets = make_random_sets()
            chunks, _ = _get_ordered_chunks_and_composition_array(sets, chunks_ordering='size',
                                                                  reverse_chunks_order=False)
            chunk_sizes_ascending = is_ascending([len(chunk) for chunk in chunks])
            self.assertTrue(chunk_sizes_ascending)

    def test_order_chunks_occurence_descending(self):
        for _ in range(10):
            sets = make_random_sets()
            _, composition_matrix = _get_ordered_chunks_and_composition_array(sets, chunks_ordering='occurrence')
            occurences = composition_matrix.sum(0)
            occurences_descending = is_ascending(occurences[::-1])
            self.assertTrue(occurences_descending)

    def test_order_chunks_occurence_ascending(self):
        for _ in range(10):
            sets = make_random_sets()
            _, composition_matrix = _get_ordered_chunks_and_composition_array(sets, chunks_ordering='occurrence',
                                                                              reverse_chunks_order=False)
            occurences = composition_matrix.sum(0)
            occurences_ascending = is_ascending(occurences)
            self.assertTrue(occurences_ascending)

    def test_order_chunks_random(self):
        """
        Test that in 10 runs not all matrices are equal.
        :return:
        """
        done = False
        while not done:
            sets = make_random_sets(min_sets_count=9)
            chunks, composition_matrix = get_chunks_and_composition_array(sets)
            if len(chunks) == 1:
                continue
            representations_set = set()
            for i in range(10):
                _, composition_matrix = _get_ordered_chunks_and_composition_array(sets, chunks_ordering='random')
                representations_set.add(str(composition_matrix))
            self.assertGreater(len(representations_set), 1)
            done = True

    def test_order_sets_size_descending(self):
        for _ in range(1):
            sets = make_random_sets()
            chunks, composition_matrix = _get_ordered_chunks_and_composition_array(sets, sets_ordering='size')
            chunk_sizes = np.array([len(chunk) for chunk in chunks])
            set_sizes = composition_matrix.dot(chunk_sizes.reshape((-1, 1))).flatten()
            set_sizes_descending = is_ascending(set_sizes[::-1])
            self.assertTrue(set_sizes_descending)

    def test_order_sets_size_ascending(self):
        for _ in range(1):
            sets = make_random_sets()
            chunks, composition_matrix = _get_ordered_chunks_and_composition_array(sets, sets_ordering='size',
                                                                                   reverse_sets_order=False)
            chunk_sizes = np.array([len(chunk) for chunk in chunks])
            set_sizes = composition_matrix.dot(chunk_sizes.reshape((-1, 1))).flatten()
            set_sizes_ascending = is_ascending(set_sizes)
            self.assertTrue(set_sizes_ascending)

    def test_order_sets_chunk_count_descending(self):
        for _ in range(10):
            sets = make_random_sets()
            _, composition_matrix = _get_ordered_chunks_and_composition_array(sets, sets_ordering='chunk count')
            chunk_counts = composition_matrix.sum(1)
            chunk_counts_descending = is_ascending(chunk_counts[::-1])
            self.assertTrue(chunk_counts_descending)

    def test_order_sets_chunk_count_ascending(self):
        for _ in range(10):
            sets = make_random_sets()
            _, composition_matrix = _get_ordered_chunks_and_composition_array(sets, sets_ordering='chunk count',
                                                                              reverse_sets_order=False)
            chunk_counts = composition_matrix.sum(1)
            chunk_counts_ascending = is_ascending(chunk_counts)
            self.assertTrue(chunk_counts_ascending)

    def test_order_sets_random(self):
        done = False
        while not done:
            sets = make_random_sets(min_sets_count=9)
            chunks, composition_matrix = get_chunks_and_composition_array(sets)
            if len(chunks) == 1:
                continue
            representations_set = set()
            for i in range(10):
                _, composition_matrix = _get_ordered_chunks_and_composition_array(sets, sets_ordering='random')
                representations_set.add(str(composition_matrix))
            self.assertGreater(len(representations_set), 1)
            done = True


if __name__ == '__main__':
    unittest.main()
