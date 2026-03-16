# -*- coding: utf-8 -*-

from sparse_dot_topn import awesome_cossim_topn
from scipy.sparse.csr import csr_matrix
from scipy.sparse import coo_matrix
from scipy.sparse import rand
import numpy as np
import pandas as pd
import multiprocessing
import pytest

PRUNE_THRESHOLD = 0.1
NUM_CANDIDATES = 3
MAX_N_PROCESSES = min(8, multiprocessing.cpu_count()) - 1
LIST_N_JOBS = [n for n in range(MAX_N_PROCESSES + 1)]

def get_n_top_sparse(mat, n_top=10):
    """
    Get list of (index, value) of the n largest elements in a 1-dimensional sparse matrix

    :param mat: input sparse matrix
    :param n_top: number of largest elements, default is 10.
    :return: sorted list of largest elements
    """
    length = mat.getnnz()
    if length == 0:
        return None
    if length <= n_top:
        result = list(zip(mat.indices, mat.data))
    else:
        arg_idx = np.argpartition(mat.data, -n_top)[-n_top:]
        result = list(zip(mat.indices[arg_idx], mat.data[arg_idx]))
    return sorted(result, key=lambda x: -x[1])


def awesome_cossim_topn_wrapper(A, B, ntop, lower_bound=0, use_threads=False, n_jobs=1, return_best_ntop=False, test_nnz_max=-1, expect_best_ntop=None):
    """
    This function is running awesome_cossim_topn()
    with and without return_best_ntop and checking if we get the expected result and if both results are the same.
    It has the same signature, but has an extra parameter: expect_best_ntop
    """

    result1, best_ntop = awesome_cossim_topn(A, B, ntop, lower_bound, use_threads, n_jobs, True, test_nnz_max)

    assert expect_best_ntop == best_ntop

    result2 = awesome_cossim_topn(A, B, ntop, lower_bound, use_threads, n_jobs, False, test_nnz_max)

    assert (result1 != result2).nnz == 0  # The 2 CSR matrix are the same

    return result1


def awesome_cossim_topn_array_wrapper_test(
        A, B, ntop, lower_bound=0, use_threads=False, n_jobs=1, return_best_ntop=False, test_nnz_max=-1, expect_best_ntop=None):
    """
    This function is running awesome_cossim_topn_wrapper()
    with and without test_nnz_max=1 and checking if we get the expected result and if both results are the same.
    It has the same signature as awesome_cossim_topn(), but has an extra parameter: expect_best_ntop
    """

    result1 = awesome_cossim_topn_wrapper(A, B, ntop, lower_bound, use_threads, n_jobs, expect_best_ntop=expect_best_ntop)

    result2 = awesome_cossim_topn_wrapper(A, B, ntop, lower_bound, use_threads, n_jobs, test_nnz_max=1, expect_best_ntop=expect_best_ntop)

    assert (result1 != result2).nnz == 0  # The 2 CSR matrix are the same
    assert result1.nnz == result2.nnz

    return result1


def pick_helper_awesome_cossim_topn_dense(
        a_dense,
        b_dense,
        dtype,
        n_jobs=0
    ):
    if n_jobs == 0:
        helper_awesome_cossim_topn_dense(a_dense, b_dense, dtype=dtype)
    elif n_jobs > 0:
        helper_awesome_cossim_topn_dense(a_dense, b_dense, dtype=dtype, use_threads=True, n_jobs=n_jobs)


def helper_awesome_cossim_topn_dense(
        a_dense,
        b_dense,
        dtype,
        use_threads=False,
        n_jobs=1,
    ):
    dense_result = np.dot(a_dense, np.transpose(b_dense))  # dot product
    max_ntop_dense = max(len(row[row > 0]) for row in dense_result)
    sparse_result = csr_matrix(dense_result).astype(dtype)
    max_ntop_sparse = max(row.nnz for row in sparse_result)
    assert max_ntop_dense == max_ntop_sparse
    sparse_result_top3 = [get_n_top_sparse(row, NUM_CANDIDATES)
                          for row in sparse_result]  # get ntop using the old method

    pruned_dense_result = dense_result.copy()
    pruned_dense_result[pruned_dense_result < PRUNE_THRESHOLD] = 0  # prune low similarity
    max_ntop_pruned_dense = max(len(row[row > 0]) for row in pruned_dense_result)
    pruned_sparse_result = csr_matrix(pruned_dense_result)
    max_ntop_pruned_sparse = max(row.nnz for row in pruned_sparse_result)
    assert max_ntop_pruned_dense == max_ntop_pruned_sparse
    pruned_sparse_result_top3 = [get_n_top_sparse(row, NUM_CANDIDATES) for row in pruned_sparse_result]

    a_csr = csr_matrix(a_dense).astype(dtype)
    b_csr_t = csr_matrix(b_dense).T.astype(dtype)
    
    awesome_result = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        len(b_dense),
        0.0,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_dense
    )

    awesome_result_top3 = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        NUM_CANDIDATES,
        0.0,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_dense
    )

    awesome_result_top3 = [list(zip(row.indices, row.data)) if len(
        row.data) > 0 else None for row in awesome_result_top3]  # make comparable, normally not needed

    pruned_awesome_result = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        len(b_dense),
        PRUNE_THRESHOLD,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_dense
    )

    pruned_awesome_result_top3 = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        NUM_CANDIDATES,
        PRUNE_THRESHOLD,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_dense
    )

    pruned_awesome_result_top3 = [list(zip(row.indices, row.data)) if len(
        row.data) > 0 else None for row in pruned_awesome_result_top3]

    # no candidate selection, no pruning
    assert awesome_result.nnz == sparse_result.nnz
    # no candidate selection, below PRUNE_THRESHOLD similarity pruned
    assert pruned_awesome_result.nnz == pruned_sparse_result.nnz

    all_none1 = np.all(pd.isnull(awesome_result_top3)) and np.all(pd.isnull(sparse_result_top3))
    all_none2 = np.all(pd.isnull(pruned_awesome_result_top3)) and np.all(pd.isnull(pruned_sparse_result_top3))

    # top NUM_CANDIDATES candidates selected, no pruning
    if not all_none1:
        # Sometime we can have this test failing for test_awesome_cossim_topn_manually()
        # when we have rows giving the same dot product value and then we have them in random different order.
        np.testing.assert_array_almost_equal(awesome_result_top3, sparse_result_top3)
    else:
        assert len(awesome_result_top3) == len(sparse_result_top3)
    # top NUM_CANDIDATES candidates selected, below PRUNE_THRESHOLD similarity pruned
    if not all_none2:
        np.testing.assert_array_almost_equal(pruned_awesome_result_top3, pruned_sparse_result_top3)
    else:
        assert len(pruned_awesome_result_top3) == len(pruned_sparse_result_top3)


def pick_helper_awesome_cossim_topn_sparse(
        a_sparse,
        b_sparse,
        flag=True,
        n_jobs=0
    ):
    if n_jobs == 0:
        helper_awesome_cossim_topn_sparse(a_sparse, b_sparse, flag=flag)
    elif n_jobs > 0:
        helper_awesome_cossim_topn_sparse(a_sparse, b_sparse, flag=flag, use_threads=True, n_jobs=n_jobs)


def helper_awesome_cossim_topn_sparse(
        a_sparse,
        b_sparse,
        flag=True,
        use_threads=False,
        n_jobs=1
    ):
    # Note: helper function using awesome_cossim_topn
    sparse_result = a_sparse.dot(b_sparse.T)  # dot product
    max_ntop_sparse = max(row.nnz for row in sparse_result)
    sparse_result_top3 = [get_n_top_sparse(row, NUM_CANDIDATES)
                          for row in sparse_result]  # get ntop using the old method

    pruned_sparse_result = sparse_result.copy()
    pruned_sparse_result[pruned_sparse_result < PRUNE_THRESHOLD] = 0  # prune low similarity
    pruned_sparse_result.eliminate_zeros()
    max_ntop_pruned_sparse = max(row.nnz for row in pruned_sparse_result)
    pruned_sparse_result_top3 = [get_n_top_sparse(row, NUM_CANDIDATES) for row in pruned_sparse_result]

    a_csr = csr_matrix(a_sparse)
    b_csr_t = csr_matrix(b_sparse).T

    awesome_result = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        b_sparse.shape[0],
        0.0,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_sparse
    )

    awesome_result_top3 = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        NUM_CANDIDATES,
        0.0,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_sparse
    )

    awesome_result_top3 = [list(zip(row.indices, row.data)) if len(
        row.data) > 0 else None for row in awesome_result_top3]  # make comparable, normally not needed

    pruned_awesome_result = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        b_sparse.shape[0],
        PRUNE_THRESHOLD,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_pruned_sparse
    )

    pruned_awesome_result_top3 = awesome_cossim_topn_array_wrapper_test(
        a_csr,
        b_csr_t,
        NUM_CANDIDATES,
        PRUNE_THRESHOLD,
        use_threads=use_threads,
        n_jobs=n_jobs,
        expect_best_ntop=max_ntop_pruned_sparse
    )

    pruned_awesome_result_top3 = [list(zip(row.indices, row.data)) if len(
        row.data) > 0 else None for row in pruned_awesome_result_top3]

    # no candidate selection, no pruning
    assert awesome_result.nnz == sparse_result.nnz
    # no candidate selection, below PRUNE_THRESHOLD similarity pruned
    assert pruned_awesome_result.nnz == pruned_sparse_result.nnz

    if flag:
        all_none1 = np.all(pd.isnull(awesome_result_top3)) and np.all(pd.isnull(sparse_result_top3))
        all_none2 = np.all(pd.isnull(pruned_awesome_result_top3)) and np.all(pd.isnull(pruned_sparse_result_top3))

        # top NUM_CANDIDATES candidates selected, no pruning
        if not all_none1:
            np.testing.assert_array_almost_equal(awesome_result_top3, sparse_result_top3)
        else:
            assert len(awesome_result_top3) == len(sparse_result_top3)
        # top NUM_CANDIDATES candidates selected, below PRUNE_THRESHOLD similarity pruned
        if not all_none2:
            np.testing.assert_array_almost_equal(pruned_awesome_result_top3, pruned_sparse_result_top3)
        else:
            assert len(pruned_awesome_result_top3) == len(pruned_sparse_result_top3)
    else:
        assert awesome_result_top3 == sparse_result_top3
        assert pruned_awesome_result_top3 == pruned_sparse_result_top3


@pytest.mark.parametrize("dtype", [np.float64, np.float32])
@pytest.mark.parametrize("n_jobs", LIST_N_JOBS)
def test_awesome_cossim_topn_manually(dtype, n_jobs):
    # a simple case
    a_dense = [[0.2, 0.1, 0.0, 0.9, 0.31],
               [0.7, 0.0, 0.0, 0.2, 0.21],
               [0.0, 0.0, 0.0, 0.2, 0.11],
               [0.5, 0.4, 0.5, 0.0, 0.00]]

    b_dense = [[0.4, 0.2, 0.32, 0.2, 0.7],
               [0.9, 0.4, 0.52, 0.1, 0.4],
               [0.3, 0.8, 0.00, 0.2, 0.5],
               [0.3, 0.0, 0.12, 0.1, 0.6],
               [0.6, 0.1, 0.22, 0.8, 0.1],
               [0.9, 0.1, 0.62, 0.4, 0.3]]
    pick_helper_awesome_cossim_topn_dense(a_dense, b_dense, dtype=dtype, n_jobs=n_jobs)

    # boundary checking, there is no matching at all in this case
    c_dense = [[0.2, 0.1, 0.3, 0, 0],
               [0.7, 0.2, 0.7, 0, 0],
               [0.3, 0.9, 0.6, 0, 0],
               [0.5, 0.4, 0.5, 0, 0]]
    d_dense = [[0, 0, 0, 0.6, 0.9],
               [0, 0, 0, 0.1, 0.1],
               [0, 0, 0, 0.2, 0.6],
               [0, 0, 0, 0.8, 0.4],
               [0, 0, 0, 0.1, 0.3],
               [0, 0, 0, 0.7, 0.5]]
    pick_helper_awesome_cossim_topn_dense(c_dense, d_dense, dtype=dtype, n_jobs=n_jobs)


@pytest.mark.filterwarnings("ignore:Comparing a sparse matrix with a scalar greater than zero")
@pytest.mark.filterwarnings("ignore:Changing the sparsity structure of a csr_matrix is expensive")
@pytest.mark.parametrize("dtype", [np.float64, np.float32])
@pytest.mark.parametrize("n_jobs", LIST_N_JOBS)
def test_awesome_cossim_top_one_zeros(dtype, n_jobs):
    # test with one row matrix with all zeros
    # helper_awesome_cossim_top_sparse uses a local function awesome_cossim_top
    nr_vocab = 1000
    density = 0.1
    for _ in range(3):
        a_sparse = csr_matrix(np.zeros((1, nr_vocab))).astype(dtype)
        b_sparse = rand(800, nr_vocab, density=density, format='csr').astype(dtype)
        pick_helper_awesome_cossim_topn_sparse(a_sparse, b_sparse, n_jobs=n_jobs)


@pytest.mark.filterwarnings("ignore:Comparing a sparse matrix with a scalar greater than zero")
@pytest.mark.filterwarnings("ignore:Changing the sparsity structure of a csr_matrix is expensive")
@pytest.mark.parametrize("dtype", [np.float64, np.float32])
@pytest.mark.parametrize("n_jobs", LIST_N_JOBS)
def test_awesome_cossim_top_all_zeros(dtype, n_jobs):
    # test with all zeros matrix
    # helper_awesome_cossim_top_sparse uses a local function awesome_cossim_top
    nr_vocab = 1000
    density = 0.1
    for _ in range(3):
        a_sparse = csr_matrix(np.zeros((2, nr_vocab))).astype(dtype)
        b_sparse = rand(800, nr_vocab, density=density, format='csr').astype(dtype)
        pick_helper_awesome_cossim_topn_sparse(a_sparse, b_sparse, n_jobs=n_jobs)


@pytest.mark.filterwarnings("ignore:Comparing a sparse matrix with a scalar greater than zero")
@pytest.mark.filterwarnings("ignore:Changing the sparsity structure of a csr_matrix is expensive")
@pytest.mark.parametrize("dtype", [np.float64, np.float32])
@pytest.mark.parametrize("n_jobs", LIST_N_JOBS)
def test_awesome_cossim_top_small_matrix(dtype, n_jobs):
    # test with small matrix
    nr_vocab = 1000
    density = 0.1
    for _ in range(10):
        a_sparse = rand(300, nr_vocab, density=density, format='csr').astype(dtype)
        b_sparse = rand(800, nr_vocab, density=density, format='csr').astype(dtype)
        pick_helper_awesome_cossim_topn_sparse(a_sparse, b_sparse, False, n_jobs=n_jobs)


@pytest.mark.filterwarnings("ignore:Comparing a sparse matrix with a scalar greater than zero")
@pytest.mark.filterwarnings("ignore:Changing the sparsity structure of a csr_matrix is expensive")
@pytest.mark.parametrize("dtype", [np.float64, np.float32])
@pytest.mark.parametrize("n_jobs", LIST_N_JOBS)
def test_awesome_cossim_top_large_matrix(dtype, n_jobs):
    # MB: I reduced the size of the matrix so the test also runs in small memory.
    # test with large matrix
    nr_vocab = 2 << 24
    density = 1e-6
    n_samples = 1000
    nnz = int(n_samples * nr_vocab * density)

    rng1 = np.random.RandomState(42)
    rng2 = np.random.RandomState(43)

    for _ in range(1):
        # scipy.sparse.rand has very high memory usage
        # see for details: https://github.com/scipy/scipy/issues/9699
        # a_sparse = rand(500, nr_vocab, density=density, format='csr')
        # b_sparse = rand(80000, nr_vocab, density=density, format='csr')

        # switching to alternative random method below, which is also a lot faster
        row = rng1.randint(500, size=nnz)
        cols = rng2.randint(nr_vocab, size=nnz)
        data = rng1.rand(nnz)

        a_sparse = coo_matrix((data, (row, cols)), shape=(n_samples, nr_vocab))
        a_sparse = a_sparse.tocsr().astype(dtype)

        row = rng1.randint(n_samples, size=nnz)
        cols = rng2.randint(nr_vocab, size=nnz)
        data = rng1.rand(nnz)

        b_sparse = coo_matrix((data, (row, cols)), shape=(n_samples, nr_vocab))
        b_sparse = b_sparse.tocsr().astype(dtype)

        pick_helper_awesome_cossim_topn_sparse(a_sparse, b_sparse, False, n_jobs=n_jobs)
