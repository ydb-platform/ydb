import sys
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse import isspmatrix_csr

if sys.version_info[0] >= 3:
    from sparse_dot_topn import sparse_dot_topn as ct
    from sparse_dot_topn import sparse_dot_topn_threaded as ct_thread
else:
    import sparse_dot_topn as ct
    import sparse_dot_topn_threaded as ct_thread


def awesome_cossim_topn(
        A, B, ntop, lower_bound=0, use_threads=False, n_jobs=1, return_best_ntop=False, test_nnz_max=-1):
    """
    This function will return a matrix C in CSR format, where
    C = [sorted top n results > lower_bound for each row of A * B].
    If return_best_ntop=True then best_ntop
    (the true maximum number of elements > lower_bound per row of A * B)
    will also be returned in a tuple together with C as (C, best_ntop).

    Input:
        A and B: two CSR matrices
        ntop: top n results
        lower_bound: a threshold that the element of A*B must be greater than
        use_threads: use multi-thread or not
        n_jobs: number of thread, must be >= 1
        return_best_ntop: (default: False) if True, will return best_ntop together 
                          with C as a tuple: (C, best_ntop)

    Output:
        C: result matrix (returned alone, if return_best_ntop=False)
        best_ntop: The true maximum number of elements > lower_bound per row of 
                   A * B returned together with C as a tuple: (C, best_ntop). It is 
                   returned only if return_best_ntop=True.

    N.B. if A and B are not in CSR format, they will be converted to CSR
    """
    def try_malloc(sz: int, idx_dtype, data_dtype) -> bool:
        try:
            ind_arr = np.empty(sz, dtype=idx_dtype)
            dat_arr = np.empty(sz, dtype=data_dtype)
            del ind_arr, dat_arr
            return True
        except MemoryError:
            return False
        
    if not isspmatrix_csr(A):
        A = A.tocsr()
    if not isspmatrix_csr(B):
        B = B.tocsr()

    dtype = A.dtype
    assert B.dtype == dtype
    lower_bound = dtype.type(lower_bound)  # Casting this scalar to the same type

    M, K1 = A.shape
    K2, N = B.shape

    if K1 != K2:
        err_str = 'A matrix multiplication will be operated. A.shape[1] must be equal to B.shape[0]!'
        raise ValueError(err_str)

    idx_dtype = np.int32

    nnz_max = M*ntop

    # basic check. if A or B are all zeros matrix, return all zero matrix directly
    if len(A.indices) == 0 or len(B.indices) == 0:
        indptr = np.zeros(M + 1, dtype=idx_dtype)
        indices = np.zeros(nnz_max, dtype=idx_dtype)
        data = np.zeros(nnz_max, dtype=A.dtype)
        output = csr_matrix((data, indices, indptr), shape=(M, N))
        if return_best_ntop:
            return output, 0
        else:
            return output

    indptr = np.empty(M + 1, dtype=idx_dtype)

    # reduce nnz_max if too large to fit in available memory:
    nnz_max = 16*nnz_max
    while (not try_malloc(nnz_max, idx_dtype, A.dtype)):
        nnz_max = nnz_max//2

    # take a chance on high matrix-sparsity and reduce further:
    nnz_max = max(M, nnz_max//16)
    
    # this line is only for testing purposes, designed to enable the user to 
    # force C/C++ to reallocate memory during the matrix multiplication
    nnz_max = test_nnz_max if test_nnz_max > 0 else nnz_max
    
    # filled matrices from here on
    indices = np.empty(nnz_max, dtype=idx_dtype)
    data = np.empty(nnz_max, dtype=A.dtype)

    best_ntop_arr = np.full(1, 0, dtype=idx_dtype)

    if not use_threads:

        alt_indices, alt_data = ct.sparse_dot_topn_extd(
            M, N, np.asarray(A.indptr, dtype=idx_dtype),
            np.asarray(A.indices, dtype=idx_dtype),
            A.data,
            np.asarray(B.indptr, dtype=idx_dtype),
            np.asarray(B.indices, dtype=idx_dtype),
            B.data,
            ntop,
            lower_bound,
            indptr, indices, data, best_ntop_arr
        )

    else:
        if n_jobs < 1:
            err_str = 'Whenever you select the multi-thread mode, n_job must be greater than or equal to 1!'
            raise ValueError(err_str)

        alt_indices, alt_data = ct_thread.sparse_dot_topn_extd_threaded(
            M, N, np.asarray(A.indptr, dtype=idx_dtype),
            np.asarray(A.indices, dtype=idx_dtype),
            A.data,
            np.asarray(B.indptr, dtype=idx_dtype),
            np.asarray(B.indices, dtype=idx_dtype),
            B.data,
            ntop,
            lower_bound,
            indptr, indices, data, best_ntop_arr, n_jobs
        )

    if alt_indices is not None:
        indices = alt_indices
        data = alt_data

    # prepare and return the output:
    output = csr_matrix((data, indices, indptr), shape=(M, N))
    if return_best_ntop:
        return output, best_ntop_arr[0]
    else:
        return output
