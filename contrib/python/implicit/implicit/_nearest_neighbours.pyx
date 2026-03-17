import cython
from cython cimport floating, integral
import numpy as np
import scipy.sparse
import threading

from cython.operator import dereference
from cython.parallel import parallel, prange

from libcpp cimport bool
from libcpp.vector cimport vector
from libcpp.utility cimport pair

from tqdm.auto import tqdm


cdef extern from "nearest_neighbours.h" namespace "implicit" nogil:
    cdef cppclass TopK[Index, Value]:
        TopK(size_t K)
        vector[pair[Value, Index]] results

    cdef cppclass SparseMatrixMultiplier[Index, Value]:
        SparseMatrixMultiplier(Index item_count)
        void add(Index index, Value value)
        void foreach[Function](Function & f)
        vector[Value] sums


cdef class NearestNeighboursScorer(object):
    """ Class to return the top K items from multipying a users likes
    by a precomputed similarity vector. """
    cdef SparseMatrixMultiplier[int, double] * neighbours

    cdef int[:] similarity_indptr
    cdef int[:] similarity_indices
    cdef double[:] similarity_data

    cdef object lock

    def __cinit__(self, similarity):
        self.neighbours = new SparseMatrixMultiplier[int, double](similarity.shape[0])
        self.similarity_indptr = similarity.indptr
        self.similarity_indices = similarity.indices
        self.similarity_data = similarity.data.astype(np.float64)
        self.lock = threading.RLock()

    @cython.boundscheck(False)
    def recommend(self, int u, int[:] user_indptr, int[:] user_indices, floating[:] user_data,
                  int K=10, bool remove_own_likes=True):
        cdef int index1, index2, i, count
        cdef double weight
        cdef double temp
        cdef pair[double, int] result

        cdef int[:] indices
        cdef double[:] data

        cdef TopK[int, double] * topK = new TopK[int, double](K)
        try:
            with self.lock:
                with nogil:
                    for index1 in range(user_indptr[u], user_indptr[u+1]):
                        i = user_indices[index1]
                        weight = user_data[index1]

                        for index2 in range(self.similarity_indptr[i], self.similarity_indptr[i+1]):
                            self.neighbours.add(self.similarity_indices[index2],
                                                self.similarity_data[index2] * weight)

                    if remove_own_likes:
                        # set the score to 0 for things already liked
                        for index1 in range(user_indptr[u], user_indptr[u+1]):
                            i = user_indices[index1]
                            self.neighbours.sums[i] = 0

                    self.neighbours.foreach(dereference(topK))

            count = topK.results.size()
            indices = ret_indices = np.zeros(count, dtype=np.int32)
            data = ret_data = np.zeros(count)

            with nogil:
                i = 0
                for result in topK.results:
                    indices[i] = result.second
                    data[i] = result.first
                    i += 1
            return ret_indices, ret_data

        finally:
            del topK

    def __dealloc__(self):
        del self.neighbours


@cython.boundscheck(False)
def all_pairs_knn(items, unsigned int K=100, int num_threads=0, show_progress=True):
    """ Returns the top K nearest neighbours for each row in the matrix.
    """
    items = items.tocsr()
    users = items.T.tocsr()

    cdef int item_count = items.shape[0]
    cdef int i, u, index1, index2, j
    cdef double w1, w2

    cdef int[:] item_indptr = items.indptr, item_indices = items.indices
    cdef double[:] item_data = items.data

    cdef int[:] user_indptr = users.indptr, user_indices = users.indices
    cdef double[:] user_data = users.data

    cdef SparseMatrixMultiplier[int, double] * neighbours
    cdef TopK[int, double] * topk
    cdef pair[double, int] result

    # holds triples of output
    cdef double[:] values = np.zeros(item_count * K)
    cdef long[:] rows = np.zeros(item_count * K, dtype=int)
    cdef long[:] cols = np.zeros(item_count * K, dtype=int)

    progress = tqdm(total=item_count, disable=not show_progress)
    with nogil, parallel(num_threads=num_threads):
        # allocate memory per thread
        neighbours = new SparseMatrixMultiplier[int, double](item_count)
        topk = new TopK[int, double](K)

        try:
            for i in prange(item_count, schedule='guided'):
                for index1 in range(item_indptr[i], item_indptr[i+1]):
                    u = item_indices[index1]
                    w1 = item_data[index1]

                    for index2 in range(user_indptr[u], user_indptr[u+1]):
                        neighbours.add(user_indices[index2], user_data[index2] * w1)

                topk.results.clear()
                neighbours.foreach(dereference(topk))

                index2 = K * i
                for result in topk.results:
                    rows[index2] = i
                    cols[index2] = result.second
                    values[index2] = result.first
                    index2 = index2 + 1
                with gil:
                    progress.update(1)

        finally:
            del neighbours
            del topk
    progress.close()
    return scipy.sparse.coo_matrix((values, (rows, cols)),
                                   shape=(item_count, item_count))
