"""Dataset abstractions for sequential data access."""

from sklearn.utils._typedefs cimport float32_t, float64_t, intp_t, uint32_t

# SequentialDataset and its two concrete subclasses are (optionally randomized)
# iterators over the rows of a matrix X and corresponding target values y.

#------------------------------------------------------------------------------

cdef class SequentialDataset64:
    cdef int current_index
    cdef int[::1] index
    cdef int *index_data_ptr
    cdef Py_ssize_t n_samples
    cdef uint32_t seed

    cdef void shuffle(self, uint32_t seed) noexcept nogil
    cdef int _get_next_index(self) noexcept nogil
    cdef int _get_random_index(self) noexcept nogil

    cdef void _sample(self, float64_t **x_data_ptr, int **x_ind_ptr,
                      int *nnz, float64_t *y, float64_t *sample_weight,
                      int current_index) noexcept nogil
    cdef void next(self, float64_t **x_data_ptr, int **x_ind_ptr,
                   int *nnz, float64_t *y, float64_t *sample_weight) noexcept nogil
    cdef int random(self, float64_t **x_data_ptr, int **x_ind_ptr,
                    int *nnz, float64_t *y, float64_t *sample_weight) noexcept nogil


cdef class ArrayDataset64(SequentialDataset64):
    cdef const float64_t[:, ::1] X
    cdef const float64_t[::1] Y
    cdef const float64_t[::1] sample_weights
    cdef Py_ssize_t n_features
    cdef intp_t X_stride
    cdef float64_t *X_data_ptr
    cdef float64_t *Y_data_ptr
    cdef const int[::1] feature_indices
    cdef int *feature_indices_ptr
    cdef float64_t *sample_weight_data


cdef class CSRDataset64(SequentialDataset64):
    cdef const float64_t[::1] X_data
    cdef const int[::1] X_indptr
    cdef const int[::1] X_indices
    cdef const float64_t[::1] Y
    cdef const float64_t[::1] sample_weights
    cdef float64_t *X_data_ptr
    cdef int *X_indptr_ptr
    cdef int *X_indices_ptr
    cdef float64_t *Y_data_ptr
    cdef float64_t *sample_weight_data

#------------------------------------------------------------------------------

cdef class SequentialDataset32:
    cdef int current_index
    cdef int[::1] index
    cdef int *index_data_ptr
    cdef Py_ssize_t n_samples
    cdef uint32_t seed

    cdef void shuffle(self, uint32_t seed) noexcept nogil
    cdef int _get_next_index(self) noexcept nogil
    cdef int _get_random_index(self) noexcept nogil

    cdef void _sample(self, float32_t **x_data_ptr, int **x_ind_ptr,
                      int *nnz, float32_t *y, float32_t *sample_weight,
                      int current_index) noexcept nogil
    cdef void next(self, float32_t **x_data_ptr, int **x_ind_ptr,
                   int *nnz, float32_t *y, float32_t *sample_weight) noexcept nogil
    cdef int random(self, float32_t **x_data_ptr, int **x_ind_ptr,
                    int *nnz, float32_t *y, float32_t *sample_weight) noexcept nogil


cdef class ArrayDataset32(SequentialDataset32):
    cdef const float32_t[:, ::1] X
    cdef const float32_t[::1] Y
    cdef const float32_t[::1] sample_weights
    cdef Py_ssize_t n_features
    cdef intp_t X_stride
    cdef float32_t *X_data_ptr
    cdef float32_t *Y_data_ptr
    cdef const int[::1] feature_indices
    cdef int *feature_indices_ptr
    cdef float32_t *sample_weight_data


cdef class CSRDataset32(SequentialDataset32):
    cdef const float32_t[::1] X_data
    cdef const int[::1] X_indptr
    cdef const int[::1] X_indices
    cdef const float32_t[::1] Y
    cdef const float32_t[::1] sample_weights
    cdef float32_t *X_data_ptr
    cdef int *X_indptr_ptr
    cdef int *X_indices_ptr
    cdef float32_t *Y_data_ptr
    cdef float32_t *sample_weight_data
