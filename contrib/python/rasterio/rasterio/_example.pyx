# cython: boundscheck=False

import numpy as np


def compute(unsigned char[:, :, :] input):
    """reverses bands inefficiently

    Given input and output uint8 arrays, fakes an CPU-intensive
    computation.
    """
    cdef int I, J, K
    cdef int i, j, k, l
    cdef double val
    I = input.shape[0]
    J = input.shape[1]
    K = input.shape[2]
    output = np.empty((I, J, K), dtype=np.uint8)
    cdef unsigned char[:, :, :] output_view = output
    with nogil:
        for i in range(I):
            for j in range(J):
                for k in range(K):
                    val = <double>input[i, j, k]
                    for l in range(2000):
                        val += 1.0
                    val -= 2000.0
                    output_view[~i, j, k] = <unsigned char>val
    return output
