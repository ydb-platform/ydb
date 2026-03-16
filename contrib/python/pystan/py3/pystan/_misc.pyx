# cython: language_level=2
import cython

import numpy as np


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef get_kept_samples(int n, dict sim):
    """See documentation in misc.py"""
    cdef int i, j, num_chains, num_samples, num_warmup, ss_index, s_index
    cdef double[:] s, ss
    cdef long[:] perm
    num_chains = sim['chains']
    nth_key = list(sim['samples'][0]['chains'].keys())[n]

    # the following assumes each chain has same length, same warmup
    num_samples = sim['samples'][0]['chains'][nth_key].shape[0]
    num_warmup = sim['warmup2'][0]

    ss = np.empty((num_samples - num_warmup) * num_chains)
    for i in range(num_chains):
        perm = sim['permutation'][i]
        s = sim['samples'][i]['chains'][nth_key]
        for j in range(num_samples - num_warmup):
            ss_index = i * (num_samples - num_warmup) + j
            s_index = num_warmup + perm[j]
            ss[ss_index] = s[s_index]
    return ss


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef get_samples(int n, dict sim, inc_warmup):
    """See documentation in misc.py"""
    cdef int i
    cdef double[:] s
    cdef int num_chains = sim['chains']
    if sim['warmup'] == 0:
        inc_warmup = True

    # the following assumes each chain has same length, same warmup
    cdef int num_warmup = sim['warmup2'][0]

    nth_key = list(sim['samples'][0]['chains'].keys())[n]
    ss = []
    for i in range(num_chains):
        s = sim['samples'][i]['chains'][nth_key]
        if inc_warmup:
            ss.append(s)
        else:
            ss.append(s[num_warmup:])
    return ss

