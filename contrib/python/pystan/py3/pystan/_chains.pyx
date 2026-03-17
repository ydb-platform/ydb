#cython: language_level=3
#cython: boundscheck=False
#cython: wraparound=False

from libcpp.vector cimport vector
from libc.math cimport sqrt
cimport cython

import numpy as np

import pystan.constants


# autocovariance is a template function, which Cython doesn't yet support
cdef extern from "stan/math/prim/mat/fun/autocovariance.hpp" namespace "stan::math":
    void stan_autocovariance "stan::math::autocovariance<double>"(const vector[double]& y, vector[double]& acov)

cdef extern from "stan/math/prim/mat/fun/sum.hpp" namespace "stan::math":
    double stan_sum "stan::math::sum"(vector[double]& x)

cdef extern from "stan/math/prim/mat/fun/mean.hpp" namespace "stan::math":
    double stan_mean "stan::math::mean"(vector[double]& x)

cdef extern from "stan/math/prim/mat/fun/variance.hpp" namespace "stan::math":
    double stan_variance "stan::math::variance"(vector[double]& x)


cdef void get_kept_samples(dict sim, int k, int n, vector[double]& samples):
    """

    Parameters
    ----------
    k : unsigned int
        Chain index
    n : unsigned int
        Parameter index
    """
    cdef int i
    cdef long[:] warmup2 = np.array(sim['warmup2'])

    slst = sim['samples'][k]['chains']  # chain k, an OrderedDict
    param_names = list(slst.keys())  # e.g., 'beta[1]', 'beta[2]', ...
    cdef double[:] nv = slst[param_names[n]]  # parameter n
    samples.clear()
    for i in range(nv.shape[0] - warmup2[k]):
        samples.push_back(nv[warmup2[k] + i])


cdef double get_chain_mean(dict sim, int k, int n):
    cdef long[:] warmup2 = np.array(sim['warmup2'])
    slst = sim['samples'][k]['chains']  # chain k, an OrderedDict
    param_names = list(slst.keys())  # e.g., 'beta[1]', 'beta[2]', ...
    cdef vector[double] nv = slst[param_names[n]]  # parameter n
    return stan_mean(nv[warmup2[k]:])


cdef vector[double] autocovariance(dict sim, int k, int n):
    """
    Returns the autocovariance for the specified parameter in the
    kept samples of the chain specified.

    Parameters
    ----------
    k : unsigned int
        Chain index
    n : unsigned int
        Parameter index

    Returns
    -------
    acov : vector[double]

    Note
    ----
    PyStan is profligate with memory here in comparison to RStan. A variety
    of copies are made where RStan passes around references. This is done
    mainly for convenience; the Cython code is simpler.
    """
    cdef vector[double] samples, acov
    get_kept_samples(sim, k, n, samples)
    stan_autocovariance(samples, acov)
    return acov


@cython.cdivision(True)
def effective_sample_size(dict sim, int n):
    """
    Return the effective sample size for the specified parameter
    across all kept samples.

    The implementation is close to the effective sample size
    description in BDA3 (p. 286-287).  See more details in Stan
    reference manual section "Effective Sample Size".

    Current implementation takes the minimum number of samples
    across chains as the number of samples per chain.

    Parameters
    ----------
    sim : dict
        Contains samples as well as related information (warmup, number
        of iterations, etc).
    n : int
        Parameter index

    Returns
    -------
    ess : int
    """
    cdef int i, chain
    cdef int m = sim['chains']

    cdef vector[int] ns_save = sim['n_save']
    cdef vector[int] ns_warmup2 = sim['warmup2']
    cdef vector[int] ns_kept = [s - w for s, w in zip(sim['n_save'], sim['warmup2'])]

    cdef int n_samples = min(ns_kept)

    cdef vector[vector[double]] acov
    for chain in range(m):
        acov.push_back(autocovariance(sim, chain, n))

    cdef vector[double] chain_mean
    cdef vector[double] chain_var
    # double rather than int to deal with Cython quirk, see issue #186
    cdef double n_kept_samples
    for chain in range(m):
        n_kept_samples = ns_kept[chain]
        if n_kept_samples == 1:
            # fix crash for mingw on Windows
            return np.nan
        chain_mean.push_back(get_chain_mean(sim, chain, n))
        chain_var.push_back(acov[chain][0] * n_kept_samples / (n_kept_samples-1))

    cdef double mean_var = stan_mean(chain_var)
    cdef double var_plus = mean_var * (n_samples-1) / n_samples

    if m > 1:
        var_plus = var_plus + stan_variance(chain_mean)

    cdef vector[double] rho_hat_t
    for _ in range(n_samples):
        rho_hat_t.push_back(0)
    cdef vector[double] acov_t
    acov_t.clear()
    for chain in range(m):
        acov_t.push_back(acov[chain][1])
    cdef double rho_hat_even = 1
    rho_hat_t[0] = rho_hat_even
    cdef double rho_hat_odd = 1 - (mean_var - stan_mean(acov_t)) / var_plus
    rho_hat_t[1] = rho_hat_odd
    # Geyer's initial positive sequence
    cdef int max_t = 1
    cdef int t = 1
    while t < (n_samples - 2) and (rho_hat_even + rho_hat_odd) >= 0:
        acov_t.clear()
        for chain in range(m):
            acov_t.push_back(acov[chain][t + 1])
        rho_hat_even = 1 - (mean_var - stan_mean(acov_t)) / var_plus
        acov_t.clear()
        for chain in range(m):
            acov_t.push_back(acov[chain][t + 2])
        rho_hat_odd = 1 - (mean_var - stan_mean(acov_t)) / var_plus
        if (rho_hat_even + rho_hat_odd) >= 0:
            rho_hat_t[t + 1] = rho_hat_even
            rho_hat_t[t + 2] = rho_hat_odd
        max_t = t + 2
        t += 2
    # Geyer's initial monotone sequence
    t = 3
    while t <= max_t - 2:
        if rho_hat_t[t + 1] + rho_hat_t[t + 2] > rho_hat_t[t - 1] + rho_hat_t[t]:
            rho_hat_t[t + 1] = (rho_hat_t[t - 1] + rho_hat_t[t]) / 2
            rho_hat_t[t + 2] = rho_hat_t[t + 1]
        t += 2
    cdef double ess = m * n_samples
    ess = ess / (-1 + 2 * stan_sum(rho_hat_t))
    return ess


@cython.cdivision(True)
def split_potential_scale_reduction(dict sim, int n):
    """
    Return the split potential scale reduction (split R hat) for the
    specified parameter.

    Current implementation takes the minimum number of samples
    across chains as the number of samples per chain.

    Parameters
    ----------
    n : unsigned int
        Parameter index

    Returns
    -------
    rhat : float
        Split R hat

    """
    cdef int i, chain
    cdef double srhat

    cdef int n_chains = sim['chains']

    cdef vector[int] ns_save = sim['n_save']
    cdef vector[int] ns_warmup2 = sim['warmup2']
    cdef vector[int] ns_kept = [s - w for s, w in zip(sim['n_save'], sim['warmup2'])]

    cdef int n_samples = min(ns_kept)

    if n_samples % 2 == 1:
        n_samples = n_samples - 1

    cdef vector[double] split_chain_mean, split_chain_var
    cdef vector[double] samples, split_chain
    for chain in range(n_chains):
        samples.clear()
        get_kept_samples(sim, chain, n, samples)
        split_chain.clear()
        for i in range(n_samples / 2):
            split_chain.push_back(samples[i])
        split_chain_mean.push_back(stan_mean(split_chain))
        split_chain_var.push_back(stan_variance(split_chain))

        split_chain.clear()
        for i in range(n_samples / 2, n_samples):
            split_chain.push_back(samples[i])
        split_chain_mean.push_back(stan_mean(split_chain))
        split_chain_var.push_back(stan_variance(split_chain))

    cdef double var_between = n_samples / 2 * stan_variance(split_chain_mean)
    cdef double var_within = stan_mean(split_chain_var)

    srhat = sqrt((var_between / var_within + n_samples / 2 - 1) / (n_samples / 2))
    return srhat


def _test_autocovariance(dict sim, int k, int n):
    '''Test point for autocovariance function'''
    return autocovariance(sim, k, n)


def _test_stan_functions():
    y = np.arange(10)
    cdef vector[double] acov
    stan_autocovariance(y, acov)
    assert sum(acov) == -40.0, sum(acov)
    assert stan_sum(y) == sum(y)
    assert stan_mean(y) == np.mean(y)
    assert stan_variance(y) == np.var(y, ddof=1)
