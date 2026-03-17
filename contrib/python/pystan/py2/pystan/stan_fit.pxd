# distutils: language = c++
#-----------------------------------------------------------------------------
# Copyright (c) 2013, Allen B. Riddell
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------

from libcpp cimport bool
from libcpp.map cimport map
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

ctypedef unsigned int uint  # needed for templates

cdef extern from "stan_fit.hpp" namespace "pystan":
    ctypedef map[string, pair[vector[double], vector[size_t]]] vars_r_t
    ctypedef map[string, pair[vector[int], vector[size_t]]] vars_i_t
    ctypedef enum sampling_algo_t:
        NUTS = 1
        HMC = 2
        Metropolis = 3
        Fixed_param = 4
    ctypedef enum optim_algo_t:
        Newton = 1
        BFGS = 3
        LBFGS = 4
    ctypedef enum variational_algo_t:
        MEANFIELD = 1
        FULLRANK = 2
    ctypedef enum sampling_metric_t:
        UNIT_E = 1
        DIAG_E = 2
        DENSE_E = 3
    ctypedef enum stan_args_method_t:
        SAMPLING = 1
        OPTIM = 2
        TEST_GRADIENT = 3
        VARIATIONAL = 4

    cdef cppclass stan_fit[M, R]:
        stan_fit(vars_r_t& vars_r, vars_i_t& vars_i, uint random_seed) except +
        bool update_param_oi(vector[string] pars)
        vector[double] unconstrain_pars(vars_r_t& vars_r, vars_i_t& vars_i) except +
        vector[double] constrain_pars(vector[double]& params_r) except +
        double log_prob(vector[double] upar, bool jacobian_adjust_transform, bool gradient) except +
        vector[double] grad_log_prob(vector[double] upar, bool jacobian_adjust_transform) except +
        int num_pars_unconstrained()
        vector[string] unconstrained_param_names(bool include_tparams, bool include_gqs)
        vector[string] constrained_param_names(bool include_tparams, bool include_gqs)
        int call_sampler(StanArgs&, StanHolder&) except +
        vector[string] param_names()
        vector[string] param_names_oi()
        vector[vector[uint]] param_dims()
        vector[vector[uint]] param_dims_oi()
        vector[string] param_fnames_oi()

    cdef struct sampling_t:
        int iter
        int refresh
        sampling_algo_t algorithm
        int warmup
        int thin
        bool save_warmup  # whether to save warmup samples (always true now)
        int iter_save # number of iterations saved
        int iter_save_wo_warmup  # number of iterations saved wo warmup
        bool adapt_engaged
        double adapt_gamma
        double adapt_delta
        double adapt_kappa
        uint adapt_init_buffer
        uint adapt_term_buffer
        uint adapt_window
        double adapt_t0
        sampling_metric_t metric  # UNIT_E, DIAG_E, DENSE_E
        # string init_inv_metric
        double stepsize  # default to 1
        double stepsize_jitter
        int max_treedepth  # for NUTS, default to 10.
        double int_time  # for HMC, default to 2 * pi

    cdef struct optim_t:
        int iter  # default to 2000
        int refresh  # default to 100
        optim_algo_t algorithm  # Newton (L)BFGS
        bool save_iterations  # default to false
        double init_alpha
        double tol_obj
        double tol_grad
        double tol_param
        double tol_rel_obj
        double tol_rel_grad
        int history_size

    cdef struct variational_t:
        int iter
        variational_algo_t algorithm
        int grad_samples
        int elbo_samples
        int eval_elbo
        int output_samples
        double eta
        bool adapt_engaged
        int adapt_iter
        double tol_rel_obj

    cdef struct test_grad_t:
        double epsilon # default to 1e-6, for test_grad
        double error # default to 1e-6, for test_grad

    cdef union ctrl_t:
        sampling_t sampling
        optim_t optim
        variational_t variational
        test_grad_t test_grad

    cdef cppclass StanArgs:
        uint random_seed
        uint chain_id
        string init
        map[string, pair[vector[double], vector[size_t] ] ] init_vars_r
        map[string, pair[vector[int], vector[size_t] ] ] init_vars_i
        double init_radius
        bool enable_random_init  # enable randomly partially specifying inits
        string sample_file
        bool append_samples
        bool sample_file_flag
        stan_args_method_t method
        string diagnostic_file
        bool diagnostic_file_flag
        ctrl_t ctrl
        string metric_file
        bool metric_file_flag

    cdef cppclass StanHolder:
        int num_failed
        bool test_grad
        vector[double] inits
        vector[double] par
        double value
        vector[vector[double] ] chains
        vector[string] chain_names
        StanArgs args
        vector[double] mean_pars
        vector[string] mean_par_names
        double mean_lp__
        string adaptation_info
        vector[vector[double] ] sampler_params
        vector[string] sampler_param_names

    # free standing functions
    void get_all_flatnames(vector[string] names, vector[vector[uint]]& dims, vector[string] fnames, bool col_major)
