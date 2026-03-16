# distutils: language = c++
# cython: language_level=2
#-----------------------------------------------------------------------------
# Copyright (c) 2013, Allen B. Riddell
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# THIS IS A TEMPLATE, not a proper Cython .pyx file
#
# A template variable $model_cppname needs to be substituted before
# compilation.
#
# $model_cppname.hpp should be in the same directory as this
# file (after substitutions have been made).
#
#-----------------------------------------------------------------------------

# cython imports
from libcpp cimport bool
from libcpp.map cimport map
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

from cython.operator cimport dereference as deref, preincrement as inc

cimport numpy as np

from pystan.io cimport py_var_context, var_context
from pystan.stan_fit cimport stan_fit, StanArgs, StanHolder, get_all_flatnames

# Initialize numpy for use from C. When using numpy from C or Cython this must always be done.
np.import_array()

# python imports
from collections import OrderedDict
import logging
import warnings

import numpy as np

import pystan.misc
import pystan.plots
from pystan._compat import PY2, string_types
from pystan.constants import (sampling_algo_t, optim_algo_t, variational_algo_t,
                              sampling_metric_t, stan_args_method_t)

logger = logging.getLogger('pystan')

cdef extern from "boost/random/additive_combine.hpp" namespace "boost::random":
    cdef cppclass additive_combine_engine[T, U]:
        pass
    ctypedef additive_combine_engine ecuyer1988

cdef extern from "$model_cppname.hpp" namespace "${model_cppname}_namespace":
    cdef cppclass $model_cppname:
        $model_cppname(var_context& context) except +

# NOTE: Methods that aren't intended for public use are prefixed by '_'. For
# example, _update_param_oi probably shouldn't be called unless you know
# something about the state of the C++ class instance wrapped by the class.

ctypedef map[string, pair[vector[double], vector[size_t]]] vars_r_t
ctypedef map[string, pair[vector[int], vector[size_t]]] vars_i_t


cdef class PyStanHolder:
    """Allow access to a StanHolder instance from Python

    A PyStanHolder instance wraps a StanHolder instance. When the PyStanHolder
    instance is deleted, the StanHolder instance will be as well.

    There are slight differences between the StanHolder and PyStanHolder. For
    example, chains is an OrderedDict in the PyStanHolder where a StanHolder
    tracks the same information in the fields ``chains`` and ``chain_names``.
    The same holds for ``sampler_params``.
    """
    cdef public int num_failed
    cdef public bool test_grad
    cdef public list inits
    cdef public list par
    cdef public double value
    cdef public chains
    cdef public dict args
    cdef public mean_pars
    cdef public list mean_par_names
    cdef public double mean_lp__
    cdef public adaptation_info
    cdef public sampler_params
    cdef public list sampler_param_names
    cdef StanHolder * holderptr

    # for backward compatibility allow holder[attr]
    def __getitem__(self, key):
        return getattr(self, key)

    def __dealloc__(self):
        del self.holderptr

    # the following three methods give Cython classes instructions for pickling
    def __getstate__(self):
        attr_names = ('num_failed test_grad inits par value chains args mean_pars mean_lp__ '
                      'adaptation_info sampler_params sampler_param_names').split()
        state = dict((k, getattr(self, k)) for k in attr_names)
        return state

    def __setstate__(self, state):
        for k in state:
            setattr(self, k, state[k])

    def __reduce__(self):
        return (PyStanHolder, tuple(), self.__getstate__(), None, None)


cdef PyStanHolder _pystanholder_from_stanholder(StanHolder* holder):
    cdef int num_iter
    cdef double* data_ptr
    cdef np.npy_intp dims[1]

    h = PyStanHolder()
    h.holderptr = holder
    h.num_failed = holder.num_failed
    h.test_grad = holder.test_grad
    h.inits = holder.inits
    h.par = holder.par
    h.value = holder.value

    chains = []
    cdef vector[vector[double] ].iterator it = holder.chains.begin()
    while it != holder.chains.end():
        num_iter = deref(it).size()
        dims[0] = <np.npy_intp> num_iter
        data_ptr = &(deref(it).front())
        ch = np.PyArray_SimpleNewFromData(1, dims, np.NPY_DOUBLE, data_ptr)
        chains.append(ch)
        inc(it)
    chain_names = [n.decode('utf-8') for n in holder.chain_names]
    h.chains = OrderedDict(zip(chain_names, chains))

    # NOTE: when _pystanholder_from_stanholder is called we also have a pointer
    # to holder.args available so we will use it directly from there. Strictly
    # speaking it should be done here, but Cython kept throwing errors
    # FIXME: figure out origins of difficulties
    # r['args'] = _dict_from_stanargs(holder.args)
    h.mean_pars = holder.mean_pars
    h.mean_par_names = [n.decode('utf-8') for n in holder.mean_par_names]
    h.mean_lp__ = holder.mean_lp__
    h.adaptation_info = holder.adaptation_info.decode('utf-8')
    h.sampler_params = holder.sampler_params
    h.sampler_param_names = [n.decode('utf-8') for n in holder.sampler_param_names]
    return h


cdef dict _dict_from_stanargs(StanArgs* args):
    d = dict()
    ctrl_d = dict()
    d['random_seed'] = str(args.random_seed)
    d['chain_id'] = args.chain_id
    d['init'] = args.init
    # FIXME: reconstructing d['init_list'] from args.init_vars_r and
    # args.init_vars_i requires additional work. The initial values for each
    # chain are accessible with the method get_inits()
    d['init_radius'] = args.init_radius
    d['enable_random_init'] = args.enable_random_init
    d['append_samples'] = args.append_samples
    if args.sample_file_flag:
        d['sample_file'] = args.sample_file
    if args.diagnostic_file_flag:
        d['diagnostic_file'] = args.diagnostic_file

    method = stan_args_method_t(args.method)
    if method == stan_args_method_t.SAMPLING:
        d["method"] = method.name
        d["iter"] = args.ctrl.sampling.iter
        d["warmup"] = args.ctrl.sampling.warmup
        d["thin"] = args.ctrl.sampling.thin
        d["refresh"] = args.ctrl.sampling.refresh
        d["test_grad"] = False
        ctrl_d["adapt_engaged"] = args.ctrl.sampling.adapt_engaged
        ctrl_d["adapt_gamma"] = args.ctrl.sampling.adapt_gamma
        ctrl_d["adapt_delta"] = args.ctrl.sampling.adapt_delta
        ctrl_d["adapt_kappa"] = args.ctrl.sampling.adapt_kappa
        ctrl_d["adapt_init_buffer"] = args.ctrl.sampling.adapt_init_buffer
        ctrl_d["adapt_term_buffer"] = args.ctrl.sampling.adapt_term_buffer
        ctrl_d["adapt_window"] = args.ctrl.sampling.adapt_window
        ctrl_d["adapt_t0"] = args.ctrl.sampling.adapt_t0
        ctrl_d["stepsize"] = args.ctrl.sampling.stepsize
        ctrl_d["stepsize_jitter"] = args.ctrl.sampling.stepsize_jitter
        d["sampler_t"] = algorithm = sampling_algo_t(args.ctrl.sampling.algorithm).name
        if algorithm == sampling_algo_t.NUTS:
            ctrl_d["max_treedepth"] = args.ctrl.sampling.max_treedepth
        elif algorithm == sampling_algo_t.HMC:
            ctrl_d["int_time"] = args.ctrl.sampling.int_time
        elif algorithm == sampling_algo_t.Metropolis:
            pass
        else:
            # included here to mirror rstan code
            pass
        if algorithm != sampling_algo_t.Metropolis:
            metric = sampling_metric_t(args.ctrl.sampling.metric).name
            if metric == sampling_metric_t.UNIT_E:
                ctrl_d["metric"] = "unit_e"
                d["sampler_t"] = d["sampler_t"] + "(unit_e)"
            elif metric == sampling_metric_t.DIAG_E:
                ctrl_d["metric"] = "diag_e"
                d["sampler_t"] = d["sampler_t"] + "(diag_e)"
            elif metric == sampling_metric_t.DENSE_E:
                ctrl_d["metric"] = "dense_e"
                d["sampler_t"] = d["sampler_t"] + "(dense_e)"
        d["control"] = ctrl_d
    elif method == stan_args_method_t.VARIATIONAL:
        d["method"] = method.name
        d["iter"] = args.ctrl.variational.iter
        d["grad_samples"] = args.ctrl.variational.grad_samples
        d["elbo_samples"] = args.ctrl.variational.elbo_samples
        d["eval_elbo"] = args.ctrl.variational.eval_elbo
        d["output_samples"] = args.ctrl.variational.output_samples
        d["eta"] = args.ctrl.variational.eta
        d["adapt_engaged"] = args.ctrl.variational.adapt_engaged
        d["adapt_iter"] = args.ctrl.variational.adapt_iter
        d["tol_rel_obj"] = args.ctrl.variational.tol_rel_obj
        algorithm = variational_algo_t(args.ctrl.variational.algorithm)
        d['algorithm'] = algorithm.name
    elif method == stan_args_method_t.OPTIM:
        d["method"] = method.name
        d["iter"] = args.ctrl.optim.iter
        d["refresh"] = args.ctrl.optim.refresh
        d["save_iterations"] = args.ctrl.optim.save_iterations
        algorithm = optim_algo_t(args.ctrl.optim.algorithm)
        d["algorithm"] = algorithm.name
        if algorithm == optim_algo_t.Newton:
            pass
        elif algorithm == optim_algo_t.LBFGS:
            d["init_alpha"] = args.ctrl.optim.init_alpha
            d["tol_param"] = args.ctrl.optim.tol_param
            d["tol_obj"] = args.ctrl.optim.tol_obj
            d["tol_grad"] = args.ctrl.optim.tol_grad
            d["tol_rel_obj"] = args.ctrl.optim.tol_obj
            d["tol_rel_grad"] = args.ctrl.optim.tol_grad
            d["tol_history_size"] = args.ctrl.optim.tol_grad
        elif algorithm == optim_algo_t.BFGS:
            d["init_alpha"] = args.ctrl.optim.init_alpha
            d["tol_param"] = args.ctrl.optim.tol_param
            d["tol_obj"] = args.ctrl.optim.tol_obj
            d["tol_grad"] = args.ctrl.optim.tol_grad
            d["tol_rel_obj"] = args.ctrl.optim.tol_obj
            d["tol_rel_grad"] = args.ctrl.optim.tol_grad
    elif method == stan_args_method_t.TEST_GRADIENT:
        d["method"] = "test_grad"
        d["test_grad"] = True
        ctrl_d["epsilon"] = args.ctrl.test_grad.epsilon
        ctrl_d["error"] = args.ctrl.test_grad.error
        d["control"] = ctrl_d
    return d


cdef void _set_stanargs_from_dict(StanArgs* p, dict args):
    """Insert values in dictionary `args` into `p`"""
    # _call_sampler requires a specially crafted dictionary of arguments
    # intended for the c++ function sampler_command(...) in stan_fit.hpp
    # If the dictionary doesn't contain the correct keys (arguments),
    # the function will raise a KeyError exception (as it should!).
    cdef vars_r_t init_vars_r
    cdef vars_i_t init_vars_i
    p.random_seed = <unsigned int> args.get('random_seed', 0)
    p.chain_id = <unsigned int> args['chain_id']
    p.init = args['init']
    if args['init'] == b'user':
        init_r, init_i = pystan.misc._split_data(args['init_list'])
        init_vars_r = _dict_to_vars_r(init_r)
        init_vars_i = _dict_to_vars_i(init_i)
        p.init_vars_r = init_vars_r
        p.init_vars_i = init_vars_i
    p.init_radius = args['init_radius']
    p.sample_file = args['sample_file']
    p.append_samples = args['append_samples']
    p.sample_file_flag = args['sample_file_flag']
    p.method = args['method'].value
    p.diagnostic_file = args['diagnostic_file']
    p.diagnostic_file_flag = args['diagnostic_file_flag']
    p.metric_file = args['metric_file']
    p.metric_file_flag = args['metric_file_flag']
    if args['method'] == stan_args_method_t.SAMPLING:
        p.ctrl.sampling.iter = args['ctrl']['sampling']['iter']
        p.ctrl.sampling.refresh = args['ctrl']['sampling']['refresh']
        p.ctrl.sampling.algorithm = args['ctrl']['sampling']['algorithm'].value
        p.ctrl.sampling.warmup = args['ctrl']['sampling']['warmup']
        p.ctrl.sampling.thin = args['ctrl']['sampling']['thin']
        p.ctrl.sampling.save_warmup = args['ctrl']['sampling']['save_warmup']
        p.ctrl.sampling.iter_save = args['ctrl']['sampling']['iter_save']
        p.ctrl.sampling.iter_save_wo_warmup = args['ctrl']['sampling']['iter_save_wo_warmup']
        p.ctrl.sampling.adapt_engaged = args['ctrl']['sampling']['adapt_engaged']
        p.ctrl.sampling.adapt_gamma = args['ctrl']['sampling']['adapt_gamma']
        p.ctrl.sampling.adapt_delta = args['ctrl']['sampling']['adapt_delta']
        p.ctrl.sampling.adapt_kappa = args['ctrl']['sampling']['adapt_kappa']
        p.ctrl.sampling.adapt_init_buffer = args['ctrl']['sampling']['adapt_init_buffer']
        p.ctrl.sampling.adapt_term_buffer = args['ctrl']['sampling']['adapt_term_buffer']
        p.ctrl.sampling.adapt_window = args['ctrl']['sampling']['adapt_window']
        p.ctrl.sampling.adapt_t0 = args['ctrl']['sampling']['adapt_t0']
        p.ctrl.sampling.metric = args['ctrl']['sampling']['metric'].value
        p.ctrl.sampling.stepsize = args['ctrl']['sampling']['stepsize']
        p.ctrl.sampling.stepsize_jitter = args['ctrl']['sampling']['stepsize_jitter']
        if args['ctrl']['sampling']['algorithm'] == sampling_algo_t.NUTS:
            p.ctrl.sampling.max_treedepth = args['ctrl']['sampling']['max_treedepth']
        elif args['ctrl']['sampling']['algorithm'] == sampling_algo_t.HMC:
            p.ctrl.sampling.int_time = args['ctrl']['sampling']['int_time']
    elif args['method'] == stan_args_method_t.OPTIM:
        p.ctrl.optim.iter = args['ctrl']['optim']['iter']
        p.ctrl.optim.refresh = args['ctrl']['optim']['refresh']
        p.ctrl.optim.algorithm = args['ctrl']['optim']['algorithm'].value
        p.ctrl.optim.save_iterations = args['ctrl']['optim']['save_iterations']
        p.ctrl.optim.init_alpha = args['ctrl']['optim']['init_alpha']
        p.ctrl.optim.tol_obj = args['ctrl']['optim']['tol_obj']
        p.ctrl.optim.tol_grad = args['ctrl']['optim']['tol_grad']
        p.ctrl.optim.tol_param = args['ctrl']['optim']['tol_param']
        p.ctrl.optim.tol_rel_obj = args['ctrl']['optim']['tol_rel_obj']
        p.ctrl.optim.tol_rel_grad = args['ctrl']['optim']['tol_rel_grad']
        p.ctrl.optim.history_size = args['ctrl']['optim']['history_size']
    elif args['method'] == stan_args_method_t.TEST_GRADIENT:
        p.ctrl.test_grad.epsilon = args['ctrl']['test_grad']['epsilon']
        p.ctrl.test_grad.error = args['ctrl']['test_grad']['error']
    elif args['method'] == stan_args_method_t.VARIATIONAL:
        p.ctrl.variational.algorithm = args['ctrl']['variational']['algorithm'].value
        p.ctrl.variational.iter = args['ctrl']['variational']['iter']
        p.ctrl.variational.grad_samples = args['ctrl']['variational']['grad_samples']
        p.ctrl.variational.elbo_samples = args['ctrl']['variational']['elbo_samples']
        p.ctrl.variational.eval_elbo = args['ctrl']['variational']['eval_elbo']
        p.ctrl.variational.output_samples = args['ctrl']['variational']['output_samples']
        p.ctrl.variational.eta = args['ctrl']['variational']['eta']
        p.ctrl.variational.adapt_engaged = args['ctrl']['variational']['adapt_engaged']
        p.ctrl.variational.tol_rel_obj = args['ctrl']['variational']['tol_rel_obj']
        p.ctrl.variational.adapt_iter = args['ctrl']['variational']['adapt_iter']

cdef vars_r_t _dict_to_vars_r(data_r):
    """Converts a dict or OrderedDict to a C++ map of string, double pairs"""
    cdef vars_r_t vars_r

    # The dimension for a single value is an empty vector. A list of
    # values is indicated by an entry with the number of values.
    # The dimensions of an array are indicated as one would expect.
    #
    # note, array.flat yields values in C-contiguous style, with the
    # last index varying the fastest. So the transpose is taken
    # so that the ordering matches that used by stan.
    for key in data_r:
        assert isinstance(key, bytes), "Variable name must be bytes."
        val = (data_r[key].T.flat, data_r[key].shape)
        vars_r[key] = val
    return vars_r


cdef vars_i_t _dict_to_vars_i(data_i):
    """Converts a dict or OrdereDict to a C++ map of string, int pairs"""
    cdef vars_i_t vars_i

    # The dimension for a single value is an empty vector. A list of
    # values is indicated by an entry with the number of values.
    # The dimensions of an array are indicated as one would expect.
    #
    # note, array.flat yields values in C-contiguous style, with the
    # last index varying the fastest. So the transpose is taken
    # so that the ordering matches that used by stan.
    for key in data_i:
        assert isinstance(key, bytes), "Variable name must be bytes."
        val = (data_i[key].T.flat, data_i[key].shape)
        vars_i[key] = val
    return vars_i

def _call_sampler_star(data_args):
    return _call_sampler(*data_args)

def _call_sampler(data, args, pars_oi=None):
    """Wrapper for call_sampler in stan_fit

    This function is self-contained and suitable for parallel invocation.

    """
    data_r, data_i = pystan.misc._split_data(data)
    cdef StanHolder *holderptr = new StanHolder()
    cdef StanArgs *argsptr = new StanArgs()
    if not holderptr:
        raise MemoryError("Couldn't allocate space for StanHolder.")
    if not argsptr:
        raise MemoryError("Couldn't allocate space for StanArgs.")
    chain_id = args['chain_id']
    for handler in logger.handlers:
        handler.flush()
    _set_stanargs_from_dict(argsptr, args)

    cdef stan_fit[$model_cppname, ecuyer1988] *fitptr
    cdef vars_r_t vars_r = _dict_to_vars_r(data_r)
    cdef vars_i_t vars_i = _dict_to_vars_i(data_i)
    fitptr = new stan_fit[$model_cppname, ecuyer1988](vars_r, vars_i, argsptr.random_seed)
    if not fitptr:
        raise MemoryError("Couldn't allocate space for stan_fit.")
    # Implementation note: there is an extra stan_fit instance associated
    # with the model (which enables access to some methods). This is a
    # horrible, confusing idea which will hopefully be fixed in Stan 3.
    if pars_oi is not None:
        pars_oi_bytes = [n.encode('ascii') for n in pars_oi]
        if len(pars_oi_bytes) != fitptr.param_names_oi().size():
            fitptr.update_param_oi(pars_oi_bytes)
    ret = fitptr.call_sampler(deref(argsptr), deref(holderptr))
    holder = _pystanholder_from_stanholder(holderptr)
    # FIXME: rather than fetching the args from the holderptr, we just use
    # the argsptr we passed directly. This is a hack to solve a problem
    # that holder.args gets dropped somewhere in C++.
    holder.args = _dict_from_stanargs(argsptr)
    del argsptr
    del fitptr
    return ret, holder

def _split_pars_locs(fnames, pars):
    """Split flatnames to par and location"""
    par_keys = OrderedDict((par, []) for par in pars)
    for key in fnames:
        par_tail = key.split("[")
        par = par_tail[0]
        loc = [Ellipsis]
        for tail in par_tail[1:]:
            loc = []
            for i in tail[:-1].split(","):
                loc.append(int(i)-1)
        par_keys[par].append((key, loc))
    return par_keys

cdef class StanFit4Model:
    """Holder for results obtained from running a Stan model with data

    Attributes
    ----------
    sim : dict
        Holder for runs. Stores samples in sim['samples']
    data : dict
        Data used to fit model.

    Note
    ----
    The only unexpected difference between PyStan and RStan is this: where RStan
    stores samples for a parameter directly in, say, fit@sim$samples[[1]]$theta,
    in PyStan they are in fit.sim['samples'][0]['chains']['theta'].

    The difference is due to Python lacking a dictionary structure that can also
    have attributes.

    """

    cdef stan_fit[$model_cppname, ecuyer1988] *thisptr

    # attributes populated by methods of StanModel
    cdef public data  # dict or OrderedDict
    cdef public random_seed
    cdef public dict sim
    cdef public model_name
    cdef public model_pars
    cdef public par_dims
    cdef public mode
    cdef public inits
    cdef public stan_args
    cdef public stanmodel
    cdef public date
    cdef public _repr_pars
    cdef public _repr_num

    def __cinit__(self, *args):
        # __cinit__ must be callable with no arguments for unpickling
        cdef vars_r_t vars_r
        cdef vars_i_t vars_i
        if len(args) == 2:
            data, random_seed = args
            data_r, data_i = pystan.misc._split_data(data)
            # NB: dictionary keys must be byte strings
            vars_r = _dict_to_vars_r(data_r)
            vars_i = _dict_to_vars_i(data_i)
            # TODO: the random seed needs to be known by StanFit4Model
            self.thisptr = new stan_fit[$model_cppname, ecuyer1988](vars_r, vars_i, <unsigned int> random_seed)
            if not self.thisptr:
                raise MemoryError("Couldn't allocate space for stan_fit.")

    def __init__(self, data, random_seed):
        self.data = data
        self.random_seed = random_seed
        self._set_repr_pars(None)
        self._set_repr_num (100)

    def __dealloc__(self):
        del self.thisptr

    # the following three methods give Cython classes instructions for pickling
    def __getstate__(self):
        attr_names = ('data sim model_name model_pars par_dims mode inits stan_args '
                      'stanmodel date').split()
        state = dict((k, getattr(self, k)) for k in attr_names)
        return state

    def __setstate__(self, state):
        for k in state:
            setattr(self, k, state[k])

    def __reduce__(self):
        msg = ("Pickling fit objects is an experimental feature!\n"
               "The relevant StanModel instance must be pickled along with this fit object.\n"
               "When unpickling the StanModel must be unpickled first.")
        warnings.warn(msg)
        return (StanFit4Model, (self.data, self.random_seed), self.__getstate__(), None, None)

    # public methods

    def plot(self, pars=None, dtypes=None):
        """Visualize samples from posterior distributions

        Parameters
        ---------
        pars : {str, sequence of str}
            parameter name(s); by default use all parameters of interest
        dtypes : dict
            datatype of parameter(s).
            If nothing is passed, np.float will be used for all parameters.
            If np.int is specified, the histogram will be visualized, not but
            kde.

        Note
        ----
        This is currently an alias for the `traceplot` method.
        """
        if pars is None:
            pars = [par for par in self.sim['pars_oi'] if par != 'lp__']
        elif isinstance(pars, string_types):
            pars = [pars]
        pars = pystan.misc._remove_empty_pars(pars, self.sim['pars_oi'], self.sim['dims_oi'])
        return pystan.plots.traceplot(self, pars, dtypes)

    def traceplot(self, pars=None, dtypes=None):
        """Visualize samples from posterior distributions

        Parameters
        ---------
        pars : {str, sequence of str}, optional
            parameter name(s); by default use all parameters of interest
        dtypes : dict
            datatype of parameter(s).
            If nothing is passed, np.float will be used for all parameters.
            If np.int is specified, the histogram will be visualized, not but
            kde.
        """
        # FIXME: for now plot and traceplot do the same thing
        return self.plot(pars, dtypes=dtypes)

    def extract(self, pars=None, permuted=True, inc_warmup=False, dtypes=None):
        """Extract samples in different forms for different parameters.

        Parameters
        ----------
        pars : {str, sequence of str}
           parameter (or quantile) name(s).
        permuted : bool
           If True, returned samples are permuted. All chains are
           merged and warmup samples are discarded.
        inc_warmup : bool
           If True, warmup samples are kept; otherwise they are
           discarded. If `permuted` is True, `inc_warmup` is ignored.
        dtypes : dict
            datatype of parameter(s).
            If nothing is passed, np.float will be used for all parameters.

        Returns
        -------
        samples : dict or array
        If `permuted` is True, return dictionary with samples for each
        parameter (or other quantity) named in `pars`.

        If `permuted` is False and `pars` is None, an array is returned. The first dimension of
        the array is for the iterations; the second for the number of chains;
        the third for the parameters. Vectors and arrays are expanded to one
        parameter (a scalar) per cell, with names indicating the third dimension.
        Parameters are listed in the same order as `model_pars` and `flatnames`.

        If `permuted` is False and `pars` is not None, return dictionary with samples for each
        parameter (or other quantity) named in `pars`. The first dimension of
        the sample array is for the iterations; the second for the number of chains;
        the rest for the parameters. Parameters are listed in the same order as `pars`.

        """
        self._verify_has_samples()
        if inc_warmup is True and permuted is True:
            logger.warning("`inc_warmup` ignored when `permuted` is True.")
            inc_warmup = False
        if dtypes is not None and permuted is False and pars is None:
            logger.warning("`dtypes` ignored when `permuted` is False and `pars` is None")

        pars_original = pars
        if pars is None:
            pars = self.sim['pars_oi']
        elif isinstance(pars, string_types):
            pars = [pars]
        pars = pystan.misc._remove_empty_pars(pars, self.sim['pars_oi'], self.sim['dims_oi'])
        if dtypes is None:
            dtypes = {}

        allpars = self.sim['pars_oi'] + self.sim['fnames_oi']
        pystan.misc._check_pars(allpars, pars)

        n_kept = [s if inc_warmup else s-w for s, w in zip(self.sim['n_save'], self.sim['warmup2'])]
        chains = len(self.sim['samples'])

        # return array (n, chains, flat_pars)
        if (not permuted) and (pars_original is None):
            n = n_kept[0]
            arr_shape = [n, chains, len(self.sim['fnames_oi'])]
            arr = np.empty(arr_shape, order='F')
            for chain, (pyholder, n) in enumerate(zip(self.sim['samples'], n_kept)):
                for i, item in enumerate(pyholder.chains.values()):
                    arr[:, chain, i] = item[-n:]
            return arr


        par_keys = _split_pars_locs(self.sim['fnames_oi'], self.sim['pars_oi'])

        shapes = dict(zip(self.sim['pars_oi'], self.sim['dims_oi']))

        extracted = OrderedDict()
        for par in pars:
            if par in extracted:
                continue
            keys_locs = par_keys.get(par, [(par, [Ellipsis])])
            shape = shapes.get(par, [])
            dtype = dtypes.get(par)

            if permuted:
                arr_shape = [sum(n_kept)] + shape
                arr = np.empty(arr_shape, dtype=dtype, order='F')
                for chain, (pyholder, permutation, n) in enumerate(zip(self.sim['samples'], self.sim['permutation'], n_kept)):
                    n_processed = sum(n_kept[:chain])
                    axes = [slice(n_processed, n_processed+n)]
                    for key, loc in keys_locs:
                        arr_slice = tuple(axes + loc)
                        arr[arr_slice] = pyholder.chains[key][-n:][permutation]
                extracted[par] = arr

            else:
                n = n_kept[0]
                arr_shape = [n, chains] + shape
                arr = np.empty(arr_shape, dtype=dtype, order='F')
                for chain, (pyholder, n) in enumerate(zip(self.sim['samples'], n_kept)):
                    axes = [slice(None), chain]
                    for key, loc in keys_locs:
                        arr_slice = tuple(axes + loc)
                        arr[arr_slice] = pyholder.chains[key][-n:]
                extracted[par] = arr

        return extracted

    def __unicode__(self):
        # for Python 2.x
        return pystan.misc.stansummary(self)

    def __str__(self):
        s = pystan.misc.stansummary(self)
        return s.encode('utf-8') if PY2 else s

    def _get_repr_num(self):
      return self._repr_num

    def _get_repr_pars(self):
        update = False
        if self._repr_pars is not None:
            update = len(self._repr_pars)-1 != self._repr_num
        if self._repr_pars is not None and not update:
            return self._repr_pars
        elif len(self.sim["fnames_oi"]) > self._repr_num:
            logger.warning("Truncated summary with the 'fit.__repr__' method. For the full summary use 'print(fit)'")
            self._set_repr_pars(self.sim["fnames_oi"][:self._repr_num-1] + ['lp__'])
            return self._repr_pars

    def _set_repr_num(self, n):
        self._repr_num = n

    def _set_repr_pars(self, pars):
        self._repr_pars = pars

    def __repr__(self):
        pars = self._get_repr_pars()
        if pars is not None and len(self.sim["fnames_oi"]) > len(pars):
            s = "\nWarning: Shown data is truncated to {} parameters".format(len(pars))
            s += "\nFor the full summary use 'print(fit)'\n\n"
            s += pystan.misc.stansummary(self, pars=pars)
        else:
            s = pystan.misc.stansummary(self)
        return s.encode('utf-8') if PY2 else s

    def __getitem__(self, key):
        extr = self.extract(pars=(key,))
        return extr[key]

    def stansummary(self, pars=None, probs=(0.025, 0.25, 0.5, 0.75, 0.975), digits_summary=2):
        """
        Summary statistic table.

        Parameters
        ----------
        pars : str or sequence of str, optional
            Parameter names. By default use all parameters
        probs : sequence of float, optional
            Quantiles. By default, (0.025, 0.25, 0.5, 0.75, 0.975)
        digits_summary : int, optional
            Number of significant digits. By default, 2
        Returns
        -------
        summary : string
            Table includes mean, se_mean, sd, probs_0, ..., probs_n, n_eff and Rhat.

        Examples
        --------
        >>> model_code = 'parameters {real y;} model {y ~ normal(0,1);}'
        >>> m = StanModel(model_code=model_code, model_name="example_model")
        >>> fit = m.sampling()
        >>> print(fit.stansummary())
        Inference for Stan model: example_model.
        4 chains, each with iter=2000; warmup=1000; thin=1;
        post-warmup draws per chain=1000, total post-warmup draws=4000.

               mean se_mean     sd   2.5%    25%    50%    75%  97.5%  n_eff   Rhat
        y      0.01    0.03    1.0  -2.01  -0.68   0.02   0.72   1.97   1330    1.0
        lp__   -0.5    0.02   0.68  -2.44  -0.66  -0.24  -0.05-5.5e-4   1555    1.0

        Samples were drawn using NUTS at Thu Aug 17 00:52:25 2017.
        For each parameter, n_eff is a crude measure of effective sample size,
        and Rhat is the potential scale reduction factor on split chains (at
        convergence, Rhat=1).
        """
        return pystan.misc.stansummary(fit=self, pars=pars, probs=probs, digits_summary=digits_summary)

    def summary(self, pars=None, probs=None):
        """Summarize samples (compute mean, SD, quantiles) in all chains.
        REF: stanfit-class.R summary method
        Parameters
        ----------
        pars : str or sequence of str, optional
            Parameter names. By default use all parameters
        probs : sequence of float, optional
            Quantiles. By default, (0.025, 0.25, 0.5, 0.75, 0.975)
        Returns
        -------
        summaries : OrderedDict of array
            Array indexed by 'summary' has dimensions (num_params, num_statistics).
            Parameters are unraveled in *row-major order*. Statistics include: mean,
            se_mean, sd, probs_0, ..., probs_n, n_eff, and Rhat. Array indexed by
            'c_summary' breaks down the statistics by chain and has dimensions
            (num_params, num_statistics_c_summary, num_chains). Statistics for
            `c_summary` are the same as for `summary` with the exception that
            se_mean, n_eff, and Rhat are absent. Row names and column names are
            also included in the OrderedDict.
        """
        return pystan.misc._summary(self, pars, probs)

    def log_prob(self, upar, adjust_transform=True, gradient=False):
        """
        Expose the log_prob of the model to stan_fit so user can call
        this function.

        Parameters
        ----------
        upar : array
            The real parameters on the unconstrained space.
        adjust_transform : bool
            Whether we add the term due to the transform from constrained
            space to unconstrained space implicitly done in Stan.

        Note
        ----
        In Stan, the parameters need be defined with their supports. For
        example, for a variance parameter, we must define it on the positive
        real line. But inside Stan's sampler, all parameters defined on the
        constrained space are transformed to unconstrained space, so the log
        density function need be adjusted (i.e., adding the log of the absolute
        value of the Jacobian determinant).  With the transformation, Stan's
        samplers work on the unconstrained space and once a new iteration is
        drawn, Stan transforms the parameters back to their supports. All the
        transformation are done inside Stan without interference from the users.
        However, when using the log density function for a model exposed to
        Python, we need to be careful.  For example, if we are interested in
        finding the mode of parameters on the constrained space, we then do not
        need the adjustment.  For this reason, there is an argument named
        `adjust_transform` for functions `log_prob` and `grad_log_prob`.

        """
        # gradient is ignored for now. Call grad_log_prob to get the gradient.
        cdef vector[double] par_r = np.asarray(upar).flat
        return self.thisptr.log_prob(par_r, adjust_transform, gradient)

    def grad_log_prob(self, upar, adjust_transform=True):
        """
        Expose the grad_log_prob of the model to stan_fit so user
        can call this function.

        Parameters
        ----------
        upar : array
            The real parameters on the unconstrained space.
        adjust_transform : bool
            Whether we add the term due to the transform from constrained
            space to unconstrained space implicitly done in Stan.
        """
        cdef vector[double] par_r, grad
        par_r = np.asarray(upar).flat
        grad = self.thisptr.grad_log_prob(par_r, adjust_transform)
        return np.asarray(grad)

    def get_adaptation_info(self):
        """Obtain adaptation information for sampler, which now only NUTS2 has.

        The results are returned as a list, each element of which is a character
        string for a chain."""
        self._verify_has_samples()
        lai =  [ch['adaptation_info'] for ch in self.sim['samples']]
        return lai

    def get_logposterior(self, inc_warmup=True):
        """Get the log-posterior (up to an additive constant) for all chains.

        Each element of the returned array is the log-posterior for
        a chain. Optional parameter `inc_warmup` indicates whether to
        include the warmup period.
        """
        self._verify_has_samples()
        llp =  [ch['chains']['lp__'] for ch in self.sim['samples']]
        return llp if inc_warmup else [x[warmup2:] for x, warmup2 in zip(llp, self.sim['warmup2'])]

    def get_sampler_params(self, inc_warmup=True):
        """Obtain the parameters used for the sampler such as `stepsize` and
        `treedepth`. The results are returned as a list, each element of which
        is an OrderedDict a chain. The dictionary has number of elements
        corresponding to the number of parameters used in the sampler. Optional
        parameter `inc_warmup` indicates whether to include the warmup period.
        """
        self._verify_has_samples()
        ldf = [OrderedDict(zip(ch['sampler_param_names'], np.array(ch['sampler_params']))) for ch in self.sim['samples']]
        if inc_warmup:
            return ldf
        else:
            for d, warmup2 in zip(ldf, self.sim['warmup2']):
                for key in d:
                    d[key] = d[key][warmup2:]
            return ldf

    def get_posterior_mean(self):
        """Get the posterior mean for all parameters

        Returns
        -------
        means : array of shape (num_parameters, num_chains)
            Order of parameters is given by self.model_pars or self.flatnames
            if parameters of interest include non-scalar parameters. An additional
            column for mean lp__ is also included.
        """
        self._verify_has_samples()
        fnames = self.flatnames
        mean_pars = np.array([ch['mean_pars'] for ch in self.sim['samples']])
        mean_lp__ = np.array([ch['mean_lp__'] for ch in self.sim['samples']])
        mean_pars = np.column_stack(mean_pars)
        assert len(fnames) == len(mean_pars)
        m = np.row_stack([mean_pars, mean_lp__])
        return m

    def constrain_pars(self, np.ndarray[double, ndim=1, mode="c"] upar not None):
        """Transform parameters from unconstrained space to defined support"""
        cdef vector[double] constrained
        constrained = self.thisptr.constrain_pars(upar)
        return np.asarray(constrained)

    def unconstrain_pars(self, par):
        """Transform parameters from defined support to unconstrained space"""
        cdef vector[double] unconstrained
        data_r, data_i = pystan.misc._split_data(par)
        cdef vars_r_t vars_r = _dict_to_vars_r(data_r)
        cdef vars_i_t vars_i = _dict_to_vars_i(data_i)
        unconstrained = self.thisptr.unconstrain_pars(vars_r, vars_i)
        return np.asarray(unconstrained)

    def get_seed(self):
        return self.stan_args[0]['seed']

    def get_inits(self):
        return self.inits

    def get_stancode(self):
        return self.stanmodel.model_code

    def get_stanmodel(self):
        return self.stanmodel

    def get_stepsize(self):
        """Parse stepsize from fit object

        Parameters
        ----------
        fit : StanFit4Model

        Returns
        -------
        list
            Returns an empty list if step sizes
            are not found in ``fit.get_adaptation_info``.
        """
        return pystan.misc.get_stepsize(fit=self)

    def get_inv_metric(self, as_dict=False):
        """Parse inverse metrics from the fit object

        Parameters
        ----------
        as_dict : bool, optional


        Returns
        -------
        list
            Returns an empty list if inverse metric
            is not found in ``fit.get_adaptation_info()``.
        """
        return pystan.misc.get_inv_metric(fit=self, as_dict=as_dict)

    def get_last_position(self, warmup=False):
        """Parse last position from fit object

        Parameters
        ----------
        warmup : bool
            If True, returns the last warmup position, when warmup has been done.
            Otherwise function returns the first sample position.

        Returns
        -------
        list
            list contains a dictionary of last draw from each chain.
        """
        return pystan.misc.get_last_position(fit=self, warmup=warmup)

    def to_dataframe(self, pars=None, permuted=False, dtypes=None, inc_warmup=False, diagnostics=True, header=True):
        """Extract samples as a pandas dataframe for different parameters.

        Parameters
        ----------
        pars : {str, sequence of str}
            parameter (or quantile) name(s).
        permuted : bool, default False
            If True, returned samples are permuted.
            If inc_warmup is True, warmup samples have negative order.
        dtypes : dict
            datatype of parameter(s).
            If nothing is passed, float will be used for all parameters.
        inc_warmup : bool
            If True, warmup samples are kept; otherwise they are
            discarded.
        diagnostics : bool
            If True, include hmc diagnostics in dataframe.
        header : bool
            If True, include header columns.


        Returns
        -------
        df : pandas dataframe
            Returned dataframe contains: [header_df]|[draws_df]|[diagnostics_df],
            where all groups are optional.
            To exclude draws_df use `pars=[]`.


        Note
        ----
        Unlike default in extract (`permuted=True`)
        `.to_dataframe` method returns non-permuted samples (`permuted=False`) with diagnostics params included.
        """
        return pystan.misc.to_dataframe(fit=self, pars=pars, permuted=permuted, dtypes=dtypes, inc_warmup=inc_warmup, diagnostics=diagnostics, header=header)


    # FIXME: when this is a normal Python class one can use @property instead
    # of this special Cython syntax.
    property flatnames:

        def __get__(self):
            # NOTE: RStan rewrites the C++ function get_all_flatnames in R (in misc.R).
            # PyStan exposes and calls the C++ function directly.
            cdef vector[string] fnames
            names = [n.encode('ascii') for n in self.model_pars]
            get_all_flatnames(names, self.par_dims, fnames, col_major=True)
            return [n.decode('ascii') for n in fnames]


    # "private" Python methods

    def _verify_has_samples(self):
        if self.mode == 1:
            msg = "Stan model {} is of mode 'test_grad';\n" \
                "sampling is not conducted."
            raise AttributeError(msg.format(self.model_name))
        elif self.mode == 2 or self.sim.get('samples') is None:
            msg = "Stan model {} does not contain samples."
            raise AttributeError(msg.format(self.model_name))

    def _update_param_oi(self, pars):
        pars_bytes = [n.encode('ascii') for n in pars]
        cdef vector[string] pars_ = pars_bytes
        cdef int ret = self.thisptr.update_param_oi(pars_)
        return ret

    def _get_param_names(self):
        cdef vector[string] param_names_bytes = self.thisptr.param_names()
        param_names = [n.decode('utf-8') for n in param_names_bytes]
        return param_names

    def _get_param_fnames_oi(self):
        cdef vector[string] param_fnames_bytes = self.thisptr.param_fnames_oi()
        param_fnames = [n.decode('utf-8') for n in param_fnames_bytes]
        return param_fnames

    def _get_param_names_oi(self):
        cdef vector[string] param_names_bytes = self.thisptr.param_names_oi()
        param_names = [n.decode('utf-8') for n in param_names_bytes]
        return param_names

    def _get_param_dims(self):
        cdef vector[vector[uint]] dims = self.thisptr.param_dims()
        dims_ = dims
        return dims_

    def _get_param_dims_oi(self):
        cdef vector[vector[uint]] dims = self.thisptr.param_dims_oi()
        dims_ = dims
        return dims_

    def constrained_param_names(self):
        cdef vector[string] param_names_bytes = self.thisptr.constrained_param_names(False, False)
        param_names = [n.decode('utf-8') for n in param_names_bytes]
        return param_names

    def unconstrained_param_names(self):
        cdef vector[string] param_names_bytes = self.thisptr.unconstrained_param_names(False, False)
        param_names = [n.decode('utf-8') for n in param_names_bytes]
        return param_names

    def _call_sampler(self, dict args, pars_oi=None):
        return _call_sampler(self.data, args, pars_oi=pars_oi)
