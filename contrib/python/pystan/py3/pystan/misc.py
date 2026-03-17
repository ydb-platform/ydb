"""PyStan utility functions

These functions validate and organize data passed to and from the
classes and functions defined in the file `stan_fit.hpp` and wrapped
by the Cython file `stan_fit.pxd`.

"""
#-----------------------------------------------------------------------------
# Copyright (c) 2013-2015, PyStan developers
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------

# REF: rstan/rstan/R/misc.R

from __future__ import unicode_literals, division
from pystan._compat import PY2, string_types

from collections import OrderedDict
if PY2:
    from collections import Callable, Iterable, Sequence
else:
    from collections.abc import Callable, Iterable, Sequence
import inspect
import io
import itertools
import logging
import math
from numbers import Number
import os
import random
import re
import sys
import shutil
import tempfile
import time

import numpy as np
try:
    from scipy.stats.mstats import mquantiles
except ImportError:
    from pystan.external.scipy.mstats import mquantiles

import pystan.chains
import pystan._misc
from pystan.constants import (MAX_UINT, sampling_algo_t, optim_algo_t,
                              variational_algo_t, sampling_metric_t, stan_args_method_t)

logger = logging.getLogger('pystan')


def stansummary(fit, pars=None, probs=(0.025, 0.25, 0.5, 0.75, 0.975), digits_summary=2):
    """
    Summary statistic table.

    Parameters
    ----------
    fit : StanFit4Model object
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
    >>> print(stansummary(fit))
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
    if fit.mode == 1:
        return "Stan model '{}' is of mode 'test_grad';\n"\
               "sampling is not conducted.".format(fit.model_name)
    elif fit.mode == 2:
        return "Stan model '{}' does not contain samples.".format(fit.model_name)

    n_kept = [s - w for s, w in zip(fit.sim['n_save'], fit.sim['warmup2'])]
    header = "Inference for Stan model: {}.\n".format(fit.model_name)
    header += "{} chains, each with iter={}; warmup={}; thin={}; \n"
    header = header.format(fit.sim['chains'], fit.sim['iter'], fit.sim['warmup'],
                           fit.sim['thin'], sum(n_kept))
    header += "post-warmup draws per chain={}, total post-warmup draws={}.\n\n"
    header = header.format(n_kept[0], sum(n_kept))
    footer = "\n\nSamples were drawn using {} at {}.\n"\
        "For each parameter, n_eff is a crude measure of effective sample size,\n"\
        "and Rhat is the potential scale reduction factor on split chains (at \n"\
        "convergence, Rhat=1)."
    sampler = fit.sim['samples'][0]['args']['sampler_t']
    date = fit.date.strftime('%c')  # %c is locale's representation
    footer = footer.format(sampler, date)
    s = _summary(fit, pars, probs)
    body = _array_to_table(s['summary'], s['summary_rownames'],
                           s['summary_colnames'], digits_summary)
    return header + body + footer

def _print_stanfit(fit, pars=None, probs=(0.025, 0.25, 0.5, 0.75, 0.975), digits_summary=2):
    # warning added in PyStan 2.17.0
    logger.warning('Function `_print_stanfit` is deprecated and will be removed in a future version. '\
                  'Use `stansummary` instead.', DeprecationWarning)
    return stansummary(fit, pars=pars, probs=probs, digits_summary=digits_summary)

def _array_to_table(arr, rownames, colnames, n_digits):
    """Print an array with row and column names

    Example:
                  mean se_mean  sd 2.5%  25%  50%  75% 97.5% n_eff Rhat
        beta[1,1]  0.0     0.0 1.0 -2.0 -0.7  0.0  0.7   2.0  4000    1
        beta[1,2]  0.0     0.0 1.0 -2.1 -0.7  0.0  0.7   2.0  4000    1
        beta[2,1]  0.0     0.0 1.0 -2.0 -0.7  0.0  0.7   2.0  4000    1
        beta[2,2]  0.0     0.0 1.0 -1.9 -0.6  0.0  0.7   2.0  4000    1
        lp__      -4.2     0.1 2.1 -9.4 -5.4 -3.8 -2.7  -1.2   317    1
    """
    assert arr.shape == (len(rownames), len(colnames))
    rownames_maxwidth = max(len(n) for n in rownames)
    max_col_width = 7
    min_col_width = 5
    max_col_header_num_width = [max(max_col_width, max(len(n) + 1, min_col_width)) for n in colnames]
    rows = []
    for row in arr:
        row_nums = []
        for j, (num, width) in enumerate(zip(row, max_col_header_num_width)):
            if colnames[j] == "n_eff":
                num = int(round(num, 0)) if not np.isnan(num) else num
            num = _format_number(num, n_digits, max_col_width - 1)
            row_nums.append(num)
            if len(num) + 1 > max_col_header_num_width[j]:
                max_col_header_num_width[j] = len(num) + 1
        rows.append(row_nums)
    widths = [rownames_maxwidth] + max_col_header_num_width
    header = '{:>{width}}'.format('', width=widths[0])
    for name, width in zip(colnames, widths[1:]):
        header += '{name:>{width}}'.format(name=name, width=width)
    lines = [header]
    for rowname, row in zip(rownames, rows):
        line = '{name:{width}}'.format(name=rowname, width=widths[0])
        for j, (num, width) in enumerate(zip(row, widths[1:])):
            line += '{num:>{width}}'.format(num=num, width=width)
        lines.append(line)
    return '\n'.join(lines)


def _number_width(n):
    """Calculate the width in characters required to print a number

    For example, -1024 takes 5 characters. -0.034 takes 6 characters.
    """
    return len(str(n))


def _format_number_si(num, n_signif_figures):
    """Format a number using scientific notation to given significant figures"""
    if math.isnan(num) or math.isinf(num):
        return str(num)
    leading, exp = '{:E}'.format(num).split('E')
    leading = round(float(leading), n_signif_figures - 1)
    exp = exp[:1] + exp[2:] if exp[1] == '0' else exp
    formatted = '{}e{}'.format(leading, exp.lstrip('+'))
    return formatted


def _format_number(num, n_signif_figures, max_width):
    """Format a number as a string while obeying space constraints.

    `n_signif_figures` is the minimum number of significant figures expressed
    `max_width` is the maximum width in characters allowed
    """
    if max_width < 6:
        raise NotImplementedError("Guaranteed formatting in fewer than 6 characters not supported.")
    if math.isnan(num) or math.isinf(num):
        return str(num)
    # add 0.5 to prevent log(0) errors; only affects n_digits calculation for num > 0
    n_digits = lambda num: math.floor(math.log10(abs(num) + 0.5)) + 1
    if abs(num) > 10**-n_signif_figures and n_digits(num) <= max_width - n_signif_figures:
        return str(round(num, n_signif_figures))[:max_width].rstrip('.')
    elif _number_width(num) <= max_width:
        if n_digits(num) >= n_signif_figures:
            # the int() is necessary for consistency between Python 2 and 3
            return str(int(round(num)))
        else:
            return str(num)
    else:
        return _format_number_si(num, n_signif_figures)


def _summary(fit, pars=None, probs=None, **kwargs):
    """Summarize samples (compute mean, SD, quantiles) in all chains.

    REF: stanfit-class.R summary method

    Parameters
    ----------
    fit : StanFit4Model object
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
    if fit.mode == 1:
        msg = "Stan model {} is of mode 'test_grad'; sampling is not conducted."
        msg = msg.format(fit.model_name)
        raise ValueError(msg)
    elif fit.mode == 2:
        msg = "Stan model {} contains no samples.".format(fit.model_name)
        raise ValueError(msg)

    if fit.sim['n_save'] == fit.sim['warmup2']:
        msg = "Stan model {} contains no samples.".format(fit.model_name)
        raise ValueError(msg)

    # rstan checks for cached summaries here

    if pars is None:
        pars = fit.sim['pars_oi']
    elif isinstance(pars, string_types):
        pars = [pars]
    pars = _remove_empty_pars(pars, fit.sim['pars_oi'], fit.sim['dims_oi'])

    if probs is None:
        probs = (0.025, 0.25, 0.5, 0.75, 0.975)
    ss = _summary_sim(fit.sim, pars, probs)
    # TODO: include sem, ess and rhat: ss['ess'], ss['rhat']
    s1 = np.column_stack([ss['msd'][:, 0], ss['sem'], ss['msd'][:, 1], ss['quan'], ss['ess'], ss['rhat']])
    s1_rownames = ss['c_msd_names']['parameters']
    s1_colnames = ((ss['c_msd_names']['stats'][0],) + ('se_mean',) +
                   (ss['c_msd_names']['stats'][1],) + ss['c_quan_names']['stats'] +
                   ('n_eff', 'Rhat'))
    s2 = _combine_msd_quan(ss['c_msd'], ss['c_quan'])
    s2_rownames = ss['c_msd_names']['parameters']
    s2_colnames = ss['c_msd_names']['stats'] + ss['c_quan_names']['stats']
    return OrderedDict(summary=s1, c_summary=s2,
                       summary_rownames=s1_rownames,
                       summary_colnames=s1_colnames,
                       c_summary_rownames=s2_rownames,
                       c_summary_colnames=s2_colnames)


def _combine_msd_quan(msd, quan):
    """Combine msd and quantiles in chain summary

    Parameters
    ----------
    msd : array of shape (num_params, 2, num_chains)
       mean and sd for chains
    cquan : array of shape (num_params, num_quan, num_chains)
        quantiles for chains

    Returns
    -------
    msdquan : array of shape (num_params, 2 + num_quan, num_chains)
    """
    dim1 = msd.shape
    n_par, _, n_chains = dim1
    ll = []
    for i in range(n_chains):
        a1 = msd[:, :, i]
        a2 = quan[:, :, i]
        ll.append(np.column_stack([a1, a2]))
    msdquan = np.dstack(ll)
    return msdquan


def _summary_sim(sim, pars, probs):
    """Summarize chains together and separately

    REF: rstan/rstan/R/misc.R

    Parameters are unraveled in *column-major order*.

    Parameters
    ----------
    sim : dict
        dict from from a stanfit fit object, i.e., fit['sim']
    pars : Iterable of str
        parameter names
    probs : Iterable of probs
        desired quantiles

    Returns
    -------
    summaries : OrderedDict of array
        This dictionary contains the following arrays indexed by the keys
        given below:
        - 'msd' : array of shape (num_params, 2) with mean and sd
        - 'sem' : array of length num_params with standard error for the mean
        - 'c_msd' : array of shape (num_params, 2, num_chains)
        - 'quan' : array of shape (num_params, num_quan)
        - 'c_quan' : array of shape (num_params, num_quan, num_chains)
        - 'ess' : array of shape (num_params, 1)
        - 'rhat' : array of shape (num_params, 1)

    Note
    ----
    `_summary_sim` has the parameters in *column-major* order whereas `_summary`
    gives them in *row-major* order. (This follows RStan.)
    """
    # NOTE: this follows RStan rather closely. Some of the calculations here
    probs_len = len(probs)
    n_chains = len(sim['samples'])
    # tidx is a dict with keys that are parameters and values that are their
    # indices using column-major ordering
    tidx = _pars_total_indexes(sim['pars_oi'], sim['dims_oi'], sim['fnames_oi'], pars)
    tidx_colm = [tidx[par] for par in pars]
    tidx_colm = list(itertools.chain(*tidx_colm))  # like R's unlist()
    tidx_rowm = [tidx[par+'_rowmajor'] for par in pars]
    tidx_rowm = list(itertools.chain(*tidx_rowm))
    tidx_len = len(tidx_colm)
    lmsdq = [_get_par_summary(sim, i, probs) for i in tidx_colm]
    msd = np.row_stack([x['msd'] for x in lmsdq])
    quan = np.row_stack([x['quan'] for x in lmsdq])
    probs_str = tuple(["{:g}%".format(100*p) for p in probs])
    msd = msd.reshape(tidx_len, 2, order='F')
    quan = quan.reshape(tidx_len, probs_len, order='F')

    c_msd = np.row_stack([x['c_msd'] for x in lmsdq])
    c_quan = np.row_stack([x['c_quan'] for x in lmsdq])
    c_msd = c_msd.reshape(tidx_len, 2, n_chains, order='F')
    c_quan = c_quan.reshape(tidx_len, probs_len, n_chains, order='F')
    sim_attr_args = sim.get('args', None)
    if sim_attr_args is None:
        cids = list(range(n_chains))
    else:
        cids = [x['chain_id'] for x in sim_attr_args]

    c_msd_names = dict(parameters=np.asarray(sim['fnames_oi'])[tidx_colm],
                       stats=("mean", "sd"),
                       chains=tuple("chain:{}".format(cid) for cid in cids))
    c_quan_names = dict(parameters=np.asarray(sim['fnames_oi'])[tidx_colm],
                        stats=probs_str,
                        chains=tuple("chain:{}".format(cid) for cid in cids))
    ess_and_rhat = np.array([pystan.chains.ess_and_splitrhat(sim, n) for n in tidx_colm])
    ess, rhat = [arr.ravel() for arr in np.hsplit(ess_and_rhat, 2)]
    return dict(msd=msd, c_msd=c_msd, c_msd_names=c_msd_names, quan=quan,
                c_quan=c_quan, c_quan_names=c_quan_names,
                sem=msd[:, 1] / np.sqrt(ess), ess=ess, rhat=rhat,
                row_major_idx=tidx_rowm, col_major_idx=tidx_colm)


def _get_par_summary(sim, n, probs):
    """Summarize chains merged and individually

    Parameters
    ----------
    sim : dict from stanfit object
    n : int
        parameter index
    probs : iterable of int
        quantiles

    Returns
    -------
    summary : dict
       Dictionary containing summaries
    """
    # _get_samples gets chains for nth parameter
    ss = _get_samples(n, sim, inc_warmup=False)
    msdfun = lambda chain: (np.mean(chain), np.std(chain, ddof=1))
    qfun = lambda chain: mquantiles(chain, probs)
    c_msd = np.array([msdfun(s) for s in ss]).flatten()
    c_quan = np.array([qfun(s) for s in ss]).flatten()
    ass = np.asarray(ss).flatten()
    msd = np.asarray(msdfun(ass))
    quan = qfun(np.asarray(ass))
    return dict(msd=msd, quan=quan, c_msd=c_msd, c_quan=c_quan)


def _split_data(data):
    data_r = {}
    data_i = {}
    # data_r and data_i are going to be converted into C++ objects of
    # type: map<string, pair<vector<double>, vector<size_t>>> and
    # map<string, pair<vector<int>, vector<size_t>>> so prepare
    # them accordingly.
    for k, v in data.items():
        if np.issubdtype(np.asarray(v).dtype, np.integer):
            data_i.update({k.encode('utf-8'): np.asarray(v, dtype=int)})
        elif np.issubdtype(np.asarray(v).dtype, np.floating):
            data_r.update({k.encode('utf-8'): np.asarray(v, dtype=float)})
        else:
            msg = "Variable {} is neither int nor float nor list/array thereof"
            raise ValueError(msg.format(k))
    return data_r, data_i


def _config_argss(chains, iter, warmup, thin,
                  init, seed, sample_file, diagnostic_file, algorithm,
                  control, **kwargs):
    # After rstan/rstan/R/misc.R (config_argss)
    iter = int(iter)
    if iter < 1:
        raise ValueError("`iter` should be a positive integer.")
    thin = int(thin)
    if thin < 1 or thin > iter:
        raise ValueError("`thin should be a positive integer "
                         "less than `iter`.")
    warmup = max(0, int(warmup))
    if warmup > iter:
        raise ValueError("`warmup` should be an integer less than `iter`.")
    chains = int(chains)
    if chains < 1:
        raise ValueError("`chains` should be a positive integer.")

    iters = [iter] * chains
    thins = [thin] * chains
    warmups = [warmup] * chains

    # use chain_id argument if specified
    if kwargs.get('chain_id') is None:
        chain_id = list(range(chains))
    else:
        chain_id = [int(id) for id in kwargs['chain_id']]
        if len(set(chain_id)) != len(chain_id):
            raise ValueError("`chain_id` has duplicated elements.")
        chain_id_len = len(chain_id)
        if chain_id_len >= chains:
            chain_id = chain_id
        else:
            chain_id = chain_id + [max(chain_id) + 1 + i
                                   for i in range(chains - chain_id_len)]
        del kwargs['chain_id']

    inits_specified = False
    # slight difference here from rstan; Python's lists are not typed.
    if isinstance(init, Number):
        init = str(init)
    if isinstance(init, string_types):
        if init in ['0', 'random']:
            inits = [init] * chains
        else:
            inits = ["random"] * chains
        inits_specified = True
    if not inits_specified and isinstance(init, Callable):
        ## test if function takes argument named "chain_id"
        if "chain_id" in inspect.getargspec(init).args:
            inits = [init(chain_id=id) for id in chain_id]
        else:
            inits = [init()] * chains
        if not isinstance(inits[0], dict):
            raise ValueError("The function specifying initial values must "
                             "return a dictionary.")
        inits_specified = True
    if not inits_specified and isinstance(init, Sequence):
        if len(init) != chains:
            raise ValueError("Length of list of initial values does not "
                             "match number of chains.")
        if not all([isinstance(d, dict) for d in init]):
            raise ValueError("Initial value list is not a sequence of "
                             "dictionaries.")
        inits = init
        inits_specified = True
    if not inits_specified:
        raise ValueError("Invalid specification of initial values.")

    ## only one seed is needed by virtue of the RNG
    seed = _check_seed(seed)

    kwargs['method'] = "test_grad" if kwargs.get('test_grad') else 'sampling'

    all_control = {
        "adapt_engaged", "adapt_gamma", "adapt_delta", "adapt_kappa",
        "adapt_t0", "adapt_init_buffer", "adapt_term_buffer", "adapt_window",
        "stepsize", "stepsize_jitter", "metric", "int_time",
        "max_treedepth", "epsilon", "error", "inv_metric"
    }
    all_metrics = {"unit_e", "diag_e", "dense_e"}

    if control is not None:
        if not isinstance(control, dict):
            raise ValueError("`control` must be a dictionary")
        if not all(key in all_control for key in control):
            unknown = set(control) - all_control
            raise ValueError("`control` contains unknown parameters: {}".format(unknown))
        if control.get('metric') and control['metric'] not in all_metrics:
            raise ValueError("`metric` must be one of {}".format(all_metrics))
        kwargs['control'] = control

    argss = [dict() for _ in range(chains)]
    for i in range(chains):
        argss[i] = dict(chain_id=chain_id[i],
                        iter=iters[i], thin=thins[i], seed=seed,
                        warmup=warmups[i], init=inits[i],
                        algorithm=algorithm)

    if sample_file is not None:
        sample_file = _writable_sample_file(sample_file)
        if chains == 1:
            argss[0]['sample_file'] = sample_file
        elif chains > 1:
            for i in range(chains):
                argss[i]['sample_file'] = _append_id(sample_file, i)

    if diagnostic_file is not None:
        raise NotImplementedError("diagnostic_file not implemented yet.")

    if control is not None and "inv_metric" in control:
        inv_metric = control.pop("inv_metric")
        metric_dir = tempfile.mkdtemp()
        if isinstance(inv_metric, dict):
            for i in range(chains):
                if i not in inv_metric:
                    msg = "Invalid value for init_inv_metric found (keys={}). " \
                          "Use either a dictionary with chain_index as keys (0,1,2,...)" \
                          "or ndarray."
                    msg = msg.format(list(metric_file.keys()))
                    raise ValueError(msg)
                mass_values = inv_metric[i]
                metric_filename = "inv_metric_chain_{}.Rdata".format(str(i))
                metric_path = os.path.join(metric_dir, metric_filename)
                if isinstance(mass_values, str):
                    if not os.path.exists(mass_values):
                        raise ValueError("inverse metric file was not found: {}".format(mass_values))
                    shutil.copy(mass_values, metric_path)
                else:
                    stan_rdump(dict(inv_metric=mass_values), metric_path)
                argss[i]['metric_file'] = metric_path
        elif isinstance(inv_metric, str):
            if not os.path.exists(inv_metric):
                raise ValueError("inverse metric  file was not found: {}".format(inv_metric))
            for i in range(chains):
                metric_filename = "inv_metric_chain_{}.Rdata".format(str(i))
                metric_path = os.path.join(metric_dir, metric_filename)
                shutil.copy(inv_metric, metric_path)
                argss[i]['metric_file'] = metric_path
        elif isinstance(inv_metric, Iterable):
            metric_filename = "inv_metric_chain_0.Rdata"
            metric_path = os.path.join(metric_dir, metric_filename)
            stan_rdump(dict(inv_metric=inv_metric), metric_path)
            argss[0]['metric_file'] = metric_path
            for i in range(1, chains):
                metric_filename = "inv_metric_chain_{}.Rdata".format(str(i))
                metric_path = os.path.join(metric_dir, metric_filename)
                shutil.copy(argss[i-1]['metric_file'], metric_path)
                argss[i]['metric_file'] = metric_path
        else:
            argss[i]['metric_file'] = ""

    stepsize_list = None
    if "control" in kwargs and "stepsize" in kwargs["control"]:
        if isinstance(kwargs["control"]["stepsize"], Sequence):
            stepsize_list = kwargs["control"]["stepsize"]
            if len(kwargs["control"]["stepsize"]) == 1:
                kwargs["control"]["stepsize"] = kwargs["control"]["stepsize"][0]
            elif len(kwargs["control"]["stepsize"]) != chains:
                raise ValueError("stepsize length needs to equal chain count.")
            else:
                stepsize_list = kwargs["control"]["stepsize"]

    for i in range(chains):
        argss[i].update(kwargs)
        if stepsize_list is not None:
            argss[i]["control"]["stepsize"] = stepsize_list[i]
        argss[i] = _get_valid_stan_args(argss[i])

    return argss


def _get_valid_stan_args(base_args=None):
    """Fill in default values for arguments not provided in `base_args`.

    RStan does this in C++ in stan_args.hpp in the stan_args constructor.
    It seems easier to deal with here in Python.

    """
    args = base_args.copy() if base_args is not None else {}
    # Default arguments, c.f. rstan/rstan/inst/include/rstan/stan_args.hpp
    # values in args are going to be converted into C++ objects so
    # prepare them accordingly---e.g., unicode -> bytes -> std::string
    args['chain_id'] = args.get('chain_id', 1)
    args['append_samples'] = args.get('append_samples', False)
    if args.get('method') is None or args['method'] == "sampling":
        args['method'] = stan_args_method_t.SAMPLING
    elif args['method'] == "optim":
        args['method'] = stan_args_method_t.OPTIM
    elif args['method'] == 'test_grad':
        args['method'] = stan_args_method_t.TEST_GRADIENT
    elif args['method'] == 'variational':
        args['method'] = stan_args_method_t.VARIATIONAL
    else:
        args['method'] = stan_args_method_t.SAMPLING
    args['sample_file_flag'] = True if args.get('sample_file') else False
    args['sample_file'] = args.get('sample_file', '').encode('ascii')
    args['diagnostic_file_flag'] = True if args.get('diagnostic_file') else False
    args['diagnostic_file'] = args.get('diagnostic_file', '').encode('ascii')
    # NB: argument named "seed" not "random_seed"
    args['random_seed'] = args.get('seed', int(time.time()))

    args['metric_file_flag'] = True if args.get('metric_file') else False
    args['metric_file'] = args.get('metric_file', '').encode('ascii')

    if args['method'] == stan_args_method_t.VARIATIONAL:
        # variational does not use a `control` map like sampling
        args['ctrl'] = args.get('ctrl', dict(variational=dict()))
        args['ctrl']['variational']['iter'] = args.get('iter', 10000)
        args['ctrl']['variational']['grad_samples'] = args.get('grad_samples', 1)
        args['ctrl']['variational']['elbo_samples'] = args.get('elbo_samples', 100)
        args['ctrl']['variational']['eval_elbo'] = args.get('eval_elbo', 100)
        args['ctrl']['variational']['output_samples'] = args.get('output_samples', 1000)
        args['ctrl']['variational']['adapt_iter'] = args.get('adapt_iter', 50)
        args['ctrl']['variational']['eta'] = args.get('eta', 1.0)
        args['ctrl']['variational']['adapt_engaged'] = args.get('adapt_engaged', True)
        args['ctrl']['variational']['tol_rel_obj'] = args.get('tol_rel_obj', 0.01)
        if args.get('algorithm', '').lower() == 'fullrank':
            args['ctrl']['variational']['algorithm'] = variational_algo_t.FULLRANK
        else:
            args['ctrl']['variational']['algorithm'] = variational_algo_t.MEANFIELD
    elif args['method'] == stan_args_method_t.SAMPLING:
        args['ctrl'] = args.get('ctrl', dict(sampling=dict()))
        args['ctrl']['sampling']['iter'] = iter = args.get('iter', 2000)
        args['ctrl']['sampling']['warmup'] = warmup = args.get('warmup', iter // 2)
        calculated_thin = iter - warmup // 1000
        if calculated_thin < 1:
            calculated_thin = 1
        args['ctrl']['sampling']['thin'] = thin = args.get('thin', calculated_thin)
        args['ctrl']['sampling']['save_warmup'] = True  # always True now
        args['ctrl']['sampling']['iter_save_wo_warmup'] = iter_save_wo_warmup = 1 + (iter - warmup - 1) // thin
        args['ctrl']['sampling']['iter_save'] = iter_save_wo_warmup + 1 + (warmup - 1) // thin
        refresh = iter // 10 if iter >= 20 else 1
        args['ctrl']['sampling']['refresh'] = args.get('refresh', refresh)

        ctrl_lst = args.get('control', dict())
        ctrl_sampling = args['ctrl']['sampling']
        # NB: if these defaults change, remember to update docstrings
        ctrl_sampling['adapt_engaged'] = ctrl_lst.get("adapt_engaged", True)
        ctrl_sampling['adapt_gamma'] = ctrl_lst.get("adapt_gamma", 0.05)
        ctrl_sampling['adapt_delta'] = ctrl_lst.get("adapt_delta", 0.8)
        ctrl_sampling['adapt_kappa'] = ctrl_lst.get("adapt_kappa", 0.75)
        ctrl_sampling['adapt_t0'] = ctrl_lst.get("adapt_t0", 10.0)
        ctrl_sampling['adapt_init_buffer'] = ctrl_lst.get("adapt_init_buffer", 75)
        ctrl_sampling['adapt_term_buffer'] = ctrl_lst.get("adapt_term_buffer", 50)
        ctrl_sampling['adapt_window'] = ctrl_lst.get("adapt_window", 25)
        ctrl_sampling['stepsize'] = ctrl_lst.get("stepsize", 1.0)
        ctrl_sampling['stepsize_jitter'] = ctrl_lst.get("stepsize_jitter", 0.0)

        algorithm = args.get('algorithm', 'NUTS')
        if algorithm == 'HMC':
            args['ctrl']['sampling']['algorithm'] = sampling_algo_t.HMC
        elif algorithm == 'Metropolis':
            args['ctrl']['sampling']['algorithm'] = sampling_algo_t.Metropolis
        elif algorithm == 'NUTS':
            args['ctrl']['sampling']['algorithm'] = sampling_algo_t.NUTS
        elif algorithm == 'Fixed_param':
            args['ctrl']['sampling']['algorithm'] = sampling_algo_t.Fixed_param
            # TODO: Setting adapt_engaged to False solves the segfault reported
            # in issue #200; find out why this hack is needed. RStan deals with
            # the setting elsewhere.
            ctrl_sampling['adapt_engaged'] = False
        else:
            msg = "Invalid value for parameter algorithm (found {}; " \
                "require HMC, Metropolis, NUTS, or Fixed_param).".format(algorithm)
            raise ValueError(msg)

        metric = ctrl_lst.get('metric', 'diag_e')
        if metric == "unit_e":
            ctrl_sampling['metric'] = sampling_metric_t.UNIT_E
        elif metric == "diag_e":
            ctrl_sampling['metric'] = sampling_metric_t.DIAG_E
        elif metric == "dense_e":
            ctrl_sampling['metric'] = sampling_metric_t.DENSE_E

        if ctrl_sampling['algorithm'] == sampling_algo_t.NUTS:
            ctrl_sampling['max_treedepth'] = ctrl_lst.get("max_treedepth", 10)
        elif ctrl_sampling['algorithm'] == sampling_algo_t.HMC:
            ctrl_sampling['int_time'] = ctrl_lst.get('int_time', 6.283185307179586476925286766559005768e+00)
        elif ctrl_sampling['algorithm'] == sampling_algo_t.Metropolis:
            pass
        elif ctrl_sampling['algorithm'] == sampling_algo_t.Fixed_param:
            pass

    elif args['method'] == stan_args_method_t.OPTIM:
        args['ctrl'] = args.get('ctrl', dict(optim=dict()))
        args['ctrl']['optim']['iter'] = iter = args.get('iter', 2000)
        algorithm = args.get('algorithm', 'LBFGS')
        if algorithm == "BFGS":
            args['ctrl']['optim']['algorithm'] = optim_algo_t.BFGS
        elif algorithm == "Newton":
            args['ctrl']['optim']['algorithm'] = optim_algo_t.Newton
        elif algorithm == "LBFGS":
            args['ctrl']['optim']['algorithm'] = optim_algo_t.LBFGS
        else:
            msg = "Invalid value for parameter algorithm (found {}; " \
                  "require (L)BFGS or Newton).".format(algorithm)
            raise ValueError(msg)
        refresh = args['ctrl']['optim']['iter'] // 100
        args['ctrl']['optim']['refresh'] = args.get('refresh', refresh)
        if args['ctrl']['optim']['refresh'] < 1:
            args['ctrl']['optim']['refresh'] = 1
        args['ctrl']['optim']['init_alpha'] = args.get("init_alpha", 0.001)
        args['ctrl']['optim']['tol_obj'] = args.get("tol_obj", 1e-12)
        args['ctrl']['optim']['tol_grad'] = args.get("tol_grad", 1e-8)
        args['ctrl']['optim']['tol_param'] = args.get("tol_param", 1e-8)
        args['ctrl']['optim']['tol_rel_obj'] = args.get("tol_rel_obj", 1e4)
        args['ctrl']['optim']['tol_rel_grad'] = args.get("tol_rel_grad", 1e7)
        args['ctrl']['optim']['save_iterations'] = args.get("save_iterations", True)
        args['ctrl']['optim']['history_size'] = args.get("history_size", 5)
    elif args['method'] == stan_args_method_t.TEST_GRADIENT:
        args['ctrl'] = args.get('ctrl', dict(test_grad=dict()))
        args['ctrl']['test_grad']['epsilon'] = args.get("epsilon", 1e-6)
        args['ctrl']['test_grad']['error'] = args.get("error", 1e-6)

    init = args.get('init', "random")
    if isinstance(init, string_types):
        args['init'] = init.encode('ascii')
    elif isinstance(init, dict):
        args['init'] = "user".encode('ascii')
        # while the name is 'init_list', it is a dict; the name comes from rstan,
        # where list elements can have names
        args['init_list'] = init
    else:
        args['init'] = "random".encode('ascii')

    args['init_radius'] = args.get('init_r', 2.0)
    if (args['init_radius'] <= 0):
        args['init'] = b"0"

    # 0 initialization requires init_radius = 0
    if (args['init'] == b"0" or args['init'] == 0):
        args['init_radius'] = 0.0

    args['enable_random_init'] = args.get('enable_random_init', True)
    # RStan calls validate_args() here
    return args


def _check_seed(seed):
    """If possible, convert `seed` into a valid form for Stan (an integer
    between 0 and MAX_UINT, inclusive). If not possible, use a random seed
    instead and raise a warning if `seed` was not provided as `None`.
    """
    if isinstance(seed, (Number, string_types)):
        try:
            seed = int(seed)
        except ValueError:
            logger.warning("`seed` must be castable to an integer")
            seed = None
        else:
            if seed < 0:
                logger.warning("`seed` may not be negative")
                seed = None
            elif seed > MAX_UINT:
                raise ValueError('`seed` is too large; max is {}'.format(MAX_UINT))
    elif isinstance(seed, np.random.RandomState):
        seed = seed.randint(0, MAX_UINT)
    elif seed is not None:
        logger.warning('`seed` has unexpected type')
        seed = None

    if seed is None:
        seed = random.randint(0, MAX_UINT)

    return seed


def _organize_inits(inits, pars, dims):
    """Obtain a list of initial values for each chain.

    The parameter 'lp__' will be removed from the chains.

    Parameters
    ----------
    inits : list
        list of initial values for each chain.
    pars : list of str
    dims : list of list of int
        from (via cython conversion) vector[vector[uint]] dims

    Returns
    -------
    inits : list of dict

    """
    try:
        idx_of_lp = pars.index('lp__')
        del pars[idx_of_lp]
        del dims[idx_of_lp]
    except ValueError:
        pass
    starts = _calc_starts(dims)
    return [_par_vector2dict(init, pars, dims, starts) for init in inits]


def _calc_starts(dims):
    """Calculate starting indexes

    Parameters
    ----------
    dims : list of list of int
        from (via cython conversion) vector[vector[uint]] dims

    Examples
    --------
    >>> _calc_starts([[8, 2], [5], [6, 2]])
    [0, 16, 21]

    """
    # NB: Python uses 0-indexing; R uses 1-indexing.
    l = len(dims)
    s = [np.prod(d) for d in dims]
    starts = np.cumsum([0] + s)[0:l].tolist()
    # coerce things into ints before returning
    return [int(i) for i in starts]


def _par_vector2dict(v, pars, dims, starts=None):
    """Turn a vector of samples into an OrderedDict according to param dims.

    Parameters
    ----------
    y : list of int or float
    pars : list of str
        parameter names
    dims : list of list of int
        list of dimensions of parameters

    Returns
    -------
    d : dict

    Examples
    --------
    >>> v = list(range(31))
    >>> dims = [[5], [5, 5], []]
    >>> pars = ['mu', 'Phi', 'eta']
    >>> _par_vector2dict(v, pars, dims)  # doctest: +ELLIPSIS
    OrderedDict([('mu', array([0, 1, 2, 3, 4])), ('Phi', array([[ 5, ...

    """
    if starts is None:
        starts = _calc_starts(dims)
    d = OrderedDict()
    for i in range(len(pars)):
        l = int(np.prod(dims[i]))
        start = starts[i]
        end = start + l
        y = np.asarray(v[start:end])
        if len(dims[i]) > 1:
            y = y.reshape(dims[i], order='F')  # 'F' = Fortran, column-major
        d[pars[i]] = y.squeeze() if y.shape == (1,) else y
    return d


def _check_pars(allpars, pars):
    if len(pars) == 0:
        raise ValueError("No parameter specified (`pars` is empty).")
    for par in pars:
        if par not in allpars:
            raise ValueError("No parameter {}".format(par))


def _pars_total_indexes(names, dims, fnames, pars):
    """Obtain all the indexes for parameters `pars` in the sequence of names.

    `names` references variables that are in column-major order

    Parameters
    ----------
    names : sequence of str
        All the parameter names.
    dim : sequence of list of int
        Dimensions, in same order as `names`.
    fnames : sequence of str
        All the scalar parameter names
    pars : sequence of str
        The parameters of interest. It is assumed all elements in `pars` are in
        `names`.

    Returns
    -------
    indexes : OrderedDict of list of int
        Dictionary uses parameter names as keys. Indexes are column-major order.
        For each parameter there is also a key `par`+'_rowmajor' that stores the
        row-major indexing.

    Note
    ----
    Inside each parameter (vector or array), the sequence uses column-major
    ordering. For example, if we have parameters alpha and beta, having
    dimensions [2, 2] and [2, 3] respectively, the whole parameter sequence
    is alpha[0,0], alpha[1,0], alpha[0, 1], alpha[1, 1], beta[0, 0],
    beta[1, 0], beta[0, 1], beta[1, 1], beta[0, 2], beta[1, 2]. In short,
    like R matrix(..., bycol=TRUE).

    Example
    -------
    >>> pars_oi = ['mu', 'tau', 'eta', 'theta', 'lp__']
    >>> dims_oi = [[], [], [8], [8], []]
    >>> fnames_oi = ['mu', 'tau', 'eta[1]', 'eta[2]', 'eta[3]', 'eta[4]',
    ... 'eta[5]', 'eta[6]', 'eta[7]', 'eta[8]', 'theta[1]', 'theta[2]',
    ... 'theta[3]', 'theta[4]', 'theta[5]', 'theta[6]', 'theta[7]',
    ... 'theta[8]', 'lp__']
    >>> pars = ['mu', 'tau', 'eta', 'theta', 'lp__']
    >>> _pars_total_indexes(pars_oi, dims_oi, fnames_oi, pars)
    ... # doctest: +ELLIPSIS
    OrderedDict([('mu', (0,)), ('tau', (1,)), ('eta', (2, 3, ...

    """
    starts = _calc_starts(dims)

    def par_total_indexes(par):
        # if `par` is a scalar, it will match one of `fnames`
        if par in fnames:
            p = fnames.index(par)
            idx = tuple([p])
            return OrderedDict([(par, idx), (par+'_rowmajor', idx)])
        else:
            p = names.index(par)
            idx = starts[p] + np.arange(np.prod(dims[p]))
            idx_rowmajor = starts[p] + _idx_col2rowm(dims[p])
        return OrderedDict([(par, tuple(idx)), (par+'_rowmajor', tuple(idx_rowmajor))])

    indexes = OrderedDict()
    for par in pars:
        indexes.update(par_total_indexes(par))
    return indexes


def _idx_col2rowm(d):
    """Generate indexes to change from col-major to row-major ordering"""
    if 0 == len(d):
        return 1
    if 1 == len(d):
        return np.arange(d[0])
    # order='F' indicates column-major ordering
    idx = np.array(np.arange(np.prod(d))).reshape(d, order='F').T
    return idx.flatten(order='F')


def _get_kept_samples(n, sim):
    """Get samples to be kept from the chain(s) for `n`th parameter.

    Samples from different chains are merged.

    Parameters
    ----------
    n : int
    sim : dict
        A dictionary tied to a StanFit4Model instance.

    Returns
    -------
    samples : array
        Samples being kept, permuted and in column-major order.

    """
    return pystan._misc.get_kept_samples(n, sim)


def _get_samples(n, sim, inc_warmup=True):
    # NOTE: this is in stanfit-class.R in RStan (rather than misc.R)
    """Get chains for `n`th parameter.

    Parameters
    ----------
    n : int
    sim : dict
        A dictionary tied to a StanFit4Model instance.

    Returns
    -------
    chains : list of array
        Each chain is an element in the list.

    """
    return pystan._misc.get_samples(n, sim, inc_warmup)


def _redirect_stderr():
    """Redirect stderr for subprocesses to /dev/null

    Silences copious compilation messages.

    Returns
    -------
    orig_stderr : file descriptor
        Copy of original stderr file descriptor
    """
    sys.stderr.flush()
    stderr_fileno = sys.stderr.fileno()
    orig_stderr = os.dup(stderr_fileno)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, stderr_fileno)
    os.close(devnull)
    return orig_stderr


def _has_fileno(stream):
    """Returns whether the stream object seems to have a working fileno()

    Tells whether _redirect_stderr is likely to work.

    Parameters
    ----------
    stream : IO stream object

    Returns
    -------
    has_fileno : bool
        True if stream.fileno() exists and doesn't raise OSError or
        UnsupportedOperation
    """
    try:
        stream.fileno()
    except (AttributeError, OSError, IOError, io.UnsupportedOperation):
        return False
    return True


def _append_id(file, id, suffix='.csv'):
    fname = os.path.basename(file)
    fpath = os.path.dirname(file)
    fname2 = re.sub(r'\.csv\s*$', '_{}.csv'.format(id), fname)
    if fname2 == fname:
        fname2 = '{}_{}.csv'.format(fname, id)
    return os.path.join(fpath, fname2)


def _writable_sample_file(file, warn=True, wfun=None):
    """Check to see if file is writable, if not use temporary file"""
    if wfun is None:
        wfun = lambda x, y: '"{}" is not writable; use "{}" instead'.format(x, y)
    dir = os.path.dirname(file)
    dir = os.getcwd() if dir == '' else dir
    if os.access(dir, os.W_OK):
        return file
    else:
        dir2 = tempfile.mkdtemp()
        if warn:
            logger.warning(wfun(dir, dir2))
        return os.path.join(dir2, os.path.basename(file))


def is_legal_stan_vname(name):
    stan_kw1 = ('for', 'in', 'while', 'repeat', 'until', 'if', 'then', 'else',
                'true', 'false')
    stan_kw2 = ('int', 'real', 'vector', 'simplex', 'ordered', 'positive_ordered',
                'row_vector', 'matrix', 'corr_matrix', 'cov_matrix', 'lower', 'upper')
    stan_kw3 = ('model', 'data', 'parameters', 'quantities', 'transformed', 'generated')
    cpp_kw = ("alignas", "alignof", "and", "and_eq", "asm", "auto", "bitand", "bitor", "bool",
              "break", "case", "catch", "char", "char16_t", "char32_t", "class", "compl",
              "const", "constexpr", "const_cast", "continue", "decltype", "default", "delete",
              "do", "double", "dynamic_cast", "else", "enum", "explicit", "export", "extern",
              "false", "float", "for", "friend", "goto", "if", "inline", "int", "long", "mutable",
              "namespace", "new", "noexcept", "not", "not_eq", "nullptr", "operator", "or", "or_eq",
              "private", "protected", "public", "register", "reinterpret_cast", "return",
              "short", "signed", "sizeof", "static", "static_assert", "static_cast", "struct",
              "switch", "template", "this", "thread_local", "throw", "true", "try", "typedef",
              "typeid", "typename", "union", "unsigned", "using", "virtual", "void", "volatile",
              "wchar_t", "while", "xor", "xor_eq")
    illegal = stan_kw1 + stan_kw2 + stan_kw3 + cpp_kw
    if re.findall(r'(\.|^[0-9]|__$)', name):
        return False
    return not name in illegal


def _dict_to_rdump(data):
    parts = []
    for name, value in data.items():
        if isinstance(value, (Sequence, Number, np.number, np.ndarray, int, bool, float)) \
           and not isinstance(value, string_types):
            value = np.asarray(value)
        else:
            raise ValueError("Variable {} is not a number and cannot be dumped.".format(name))

        if value.dtype == np.bool:
            value = value.astype(int)

        if value.ndim == 0:
            s = '{} <- {}\n'.format(name, str(value))
        elif value.ndim == 1:
            s = '{} <-\nc({})\n'.format(name, ', '.join(str(v) for v in value))
        elif value.ndim > 1:
            tmpl = '{} <-\nstructure(c({}), .Dim = c({}))\n'
            # transpose value as R uses column-major
            # 'F' = Fortran, column-major
            s = tmpl.format(name,
                            ', '.join(str(v) for v in value.flatten(order='F')),
                            ', '.join(str(v) for v in value.shape))
        parts.append(s)
    return ''.join(parts)


def stan_rdump(data, filename):
    """
    Dump a dictionary with model data into a file using the R dump format that
    Stan supports.

    Parameters
    ----------
    data : dict
    filename : str

    """
    for name in data:
        if not is_legal_stan_vname(name):
            raise ValueError("Variable name {} is not allowed in Stan".format(name))
    with open(filename, 'w') as f:
        f.write(_dict_to_rdump(data))


def _rdump_value_to_numpy(s):
    """
    Convert a R dump formatted value to Numpy equivalent

    For example, "c(1, 2)" becomes ``array([1, 2])``

    Only supports a few R data structures. Will not work with European decimal format.
    """
    if "structure" in s:
        vector_str, shape_str = re.findall(r'c\([^\)]+\)', s)
        shape = [int(d) for d in shape_str[2:-1].split(',')]
        if '.' in vector_str:
            arr = np.array([float(v) for v in vector_str[2:-1].split(',')])
        else:
            arr = np.array([int(v) for v in vector_str[2:-1].split(',')])
        # 'F' = Fortran, column-major
        arr = arr.reshape(shape, order='F')
    elif "c(" in s:
        if '.' in s:
            arr = np.array([float(v) for v in s[2:-1].split(',')], order='F')
        else:
            arr = np.array([int(v) for v in s[2:-1].split(',')], order='F')
    else:
        arr = np.array(float(s) if '.' in s else int(s))
    return arr


def _remove_empty_pars(pars, pars_oi, dims_oi):
    """
    Remove parameters that are actually empty. For example, the parameter
    y would be removed with the following model code:

        transformed data { int n; n <- 0; }
        parameters { real y[n]; }

    Parameters
    ----------
    pars: iterable of str
    pars_oi: list of str
    dims_oi: list of list of int

    Returns
    -------
    pars_trimmed: list of str
    """
    pars = list(pars)
    for par, dim in zip(pars_oi, dims_oi):
        if par in pars and np.prod(dim) == 0:
            del pars[pars.index(par)]
    return pars


def read_rdump(filename):
    """
    Read data formatted using the R dump format

    Parameters
    ----------
    filename: str

    Returns
    -------
    data : OrderedDict
    """
    contents = open(filename).read().strip()
    names = [name.strip() for name in re.findall(r'^(\w+) <-', contents, re.MULTILINE)]
    values = [value.strip() for value in re.split('\w+ +<-', contents) if value]
    if len(values) != len(names):
        raise ValueError("Unable to read file. Unable to pair variable name with value.")
    d = OrderedDict()
    for name, value in zip(names, values):
        d[name.strip()] = _rdump_value_to_numpy(value.strip())
    return d

def to_dataframe(fit, pars=None, permuted=False, dtypes=None, inc_warmup=False, diagnostics=True, header=True):
    """Extract samples as a pandas dataframe for different parameters.

    Parameters
    ----------
    pars : {str, sequence of str}
        parameter (or quantile) name(s).
    permuted : bool
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

    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Pandas module not found. You can install pandas with: pip install pandas")

    fit._verify_has_samples()
    pars_original = pars
    if pars is None:
        pars = fit.sim['pars_oi']
    elif isinstance(pars, string_types):
        pars = [pars]
    if pars:
        pars = pystan.misc._remove_empty_pars(pars, fit.sim['pars_oi'], fit.sim['dims_oi'])
        allpars = fit.sim['pars_oi'] + fit.sim['fnames_oi']
        _check_pars(allpars, pars)

    if dtypes is None:
        dtypes = {}

    n_kept = [s if inc_warmup else s-w for s, w in zip(fit.sim['n_save'], fit.sim['warmup2'])]
    chains = len(fit.sim['samples'])

    diagnostic_type = {'divergent__':int,
                       'energy__':float,
                       'treedepth__':int,
                       'accept_stat__':float,
                       'stepsize__':float,
                       'n_leapfrog__':int}

    header_dict = OrderedDict()
    if header:
        idx = np.concatenate([np.full(n_kept[chain], chain, dtype=int) for chain in range(chains)])
        warmup = [np.zeros(n_kept[chain], dtype=np.int64) for chain in range(chains)]

        if inc_warmup:
            draw = []
            for chain, w in zip(range(chains), fit.sim['warmup2']):
                warmup[chain][:w] = 1
                draw.append(np.arange(n_kept[chain], dtype=np.int64) - w)
            draw = np.concatenate(draw)
        else:
            draw = np.concatenate([np.arange(n_kept[chain], dtype=np.int64) for chain in range(chains)])
        warmup = np.concatenate(warmup)

        header_dict = OrderedDict(zip(['chain', 'draw', 'warmup'], [idx, draw, warmup]))

    if permuted:
        if inc_warmup:
            chain_permutation = []
            chain_permutation_order = []
            permutation = []
            permutation_order = []
            for chain, p, w in zip(range(chains), fit.sim['permutation'], fit.sim['warmup2']):
                chain_permutation.append(list(range(-w, 0)) + p)
                chain_permutation_order.append(list(range(-w, 0)) + list(np.argsort(p)))
                permutation.append(sum(n_kept[:chain])+chain_permutation[-1]+w)
                permutation_order.append(sum(n_kept[:chain])+chain_permutation_order[-1]+w)
            chain_permutation = np.concatenate(chain_permutation)
            chain_permutation_order = np.concatenate(chain_permutation_order)
            permutation = np.concatenate(permutation)
            permutation_order = np.concatenate(permutation_order)

        else:
            chain_permutation = np.concatenate(fit.sim['permutation'])
            chain_permutation_order = np.concatenate([np.argsort(item) for item in fit.sim['permutation']])
            permutation = np.concatenate([sum(n_kept[:chain])+p for chain, p in enumerate(fit.sim['permutation'])])
            permutation_order = np.argsort(permutation)

        header_dict["permutation"] = permutation
        header_dict["chain_permutation"] = chain_permutation
        header_dict["permutation_order"] = permutation_order
        header_dict["chain_permutation_order"] = chain_permutation_order

    if header:
        header_df = pd.DataFrame.from_dict(header_dict)
    else:
        if permuted:
            header_df = pd.DataFrame.from_dict({"permutation_order" : header_dict["permutation_order"]})
        else:
            header_df = pd.DataFrame()

    fnames_set = set(fit.sim['fnames_oi'])
    pars_set = set(pars)
    if pars_original is None or fnames_set == pars_set:
        dfs = [pd.DataFrame.from_dict(pyholder.chains).iloc[-n:] for pyholder, n in zip(fit.sim['samples'], n_kept)]
        df = pd.concat(dfs, axis=0, sort=False, ignore_index=True)
        if dtypes:
            if not fnames_set.issuperset(pars_set):
                par_keys = OrderedDict([(par, []) for par in fit.sim['pars_oi']])
                for key in fit.sim['fnames_oi']:
                    par = key.split("[")
                    par = par[0]
                    par_keys[par].append(key)

            for par, dtype in dtypes.items():
                if isinstance(dtype, (float, np.float64)):
                    continue
                for key in par_keys.get(par, [par]):
                    df.loc[:, key] = df.loc[:, key].astype(dtype)

    elif pars:
        par_keys = dict()
        if not fnames_set.issuperset(pars_set):
            par_keys = OrderedDict([(par, []) for par in fit.sim['pars_oi']])
            for key in fit.sim['fnames_oi']:
                par = key.split("[")
                par = par[0]
                par_keys[par].append(key)

        columns = []
        for par in pars:
            columns.extend(par_keys.get(par, [par]))
        columns = list(np.unique(columns))

        df = pd.DataFrame(index=np.arange(sum(n_kept)), columns=columns, dtype=float)
        for key in columns:
            key_values = []
            for chain, (pyholder, n) in enumerate(zip(fit.sim['samples'], n_kept)):
                key_values.append(pyholder.chains[key][-n:])
            df.loc[:, key] = np.concatenate(key_values)

        for par, dtype in dtypes.items():
            if isinstance(dtype, (float, np.float64)):
                continue
            for key in par_keys.get(par, [par]):
                df.loc[:, key] = df.loc[:, key].astype(dtype)
    else:
        df = pd.DataFrame()

    if diagnostics:
        diagnostics_dfs = []
        for idx, (pyholder, permutation, n) in enumerate(zip(fit.sim['samples'], fit.sim['permutation'], n_kept), 1):
            diagnostics_df = pd.DataFrame(pyholder['sampler_params'], index=pyholder['sampler_param_names']).T
            diagnostics_df = diagnostics_df.iloc[-n:, :]
            for key, dtype in diagnostic_type.items():
                if key in diagnostics_df:
                    diagnostics_df.loc[:, key] = diagnostics_df.loc[:, key].astype(dtype)
            diagnostics_dfs.append(diagnostics_df)
        if diagnostics_dfs:
            diagnostics_df = pd.concat(diagnostics_dfs, axis=0, sort=False, ignore_index=True)
        else:
            diagnostics_df = pd.DataFrame()
    else:
        diagnostics_df = pd.DataFrame()

    df = pd.concat((header_df, df, diagnostics_df), axis=1, sort=False)
    if permuted:
        df.sort_values(by='permutation_order', inplace=True)
        if not header:
            df.drop(columns='permutation_order', inplace=True)
    return df

def get_stepsize(fit):
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
    fit._verify_has_samples()
    stepsizes = []
    for adaptation_info in fit.get_adaptation_info():
        for line in adaptation_info.splitlines():
            if "Step size" in line:
                stepsizes.append(float(line.split("=")[1].strip()))
                break
    return stepsizes

def get_inv_metric(fit, as_dict=False):
    """Parse inverse metric from the fit object

    Parameters
    ----------
    fit : StanFit4Model
    as_dict : bool, optional

    Returns
    -------
    list or dict
        Returns an empty list if inverse metric
        is not found in ``fit.get_adaptation_info()``.
        If `as_dict` returns a dictionary which can be used with
        `.sampling` method.
    """
    fit._verify_has_samples()
    inv_metrics = []
    if not (("ctrl" in fit.stan_args[0]) and ("sampling" in fit.stan_args[0]["ctrl"])):
        return inv_metrics
    metric = [args["ctrl"]["sampling"]["metric"].name for args in fit.stan_args]
    for adaptation_info, metric_name in zip(fit.get_adaptation_info(), metric):
        iter_adaptation_info = iter(adaptation_info.splitlines())
        inv_metric_list = []
        for line in iter_adaptation_info:
            if any(value in line for value in ["Step size", "Adaptation"]):
                continue
            elif "inverse mass matrix" in line:
                for line in iter_adaptation_info:
                    stripped_set = set(line.replace("# ", "").replace(" ", "").replace(",", ""))
                    if stripped_set.issubset(set(".-1234567890e")):
                        inv_metric = np.array(list(map(float, line.replace("# ", "").strip().split(","))))
                        if metric_name == "DENSE_E":
                            inv_metric = np.atleast_2d(inv_metric)
                        inv_metric_list.append(inv_metric)
                    else:
                        break
        inv_metrics.append(np.concatenate(inv_metric_list))
    return inv_metrics if not as_dict else dict(enumerate(inv_metrics))

def get_last_position(fit, warmup=False):
    """Parse last position from fit object

    Parameters
    ----------
    fit : StanFit4Model
    warmup : bool
        If True, returns the last warmup position, when warmup has been done.
        Otherwise function returns the first sample position.

    Returns
    -------
    list
        list contains a dictionary of last draw from each chain.
    """
    fit._verify_has_samples()
    positions = []
    extracted = fit.extract(permuted=False, pars=fit.model_pars, inc_warmup=warmup)

    draw_location = -1
    if warmup:
        draw_location += max(1, fit.sim["warmup"])

    chains = fit.sim["chains"]
    for i in range(chains):
        extract_pos = {key : values[draw_location, i] for key, values in extracted.items()}
        positions.append(extract_pos)
    return positions
