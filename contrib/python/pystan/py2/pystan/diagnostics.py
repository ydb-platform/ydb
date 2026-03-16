import pystan
from pystan.misc import _check_pars, _remove_empty_pars
from pystan._compat import string_types
import numpy as np
import logging

logger = logging.getLogger('pystan')

# Diagnostics modified from Betancourt's stan_utility.py module

def check_div(fit, verbose=True, per_chain=False):
    """Check for transitions that ended with a divergence

    Parameters
    ----------
    fit : StanFit4Model object
    verbose : bool or int, optional
        If ``verbose`` is ``False`` or a nonpositive integer, no
        diagnostic messages are printed, and only the return value of
        the function conveys diagnostic information. If it is ``True``
        (the default) or an integer greater than zero, then a
        diagnostic message is printed only if there are divergent
        transitions. If it is an integer greater than 2, then extra
        diagnostic messages are printed.
    per_chain : bool, optional
        Print the number of divergent transitions in each chain

    Returns
    -------
    bool
        ``True`` if there are no problems with divergent transitions
        and ``False`` otherwise.

    Raises
    ------
    ValueError
        If ``fit`` has no information about divergent transitions.

    """

    verbosity = int(verbose)

    sampler_params = fit.get_sampler_params(inc_warmup=False)

    try:
        divergent = np.column_stack([y['divergent__'].astype(bool) for y in sampler_params])
    except:
        raise ValueError('Cannot access divergence information from fit object')

    n_for_chains = divergent.sum(axis=0)

    n = n_for_chains.sum()

    if n > 0:

        if verbosity > 0:

            N = divergent.size
            logger.warning('{} of {} iterations ended '.format(n, N) +
                           'with a divergence ({:.3g} %).'.format(100 * n / N))

            if per_chain:
                chain_len, num_chains = divergent.shape

                for chain_num in range(num_chains):
                    if n_for_chains[chain_num] > 0:
                        logger.warning('Chain {}: {} of {} iterations ended '.format(chain_num + 1,
                                                                                     n_for_chains[chain_num],
                                                                                     chain_len) +
                                       'with a divergence ({:.3g} %).'.format(100 * n_for_chains[chain_num] /
                                                                         chain_len))

            try:
                adapt_delta = fit.stan_args[0]['ctrl']['sampling']['adapt_delta']
            except:
                logger.warning('Cannot obtain value of adapt_delta from fit object')
                adapt_delta = None

            if adapt_delta != None:
                logger.warning('Try running with adapt_delta larger than {}'.format(adapt_delta) +
                               ' to remove the divergences.')
            else:
                logger.warning('Try running with larger adapt_delta to remove the divergences.')

        return False
    else:
        if verbosity > 2:
            logger.info('No divergent transitions found.')

        return True


def check_treedepth(fit, verbose=True, per_chain=False):
    """Check for transitions that ended prematurely due to maximum tree
    depth limit

    Parameters
    ----------
    fit : StanFit4Model object
    verbose : bool or int, optional
        If ``verbose`` is ``False`` or a nonpositive integer, no
        diagnostic messages are printed, and only the return value of
        the function conveys diagnostic information. If it is ``True``
        (the default) or an integer greater than zero, then a
        diagnostic message is printed only if there are transitions
        that ended ended prematurely due to maximum tree depth
        limit. If it is an integer greater than 2, then extra
        diagnostic messages are printed.
    per_chain : bool, optional
        Print the number of prematurely ending transitions in each chain

    Returns
    -------
    bool
        ``True`` if there are no problems with tree depth and
        ``False`` otherwise.

    Raises
    ------
    ValueError
        If ``fit`` has no information about tree depth. This could
        happen if ``fit`` was generated from a sampler other than
        NUTS.

    """

    verbosity = int(verbose)

    sampler_params = fit.get_sampler_params(inc_warmup=False)

    try:
        depths = np.column_stack([y['treedepth__'].astype(int) for y in sampler_params])
    except:
        raise ValueError('Cannot access tree depth information from fit object')

    try:
        max_treedepth = int(fit.stan_args[0]['ctrl']['sampling']['max_treedepth'])
    except:
        raise ValueError('Cannot obtain value of max_treedepth from fit object')

    n_for_chains =  (depths >= max_treedepth).sum(axis=0)

    n = n_for_chains.sum()

    if n > 0:
        if verbosity > 0:
            N = depths.size
            logger.warning(('{} of {} iterations saturated the maximum tree depth of {}'
                                + ' ({:.3g} %)').format(n, N, max_treedepth, 100 * n / N))

            if per_chain:
                chain_len, num_chains = depths.shape

                for chain_num in range(num_chains):
                    if n_for_chains[chain_num] > 0:
                        logger.warning('Chain {}: {} of {} saturated '.format(chain_num + 1,
                                                                              n_for_chains[chain_num],
                                                                              chain_len) +
                                       'the maximum tree depth of {} ({:.3g} %).'.format(max_treedepth,
                                                                                    100 * n_for_chains[chain_num] /
                                                                                    chain_len))

            logger.warning('Run again with max_treedepth larger than {}'.format(max_treedepth) +
                           ' to avoid saturation')

        return False
    else:
        if verbosity > 2:
            logger.info('No transitions that ended prematurely due to maximum tree depth limit')

        return True

def check_energy(fit, verbose=True):
    """Checks the energy Bayesian fraction of missing information (E-BFMI)

    Parameters
    ----------
    fit : StanFit4Model object
    verbose : bool or int, optional
        If ``verbose`` is ``False`` or a nonpositive integer, no
        diagnostic messages are printed, and only the return value of
        the function conveys diagnostic information. If it is ``True``
        (the default) or an integer greater than zero, then a
        diagnostic message is printed only if there is low E-BFMI in
        one or more chains. If it is an integer greater than 2, then
        extra diagnostic messages are printed.


    Returns
    -------
    bool
        ``True`` if there are no problems with E-BFMI and ``False``
        otherwise.

    Raises
    ------
    ValueError
        If ``fit`` has no information about E-BFMI.

    """

    verbosity = int(verbose)

    sampler_params = fit.get_sampler_params(inc_warmup=False)

    try:
        energies = np.column_stack([y['energy__'] for y in sampler_params])
    except:
        raise ValueError('Cannot access energy information from fit object')

    chain_len, num_chains = energies.shape

    numer = ((np.diff(energies, axis=0)**2).sum(axis=0)) / chain_len

    denom = np.var(energies, axis=0)

    e_bfmi = numer / denom

    no_warning = True
    for chain_num in range(num_chains):

        if e_bfmi[chain_num] < 0.2:
            if verbosity > 0:
                logger.warning('Chain {}: E-BFMI = {:.3g}'.format(chain_num + 1,
                                                              e_bfmi[chain_num]))

            no_warning = False
        else:
            if verbosity > 2:
                logger.info('Chain {}: E-BFMI (= {:.3g}) '.format(chain_num + 1,
                                                              e_bfmi[chain_num]) +
                            'equals or exceeds threshold of 0.2.')

    if no_warning:
        if verbosity > 2:
            logger.info('E-BFMI indicated no pathological behavior')

        return True
    else:
        if verbosity > 0:
            logger.warning('E-BFMI below 0.2 indicates you may need to reparameterize your model')

        return False

def check_n_eff(fit, pars=None, verbose=True):
    """Checks the effective sample size per iteration

    Parameters
    ----------
    fit : StanFit4Model object
    pars : {str, sequence of str}, optional
        Parameter (or quantile) name(s). Test only specific parameters.
        Raises an exception if parameter is not valid.
    verbose : bool or int, optional
        If ``verbose`` is ``False`` or a nonpositive integer, no
        diagnostic messages are printed, and only the return value of
        the function conveys diagnostic information. If it is ``True``
        (the default) or an integer greater than zero, then a
        diagnostic message is printed only if there are effective
        sample sizes that appear pathologically low. If it is an
        integer greater than 1, then parameter (quantile) diagnostics
        are printed. If integer is greater than 2 extra diagnostic messages are
        printed.


    Returns
    -------
    bool
        ``True`` if there are no problems with effective sample size
        and ``False`` otherwise.

    """

    verbosity = int(verbose)

    n_iter = sum(fit.sim['n_save'])-sum(fit.sim['warmup2'])

    if pars is None:
        pars = fit.sim['fnames_oi']
    else:
        if isinstance(pars, string_types):
            pars = [pars]
        pars = _remove_empty_pars(pars, fit.sim['pars_oi'], fit.sim['dims_oi'])
        allpars = fit.sim['pars_oi'] + fit.sim['fnames_oi']
        _check_pars(allpars, pars)
        packed_pars = set(pars) - set(fit.sim['fnames_oi'])
        if packed_pars:
            unpack_dict = {}
            for par_unpacked in fit.sim['fnames_oi']:
                par_packed = par_unpacked.split("[")[0]
                if par_packed not in unpack_dict:
                    unpack_dict[par_packed] = []
                unpack_dict[par_packed].append(par_unpacked)
            pars_unpacked = []
            for par in pars:
                if par in packed_pars:
                    pars_unpacked.extend(unpack_dict[par])
                else:
                    pars_unpacked.append(par)
            pars = pars_unpacked

    par_n_dict = {}
    for n, par in enumerate(fit.sim['fnames_oi']):
        par_n_dict[par] = n

    no_warning = True
    for name in pars:
        n = par_n_dict[name]
        n_eff = pystan.chains.ess(fit.sim, n)
        ratio = n_eff / n_iter
        if ((ratio < 0.001) or np.isnan(ratio) or np.isinf(ratio)):
            if verbosity > 1:
                logger.warning('n_eff / iter for parameter {} is {:.3g}!'.format(name, ratio))

            no_warning = False
            if verbosity <= 1:
                break

    if no_warning:
        if verbosity > 2:
            logger.info('n_eff / iter looks reasonable for all parameters')

        return True
    else:
        if verbosity > 0:
            logger.warning('n_eff / iter below 0.001 indicates that the effective sample size has likely been overestimated')

        return False

def check_rhat(fit, pars=None, verbose=True):
    """Checks the potential scale reduction factors, i.e., Rhat values

    Parameters
    ----------
    fit : StanFit4Model object
    pars : {str, sequence of str}, optional
        Parameter (or quantile) name(s). Test only specific parameters.
        Raises an exception if parameter is not valid.
    verbose : bool or int, optional
        If ``verbose`` is ``False`` or a nonpositive integer, no
        diagnostic messages are printed, and only the return value of
        the function conveys diagnostic information. If it is ``True``
        (the default) or an integer greater than zero, then a
        diagnostic message is printed only if there are Rhat values
        too far from 1. If ``verbose`` is an integer greater than 1,
        parameter (quantile) diagnostics are printed. If ``verbose``
        is an integer greater than 2, then extra diagnostic messages are printed.


    Returns
    -------
    bool
        ``True`` if there are no problems with with Rhat and ``False``
        otherwise.

    """

    verbosity = int(verbose)

    if pars is None:
        pars = fit.sim['fnames_oi']
    else:
        if isinstance(pars, string_types):
            pars = [pars]
        pars = _remove_empty_pars(pars, fit.sim['pars_oi'], fit.sim['dims_oi'])
        allpars = fit.sim['pars_oi'] + fit.sim['fnames_oi']
        _check_pars(allpars, pars)
        packed_pars = set(pars) - set(fit.sim['fnames_oi'])
        if packed_pars:
            unpack_dict = {}
            for par_unpacked in fit.sim['fnames_oi']:
                par_packed = par_unpacked.split("[")[0]
                if par_packed not in unpack_dict:
                    unpack_dict[par_packed] = []
                unpack_dict[par_packed].append(par_unpacked)
            pars_unpacked = []
            for par in pars:
                if par in packed_pars:
                    pars_unpacked.extend(unpack_dict[par])
                else:
                    pars_unpacked.append(par)
            pars = pars_unpacked

    par_n_dict = {}
    for n, par in enumerate(fit.sim['fnames_oi']):
        par_n_dict[par] = n

    no_warning = True
    for name in pars:
        n = par_n_dict[name]
        rhat = pystan.chains.splitrhat(fit.sim, n)

        if (np.isnan(rhat) or np.isinf(rhat) or (rhat > 1.1) or (rhat < 0.9)):

            if verbosity > 1:
                logger.warning('Rhat for parameter {} is {:.3g}!'.format(name, rhat))

            no_warning = False
            if verbosity <= 1:
                break

    if no_warning:
        if verbosity > 2:
            logger.info('Rhat looks reasonable for all parameters')

        return True
    else:
        if verbosity > 0:
            logger.warning('Rhat above 1.1 or below 0.9 indicates that the chains very likely have not mixed')

        return False

def check_hmc_diagnostics(fit, pars=None, verbose=True, per_chain=False, checks=None):
    """Checks all hmc diagnostics

    Parameters
    ----------
    fit : StanFit4Model object
    verbose : bool or int, optional
        If ``verbose`` is ``False`` or a nonpositive integer, no
        diagnostic messages are printed, and only the return value of
        the function conveys diagnostic information. If it is ``True``
        (the default) or an integer greater than zero, then diagnostic
        messages are printed only for diagnostic checks that fail. If
        ``verbose`` is an integer greater than 1, then parameter
        (quantile) diagnostics are printed. If ``verbose`` is
        greater than 2, then extra diagnostic messages are printed.
    per_chain : bool, optional
        Where applicable, print diagnostics on a per-chain basis. This
        applies mainly to the divergence and treedepth checks.
    checks : list, {"n_eff", "Rhat", "divergence", "treedepth", "energy"}, optional
        By default run all checks. If ``checks`` is defined, run only
        checks given in ``checks``


    Returns
    -------
    out_dict : dict
        A dictionary where each key is the name of a diagnostic check,
        and the value associated with each key is a Boolean value that
        is True if the check passed and False otherwise.  Possible
        valid keys are 'n_eff', 'Rhat', 'divergence', 'treedepth', and
        'energy', though which keys are available will depend upon the
        sampling algorithm used.

    """

    # For consistency with the individual diagnostic functions
    verbosity = int(verbose)

    all_checks = {"n_eff", "Rhat", "divergence", "treedepth", "energy"}
    if checks is None:
        checks = all_checks
    else:
        undefined_checks = []
        for c in checks:
            # accept lowercase Rhat
            if c == "rhat":
                continue
            if c not in all_checks:
                undefined_checks.append(c)
        if undefined_checks:
            ucstr = "[" + ", ".join(undefined_checks) + "]"
            msg = "checks: {} are not legal checks: {}".format(ucstr, all_checks)
            raise TypeError(msg)

    out_dict = {}

    if "n_eff" in checks:
        try:
            out_dict['n_eff'] = check_n_eff(fit, pars, verbose)
        except ValueError:
            if verbosity > 0:
                logger.warning('Skipping check of effective sample size (n_eff)')

    if ("Rhat" in checks) or ("rhat" in checks):
        try:
            out_dict['Rhat'] = check_rhat(fit, pars, verbose)
        except ValueError:
            if verbosity > 0:
                logger.warning('Skipping check of potential scale reduction factors (Rhat)')

    if "divergence" in checks:
        try:
            out_dict['divergence'] = check_div(fit, verbose, per_chain)
        except ValueError:
            if verbosity > 0:
                logger.warning('Skipping check of divergent transitions (divergence)')

    if "treedepth" in checks:
        try:
            out_dict['treedepth'] = check_treedepth(fit, verbose, per_chain)
        except ValueError:
            if verbosity > 0:
                logger.warning('Skipping check of transitions ending prematurely due to maximum tree depth limit (treedepth)')

    if "energy" in checks:
        try:
            out_dict['energy'] = check_energy(fit, verbose)
        except ValueError:
            if verbosity > 0:
                logger.warning('Skipping check of E-BFMI (energy)')

    return out_dict
