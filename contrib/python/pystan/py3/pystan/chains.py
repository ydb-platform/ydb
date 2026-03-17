import pystan._chains as _chains
from numpy import nan

def ess(sim, n):
    """Calculate effective sample size

    Parameters
    ----------
    sim : chains
    n : int
        Parameter index starting from 0
    """
    try:
        ess = _chains.effective_sample_size(sim, n)
    except (ValueError, ZeroDivisionError):
        ess = nan
    return ess


def splitrhat(sim, n):
    """Calculate rhat

    Parameters
    ----------
    sim : chains
    n : int
        Parameter index starting from 0
    """
    try:
        rhat = _chains.split_potential_scale_reduction(sim, n)
    except (ValueError, ZeroDivisionError):
        rhat = nan
    return rhat

def ess_and_splitrhat(sim, n):
    """Calculate ess and rhat

    This saves time by creating just one stan::mcmc::chains instance.
    """
    # FIXME: does not yet save time
    return (ess(sim, n), splitrhat(sim, n))
