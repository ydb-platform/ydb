import numpy as np
import logging

logger = logging.getLogger('pystan')


def traceplot(fit, pars, dtypes, **kwargs):
    """
    Use pymc's traceplot to display parameters.

    Additional arguments are passed to pymc.plots.traceplot.
    """
    # FIXME: eventually put this in the StanFit object
    # FIXME: write a to_pymc(_trace) function
    # Deprecation warning added in PyStan 2.18
    logger.warning("Deprecation warning."\
                   " PyStan plotting deprecated, use ArviZ library (Python 3.5+)."\
                   " `pip install arviz`; `arviz.plot_trace(fit)`)")
    try:
        from pystan.external.pymc import plots
    except ImportError:
        logger.critical("matplotlib required for plotting.")
        raise
    if pars is None:
        pars = list(fit.model_pars) + ["lp__"]
    values = fit.extract(dtypes=dtypes, pars=pars, permuted=False)
    values = {key : arr.reshape(-1, int(np.multiply.reduce(arr.shape[2:])), order="F") for key, arr in values.items()}
    return plots.traceplot(values, pars, **kwargs)
