#-----------------------------------------------------------------------------
# Copyright (c) 2013-2015, PyStan developers
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------

import hashlib
import io
import logging
import os

import pystan._api  # stanc wrapper
from pystan._compat import string_types, PY2
from pystan.model import StanModel

logger = logging.getLogger('pystan')


def stanc(file=None, charset='utf-8', model_code=None, model_name="anon_model",
          include_paths=None, verbose=False, obfuscate_model_name=True,
          allow_undefined=False):
    """Translate Stan model specification into C++ code.

    Parameters
    ----------
    file : {string, file}, optional
        If filename, the string passed as an argument is expected to
        be a filename containing the Stan model specification.

        If file, the object passed must have a 'read' method (file-like
        object) that is called to fetch the Stan model specification.

    charset : string, 'utf-8' by default
        If bytes or files are provided, this charset is used to decode.

    model_code : string, optional
        A string containing the Stan model specification. Alternatively,
        the model may be provided with the parameter `file`.

    model_name: string, 'anon_model' by default
        A string naming the model. If none is provided 'anon_model' is
        the default. However, if `file` is a filename, then the filename
        will be used to provide a name.

    include_paths: list of strings, optional
        Paths for #include files defined in Stan code.

    verbose : boolean, False by default
        Indicates whether intermediate output should be piped to the
        console. This output may be useful for debugging.

    obfuscate_model_name : boolean, True by default
        If False the model name in the generated C++ code will not be made
        unique by the insertion of randomly generated characters.
        Generally it is recommended that this parameter be left as True.

    allow_undefined : boolean, False by default
        If True, the C++ code can be written even if there are undefined
        functions.

    Returns
    -------
    stanc_ret : dict
        A dictionary with the following keys: model_name, model_code,
        cpp_code, and status. Status indicates the success of the translation
        from Stan code into C++ code (success = 0, error = -1).

    Notes
    -----
    C++ reserved words and Stan reserved words may not be used for
    variable names; see the Stan User's Guide for a complete list.

    The `#include` method follows a C/C++ syntax `#include foo/my_gp_funs.stan`.
    The method needs to be at the start of the row, no whitespace is allowed.
    After the included file no whitespace or comments are allowed.
    `pystan.experimental`(PyStan 2.18) has a `fix_include`-function to clean the `#include`
    statements from the `model_code`.
    Example:
    `from pystan.experimental import fix_include`
    `model_code = fix_include(model_code)`

    See also
    --------
    StanModel : Class representing a compiled Stan model
    stan : Fit a model using Stan

    References
    ----------
    The Stan Development Team (2013) *Stan Modeling Language User's
    Guide and Reference Manual*.  <http://mc-stan.org/>.

    Examples
    --------
    >>> stanmodelcode = '''
    ... data {
    ...   int<lower=0> N;
    ...   real y[N];
    ... }
    ...
    ... parameters {
    ...   real mu;
    ... }
    ...
    ... model {
    ...   mu ~ normal(0, 10);
    ...   y ~ normal(mu, 1);
    ... }
    ... '''
    >>> r = stanc(model_code=stanmodelcode, model_name = "normal1")
    >>> sorted(r.keys())
    ['cppcode', 'model_code', 'model_cppname', 'model_name', 'status']
    >>> r['model_name']
    'normal1'

    """
    if file and model_code:
        raise ValueError("Specify stan model with `file` or `model_code`, "
                         "not both.")
    if file is None and model_code is None:
        raise ValueError("Model file missing and empty model_code.")
    if file is not None:
        if isinstance(file, string_types):
            try:
                with io.open(file, 'rt', encoding=charset) as f:
                    model_code = f.read()
            except:
                logger.critical("Unable to read file specified by `file`.")
                raise
        else:
            model_code = file.read()

    # bytes, going into C++ code
    model_code_bytes = model_code.encode('utf-8')

    if include_paths is None:
        include_paths = [os.path.abspath('.')]
    elif isinstance(include_paths, string_types):
        include_paths = [include_paths]
    # add trailing /
    include_paths = [os.path.join(path, "") for path in include_paths]
    include_paths_bytes = [path.encode('utf-8') for path in include_paths]

    if obfuscate_model_name:
        # Make the model name depend on the code.
        model_name = (
            model_name + '_' +
            hashlib.md5(model_code_bytes).hexdigest())

    model_name_bytes = model_name.encode('ascii')

    if not isinstance(file, string_types):
        # use default 'unknown file name'
        filename_bytes  = b'unknown file name'
    else:
        # use only the filename, used only for debug printing
        filename_bytes = os.path.split(file)[-1].encode('utf-8')

    result = pystan._api.stanc(model_code_bytes, model_name_bytes,
                               allow_undefined, filename_bytes,
                               include_paths_bytes,
                              )
    if result['status'] == -1:  # EXCEPTION_RC is -1
        msg = result['msg']
        if PY2:
            # fix problem with unicode in error message in PY2
            msg = msg.encode('ascii', 'replace')
        error_msg = "Failed to parse Stan model '{}'. Error message:\n{}".format(model_name, msg)
        raise ValueError(error_msg)
    elif result['status'] == 0:  # SUCCESS_RC is 0
        logger.debug("Successfully parsed Stan model '{}'.".format(model_name))
    del result['msg']
    result.update({'model_name': model_name})
    result.update({'model_code': model_code})
    result.update({'include_paths' : include_paths})
    return result


def stan(file=None, model_name="anon_model", model_code=None, fit=None,
         data=None, pars=None, chains=4, iter=2000, warmup=None, thin=1,
         init="random", seed=None, algorithm=None, control=None, sample_file=None,
         diagnostic_file=None, verbose=False, boost_lib=None, eigen_lib=None,
         include_paths=None, n_jobs=-1, allow_undefined=False, **kwargs):
    """Fit a model using Stan.

    The `pystan.stan` function was deprecated in version 2.17 and will be
    removed in version 3.0. Compiling and using a Stan Program (e.g., for
    drawing samples) should be done in separate steps.

    Parameters
    ----------

    file : string {'filename', file-like object}
        Model code must found via one of the following parameters: `file` or
        `model_code`.

        If `file` is a filename, the string passed as an argument is expected
        to be a filename containing the Stan model specification.

        If `file` is a file object, the object passed must have a 'read' method
        (file-like object) that is called to fetch the Stan model specification.

    charset : string, optional
        If bytes or files are provided, this charset is used to decode. 'utf-8'
        by default.

    model_code : string
        A string containing the Stan model specification. Alternatively,
        the model may be provided with the parameter `file`.

    model_name: string, optional
        A string naming the model. If none is provided 'anon_model' is
        the default. However, if `file` is a filename, then the filename
        will be used to provide a name. 'anon_model' by default.

    fit : StanFit instance
        An instance of StanFit derived from a previous fit, None by
        default. If `fit` is not None, the compiled model associated
        with a previous fit is reused and recompilation is avoided.

    data : dict
        A Python dictionary providing the data for the model. Variables
        for Stan are stored in the dictionary as expected. Variable
        names are the keys and the values are their associated values.
        Stan only accepts certain kinds of values; see Notes.

    pars : list of string, optional
        A list of strings indicating parameters of interest. By default
        all parameters specified in the model will be stored.

    chains : int, optional
        Positive integer specifying number of chains. 4 by default.

    iter : int, 2000 by default
        Positive integer specifying how many iterations for each chain
        including warmup.

    warmup : int, iter//2 by default
        Positive integer specifying number of warmup (aka burin) iterations.
        As `warmup` also specifies the number of iterations used for stepsize
        adaption, warmup samples should not be used for inference.

    thin : int, optional
        Positive integer specifying the period for saving samples.
        Default is 1.

    init : {0, '0', 'random', function returning dict, list of dict}, optional
        Specifies how initial parameter values are chosen:
        - 0 or '0' initializes all to be zero on the unconstrained support.
        - 'random' generates random initial values. An optional parameter
            `init_r` controls the range of randomly generated initial values
            for parameters in terms of their unconstrained support;
        - list of size equal to the number of chains (`chains`), where the
            list contains a dict with initial parameter values;
        - function returning a dict with initial parameter values. The
            function may take an optional argument `chain_id`.

    seed : int or np.random.RandomState, optional
        The seed, a positive integer for random number generation. Only
        one seed is needed when multiple chains are used, as the other
        chain's seeds are generated from the first chain's to prevent
        dependency among random number streams. By default, seed is
        ``random.randint(0, MAX_UINT)``.

    algorithm : {"NUTS", "HMC", "Fixed_param"}, optional
        One of the algorithms that are implemented in Stan such as the No-U-Turn
        sampler (NUTS, Hoffman and Gelman 2011) and static HMC.

    sample_file : string, optional
        File name specifying where samples for *all* parameters and other
        saved quantities will be written. If not provided, no samples
        will be written. If the folder given is not writable, a temporary
        directory will be used. When there are multiple chains, an underscore
        and chain number are appended to the file name. By default do not
        write samples to file.

    diagnostic_file : string, optional
        File name specifying where diagnostic information should be written.
        By default no diagnostic information is recorded.

    boost_lib : string, optional
        The path to a version of the Boost C++ library to use instead of
        the one supplied with PyStan.

    eigen_lib : string, optional
        The path to a version of the Eigen C++ library to use instead of
        the one in the supplied with PyStan.

    include_paths : list of strings, optional
        Paths for #include files defined in Stan code.

    verbose : boolean, optional
        Indicates whether intermediate output should be piped to the console.
        This output may be useful for debugging. False by default.

    control : dict, optional
        A dictionary of parameters to control the sampler's behavior. Default
        values are used if control is not specified.  The following are
        adaptation parameters for sampling algorithms.

        These are parameters used in Stan with similar names:

        - `adapt_engaged` : bool
        - `adapt_gamma` : float, positive, default 0.05
        - `adapt_delta` : float, between 0 and 1, default 0.8
        - `adapt_kappa` : float, between default 0.75
        - `adapt_t0`    : float, positive, default 10
        - `adapt_init_buffer` : int, positive, defaults to 75
        - `adapt_term_buffer` : int, positive, defaults to 50
        - `adapt_window` : int, positive, defaults to 25

        In addition, the algorithm HMC (called 'static HMC' in Stan) and NUTS
        share the following parameters:

        - `stepsize`: float, positive
        - `stepsize_jitter`: float, between 0 and 1
        - `metric` : str, {"unit_e", "diag_e", "dense_e"}

        In addition, depending on which algorithm is used, different parameters
        can be set as in Stan for sampling. For the algorithm HMC we can set

        - `int_time`: float, positive

        For algorithm NUTS, we can set

        - `max_treedepth` : int, positive

    n_jobs : int, optional
        Sample in parallel. If -1 all CPUs are used. If 1, no parallel
        computing code is used at all, which is useful for debugging.

    allow_undefined : boolean, False by default
        If True, the C++ code can be written even if there are undefined
        functions.

    Returns
    -------

    fit : StanFit instance

    Other parameters
    ----------------

    chain_id : int, optional
        `chain_id` can be a vector to specify the chain_id for all chains or
        an integer. For the former case, they should be unique. For the latter,
        the sequence of integers starting from the given `chain_id` are used
        for all chains.

    init_r : float, optional
        `init_r` is only valid if `init` == "random". In this case, the intial
        values are simulated from [-`init_r`, `init_r`] rather than using the
        default interval (see the manual of (Cmd)Stan).

    test_grad: bool, optional
        If `test_grad` is ``True``, Stan will not do any sampling. Instead,
        the gradient calculation is tested and printed out and the fitted
        StanFit4Model object is in test gradient mode.  By default, it is
        ``False``.

    append_samples`: bool, optional

    refresh`: int, optional
        Argument `refresh` can be used to control how to indicate the progress
        during sampling (i.e. show the progress every \code{refresh} iterations).
        By default, `refresh` is `max(iter/10, 1)`.

    obfuscate_model_name : boolean, optional
        `obfuscate_model_name` is only valid if `fit` is None. True by default.
        If False the model name in the generated C++ code will not be made
        unique by the insertion of randomly generated characters.
        Generally it is recommended that this parameter be left as True.

    Examples
    --------
    >>> from pystan import stan
    >>> import numpy as np
    >>> model_code = '''
    ... parameters {
    ...   real y[2];
    ... }
    ... model {
    ...   y[1] ~ normal(0, 1);
    ...   y[2] ~ double_exponential(0, 2);
    ... }'''
    >>> fit1 = stan(model_code=model_code, iter=10)
    >>> print(fit1)
    >>> excode = '''
    ... transformed data {
    ...     real y[20];
    ...     y[1] = 0.5796;  y[2]  = 0.2276;   y[3] = -0.2959;
    ...     y[4] = -0.3742; y[5]  = 0.3885;   y[6] = -2.1585;
    ...     y[7] = 0.7111;  y[8]  = 1.4424;   y[9] = 2.5430;
    ...     y[10] = 0.3746; y[11] = 0.4773;   y[12] = 0.1803;
    ...     y[13] = 0.5215; y[14] = -1.6044;  y[15] = -0.6703;
    ...     y[16] = 0.9459; y[17] = -0.382;   y[18] = 0.7619;
    ...     y[19] = 0.1006; y[20] = -1.7461;
    ... }
    ... parameters {
    ...     real mu;
    ...     real<lower=0, upper=10> sigma;
    ...     vector[2] z[3];
    ...     real<lower=0> alpha;
    ... }
    ... model {
    ...     y ~ normal(mu, sigma);
    ...     for (i in 1:3)
    ...     z[i] ~ normal(0, 1);
    ...     alpha ~ exponential(2);
    ... }'''
    >>>
    >>> def initfun1():
    ...     return dict(mu=1, sigma=4, z=np.random.normal(size=(3, 2)), alpha=1)
    >>> exfit0 = stan(model_code=excode, init=initfun1)
    >>> def initfun2(chain_id=1):
    ...     return dict(mu=1, sigma=4, z=np.random.normal(size=(3, 2)), alpha=1 + chain_id)
    >>> exfit1 = stan(model_code=excode, init=initfun2)
    """
    logger.warning('DeprecationWarning: pystan.stan was deprecated in version 2.17 and will be removed in version 3.0. '
                  'Compile and use a Stan program in separate steps.')
    # NOTE: this is a thin wrapper for other functions. Error handling occurs
    # elsewhere.
    if data is None:
        data = {}
    if warmup is None:
        warmup = int(iter // 2)
    obfuscate_model_name = kwargs.pop("obfuscate_model_name", True)
    if fit is not None:
        m = fit.stanmodel
    else:
        m = StanModel(file=file, model_name=model_name, model_code=model_code,
                      boost_lib=boost_lib, eigen_lib=eigen_lib,
                      include_paths=include_paths,
                      obfuscate_model_name=obfuscate_model_name, verbose=verbose,
                      allow_undefined=allow_undefined)
    # check that arguments in kwargs are valid
    valid_args = {"chain_id", "init_r", "test_grad", "append_samples", "enable_random_init",
                  "refresh", "control"}
    for arg in kwargs:
        if arg not in valid_args:
            raise ValueError("Parameter `{}` is not recognized.".format(arg))

    fit = m.sampling(data, pars=pars, chains=chains, iter=iter,
                     warmup=warmup, thin=thin, seed=seed, init=init,
                     sample_file=sample_file, diagnostic_file=diagnostic_file,
                     verbose=verbose, algorithm=algorithm, control=control,
                     n_jobs=n_jobs, **kwargs)
    return fit
