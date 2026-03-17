#-----------------------------------------------------------------------------
# Copyright (c) 2013-2015, PyStan developers
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------

from pystan._compat import PY2, string_types, implements_to_string, izip
from collections import OrderedDict
if PY2:
    from collections import Callable, Iterable
else:
    from collections.abc import Callable, Iterable
import datetime
import importlib
import io
import itertools
import logging
import numbers
import os
import platform
import shutil
import string
import sys
import tempfile
import time

import setuptools
import distutils
from distutils.core import Extension


import numpy as np

import pystan.api
import pystan.misc
import pystan.diagnostics

logger = logging.getLogger('pystan')


def load_module(module_name, module_path):
    """Load the module named `module_name` from  `module_path`
    independently of the Python version."""
    if sys.version_info >= (3,0):
        import pyximport
        pyximport.install()
        sys.path.append(module_path)
        return __import__(module_name)
    else:
        import imp
        module_info = imp.find_module(module_name, [module_path])
        return imp.load_module(module_name, *module_info)


def _map_parallel(function, args, n_jobs):
    """multiprocessing.Pool(processors=n_jobs).map with some error checking"""
    # Following the error checking found in joblib
    multiprocessing = int(os.environ.get('JOBLIB_MULTIPROCESSING', 1)) or None
    if multiprocessing:
        try:
            import multiprocessing
            import multiprocessing.pool
        except ImportError:
            multiprocessing = None
        if sys.platform.startswith("win") and PY2:
            msg = "Multiprocessing is not supported on Windows with Python 2.X. Setting n_jobs=1"
            logger.warning(msg)
            n_jobs = 1
    # 2nd stage: validate that locking is available on the system and
    #            issue a warning if not
    if multiprocessing:
        try:
            _sem = multiprocessing.Semaphore()
            del _sem  # cleanup
        except (ImportError, OSError) as e:
            multiprocessing = None
            logger.warning('{}. _map_parallel will operate in serial mode'.format(e))
    if multiprocessing and int(n_jobs) not in (0, 1):
        if n_jobs == -1:
            n_jobs = None
        try:
            pool = multiprocessing.Pool(processes=n_jobs)
            map_result = pool.map(function, args)
        finally:
            pool.close()
            pool.join()
    else:
        map_result = list(map(function, args))
    return map_result


# NOTE: StanModel instance stores references to a compiled, uninstantiated
# C++ model.
@implements_to_string
class StanModel:
    """
    Model described in Stan's modeling language compiled from C++ code.

    Instances of StanModel are typically created indirectly by the functions
    `stan` and `stanc`.

    Parameters
    ----------
    module_name : string
        Name of the Python module created by pystan_model.

    file : string {'filename', 'file'}
        If filename, the string passed as an argument is expected to
        be a filename containing the Stan model specification.

        If file, the object passed must have a 'read' method (file-like
        object) that is called to fetch the Stan model specification.

    charset : string, 'utf-8' by default
        If bytes or files are provided, this charset is used to decode.

    model_name: string, 'anon_model' by default
        A string naming the model. If none is provided 'anon_model' is
        the default. However, if `file` is a filename, then the filename
        will be used to provide a name.

    model_code : string
        A string containing the Stan model specification. Alternatively,
        the model may be provided with the parameter `file`.

    stanc_ret : dict
        A dict returned from a previous call to `stanc` which can be
        used to specify the model instead of using the parameter `file` or
        `model_code`.

    include_paths : list of strings
        Paths for #include files defined in Stan program code.

    boost_lib : string
        The path to a version of the Boost C++ library to use instead of
        the one supplied with PyStan.

    eigen_lib : string
        The path to a version of the Eigen C++ library to use instead of
        the one in the supplied with PyStan.

    verbose : boolean, False by default
        Indicates whether intermediate output should be piped to the console.
        This output may be useful for debugging.

    allow_undefined : boolean, False by default
        If True, the C++ code can be written even if there are undefined
        functions.

    includes : list, None by default
        If not None, the elements of this list will be assumed to be the
        names of custom C++ header files that should be included.

    include_dirs : list, None by default
        If not None, the directories in this list are added to the search
        path of the compiler.

    kwargs : keyword arguments
        Additional arguments passed to `stanc`.

    Attributes
    ----------
    module_name : string
    model_name : string
    model_code : string
        Stan code for the model.
    model_cpp : string
        C++ code for the model.
    module : builtins.module
        Python module created by compiling the C++ code for the model.

    Methods
    -------
    show
        Print the Stan model specification.
    sampling
        Draw samples from the model.
    optimizing
        Obtain a point estimate by maximizing the log-posterior.
    get_cppcode
        Return the C++ code for the module.
    get_cxxflags
        Return the 'CXXFLAGS' used for compiling the model.
    get_include_paths
        Return include_paths used for compiled model.

    See also
    --------
    stanc: Compile a Stan model specification
    stan: Fit a model using Stan

    Notes
    -----

    More details of Stan, including the full user's guide and
    reference manual can be found at <URL: http://mc-stan.org/>.

    There are three ways to specify the model's code for `stan_model`.

    1. parameter `model_code`, containing a string to whose value is
       the Stan model specification,

    2. parameter `file`, indicating a file (or a connection) from
       which to read the Stan model specification, or

    3. parameter `stanc_ret`, indicating the re-use of a model
         generated in a previous call to `stanc`.

    References
    ----------

    The Stan Development Team (2013) *Stan Modeling Language User's
    Guide and Reference Manual*.  <URL: http://mc-stan.org/>.

    Examples
    --------
    >>> model_code = 'parameters {real y;} model {y ~ normal(0,1);}'
    >>> model_code; m = StanModel(model_code=model_code)
    ... # doctest: +ELLIPSIS
    'parameters ...
    >>> m.model_name
    'anon_model'

    """
    def __init__(self, module_name, file=None, charset='utf-8', model_name="anon_model",
                 model_code=None, stanc_ret=None, include_paths=None,
                 boost_lib=None, eigen_lib=None, verbose=False,
                 obfuscate_model_name=True, extra_compile_args=None,
                 allow_undefined=False, include_dirs=None, includes=None):
        self.module_name = module_name
        self.module = importlib.import_module(module_name)
        self.fit_class = getattr(self.module, "StanFit4Model")
        return

        if stanc_ret is None:
            stanc_ret = pystan.api.stanc(file=file,
                                         charset=charset,
                                         model_code=model_code,
                                         model_name=model_name,
                                         verbose=verbose,
                                         include_paths=include_paths,
                                         obfuscate_model_name=obfuscate_model_name,
                                         allow_undefined=allow_undefined)

        if not isinstance(stanc_ret, dict):
            raise ValueError("stanc_ret must be an object returned by stanc.")
        stanc_ret_keys = {'status', 'model_code', 'model_cppname',
                          'cppcode', 'model_name', 'include_paths'}
        if not all(n in stanc_ret_keys for n in stanc_ret):
            raise ValueError("stanc_ret lacks one or more of the keys: "
                             "{}".format(str(stanc_ret_keys)))
        elif stanc_ret['status'] != 0:  # success == 0
            raise ValueError("stanc_ret is not a successfully returned "
                             "dictionary from stanc.")
        self.model_cppname = stanc_ret['model_cppname']
        self.model_name = stanc_ret['model_name']
        self.model_code = stanc_ret['model_code']
        self.model_cppcode = stanc_ret['cppcode']
        self.model_include_paths = stanc_ret['include_paths']

        if allow_undefined or include_dirs or includes:
            logger.warning("External C++ interface is an experimental feature. Be careful.")

        msg = "COMPILING THE C++ CODE FOR MODEL {} NOW."
        logger.info(msg.format(self.model_name))
        if verbose:
            msg = "OS: {}, Python: {}, Cython {}".format(sys.platform,
                                                         sys.version,
                                                         Cython.__version__)
            logger.info(msg)
        if boost_lib is not None:
            # FIXME: allow boost_lib, eigen_lib to be specified
            raise NotImplementedError
        if eigen_lib is not None:
            raise NotImplementedError

        # module_name needs to be unique so that each model instance has its own module
        nonce = abs(hash((self.model_name, time.time())))
        self.module_name = 'stanfit4{}_{}'.format(self.model_name, nonce)
        lib_dir = tempfile.mkdtemp(prefix='pystan_')
        pystan_dir = os.path.dirname(__file__)
        if include_dirs is None:
            include_dirs = []
        elif not isinstance(include_dirs, list):
            raise TypeError("'include_dirs' needs to be a list: type={}".format(type(include_dirs)))
        include_dirs += [
            lib_dir,
            pystan_dir,
            os.path.join(pystan_dir, "stan", "src"),
            os.path.join(pystan_dir, "stan", "lib", "stan_math"),
            os.path.join(pystan_dir, "stan", "lib", "stan_math", "lib", "eigen_3.3.3"),
            os.path.join(pystan_dir, "stan", "lib", "stan_math", "lib", "boost_1.69.0"),
            os.path.join(pystan_dir, "stan", "lib", "stan_math", "lib", "sundials_4.1.0", "include"),
            np.get_include(),
        ]

        model_cpp_file = os.path.join(lib_dir, self.model_cppname + '.hpp')
        if includes is not None:
            code = ""
            for fn in includes:
                code += '#include "{0}"\n'.format(fn)
            ind = self.model_cppcode.index("static int current_statement_begin__;")
            self.model_cppcode = "\n".join([
                self.model_cppcode[:ind], code, self.model_cppcode[ind:]
            ])
        with io.open(model_cpp_file, 'w', encoding='utf-8') as outfile:
            outfile.write(self.model_cppcode)

        pyx_file = os.path.join(lib_dir, self.module_name + '.pyx')
        pyx_template_file = os.path.join(pystan_dir, 'stanfit4model.pyx')
        with io.open(pyx_template_file, 'r', encoding='utf-8') as infile:
            s = infile.read()
            template = string.Template(s)
        with io.open(pyx_file, 'w', encoding='utf-8') as outfile:
            s = template.safe_substitute(model_cppname=self.model_cppname)
            outfile.write(s)

        stan_macros = [
            ('BOOST_RESULT_OF_USE_TR1', None),
            ('BOOST_NO_DECLTYPE', None),
            ('BOOST_DISABLE_ASSERTS', None),
        ]

        build_extension = _get_build_extension()
        # compile stan models with optimization (-O2)
        # (stanc is compiled without optimization (-O0) currently, see #33)
        if extra_compile_args is None:
            extra_compile_args = []

        if platform.platform().startswith('Win'):
            if build_extension.compiler in (None, 'msvc'):
                logger.warning("MSVC compiler is not supported")
                extra_compile_args = [
                    '/EHsc',
                    '-DBOOST_DATE_TIME_NO_LIB',
                    '/std:c++14',
                ] + extra_compile_args
            else:
                # Windows, but not msvc, likely mingw
                # fix bug in MingW-W64
                # use posix threads
                extra_compile_args = [
                    '-O2',
                    '-ftemplate-depth-256',
                    '-Wno-unused-function',
                    '-Wno-uninitialized',
                    '-std=c++1y',
                    "-D_hypot=hypot",
                    "-pthread",
                    "-fexceptions",
                ] + extra_compile_args
        else:
            # linux or macOS
            extra_compile_args = [
                '-O2',
                '-ftemplate-depth-256',
                '-Wno-unused-function',
                '-Wno-uninitialized',
                '-std=c++1y',
            ] + extra_compile_args

        distutils.log.set_verbosity(verbose)
        extension = Extension(name=self.module_name,
                              language="c++",
                              sources=[pyx_file],
                              define_macros=stan_macros,
                              include_dirs=include_dirs,
                              extra_compile_args=extra_compile_args)

        cython_include_dirs = ['.', pystan_dir]

        build_extension.extensions = cythonize([extension],
                                               include_path=cython_include_dirs,
                                               quiet=not verbose)
        build_extension.build_temp = os.path.dirname(pyx_file)
        build_extension.build_lib = lib_dir

        redirect_stderr = not verbose and pystan.misc._has_fileno(sys.stderr)
        if redirect_stderr:
            # silence stderr for compilation
            orig_stderr = pystan.misc._redirect_stderr()

        try:
            build_extension.run()
        finally:
            if redirect_stderr:
                # restore stderr
                os.dup2(orig_stderr, sys.stderr.fileno())

        self.module = load_module(self.module_name, lib_dir)
        self.module_filename = os.path.basename(self.module.__file__)
        # once the module is in memory, we no longer need the file on disk
        # but we do need a copy of the file for pickling and the module name
        with io.open(os.path.join(lib_dir, self.module_filename), 'rb') as f:
            self.module_bytes = f.read()
        shutil.rmtree(lib_dir, ignore_errors=True)
        self.fit_class = getattr(self.module, "StanFit4Model")

    def __str__(self):
        # NOTE: returns unicode even for Python 2.7, implements_to_string
        # decorator creates __unicode__ and __str__
        s = u"StanModel('{}')"
        return s.format(self.module_name)

    def show(self):
        print(self)

    @property
    def dso(self):
        # warning added in PyStan 2.8.0
        logger.warning('DeprecationWarning: Accessing the module with `dso` is deprecated and will be removed in a future version. '\
                       'Use `module` instead.')
        return self.module

    def get_cppcode(self):
        return self.model_cppcode

    def get_cxxflags(self):
        # FIXME: implement this?
        raise NotImplementedError

    def get_include_paths(self):
        return self.model_include_paths

    def __getstate__(self):
        """Specify how instances are to be pickled
        self.module is unpicklable, for example.
        """
        state = self.__dict__.copy()
        del state['module']
        del state['fit_class']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.module = importlib.import_module(self.module_name)
        self.fit_class = getattr(self.module, "StanFit4Model")
        return
        lib_dir = tempfile.mkdtemp()
        with io.open(os.path.join(lib_dir, self.module_filename), 'wb') as f:
            f.write(self.module_bytes)
        try:
            self.module = load_module(self.module_name, lib_dir)
            self.fit_class = getattr(self.module, "StanFit4Model")
        except Exception as e:
            logger.warning(e)
            logger.warning("Something went wrong while unpickling "
                            "the StanModel. Consider recompiling.")
        # once the module is in memory, we no longer need the file on disk
        shutil.rmtree(lib_dir, ignore_errors=True)

    def optimizing(self, data=None, seed=None,
                   init='random', sample_file=None, algorithm=None,
                   verbose=False, as_vector=True, **kwargs):
        """Obtain a point estimate by maximizing the joint posterior.

        Parameters
        ----------
        data : dict
            A Python dictionary providing the data for the model. Variables
            for Stan are stored in the dictionary as expected. Variable
            names are the keys and the values are their associated values.
            Stan only accepts certain kinds of values; see Notes.

        seed : int or np.random.RandomState, optional
            The seed, a positive integer for random number generation. Only
            one seed is needed when multiple chains are used, as the other
            chain's seeds are generated from the first chain's to prevent
            dependency among random number streams. By default, seed is
            ``random.randint(0, MAX_UINT)``.

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

        sample_file : string, optional
            File name specifying where samples for *all* parameters and other
            saved quantities will be written. If not provided, no samples
            will be written. If the folder given is not writable, a temporary
            directory will be used. When there are multiple chains, an
            underscore and chain number are appended to the file name.
            By default do not write samples to file.

        algorithm : {"LBFGS", "BFGS", "Newton"}, optional
            Name of optimization algorithm to be used. Default is LBFGS.

        verbose : boolean, optional
            Indicates whether intermediate output should be piped to the console.
            This output may be useful for debugging. False by default.

        as_vector : boolean, optional
            Indicates an OrderedDict will be returned rather than a nested
            dictionary with keys 'par' and 'value'.

        Returns
        -------
        optim : OrderedDict
            Depending on `as_vector`, returns either an OrderedDict having
            parameters as keys and point estimates as values or an OrderedDict
            with components 'par' and 'value'.  ``optim['par']`` is a dictionary
            of point estimates, indexed by the parameter name.
            ``optim['value']`` stores the value of the log-posterior (up to an
            additive constant, the ``lp__`` in Stan) corresponding to the point
            identified by `optim`['par'].

        Other parameters
        ----------------
        iter : int, optional
            The maximum number of iterations.
        save_iterations : bool, optional
        refresh : int, optional
        init_alpha : float, optional
            For BFGS and LBFGS, default is 0.001.
        tol_obj : float, optional
            For BFGS and LBFGS, default is 1e-12.
        tol_rel_obj : int, optional
            For BFGS and LBFGS, default is 1e4.
        tol_grad : float, optional
            For BFGS and LBFGS, default is 1e-8.
        tol_rel_grad : float, optional
            For BFGS and LBFGS, default is 1e7.
        tol_param : float, optional
            For BFGS and LBFGS, default is 1e-8.
        history_size : int, optional
            For LBFGS, default is 5.

        Refer to the manuals for both CmdStan and Stan for more details.

        Examples
        --------
        >>> from pystan import StanModel
        >>> m = StanModel(model_code='parameters {real y;} model {y ~ normal(0,1);}')
        >>> f = m.optimizing()

        """
        algorithms = {"BFGS", "LBFGS", "Newton"}
        if algorithm is None:
            algorithm = "LBFGS"
        if algorithm not in algorithms:
            raise ValueError("Algorithm must be one of {}".format(algorithms))
        if data is None:
            data = {}
        seed = pystan.misc._check_seed(seed)
        fit = self.fit_class(data, seed)

        m_pars = fit._get_param_names()
        p_dims = fit._get_param_dims()

        if 'lp__' in m_pars:
            idx_of_lp = m_pars.index('lp__')
            del m_pars[idx_of_lp]
            del p_dims[idx_of_lp]

        if isinstance(init, numbers.Number):
            init = str(init)
        elif isinstance(init, Callable):
            init = init()
        elif not isinstance(init, Iterable) and \
                not isinstance(init, string_types):
            raise ValueError("Wrong specification of initial values.")

        stan_args = dict(init=init,
                         seed=seed,
                         method="optim",
                         algorithm=algorithm)
        if sample_file is not None:
            stan_args['sample_file'] = pystan.misc._writable_sample_file(sample_file)

        # check that arguments in kwargs are valid
        valid_args = {"iter", "save_iterations", "refresh",
                      "init_alpha", "tol_obj", "tol_grad", "tol_param",
                      "tol_rel_obj", "tol_rel_grad", "history_size"}
        for arg in kwargs:
            if arg not in valid_args:
                raise ValueError("Parameter `{}` is not recognized.".format(arg))

        # This check is is to warn users of older versions of PyStan
        if kwargs.get('method'):
            raise ValueError('`method` is no longer used. Specify `algorithm` instead.')
        stan_args.update(kwargs)
        stan_args = pystan.misc._get_valid_stan_args(stan_args)

        ret, sample = fit._call_sampler(stan_args)
        pars = pystan.misc._par_vector2dict(sample['par'], m_pars, p_dims)
        if not as_vector:
            return OrderedDict([('par', pars), ('value', sample['value'])])
        else:
            return pars

    def sampling(self, data=None, pars=None, chains=4, iter=2000,
                 warmup=None, thin=1, seed=None, init='random',
                 sample_file=None, diagnostic_file=None, verbose=False,
                 algorithm=None, control=None, n_jobs=-1, **kwargs):
        """Draw samples from the model.

        Parameters
        ----------
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
            Positive integer specifying number of warmup (aka burn-in) iterations.
            As `warmup` also specifies the number of iterations used for step-size
            adaption, warmup samples should not be used for inference.
            `warmup=0` forced if `algorithm=\"Fixed_param\"`.

        thin : int, 1 by default
            Positive integer specifying the period for saving samples.

        seed : int or np.random.RandomState, optional
            The seed, a positive integer for random number generation. Only
            one seed is needed when multiple chains are used, as the other
            chain's seeds are generated from the first chain's to prevent
            dependency among random number streams. By default, seed is
            ``random.randint(0, MAX_UINT)``.

        algorithm : {"NUTS", "HMC", "Fixed_param"}, optional
            One of algorithms that are implemented in Stan such as the No-U-Turn
            sampler (NUTS, Hoffman and Gelman 2011), static HMC, or ``Fixed_param``.
            Default is NUTS.

        init : {0, '0', 'random', function returning dict, list of dict}, optional
            Specifies how initial parameter values are chosen: 0 or '0'
            initializes all to be zero on the unconstrained support; 'random'
            generates random initial values; list of size equal to the number
            of chains (`chains`), where the list contains a dict with initial
            parameter values; function returning a dict with initial parameter
            values. The function may take an optional argument `chain_id`.

        sample_file : string, optional
            File name specifying where samples for *all* parameters and other
            saved quantities will be written. If not provided, no samples
            will be written. If the folder given is not writable, a temporary
            directory will be used. When there are multiple chains, an underscore
            and chain number are appended to the file name. By default do not
            write samples to file.

        verbose : boolean, False by default
            Indicates whether intermediate output should be piped to the
            console. This output may be useful for debugging.

        control : dict, optional
            A dictionary of parameters to control the sampler's behavior. Default
            values are used if control is not specified.  The following are
            adaptation parameters for sampling algorithms.

            These are parameters used in Stan with similar names:

            - `adapt_engaged` : bool, default True
            - `adapt_gamma` : float, positive, default 0.05
            - `adapt_delta` : float, between 0 and 1, default 0.8
            - `adapt_kappa` : float, between default 0.75
            - `adapt_t0`    : float, positive, default 10

            In addition, the algorithm HMC (called 'static HMC' in Stan) and NUTS
            share the following parameters:

            - `stepsize`: float or list of floats, positive
            - `stepsize_jitter`: float, between 0 and 1
            - `metric` : str, {"unit_e", "diag_e", "dense_e"}
            - `inv_metric` : np.ndarray or str

            In addition, depending on which algorithm is used, different parameters
            can be set as in Stan for sampling. For the algorithm HMC we can set

            - `int_time`: float, positive

            For algorithm NUTS, we can set

            - `max_treedepth` : int, positive

        n_jobs : int, optional
            Sample in parallel. If -1 all CPUs are used. If 1, no parallel
            computing code is used at all, which is useful for debugging.

        Returns
        -------
        fit : StanFit4Model
            Instance containing the fitted results.

        Other parameters
        ----------------

        chain_id : int or iterable of int, optional
            `chain_id` can be a vector to specify the chain_id for all chains or
            an integer. For the former case, they should be unique. For the latter,
            the sequence of integers starting from the given `chain_id` are used
            for all chains.

        init_r : float, optional
            `init_r` is only valid if `init` == "random". In this case, the initial
            values are simulated from [-`init_r`, `init_r`] rather than using the
            default interval (see the manual of Stan).

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

        check_hmc_diagnostics : bool, optional
            After sampling run `pystan.diagnostics.check_hmc_diagnostics` function.
            Default is `True`. Checks for n_eff and rhat skipped if the flat
            parameter count is higher than 1000, unless user explicitly defines
            ``check_hmc_diagnostics=True``.


        Examples
        --------
        >>> from pystan import StanModel
        >>> m = StanModel(model_code='parameters {real y;} model {y ~ normal(0,1);}')
        >>> m.sampling(iter=100)

        """
        # NOTE: in this function, iter masks iter() the python function.
        # If this ever turns out to be a problem just add:
        # iter_ = iter
        # del iter  # now builtins.iter is available
        if diagnostic_file is not None:
            raise NotImplementedError("diagnostic_file not supported yet")
        if data is None:
            data = {}
        if warmup is None:
            warmup = int(iter // 2)
        if not all(isinstance(arg, numbers.Integral) for arg in (iter, thin, warmup)):
            raise ValueError('only integer values allowed as `iter`, `thin`, and `warmup`.')
        algorithms = ("NUTS", "HMC", "Fixed_param")  # , "Metropolis")
        algorithm = "NUTS" if algorithm is None else algorithm
        if algorithm not in algorithms:
            raise ValueError("Algorithm must be one of {}".format(algorithms))
        if algorithm=="Fixed_param":
            if warmup  > 0:
                logger.warning("`warmup=0` forced with `algorithm=\"Fixed_param\"`.")
            warmup = 0
        elif algorithm == "NUTS" and warmup == 0:
            if (isinstance(control, dict) and control.get("adapt_engaged", True)) or control is None:
                raise ValueError("Warmup samples must be greater than 0 when adaptation is enabled (`adapt_engaged=True`)")
        seed = pystan.misc._check_seed(seed)
        fit = self.fit_class(data, seed)

        m_pars = fit._get_param_names()
        p_dims = fit._get_param_dims()

        if isinstance(pars, string_types):
            pars = [pars]
        if pars is not None and len(pars) > 0:
            # Implementation note: this does not set the params_oi for the
            # instances of stan_fit which actually make the calls to
            # call_sampler. This is because we need separate instances of
            # stan_fit in each thread/process. So update_param_oi needs to
            # be called in every stan_fit instance.
            fit._update_param_oi(pars)
            if not all(p in m_pars for p in pars):
                pars = np.asarray(pars)
                unmatched = pars[np.invert(np.in1d(pars, m_pars))]
                msg = "No parameter(s): {}; sampling not done."
                raise ValueError(msg.format(', '.join(unmatched)))
        else:
            pars = m_pars

        if chains < 1:
            raise ValueError("The number of chains is less than one; sampling"
                             "not done.")

        check_hmc_diagnostics = kwargs.pop('check_hmc_diagnostics', None)
        # check that arguments in kwargs are valid
        valid_args = {"chain_id", "init_r", "test_grad", "append_samples", "refresh", "control"}
        for arg in kwargs:
            if arg not in valid_args:
                raise ValueError("Parameter `{}` is not recognized.".format(arg))

        args_list = pystan.misc._config_argss(chains=chains, iter=iter,
                                              warmup=warmup, thin=thin,
                                              init=init, seed=seed, sample_file=sample_file,
                                              diagnostic_file=diagnostic_file,
                                              algorithm=algorithm,
                                              control=control, **kwargs)

        # number of samples saved after thinning
        warmup2 = 1 + (warmup - 1) // thin
        n_kept = 1 + (iter - warmup - 1) // thin
        n_save = n_kept + warmup2

        if n_jobs is None:
            n_jobs = -1

        # disable multiprocessing if we only have a single chain
        if chains == 1:
            n_jobs = 1

        assert len(args_list) == chains
        call_sampler_args = izip(itertools.repeat(data), args_list, itertools.repeat(pars))
        call_sampler_star = self.module._call_sampler_star
        ret_and_samples = _map_parallel(call_sampler_star, call_sampler_args, n_jobs)
        samples = [smpl for _, smpl in ret_and_samples]

        # _organize_inits strips out lp__ (RStan does it in this method)
        inits_used = pystan.misc._organize_inits([s['inits'] for s in samples], m_pars, p_dims)

        random_state = np.random.RandomState(args_list[0]['seed'])
        perm_lst = [random_state.permutation(int(n_kept)) for _ in range(chains)]
        fnames_oi = fit._get_param_fnames_oi()
        n_flatnames = len(fnames_oi)
        fit.sim = {'samples': samples,
                   # rstan has this; name clashes with 'chains' in samples[0]['chains']
                   'chains': len(samples),
                   'iter': iter,
                   'warmup': warmup,
                   'thin': thin,
                   'n_save': [n_save] * chains,
                   'warmup2': [warmup2] * chains,
                   'permutation': perm_lst,
                   'pars_oi': fit._get_param_names_oi(),
                   'dims_oi': fit._get_param_dims_oi(),
                   'fnames_oi': fnames_oi,
                   'n_flatnames': n_flatnames}
        fit.model_name = self.model_name
        fit.model_pars = m_pars
        fit.par_dims = p_dims
        fit.mode = 0 if not kwargs.get('test_grad') else 1
        fit.inits = inits_used
        fit.stan_args = args_list
        fit.stanmodel = self
        fit.date = datetime.datetime.now()

        if args_list[0]["metric_file_flag"]:
            inv_metric_dir, _ = os.path.split(args_list[0]["metric_file"])
            shutil.rmtree(inv_metric_dir, ignore_errors=True)

        # If problems are found in the fit, this will print diagnostic
        # messages.
        if (check_hmc_diagnostics is None and algorithm in ("NUTS", "HMC")) and fit.mode != 1:
            if n_flatnames > 1000:
                msg = "Maximum (flat) parameter count (1000) exceeded: " +\
                      "skipping diagnostic tests for n_eff and Rhat.\n" +\
                      "To run all diagnostics call pystan.check_hmc_diagnostics(fit)"
                logger.warning(msg)
                checks = ["divergence", "treedepth", "energy"]
                pystan.diagnostics.check_hmc_diagnostics(fit, checks=checks)  # noqa
            else:
                pystan.diagnostics.check_hmc_diagnostics(fit)  # noqa
        elif (check_hmc_diagnostics and algorithm in ("NUTS", "HMC")) and fit.mode != 1:
            pystan.diagnostics.check_hmc_diagnostics(fit)  # noqa

        return fit

    def vb(self, data=None, pars=None, iter=10000,
           seed=None, init='random', sample_file=None, diagnostic_file=None, verbose=False,
           algorithm=None, **kwargs):
        """Call Stan's variational Bayes methods.

        Parameters
        ----------
        data : dict
            A Python dictionary providing the data for the model. Variables
            for Stan are stored in the dictionary as expected. Variable
            names are the keys and the values are their associated values.
            Stan only accepts certain kinds of values; see Notes.

        pars : list of string, optional
            A list of strings indicating parameters of interest. By default
            all parameters specified in the model will be stored.

        seed : int or np.random.RandomState, optional
            The seed, a positive integer for random number generation. Only
            one seed is needed when multiple chains are used, as the other
            chain's seeds are generated from the first chain's to prevent
            dependency among random number streams. By default, seed is
            ``random.randint(0, MAX_UINT)``.

        sample_file : string, optional
            File name specifying where samples for *all* parameters and other
            saved quantities will be written. If not provided, samples will be
            written to a temporary file and read back in. If the folder given is
            not writable, a temporary directory will be used. When there are
            multiple chains, an underscore and chain number are appended to the
            file name. By default do not write samples to file.

        diagnostic_file : string, optional
            File name specifying where diagnostics for the variational fit
            will be written.

        iter : int, 10000 by default
            Positive integer specifying how many iterations for each chain
            including warmup.

        algorithm : {'meanfield', 'fullrank'}
            algorithm}{One of "meanfield" and "fullrank" indicating which
            variational inference algorithm is used.  meanfield: mean-field
            approximation; fullrank: full-rank covariance. The default is
            'meanfield'.

        verbose : boolean, False by default
            Indicates whether intermediate output should be piped to the
            console. This output may be useful for debugging.

        Other optional parameters, refer to the manuals for both CmdStan
        and Stan.

        -  `iter`:  the maximum number of iterations, defaults to 10000
        -  `grad_samples` the number of samples for Monte Carlo enumerate of
           gradients, defaults to 1.
        -  `elbo_samples` the number of samples for Monte Carlo estimate of ELBO
           (objective function), defaults to 100. (ELBO stands for "the evidence
           lower bound".)
        -  `eta` positive stepsize weighting parameters for variational
           inference but is ignored if adaptation is engaged, which is the case
           by default.
        -  `adapt_engaged` flag indicating whether to automatically adapt the
           stepsize and defaults to True.
        -  `tol_rel_obj`convergence tolerance on the relative norm of the
           objective, defaults to 0.01.
        -  `eval_elbo`, evaluate ELBO every Nth iteration, defaults to 100
        -  `output_samples` number of posterior samples to draw and save,
           defaults to 1000.
        -  `adapt_iter`  number of iterations to adapt the stepsize if
           `adapt_engaged` is True and ignored otherwise.

        Returns
        -------
        results : dict
            Dictionary containing information related to results.

        Examples
        --------
        >>> from pystan import StanModel
        >>> m = StanModel(model_code='parameters {real y;} model {y ~ normal(0,1);}')
        >>> results = m.vb()
        >>> # results saved on disk in format inspired by CSV
        >>> print(results['args']['sample_file'])
        """
        if data is None:
            data = {}
        algorithms = ("meanfield", "fullrank")
        algorithm = "meanfield" if algorithm is None else algorithm
        if algorithm not in algorithms:
            raise ValueError("Algorithm must be one of {}".format(algorithms))
        seed = pystan.misc._check_seed(seed)
        fit = self.fit_class(data, seed)

        m_pars = fit._get_param_names()
        if isinstance(pars, string_types):
            pars = [pars]
        if pars is not None and len(pars) > 0:
            fit._update_param_oi(pars)
            if not all(p in m_pars for p in pars):
                pars = np.asarray(pars)
                unmatched = pars[np.invert(np.in1d(pars, m_pars))]
                msg = "No parameter(s): {}; sampling not done."
                raise ValueError(msg.format(', '.join(unmatched)))
        else:
            pars = m_pars

        if isinstance(init, numbers.Number):
            init = str(init)
        elif isinstance(init, Callable):
            init = init()
        elif not isinstance(init, Iterable) and \
                not isinstance(init, string_types):
            raise ValueError("Wrong specification of initial values.")

        stan_args = dict(iter=iter,
                         init=init,
                         chain_id=1,
                         seed=seed,
                         method="variational",
                         algorithm=algorithm)

        if sample_file is not None:
            stan_args['sample_file'] = pystan.misc._writable_sample_file(sample_file)
        else:
            stan_args['sample_file'] = os.path.join(tempfile.mkdtemp(), 'output.csv')

        if diagnostic_file is not None:
            stan_args['diagnostic_file'] = diagnostic_file

        # check that arguments in kwargs are valid
        valid_args = {'elbo_samples', 'eta', 'adapt_engaged', 'eval_elbo',
                      'grad_samples', 'output_samples', 'adapt_iter',
                      'tol_rel_obj'}
        for arg in kwargs:
            if arg not in valid_args:
                raise ValueError("Parameter `{}` is not recognized.".format(arg))

        stan_args.update(kwargs)
        stan_args = pystan.misc._get_valid_stan_args(stan_args)

        ret, sample = fit._call_sampler(stan_args, pars_oi=pars)

        logger.warning('Automatic Differentiation Variational Inference (ADVI) is an EXPERIMENTAL ALGORITHM.')
        logger.warning('ADVI samples may be found on the filesystem in the file `{}`'.format(sample.args['sample_file'].decode('utf8')))

        return OrderedDict([('args', sample.args), ('inits', sample.inits), ('sampler_params', sample.sampler_params), ('sampler_param_names', sample.sampler_param_names), ('mean_pars', sample.mean_pars), ('mean_par_names', sample.mean_par_names)])
