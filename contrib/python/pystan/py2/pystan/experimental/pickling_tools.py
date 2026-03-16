"""Experimental module to load fit without model instance."""
from distutils.core import Extension
from Cython.Build.Inline import _get_build_extension
from Cython.Build.Dependencies import cythonize
import tempfile
import logging
import io
import os
import string
import numpy as np
import pickle
import pystan
import platform
from string import ascii_letters, digits, printable, punctuation
import sys
import shutil

from pystan._compat import PY2
from pystan.model import load_module

logger = logging.getLogger('pystan')

def create_fake_model(module_name):
    """Creates a fake model with specific module name.

    Parameters
    ----------
    module_name : str

    Returns
    -------
    StanModel
        Dummy StanModel object with specific module name.
    """
    # reverse engineer the name
    model_name, nonce = module_name.replace("stanfit4", "").rsplit("_", 1)
    # create minimal code
    model_code = """generated quantities {real fake_parameter=1;}"""
    stanc_ret = pystan.api.stanc(model_code=model_code,
                                 model_name=model_name,
                                 obfuscate_model_name=False)

    model_cppname = stanc_ret['model_cppname']
    model_cppcode = stanc_ret['cppcode']

    original_module_name = module_name
    module_name = 'stanfit4{}_{}'.format(model_name, nonce)
    # module name should be same
    assert original_module_name == module_name


    lib_dir = tempfile.mkdtemp()
    pystan_dir = os.path.dirname(pystan.__file__)
    include_dirs = [
        lib_dir,
        pystan_dir,
        os.path.join(pystan_dir, "stan", "src"),
        os.path.join(pystan_dir, "stan", "lib", "stan_math"),
        os.path.join(pystan_dir, "stan", "lib", "stan_math", "lib", "eigen_3.3.3"),
        os.path.join(pystan_dir, "stan", "lib", "stan_math", "lib", "boost_1.69.0"),
        os.path.join(pystan_dir, "stan", "lib", "stan_math", "lib", "sundials_4.1.0", "include"),
        np.get_include(),
    ]

    model_cpp_file = os.path.join(lib_dir, model_cppname + '.hpp')

    with io.open(model_cpp_file, 'w', encoding='utf-8') as outfile:
        outfile.write(model_cppcode)

    pyx_file = os.path.join(lib_dir, module_name + '.pyx')
    pyx_template_file = os.path.join(pystan_dir, 'stanfit4model.pyx')
    with io.open(pyx_template_file, 'r', encoding='utf-8') as infile:
        s = infile.read()
        template = string.Template(s)
    with io.open(pyx_file, 'w', encoding='utf-8') as outfile:
        s = template.safe_substitute(model_cppname=model_cppname)
        outfile.write(s)

    stan_macros = [
        ('BOOST_RESULT_OF_USE_TR1', None),
        ('BOOST_NO_DECLTYPE', None),
        ('BOOST_DISABLE_ASSERTS', None),
    ]

    build_extension = _get_build_extension()

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
                '-O1',
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
            '-O1',
            '-ftemplate-depth-256',
            '-Wno-unused-function',
            '-Wno-uninitialized',
            '-std=c++1y',
        ] + extra_compile_args

    extension = Extension(name=module_name,
                          language="c++",
                          sources=[pyx_file],
                          define_macros=stan_macros,
                          include_dirs=include_dirs,
                          extra_compile_args=extra_compile_args)

    cython_include_dirs = ['.', pystan_dir]

    build_extension.extensions = cythonize([extension],
                                           include_path=cython_include_dirs,
                                           quiet=True)
    build_extension.build_temp = os.path.dirname(pyx_file)
    build_extension.build_lib = lib_dir

    build_extension.run()


    module = load_module(module_name, lib_dir)

    shutil.rmtree(lib_dir, ignore_errors=True)
    return module


def get_module_name(path, open_func=None, open_kwargs=None):
    if open_func is None:
        open_func = io.open
    if open_kwargs is None:
        open_kwargs = {"mode" : "rb"}

    valid_chr = digits + ascii_letters + punctuation
    break_next = False
    name_loc = False
    module_name = ""
    with open_func(path, **open_kwargs) as f:
        # scan first few bytes
        for i in range(500):
            byte, = f.read(1)
            if not PY2:
                byte = chr(byte)
            if name_loc and byte in valid_chr:
                module_name += byte

                if module_name == "stanfit":
                    break_next = True
            elif byte == "s":
                module_name += byte
                name_loc = True
            elif break_next:
                break
    return module_name


def unpickle_fit(path, open_func=None, open_kwargs=None, module_name=None, return_model=False):
    """Load pickled (compressed) fit object without model binary.

    Parameters
    ----------
    path : str
    open_func : function
        Function that takes path and returns fileobject. E.g. `io.open`, `gzip.open`.
    open_kwargs : dict
        Keyword arguments for `open_func`.
    module_name : str, optional
        Module name used for the StanModel. By default module name is extractad from pickled file.
    return_model : bool
        Return the fake model.
        WARNING: Model should not be used for anything.

    Returns
    -------
    StanFit4Model
    StanModel, optional

    Examples
    --------
    >>> import pystan
    >>> import pystan.experimental
    >>> path = "path/to/fit.pickle"
    >>> fit = pystan.experimental.load_fit(path)

    Save fit to external format

    Transform fit to dataframe and save to csv
    >>> df = pystan.misc.to_dataframe(fit)
    >>> df.to_csv("path/to/fit.csv")

    Transform fit to arviz.InferenceData and save to netCDF4
    >>> import arviz as az
    >>> idata = az.from_pystan(fit)
    >>> idata.to_netcdf("path/to/fit.nc")
    """
    if module_name is None:
        module_name = get_module_name(path, open_func, open_kwargs)
    if not module_name:
        raise ValueError("Module name not found")

    # create fake model
    model = create_fake_model(module_name)

    if open_func is None:
        open_func = io.open
    if open_kwargs is None:
        open_kwargs = {"mode":"rb"}

    with open_func(path, **open_kwargs) as f:
        fit = pickle.load(f)
    if return_model:
        logger.warning("The model binary is built against fake model code. Model should not be used.")
        return fit, model
    return fit
