m_dep = cc.find_library('m', required : false)
if m_dep.found()
  add_project_link_arguments('-lm', language : 'c')
endif

# Don't use the deprecated NumPy C API. Define this to a fixed version instead of
# NPY_API_VERSION in order not to break compilation for released PyWavelets
# versions when NumPy introduces a new deprecation.
numpy_nodepr_api = ['-DNPY_NO_DEPRECATED_API=NPY_1_22_API_VERSION']

# Uses the `numpy-config` executable (or a user's numpy.pc pkg-config file),
# will work for numpy>=2.0.0b1 and meson>=1.4.0
_numpy_dep = dependency('numpy', required: false)
if _numpy_dep.found()
  np_dep = declare_dependency(dependencies: _numpy_dep, compile_args: numpy_nodepr_api)
else
  # For cross-compilation it is often not possible to run the Python interpreter
  # in order to retrieve numpy's include directory. It can be specified in the
  # cross file instead:
  #   [properties]
  #   numpy-include-dir = /abspath/to/host-pythons/site-packages/numpy/core/include
  #
  # This uses the path as is, and avoids running the interpreter.
  incdir_numpy = meson.get_external_property('numpy-include-dir', 'not-given')
  if incdir_numpy == 'not-given'
    incdir_numpy = run_command(py,
      [
        '-c',
        '''import os
import numpy as np
try:
  incdir = os.path.relpath(np.get_include())
except Exception:
  incdir = np.get_include()
print(incdir)
    '''
      ],
      check: true
    ).stdout().strip()
  endif
  inc_np = include_directories(incdir_numpy)
  np_dep = declare_dependency(include_directories: inc_np, compile_args: numpy_nodepr_api)
endif

config_pxi = configure_file(
  input: 'config.pxi.in',
  output: 'config.pxi',
  configuration: cdata,
  install: false
)

sources = [
  'c/common.c',
  'c/convolution.c',
  'c/cwt.c',
  'c/wavelets.c',
  'c/wt.c',
]

# pyx -> c transpile output depends on copied __init__.py and pxd files
_cython_tree = [
  fs.copyfile('__init__.py'),
  fs.copyfile('common.pxd'),
  fs.copyfile('_cwt.pxd'),
  fs.copyfile('c_wt.pxd'),
  fs.copyfile('_dwt.pxd'),
  fs.copyfile('_pywt.pxd'),
  fs.copyfile('wavelet.pxd'),
  fs.copyfile('wavelets_list.pxi'),
]

# avoid compiler warnings:  tell Cython to use C99 complex types
c_args = ['-DPY_EXTENSION']
if have_c99_complex
  c_args += ['-DHAVE_C99_COMPLEX', '-DCYTHON_CCOMPLEX=1']
endif

libc_wt = static_library('c_wt',
  sources,
  c_args : c_args,
  include_directories : 'c',
  dependencies : py_dep,
)

cy = meson.get_compiler('cython')
cython_args = ['-3', '--fast-fail', '--output-file', '@OUTPUT@', '--include-dir', '@BUILD_ROOT@', '@INPUT@']
if cy.version().version_compare('>=3.1.0')
  cython_args += ['-Xfreethreading_compatible=True']
endif

cython_gen = generator(cython,
  arguments : cython_args,
  output : '@BASENAME@.c',
  depends : _cython_tree
)

pyx_files = [
  ['_cwt', fs.copyfile('_cwt.pyx')],
  ['_dwt', fs.copyfile('_dwt.pyx')],
  ['_swt', fs.copyfile('_swt.pyx')],
  ['_pywt', fs.copyfile('_pywt.pyx')],
]

cy_deps = declare_dependency(sources: [__init__py, _cython_tree])#, config_pxi])
foreach pyx_file: pyx_files
  py.extension_module(pyx_file[0],
    [cython_gen.process(pyx_file[1])],
    c_args : c_args,
    include_directories : 'c',
    dependencies : [np_dep, cy_deps],
    link_with : libc_wt,
    install: true,
    subdir: 'pywt/_extensions',
  )
endforeach
