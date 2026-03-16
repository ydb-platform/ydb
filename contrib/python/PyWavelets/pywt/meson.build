fs = import('fs')

py.install_sources(
  [
    '__init__.py',
    '_cwt.py',
    '_doc_utils.py',
    '_dwt.py',
    '_functions.py',
    '_mra.py',
    '_multidim.py',
    '_multilevel.py',
    '_pytest.py',
    '_pytesttester.py',
    '_swt.py',
    '_thresholding.py',
    '_utils.py',
    '_wavelet_packets.py',
    'conftest.py',
  ],
  subdir: 'pywt'
)

generate_version = custom_target(
  'generate-version',
  install: true,
  build_always_stale: true,
  build_by_default: true,
  output: 'version.py',
  input: '../util/version_utils.py',
  command: [py, '@INPUT@', '--source-root', '@SOURCE_ROOT@'],
  install_dir: py.get_install_dir() / 'pywt',
  install_tag: 'python-runtime',
)

cc = meson.get_compiler('c')

# Check for C99 complex types support. MSVC shouldn't have it, other compilers
# most likely will.
cdata = configuration_data()
have_c99_complex = cc.has_type('complex float', prefix: '#include <complex.h>')
if have_c99_complex
  cdata.set('have_c99_complex', 'True')
  cdata.set('have_c99_complex_cy', '1')
else
  cdata.set('have_c99_complex', 'False')
  cdata.set('have_c99_complex_cy', '0')
endif

_c99_config = configure_file(
  input: '_c99_config.py.in',
  output: '_c99_config.py',
  configuration: cdata,
  install: true,
  install_dir: py.get_install_dir() / 'pywt',
  install_tag: 'python-runtime',
)

install_subdir('data', install_dir: py.get_install_dir() / 'pywt')
install_subdir('tests', install_dir:  py.get_install_dir() / 'pywt')

# Copy needed for Cython code in _extensions
__init__py = fs.copyfile('__init__.py')

subdir('_extensions')
