project('x86-simd-sort', 'cpp',
        version : '7.0.x',
        license : 'BSD 3-clause',
        default_options : ['cpp_std=c++17'])
fs = import('fs')
cpp = meson.get_compiler('cpp')
src = include_directories('src')
lib = include_directories('lib')
bench = include_directories('benchmarks')
utils = include_directories('utils')
tests = include_directories('tests')

# Add IPP sort to benchmarks:
benchipp = false
ipplink = []
if get_option('build_ippbench')
  benchipp = true
  ipplink = ['-lipps', '-lippcore']
endif

# Essentially '-Werror' for the sanitizers; all problems become fatal with this set
if get_option('fatal_sanitizers')
  add_project_arguments([ '-fno-sanitize-recover=all' ], language: 'cpp')
endif

# Add google vqsort to benchmarks:
benchvq = false
if get_option('build_vqsortbench')
  benchvq = true
endif

# openMP:
omp = []
omp_dep = []
if get_option('use_openmp')
  omp = dependency('openmp', required : true)
  omp_dep = declare_dependency(dependencies: omp, compile_args: ['-DXSS_USE_OPENMP'])
endif

fp16code = '''#include<immintrin.h>
int main() {
  __m512h temp = _mm512_set1_ph(1.0f);
  __m512h var2 = _mm512_min_ph(temp, temp);
  return 0;
}
'''
cancompilefp16 = cpp.compiles(fp16code, args:'-march=sapphirerapids')

subdir('lib')
if get_option('lib_type') == 'shared'
  libsimdsort = shared_library('x86simdsortcpp',
                             'lib/x86simdsort.cpp',
                             include_directories : [src, utils, lib],
                             link_with : [libtargets],
                             dependencies: [omp_dep],
                             gnu_symbol_visibility : 'inlineshidden',
                             install : true,
                             soversion : 1,
                            )
else
  libsimdsort = static_library('x86simdsortcpp',
                             'lib/x86simdsort.cpp',
                             include_directories : [src, utils, lib],
                             link_with : [libtargets],
                             dependencies: [omp_dep],
                             gnu_symbol_visibility : 'inlineshidden',
                             install : true,
                             pic: true,
                            )
endif

pkg_mod = import('pkgconfig')
pkg_mod.generate(libraries : libsimdsort,
                 version : '7.0',
                 name : 'libx86simdsortcpp',
                 filebase : 'x86simdsortcpp',
                 description : 'C++ template library for high performance SIMD based sorting routines.')

# Create a new dependency variable making it easy to use this as a subproject:
x86simdsortcpp_dep = declare_dependency(
  include_directories: include_directories('lib'),
  link_with: libsimdsort,
)

# Build test suite if option build_tests set to true
if get_option('build_tests')
  gtest_dep = dependency('gtest_main', required : true, static: false)
  subdir('tests')
  testexe = executable('testexe',
                     include_directories : [lib, utils],
                     dependencies : [gtest_dep, x86simdsortcpp_dep],
                     link_whole : [libtests],
                    )
  test('x86 simd sort tests', testexe)
endif

# Build benchmarking suite if option build_benchmarks is set to true

if get_option('build_benchmarks')
  gbench_dep = dependency('benchmark', required : true, static: false)
  thread_dep = dependency('threads') # libbenchmark could need pthread_create
  subdir('benchmarks')
  benchexe = executable('benchexe',
                      include_directories : [src, lib, utils, bench],
                      dependencies : [gbench_dep, thread_dep, x86simdsortcpp_dep],
                      link_args: ['-lbenchmark_main', ipplink],
                      link_whole : [libbench],
                     )
endif

summary({
  'Can compile AVX-512 FP16 ISA': cancompilefp16,
  'Build test content': get_option('build_tests'),
  'Build benchmarks': get_option('build_benchmarks'),
  },
  section: 'Configuration',
  bool_yn: true
  )

