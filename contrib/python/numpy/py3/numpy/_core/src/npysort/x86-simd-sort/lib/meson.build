libtargets = []

if cpp.has_argument('-march=haswell')
  libtargets += static_library('libavx',
    files(
      'x86simdsort-avx2.cpp',
      ),
    include_directories : [src],
    cpp_args : ['-march=haswell'],
    gnu_symbol_visibility : 'inlineshidden',
    dependencies: [omp_dep],
    )
endif

if cpp.has_argument('-march=skylake-avx512')
  libtargets += static_library('libskx',
    files(
      'x86simdsort-skx.cpp',
      ),
    include_directories : [src],
    cpp_args : ['-march=skylake-avx512'],
    gnu_symbol_visibility : 'inlineshidden',
    dependencies: [omp_dep],
    )
endif

if cpp.has_argument('-march=icelake-client')
  libtargets += static_library('libicl',
    files(
      'x86simdsort-icl.cpp',
      ),
    include_directories : [src],
    cpp_args : ['-march=icelake-client'],
    gnu_symbol_visibility : 'inlineshidden',
    dependencies: [omp_dep],
    )
endif

if cancompilefp16
  libtargets += static_library('libspr',
    files(
      'x86simdsort-spr.cpp',
      ),
    include_directories : [src],
    cpp_args : ['-march=sapphirerapids'],
    gnu_symbol_visibility : 'inlineshidden',
    dependencies: [omp_dep],
    )
endif

install_headers('x86simdsort.h')
