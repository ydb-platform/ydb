libtests = []

# Add compile flags when needed for the ASAN CI run
testargs = []
if get_option('asan_ci_dont_validate')
  if get_option('fatal_sanitizers')
    testargs = ['-DXSS_ASAN_CI_NOCHECK=true']
  else
    error('asan_ci_dont_validate is only for the ASAN CI, should be false otherwise!')
  endif
endif

libtests += static_library('tests_qsort',
  files('test-qsort.cpp', ),
  dependencies: [omp_dep, gtest_dep],
  include_directories : [src, lib, utils],
  cpp_args : [testargs],
  )

libtests += static_library('tests_kvsort',
  files('test-keyvalue.cpp', ),
  dependencies: [omp_dep, gtest_dep],
  include_directories : [src, lib, utils],
  cpp_args : [testargs],
  )

libtests += static_library('tests_objsort',
  files('test-objqsort.cpp', ),
  dependencies: [omp_dep, gtest_dep],
  include_directories : [src, lib, utils],
  cpp_args : [testargs],
  )
