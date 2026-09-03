libtests = []

libtests += static_library('tests_qsort',
  files('test-qsort.cpp', ),
  dependencies: gtest_dep,
  include_directories : [src, lib, utils],
  )

libtests += static_library('tests_kvsort',
  files('test-keyvalue.cpp', ),
  dependencies: gtest_dep,
  include_directories : [src, lib, utils],
  )

libtests += static_library('tests_objsort',
  files('test-objqsort.cpp', ),
  dependencies: gtest_dep,
  include_directories : [src, lib, utils],
  )
