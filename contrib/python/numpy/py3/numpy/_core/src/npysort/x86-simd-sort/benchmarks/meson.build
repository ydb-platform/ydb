libbench = []

libbench += static_library('bench_qsort',
  files(
    'bench-all.cpp',
    ),
  dependencies: gbench_dep,
  include_directories : [src, lib, utils],
  cpp_args : ['-O3'],
  )

if benchvq and fs.is_file('../highway/hwy/contrib/sort/vqsort-inl.h')
  hwy = include_directories('../highway')
  libbench += static_library('bench_vqsort',
    files(
      'bench-vqsort.cpp',
      ),
    dependencies: gbench_dep,
    include_directories : [src, lib, utils, hwy],
    cpp_args : ['-O3', '-march=native'],
    )
endif

if benchipp
  libbench += static_library('bench_ippsort',
    files(
      'bench-ipp.cpp',
      ),
    dependencies: gbench_dep,
    include_directories : [src, lib, utils],
    cpp_args : ['-O3', '-march=native'],
    )
endif
