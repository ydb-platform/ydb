libaio = cc.find_library('aio', required: true)
libpthread = cc.find_library('pthread', required: true)

vhost_user_blk_test_server_includes = include_directories(
    '../'
)

vhost_user_blk_test_server = executable(
    'vhost-user-blk-test-server',
    'vhost_user_blk_test_server.c',
    link_with: libvhost,
    dependencies: [libaio, libpthread],
    include_directories: [
        vhost_user_blk_test_server_includes,
        libvhost_includes
    ]
)

# If libblkio is disabled, we have no client to run
# against vhost-user-blk-test-server, so nothing to do here.
if not libblkio_proj.found()
  subdir_done()
endif

# If libblkio subproject doesn't define blkio_bench, this yields
# a fatal error. It is OK as we pull a specific libblkio revision
# which is known to define blkio_bench, and if it is missing,
# something is certainly wrong.
libblkio_bench_dep = libblkio_proj.get_variable('blkio_bench')

envdata = environment()
envdata.append(
    'TEST_SERVER_BINARY',
    vhost_user_blk_test_server.full_path()
)
envdata.append(
    'BLKIO_BENCH_BINARY',
    libblkio_bench_dep.full_path()
)

test(
    'unit-tests',
    import('python').find_installation('python3', modules: ['pytest']),
    args: ['-m', 'pytest', '-rsv'],
    depends: [vhost_user_blk_test_server, libblkio_bench_dep],
    env: envdata,
    workdir: meson.current_source_dir(),
    timeout: 150,
    is_parallel: false,
)
