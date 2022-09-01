
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(backpressure)
add_subdirectory(base)
add_subdirectory(crypto)
add_subdirectory(dsproxy)
add_subdirectory(groupinfo)
add_subdirectory(incrhuge)
add_subdirectory(lwtrace_probes)
add_subdirectory(nodewarden)
add_subdirectory(other)
add_subdirectory(pdisk)
add_subdirectory(storagepoolmon)
add_subdirectory(testload)
add_subdirectory(ut_blobstorage)
add_subdirectory(ut_group)
add_subdirectory(ut_mirror3of4)
add_subdirectory(ut_pdiskfit)
add_subdirectory(ut_vdisk)
add_subdirectory(ut_vdisk2)
add_subdirectory(vdisk)

add_library(ydb-core-blobstorage INTERFACE)
target_link_libraries(ydb-core-blobstorage INTERFACE
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-blobstorage-backpressure
  core-blobstorage-base
  core-blobstorage-crypto
  core-blobstorage-dsproxy
  core-blobstorage-groupinfo
  core-blobstorage-incrhuge
  core-blobstorage-lwtrace_probes
  core-blobstorage-nodewarden
  core-blobstorage-other
  core-blobstorage-pdisk
  core-blobstorage-storagepoolmon
  core-blobstorage-testload
  core-blobstorage-vdisk
)
