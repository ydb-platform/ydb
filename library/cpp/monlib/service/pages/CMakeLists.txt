
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(monlib-service-pages)
target_link_libraries(monlib-service-pages PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-build_info
  cpp-malloc-api
  library-cpp-svnversion
  library-cpp-resource
  cpp-monlib-service
  monlib-encode-json
  monlib-encode-text
  monlib-encode-spack
  monlib-encode-prometheus
)
target_sources(monlib-service-pages PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/diag_mon_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/html_mon_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/index_mon_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/mon_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/pre_mon_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/resource_mon_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/templates.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/version_mon_page.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/pages/registry_mon_page.cpp
)
