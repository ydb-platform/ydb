add_library(core-wrappers-ut_helpers)
target_link_libraries(core-wrappers-ut_helpers PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-md5
  cpp-http-server
  cpp-xml-document
  library-cpp-cgiparam
)
target_sources(core-wrappers-ut_helpers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/wrappers/ut_helpers/s3_mock.cpp
)
