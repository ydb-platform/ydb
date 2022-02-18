add_library(yql-utils-fetch)
target_link_libraries(yql-utils-fetch PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-charset
  cpp-http-io
  cpp-http-misc
  cpp-openssl-io
  library-cpp-uri
  yql-utils-log
)
target_sources(yql-utils-fetch PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/fetch/fetch.cpp
)
