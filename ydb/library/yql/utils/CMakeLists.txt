add_library(library-yql-utils)
target_link_libraries(library-yql-utils PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-md5
  library-cpp-messagebus
  cpp-string_utils-quote
  cpp-threading-future
)
target_sources(library-yql-utils PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/bind_in_range.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/debug_info.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/future_action.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/hash.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/md5_stream.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/multi_resource_lock.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/parse_double.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/proc_alive.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/rand_guid.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/retry.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/swap_bytes.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/url_builder.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/utf8.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/yql_panic.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/yql_paths.cpp
)
