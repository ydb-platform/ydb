add_library(library-http_proxy-error)
target_link_libraries(library-http_proxy-error PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-http_proxy-error PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/http_proxy/error/error.cpp
)
