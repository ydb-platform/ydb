add_library(public-lib-jwt)
target_link_libraries(public-lib-jwt PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-jwt-cpp
  library-cpp-json
  impl-ydb_internal-common
)
target_sources(public-lib-jwt PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/jwt/jwt.cpp
)
