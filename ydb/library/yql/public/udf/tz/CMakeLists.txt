add_library(public-udf-tz)
target_link_libraries(public-udf-tz PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(public-udf-tz PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/udf/tz/udf_tz.cpp
)
