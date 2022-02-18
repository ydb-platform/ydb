add_library(common-unicode_base-lib)
target_compile_options(common-unicode_base-lib PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(common-unicode_base-lib PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-split
  cpp-string_utils-levenshtein_diff
  cpp-unicode-normalization
  yql-public-udf
  library-yql-utils
)
target_sources(common-unicode_base-lib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/unicode_base/lib/unicode_base_udf.cpp
)
