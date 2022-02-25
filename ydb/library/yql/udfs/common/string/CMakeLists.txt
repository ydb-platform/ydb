add_library(string_udf INTERFACE)
target_link_libraries(string_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-charset
  cpp-deprecated-split
  cpp-html-pcdata
  cpp-string_utils-base64
  cpp-string_utils-levenshtein_diff
  cpp-string_utils-quote
)

add_global_library_for(string_udf.global string_udf)
target_compile_options(string_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(string_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-charset
  cpp-deprecated-split
  cpp-html-pcdata
  cpp-string_utils-base64
  cpp-string_utils-levenshtein_diff
  cpp-string_utils-quote
)
target_sources(string_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/string/string_udf.cpp
)

add_library(string_udf.udf SHARED)
target_compile_options(string_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(string_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-charset
  cpp-deprecated-split
  cpp-html-pcdata
  cpp-string_utils-base64
  cpp-string_utils-levenshtein_diff
  cpp-string_utils-quote
)
target_sources(string_udf.udf PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/string/string_udf.cpp
)
vcs_info(string_udf.udf)
