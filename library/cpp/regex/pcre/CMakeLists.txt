add_library(cpp-regex-pcre)
target_link_libraries(cpp-regex-pcre PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-pcre
  libs-pcre-pcre16
  libs-pcre-pcre32
  cpp-containers-stack_array
)
target_sources(cpp-regex-pcre PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/regex/pcre/pcre.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/regex/pcre/regexp.cpp
)
