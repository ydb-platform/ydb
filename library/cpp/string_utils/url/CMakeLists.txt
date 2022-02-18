add_library(cpp-string_utils-url)
target_link_libraries(cpp-string_utils-url PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-string_utils-url PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/url/url.cpp
)
