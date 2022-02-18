add_library(cpp-string_utils-scan)
target_link_libraries(cpp-string_utils-scan PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-string_utils-scan PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/scan/scan.cpp
)
