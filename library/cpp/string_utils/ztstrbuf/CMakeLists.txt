add_library(cpp-string_utils-ztstrbuf)
target_link_libraries(cpp-string_utils-ztstrbuf PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-string_utils-ztstrbuf PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/string_utils/ztstrbuf/ztstrbuf.cpp
)
