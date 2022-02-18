add_library(cpp-codecs-greedy_dict)
target_link_libraries(cpp-codecs-greedy_dict PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-comptrie
  cpp-string_utils-relaxed_escaper
)
target_sources(cpp-codecs-greedy_dict PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/greedy_dict/gd_builder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/greedy_dict/gd_entry.cpp
)
