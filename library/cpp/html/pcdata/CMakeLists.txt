add_library(cpp-html-pcdata)
target_link_libraries(cpp-html-pcdata PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-html-pcdata PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/html/pcdata/pcdata.cpp
)
