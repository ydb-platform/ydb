add_library(cpp-xml-init)
target_link_libraries(cpp-xml-init PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-libxml
  library-cpp-charset
)
target_sources(cpp-xml-init PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/xml/init/ptr.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/xml/init/init.cpp
)
