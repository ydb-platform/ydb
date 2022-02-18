add_library(cpp-xml-document)
target_link_libraries(cpp-xml-document PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-xml-init
  contrib-libs-libxml
  cpp-string_utils-ztstrbuf
)
target_sources(cpp-xml-document PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/xml/document/xml-document.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/xml/document/xml-textreader.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/xml/document/xml-options.cpp
)
