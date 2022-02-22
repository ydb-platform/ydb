find_package(Python2)

add_library(library-cpp-tld)
target_link_libraries(library-cpp-tld PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-lower_case
)
target_sources(library-cpp-tld PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/tld/tld.cpp
  ${CMAKE_BINARY_DIR}/library/cpp/tld/tld.inc
)
add_custom_command(
  OUTPUT
  ${CMAKE_BINARY_DIR}/library/cpp/tld/tld.inc
  DEPENDS
  ${CMAKE_SOURCE_DIR}/library/cpp/tld/tlds-alpha-by-domain.txt
  ${CMAKE_SOURCE_DIR}/library/cpp/tld/gen_tld.py
  COMMAND
  Python2::Interpreter
  ${CMAKE_SOURCE_DIR}/library/cpp/tld/gen_tld.py
  ${CMAKE_SOURCE_DIR}/library/cpp/tld/tlds-alpha-by-domain.txt
  >
  tld.inc
)
