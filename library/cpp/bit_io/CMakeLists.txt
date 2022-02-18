add_library(library-cpp-bit_io)
target_link_libraries(library-cpp-bit_io PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-accessors
)
target_sources(library-cpp-bit_io PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/bit_io/bitinput.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/bit_io/bitinput_impl.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/bit_io/bitoutput.cpp
)
