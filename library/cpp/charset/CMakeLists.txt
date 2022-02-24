find_package(Iconv REQUIRED)

add_library(library-cpp-charset)
target_link_libraries(library-cpp-charset PUBLIC
  contrib-libs-cxxsupp
  yutil
  Iconv::Iconv
)
target_sources(library-cpp-charset PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/generated/cp_data.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/generated/encrec_data.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/codepage.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/cp_encrec.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/doccodes.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/iconv.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/ci_string.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/charset/wide.cpp
)
