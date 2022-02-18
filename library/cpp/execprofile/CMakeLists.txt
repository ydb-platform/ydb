add_library(library-cpp-execprofile)
target_link_libraries(library-cpp-execprofile PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-execprofile PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/execprofile/profile.cpp
)
