add_library(library-cpp-colorizer)
target_link_libraries(library-cpp-colorizer PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-colorizer PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/colorizer/colors.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/colorizer/output.cpp
)
