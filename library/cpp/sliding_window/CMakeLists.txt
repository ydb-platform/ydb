add_library(library-cpp-sliding_window)
target_link_libraries(library-cpp-sliding_window PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-sliding_window PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/sliding_window/sliding_window.cpp
)
