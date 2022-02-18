add_library(library-cpp-archive)
target_link_libraries(library-cpp-archive PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-archive PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/archive/yarchive.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/archive/directory_models_archive_reader.cpp
)
