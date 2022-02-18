add_library(library-cpp-packedtypes)
target_link_libraries(library-cpp-packedtypes PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-streams-zc_memory_input
)
target_sources(library-cpp-packedtypes PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/packedtypes/longs.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/packedtypes/packedfloat.cpp
)
