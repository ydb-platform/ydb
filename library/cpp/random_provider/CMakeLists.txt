add_library(library-cpp-random_provider)
target_link_libraries(library-cpp-random_provider PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-random_provider PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/random_provider/random_provider.cpp
)
