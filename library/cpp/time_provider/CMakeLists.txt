add_library(library-cpp-time_provider)
target_link_libraries(library-cpp-time_provider PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-time_provider PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/time_provider/time_provider.cpp
)
