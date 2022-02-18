add_library(library-cpp-hyperloglog)
target_link_libraries(library-cpp-hyperloglog PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-hyperloglog PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/hyperloglog/hyperloglog.cpp
)
