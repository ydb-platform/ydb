add_library(contrib-restricted-boost INTERFACE)
target_include_directories(contrib-restricted-boost INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost
)
target_link_libraries(contrib-restricted-boost INTERFACE
  contrib-libs-cxxsupp
)
