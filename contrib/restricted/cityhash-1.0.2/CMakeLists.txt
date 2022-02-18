add_library(contrib-restricted-cityhash-1.0.2)
target_include_directories(contrib-restricted-cityhash-1.0.2 PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/cityhash-1.0.2
)
target_include_directories(contrib-restricted-cityhash-1.0.2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/cityhash-1.0.2
)
target_link_libraries(contrib-restricted-cityhash-1.0.2 PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-restricted-cityhash-1.0.2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/cityhash-1.0.2/city.cc
)
