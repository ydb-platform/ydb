add_library(dragonbox)
target_include_directories(dragonbox PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/dragonbox/include
)
target_include_directories(dragonbox PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/dragonbox/include
)
target_link_libraries(dragonbox PUBLIC
  contrib-libs-cxxsupp
)
target_sources(dragonbox PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/dragonbox/source/dragonbox_to_chars.cpp
)
