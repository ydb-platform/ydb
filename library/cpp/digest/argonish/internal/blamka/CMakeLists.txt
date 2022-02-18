add_library(argonish-internal-blamka INTERFACE)
target_link_libraries(argonish-internal-blamka INTERFACE
  contrib-libs-cxxsupp
  yutil
  argonish-internal-rotations
)
