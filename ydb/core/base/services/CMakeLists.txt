add_library(core-base-services INTERFACE)
target_link_libraries(core-base-services INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-protos
  cpp-actors-interconnect
  cpp-actors-helpers
)
