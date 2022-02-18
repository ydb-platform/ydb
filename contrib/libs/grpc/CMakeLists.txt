add_library(contrib-libs-grpc INTERFACE)
target_link_libraries(contrib-libs-grpc INTERFACE
  contrib-libs-cxxsupp
  yutil
  libs-grpc-grpc
  libs-grpc-grpc++
)
