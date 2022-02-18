add_library(library-schlab-schoot)
target_link_libraries(library-schlab-schoot PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-stack_vector
  cpp-protobuf-json
  library-schlab-protos
)
target_sources(library-schlab-schoot PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schoot/schoot_gen.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schoot/schoot_gen_cfg.cpp
)
