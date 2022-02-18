add_library(utils-TableGen-GlobalISel)
target_include_directories(utils-TableGen-GlobalISel PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel
)
target_link_libraries(utils-TableGen-GlobalISel PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
)
target_sources(utils-TableGen-GlobalISel PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/CodeExpander.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/GIMatchDag.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/GIMatchDagEdge.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/GIMatchDagInstr.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/GIMatchDagOperands.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/GIMatchDagPredicate.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/GIMatchDagPredicateDependencyEdge.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/utils/TableGen/GlobalISel/GIMatchTree.cpp
)
