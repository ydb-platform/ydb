add_library(library-schlab-schine)
target_link_libraries(library-schlab-schine PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-stack_vector
  library-cpp-lwtrace
  cpp-lwtrace-mon
  library-schlab-probes
)
target_sources(library-schlab-schine PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/cbs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/cbs_bin.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/cbs_state.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/job.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/job_kind.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/job_state.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/scheduler.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schine/schlog.cpp
)
