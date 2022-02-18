add_library(ydb-core-control)
target_link_libraries(ydb-core-control PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-monlib-dynamic_counters
  ydb-core-base
  ydb-core-mon
  ydb-core-node_whiteboard
)
target_sources(ydb-core-control PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/control/immediate_control_board_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/control/immediate_control_board_control.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/control/immediate_control_board_impl.cpp
)
