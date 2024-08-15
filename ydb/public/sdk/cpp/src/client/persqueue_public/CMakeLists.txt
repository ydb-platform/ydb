add_subdirectory(codecs)
add_subdirectory(impl)

_ydb_sdk_add_library(cpp-client-ydb_persqueue_public INTERFACE)

target_link_libraries(cpp-client-ydb_persqueue_public INTERFACE
  yutil
  cpp-client-ydb_persqueue_core
  client-ydb_persqueue_public-impl
  client-ydb_persqueue_public-codecs
)

generate_enum_serilization(cpp-client-ydb_persqueue_public
  ${YDB_SDK_SOURCE_DIR}/src/client/persqueue_public/include/control_plane.h
  INCLUDE_HEADERS
  src/client/persqueue_public/include/control_plane.h
)

generate_enum_serilization(cpp-client-ydb_persqueue_public
  ${YDB_SDK_SOURCE_DIR}/src/client/persqueue_public/include/read_events.h
  INCLUDE_HEADERS
  src/client/persqueue_public/include/read_events.h
)

generate_enum_serilization(cpp-client-ydb_persqueue_public
  ${YDB_SDK_SOURCE_DIR}/src/client/persqueue_public/include/write_events.h
  INCLUDE_HEADERS
  src/client/persqueue_public/include/write_events.h
)

generate_enum_serilization(cpp-client-ydb_persqueue_public
  ${YDB_SDK_SOURCE_DIR}/src/client/persqueue_public/include/write_session.h
  INCLUDE_HEADERS
  src/client/persqueue_public/include/write_session.h
)
