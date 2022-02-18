add_library(contrib-libs-libevent INTERFACE)
target_include_directories(contrib-libs-libevent INTERFACE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libevent/include
)
target_link_libraries(contrib-libs-libevent INTERFACE
  libs-libevent-event_core
  libs-libevent-event_extra
  libs-libevent-event_openssl
  libs-libevent-event_thread
)
