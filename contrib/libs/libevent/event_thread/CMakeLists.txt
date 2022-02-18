add_library(libs-libevent-event_thread)
target_compile_options(libs-libevent-event_thread PRIVATE
  -DHAVE_CONFIG_H
  -DEVENT__HAVE_STRLCPY=1
)
target_include_directories(libs-libevent-event_thread PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libevent
  ${CMAKE_SOURCE_DIR}/contrib/libs/libevent/include
)
target_link_libraries(libs-libevent-event_thread PUBLIC
  contrib-libs-libc_compat
)
target_sources(libs-libevent-event_thread PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libevent/evthread_pthread.c
)
