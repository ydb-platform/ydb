find_package(OpenSSL)

add_library(libs-libevent-event_openssl)
target_compile_options(libs-libevent-event_openssl PRIVATE
  -DHAVE_CONFIG_H
  -DEVENT__HAVE_STRLCPY=1
)
target_include_directories(libs-libevent-event_openssl PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libevent
  ${CMAKE_SOURCE_DIR}/contrib/libs/libevent/include
)
target_link_libraries(libs-libevent-event_openssl PUBLIC
  contrib-libs-libc_compat
  OpenSSL::OpenSSL
)
target_sources(libs-libevent-event_openssl PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libevent/bufferevent_openssl.c
)
