add_library(contrib-libs-nghttp2)
target_compile_options(contrib-libs-nghttp2 PRIVATE
  -DBUILDING_NGHTTP2
  -DHAVE_CONFIG_H
)
target_include_directories(contrib-libs-nghttp2 PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/includes
)
target_include_directories(contrib-libs-nghttp2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib
)
target_sources(contrib-libs-nghttp2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_buf.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_callbacks.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_debug.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_frame.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_hd.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_hd_huffman.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_hd_huffman_data.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_helper.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_http.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_map.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_mem.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_npn.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_option.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_outbound_item.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_pq.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_priority_spec.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_queue.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_rcbuf.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_session.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_stream.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_submit.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/nghttp2/lib/nghttp2_version.c
)
