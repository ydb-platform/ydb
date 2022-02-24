find_package(ZLIB REQUIRED)

add_library(contrib-libs-hdr_histogram)
target_include_directories(contrib-libs-hdr_histogram PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/hdr_histogram/src
)
target_link_libraries(contrib-libs-hdr_histogram PUBLIC
  contrib-libs-cxxsupp
  ZLIB::ZLIB
)
target_sources(contrib-libs-hdr_histogram PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/hdr_histogram/src/hdr_encoding.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/hdr_histogram/src/hdr_interval_recorder.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/hdr_histogram/src/hdr_histogram.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/hdr_histogram/src/hdr_writer_reader_phaser.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/hdr_histogram/src/hdr_time.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/hdr_histogram/src/hdr_thread.c
)
