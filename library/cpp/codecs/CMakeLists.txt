add_library(library-cpp-codecs)
target_link_libraries(library-cpp-codecs PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-zstd
  library-cpp-bit_io
  library-cpp-blockcodecs
  cpp-codecs-greedy_dict
  library-cpp-comptable
  cpp-containers-comptrie
  cpp-deprecated-accessors
  library-cpp-packers
  cpp-string_utils-relaxed_escaper
)
target_sources(library-cpp-codecs PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/tls_cache.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/codecs.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/codecs_registry.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/comptable_codec.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/delta_codec.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/float_huffman.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/huffman_codec.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/pfor_codec.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/solar_codec.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/codecs/zstd_dict_codec.cpp
)
