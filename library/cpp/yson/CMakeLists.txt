add_library(library-cpp-yson)
target_link_libraries(library-cpp-yson PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yt-misc
  cpp-yt-yson
)
target_sources(library-cpp-yson PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/consumer.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/lexer.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/parser.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/token.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/tokenizer.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/varint.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/writer.cpp
)
