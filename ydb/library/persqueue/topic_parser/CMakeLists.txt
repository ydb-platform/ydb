add_library(library-persqueue-topic_parser)
target_link_libraries(library-persqueue-topic_parser PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  library-persqueue-topic_parser_public
  api-protos
)
target_sources(library-persqueue-topic_parser PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/persqueue/topic_parser/topic_parser.cpp
)
