add_library(cpp-mime-types)
target_link_libraries(cpp-mime-types PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
)
target_sources(cpp-mime-types PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/mime/types/mime.cpp
)
generate_enum_serilization(cpp-mime-types
  ${CMAKE_SOURCE_DIR}/library/cpp/mime/types/mime.h
  INCLUDE_HEADERS
  library/cpp/mime/types/mime.h
)
