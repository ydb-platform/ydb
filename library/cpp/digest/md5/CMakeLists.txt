add_library(cpp-digest-md5)
target_link_libraries(cpp-digest-md5 PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-nayuki_md5
  cpp-string_utils-base64
)
target_sources(cpp-digest-md5 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/md5/md5.cpp
)
