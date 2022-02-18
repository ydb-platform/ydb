add_library(cpp-actors-wilson INTERFACE)
target_link_libraries(cpp-actors-wilson INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-string_utils-base64
)
