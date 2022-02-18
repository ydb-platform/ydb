add_library(library-cpp-bucket_quoter)
target_link_libraries(library-cpp-bucket_quoter PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-bucket_quoter PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/bucket_quoter/bucket_quoter.cpp
)
