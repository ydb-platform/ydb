add_library(contrib-libs-nayuki_md5)
target_link_libraries(contrib-libs-nayuki_md5 PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(contrib-libs-nayuki_md5 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/nayuki_md5/md5-fast-x8664.S
)
