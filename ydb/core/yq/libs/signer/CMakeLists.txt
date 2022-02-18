add_library(yq-libs-signer)
target_link_libraries(yq-libs-signer PUBLIC
  contrib-libs-cxxsupp
  yutil
  yq-libs-hmac
)
target_sources(yq-libs-signer PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/signer/signer.cpp
)
