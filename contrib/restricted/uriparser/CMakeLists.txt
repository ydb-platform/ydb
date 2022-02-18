add_library(contrib-restricted-uriparser)
target_compile_options(contrib-restricted-uriparser PRIVATE
  -DURI_LIBRARY_BUILD
  -DURI_VISIBILITY
)
target_include_directories(contrib-restricted-uriparser PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/include
)
target_include_directories(contrib-restricted-uriparser PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser
)
target_link_libraries(contrib-restricted-uriparser PUBLIC
  contrib-libs-libc_compat
)
target_sources(contrib-restricted-uriparser PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriCommon.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriCompare.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriEscape.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriFile.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriIp4.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriIp4Base.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriMemory.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriNormalize.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriNormalizeBase.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriParse.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriParseBase.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriQuery.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriRecompose.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriResolve.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/uriparser/src/UriShorten.c
)
