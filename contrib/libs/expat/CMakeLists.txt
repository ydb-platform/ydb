add_library(contrib-libs-expat)
target_compile_options(contrib-libs-expat PRIVATE
  -DHAVE_EXPAT_CONFIG_H
)
target_include_directories(contrib-libs-expat PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/expat
  ${CMAKE_SOURCE_DIR}/contrib/libs/expat/lib
)
target_sources(contrib-libs-expat PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/expat/lib/xmlparse.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/expat/lib/xmlrole.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/expat/lib/xmltok.c
)
