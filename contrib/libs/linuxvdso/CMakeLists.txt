add_library(contrib-libs-linuxvdso)
target_link_libraries(contrib-libs-linuxvdso PUBLIC
  libs-linuxvdso-original
)
target_sources(contrib-libs-linuxvdso PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/linuxvdso/interface.cpp
)
