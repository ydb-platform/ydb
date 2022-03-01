
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(llvm12-lib-ProfileData)
target_include_directories(llvm12-lib-ProfileData PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData
)
target_link_libraries(llvm12-lib-ProfileData PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-Demangle
  llvm12-lib-IR
  llvm12-lib-Support
)
target_sources(llvm12-lib-ProfileData PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/GCOV.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/InstrProf.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/InstrProfReader.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/InstrProfWriter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/ProfileSummaryBuilder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/SampleProf.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/SampleProfReader.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/ProfileData/SampleProfWriter.cpp
)
