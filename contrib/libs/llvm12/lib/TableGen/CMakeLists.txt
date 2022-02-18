add_library(llvm12-lib-TableGen)
target_include_directories(llvm12-lib-TableGen PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen
)
target_link_libraries(llvm12-lib-TableGen PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  llvm12-lib-Support
)
target_sources(llvm12-lib-TableGen PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/DetailedRecordsBackend.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/Error.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/JSONBackend.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/Main.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/Record.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/SetTheory.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/StringMatcher.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/TGLexer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/TGParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/TableGenBackend.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/TableGen/TableGenBackendSkeleton.cpp
)
