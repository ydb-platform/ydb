add_library(llvm12-lib-Demangle)
target_include_directories(llvm12-lib-Demangle PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Demangle
)
target_link_libraries(llvm12-lib-Demangle PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
)
target_sources(llvm12-lib-Demangle PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Demangle/Demangle.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Demangle/ItaniumDemangle.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Demangle/MicrosoftDemangle.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Demangle/MicrosoftDemangleNodes.cpp
)
