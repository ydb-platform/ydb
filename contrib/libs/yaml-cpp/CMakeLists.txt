add_library(contrib-libs-yaml-cpp)
target_include_directories(contrib-libs-yaml-cpp PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/include
)
target_include_directories(contrib-libs-yaml-cpp PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/include
)
target_link_libraries(contrib-libs-yaml-cpp PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-yaml-cpp PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/binary.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/convert.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/directives.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/emit.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/emitfromevents.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/emitter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/emitterstate.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/emitterutils.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/exceptions.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/exp.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/memory.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/nodebuilder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/node.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/node_data.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/nodeevents.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/null.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/ostream_wrapper.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/parse.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/parser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/regex_yaml.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/scanner.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/scanscalar.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/scantag.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/scantoken.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/simplekey.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/singledocparser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/stream.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/yaml-cpp/src/tag.cpp
)
