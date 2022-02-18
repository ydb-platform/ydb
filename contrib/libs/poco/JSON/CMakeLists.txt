add_library(libs-poco-JSON)
target_include_directories(libs-poco-JSON PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/include
)
target_include_directories(libs-poco-JSON PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/include
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/Foundation/include
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src
)
target_link_libraries(libs-poco-JSON PUBLIC
  contrib-libs-cxxsupp
  libs-poco-Foundation
)
target_sources(libs-poco-JSON PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/Array.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/Handler.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/JSONException.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/Object.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/ParseHandler.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/Parser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/ParserImpl.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/PrintHandler.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/Query.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/Stringifier.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/Template.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/TemplateCache.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/poco/JSON/src/pdjson.c
)
