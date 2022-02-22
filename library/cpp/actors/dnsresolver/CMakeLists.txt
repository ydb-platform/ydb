add_library(cpp-actors-dnsresolver)
target_include_directories(cpp-actors-dnsresolver PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/c-ares/include
)
target_link_libraries(cpp-actors-dnsresolver PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
)
target_sources(cpp-actors-dnsresolver PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/dnsresolver/dnsresolver.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/dnsresolver/dnsresolver_caching.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/dnsresolver/dnsresolver_ondemand.cpp
)
