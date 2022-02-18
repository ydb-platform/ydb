add_library(cpp-logger-global)
target_link_libraries(cpp-logger-global PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-logger
)
target_sources(cpp-logger-global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/logger/global/common.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/logger/global/global.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/logger/global/rty_formater.cpp
)
