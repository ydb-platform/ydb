add_library(library-cpp-sighandler)
target_link_libraries(library-cpp-sighandler PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-sighandler PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/sighandler/async_signals_handler.cpp
)
