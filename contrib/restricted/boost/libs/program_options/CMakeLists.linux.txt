
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(boost-libs-program_options)
target_compile_options(boost-libs-program_options PRIVATE
  -DBOOST_ATOMIC_STATIC_LINK=1
  -DBOOST_All_STATIC_LINK=1
  -DBOOST_CHRONO_STATIC_LINK=1
  -DBOOST_SYSTEM_STATIC_LINK=1
  -DBOOST_TIMER_STATIC_LINK=1
  -DBOOST_ALL_NO_LIB=1
  -DBOOST_ATOMIC_SOURCE
  -DBOOST_COROUTINES_SOURCE
  -DBOOST_DISABLE_ASSERTS
  -DBOOST_SPIRIT_USE_PHOENIX_V3=1
  -DBOOST_SYSTEM_NO_DEPRECATED
  -DBOOST_THREAD_BUILD_LIB=1
  -DBOOST_THREAD_DONT_USE_CHRONO=1
  -DBOOST_THREAD_NO_LIB=1
  -DBOOST_THREAD_USE_LIB=1
  -DDATE_TIME_INLINE
  -DBOOST_THREAD_DONT_USE_CHRONO
  -DBOOST_THREAD_POSIX
)
target_include_directories(boost-libs-program_options PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/boost/predef
)
target_link_libraries(boost-libs-program_options PUBLIC
  contrib-libs-cxxsupp
  contrib-restricted-boost
)
target_sources(boost-libs-program_options PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/cmdline.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/config_file.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/convert.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/options_description.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/parsers.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/positional_options.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/split.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/utf8_codecvt_facet.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/value_semantic.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/variables_map.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/libs/program_options/src/winmain.cpp
)
