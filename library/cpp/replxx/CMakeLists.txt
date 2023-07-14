cmake_minimum_required(VERSION 3.5)
project(
	replxx
	# HOMEPAGE_URL "https://github.com/AmokHuginnsson/replxx"
	# DESCRIPTION "replxx - Read Evaluate Print Loop library"
	VERSION 0.0.4
	LANGUAGES CXX C
)

if (NOT DEFINED CMAKE_CXX_STANDARD)
	set(CMAKE_CXX_STANDARD 11)
endif()

if (NOT DEFINED CMAKE_C_STANDARD)
	set(CMAKE_C_STANDARD 99)
endif()

set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

include(CMakePackageConfigHelpers)
include(CMakeDependentOption)
include(GenerateExportHeader)
include(GNUInstallDirs)

find_package(Threads)

cmake_dependent_option(
	REPLXX_BUILD_EXAMPLES
	"Build the examples" ON
	"CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR" OFF
)
cmake_dependent_option(
	REPLXX_BUILD_PACKAGE
	"Generate package target" ON
	"CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR" OFF
)

if (NOT CMAKE_BUILD_TYPE)
	message(AUTHOR_WARNING "CMAKE_BUILD_TYPE not set. Defaulting to Release")
	set(CMAKE_BUILD_TYPE Release)
endif()

# INFO
set(REPLXX_URL_INFO_ABOUT "https://github.com/AmokHuginnsson/replxx")
set(REPLXX_DISPLAY_NAME "replxx")
set(REPLXX_CONTACT "amok@codestation.org")

set(is-clang $<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>)
set(is-msvc $<CXX_COMPILER_ID:MSVC>)
set(is-gnu $<CXX_COMPILER_ID:GNU>)
set(compiler-id-clang-or-gnu $<OR:${is-clang},${is-gnu}>)

set(coverage-config $<AND:$<CONFIG:Coverage>,$<OR:${is-gnu},${is-clang}>>)

set(replxx-source-patterns "src/*.cpp" "src/*.cxx")

if (CMAKE_VERSION VERSION_GREATER 3.11)
	list(INSERT replxx-source-patterns 0 CONFIGURE_DEPENDS)
endif()

file(GLOB replxx-sources ${replxx-source-patterns})

add_library(replxx ${replxx-sources})
add_library(replxx::replxx ALIAS replxx)

target_include_directories(
	replxx
	PUBLIC
		$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
		$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
	PRIVATE
		$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
)
target_compile_definitions(
	replxx
	PUBLIC
		$<$<NOT:$<BOOL:${BUILD_SHARED_LIBS}>>:REPLXX_STATIC>
		$<$<BOOL:${BUILD_SHARED_LIBS}>:REPLXX_BUILDING_DLL>
	PRIVATE
		$<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS=1 /ignore:4503>
)
target_compile_options(
	replxx
	PRIVATE
		$<$<AND:$<CONFIG:RelWithDebInfo>,${compiler-id-clang-or-gnu}>:-fomit-frame-pointer>
		$<$<AND:$<CONFIG:MinSizeRel>,${compiler-id-clang-or-gnu}>:-Os>
		$<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:GNU>>:-g -ggdb -g3 -ggdb3>
		$<${coverage-config}:-O0 --coverage>
		$<${coverage-config}:-fno-inline -fno-default-inline>
		$<${coverage-config}:-fno-inline-small-functions>
		$<${compiler-id-clang-or-gnu}:-Wall -Wextra>
		$<$<CXX_COMPILER_ID:GNU>:-Wno-unknown-pragmas>
)
if (NOT CMAKE_VERSION VERSION_LESS 3.13)
	target_link_options(
		replxx
		PRIVATE
			$<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:GNU>>:-g -ggdb -g3 -ggdb3>
			$<${coverage-config}:--coverage>
			$<${is-msvc}:/ignore:4099>
	)
else()
# "safest" way prior to 3.13
	target_link_libraries(
		replxx
		PRIVATE
			$<${coverage-config}:--coverage>
			$<${is-msvc}:/ignore:4099>
	)
endif()
target_link_libraries(replxx PUBLIC Threads::Threads contrib-libs-linux-headers contrib-libs-cxxsupp)
set_target_properties(replxx PROPERTIES VERSION ${PROJECT_VERSION})

set_property(TARGET replxx PROPERTY DEBUG_POSTFIX -d)
set_property(TARGET replxx PROPERTY RELWITHDEBINFO_POSTFIX -rd)
set_property(TARGET replxx PROPERTY MINSIZEREL_POSTFIX)
if ( NOT BUILD_SHARED_LIBS AND MSVC )
	set_property(TARGET replxx PROPERTY OUTPUT_NAME replxx-static)
endif()

generate_export_header(replxx)

configure_package_config_file(
	"${PROJECT_SOURCE_DIR}/replxx-config.cmake.in"
	"${PROJECT_BINARY_DIR}/replxx-config.cmake"
	INSTALL_DESTINATION ${CMAKE_INSTALL_DATADIR}/cmake/replxx
	NO_CHECK_REQUIRED_COMPONENTS_MACRO
	NO_SET_AND_CHECK_MACRO
)

write_basic_package_version_file(
	"${PROJECT_BINARY_DIR}/replxx-config-version.cmake"
	COMPATIBILITY AnyNewerVersion
)

# install(
# 	TARGETS replxx EXPORT replxx-targets
# 	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
# 	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
# 	ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
# )

# install(
# 	EXPORT replxx-targets
# 	NAMESPACE replxx::
# 	DESTINATION ${CMAKE_INSTALL_DATADIR}/cmake/replxx
# )

# install(
# 	FILES
# 		"${PROJECT_BINARY_DIR}/replxx-config-version.cmake"
# 		"${PROJECT_BINARY_DIR}/replxx-config.cmake"
# 	DESTINATION
# 		${CMAKE_INSTALL_DATADIR}/cmake/replxx
# )

# # headers
# install(
# 	FILES
# 		include/replxx.hxx
# 		include/replxx.h
# 	DESTINATION
# 		${CMAKE_INSTALL_INCLUDEDIR}
# )

# if (REPLXX_BUILD_EXAMPLES)
# 	add_executable(replxx-example-cxx-api "")
# 	add_executable(replxx-example-c-api "")

# 	target_sources(
# 		replxx-example-cxx-api
# 		PRIVATE
# 			examples/cxx-api.cxx
# 			examples/util.c
# 	)
# 	target_sources(
# 		replxx-example-c-api
# 		PRIVATE
# 			examples/c-api.c
# 			examples/util.c
# 	)
# 	target_compile_definitions(replxx-example-cxx-api PRIVATE REPLXX_STATIC $<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS=1>)
# 	target_compile_definitions(replxx-example-c-api PRIVATE REPLXX_STATIC $<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS=1>)
# 	target_link_options(replxx-example-cxx-api PRIVATE $<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:GNU>>:-g -ggdb -g3 -ggdb3>)
# 	target_link_options(replxx-example-c-api PRIVATE $<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:GNU>>:-g -ggdb -g3 -ggdb3>)
# 	target_link_libraries(replxx-example-cxx-api PRIVATE replxx::replxx)
# 	target_link_libraries(
# 		replxx-example-c-api
# 		PRIVATE
# 		replxx::replxx
# 		$<${compiler-id-clang-or-gnu}:stdc++>
# 		$<${is-clang}:m>
# 		$<${coverage-config}:--coverage>
# 	)
# 	target_link_libraries(
# 		replxx-example-cxx-api
# 		PRIVATE
# 		$<${coverage-config}:--coverage>
# 	)
# endif()

if (NOT REPLXX_BUILD_PACKAGE)
	return()
endif()

include(CPack)

set(CPACK_SET_DESTDIR ON)

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Readline and libedit replacement library")
set(CPACK_PACKAGE_HOMEPAGE_URL "${REPLXX_URL_INFO_ABOUT}")
set(CPACK_PACKAGE_VENDOR	"codestation.org")
set(CPACK_PACKAGE_CONTACT "amok@codestation.org")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")

set(CPACK_RESOURCE_FILE_LICENSE "${PROJECT_SOURCE_DIR}/LICENSE")

set(CPACK_STRIP_FILES "ON")

set(CPACK_DEBIAN_PACKAGE_SECTION "utilities")

