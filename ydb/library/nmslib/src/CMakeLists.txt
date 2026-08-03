#
# Non-metric Space Library
#
# Authors: Bilegsaikhan Naidan, Leonid Boytsov.
#
# This code is released under the
# Apache License Version 2.0 http://www.apache.org/licenses/.
#
#

file(GLOB PROJ_HDR_FILES ${PROJECT_SOURCE_DIR}/include/*.h ${PROJECT_SOURCE_DIR}/include/method/*.h ${PROJECT_SOURCE_DIR}/include/space/*.h ${PROJECT_SOURCE_DIR}/include/factory/*.h ${PROJ_HDR_FILES}/include/factory/*/*.h)
file(GLOB OTH_HDR_FILES)
set(HDR_FILES ${PROJ_HDR_FILES} ${OTH_HDR_FILES})
file(GLOB SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cc ${PROJECT_SOURCE_DIR}/src/space/*.cc ${PROJECT_SOURCE_DIR}/src/method/*.cc)

if (NOT WITH_EXTRAS)
  # Extra spaces
  list(REMOVE_ITEM SRC_FILES ${PROJECT_SOURCE_DIR}/src/space/space_sqfd.cc)
endif()

include_directories(${PROJECT_SOURCE_DIR}/include)
message(STATUS "Header files: ${HDR_FILES}")
message(STATUS "Source files: ${SRC_FILES}")

link_directories(${Boost_LIBRARY_DIRS})

find_package (Threads)
if (Threads_FOUND)
    message (STATUS "Found Threads.")
else (Threads_FOUND)
    message (STATUS "Could not locate Threads.")
endif (Threads_FOUND)

add_library (NonMetricSpaceLib ${SRC_FILES} ${HDR_FILES})

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set (LIBRARY_OUTPUT_PATH "${PROJECT_SOURCE_DIR}/release/")
    set (EXECUTABLE_OUTPUT_PATH "${PROJECT_SOURCE_DIR}/release/")
else ()
    set (LIBRARY_OUTPUT_PATH "${PROJECT_SOURCE_DIR}/debug/")
    set (EXECUTABLE_OUTPUT_PATH "${PROJECT_SOURCE_DIR}/debug/")
endif ()

string(LENGTH ${PROJECT_SOURCE_DIR} PREFIX_LEN) 
MATH(EXPR PREFIX_LEN "${PREFIX_LEN}+1")
foreach(F ${PROJ_HDR_FILES}) 
  GET_FILENAME_COMPONENT(FP ${F} PATH)
  string(SUBSTRING ${FP} ${PREFIX_LEN} -1 FS)
  #message(${PREFIX_LEN} ":" ${FS})
  install(FILES "${F}" DESTINATION ${FS})
endforeach(F)

install(TARGETS NonMetricSpaceLib   
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  COMPONENT library
)

