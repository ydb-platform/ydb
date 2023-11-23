add_executable(fastops_test fastops_ut.cpp)
target_link_libraries(fastops_test fastops)

install(TARGETS fastops_test DESTINATION bin)
