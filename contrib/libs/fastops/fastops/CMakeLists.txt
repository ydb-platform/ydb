add_subdirectory(avx)
add_subdirectory(avx2)
add_subdirectory(plain)

add_library(fastops fastops.cpp core/avx_id.cpp)
target_link_libraries(fastops avx avx2 plain)

install(TARGETS fastops DESTINATION lib)
install(FILES fastops.h DESTINATION include)
install(FILES avx/ops_avx.h DESTINATION include/avx)
install(FILES avx2/ops_avx2.h DESTINATION include/avx2)
install(FILES core/FastIntrinsics.h core/SIMDFunctions.h core/avx_id.h DESTINATION include/core)
install(FILES plain/ops_plain.h DESTINATION include/plain)

add_subdirectory(ut)
