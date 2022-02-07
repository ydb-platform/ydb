#pragma once

#ifdef WITH_VALGRIND
#define ENABLE_VALGRIND_REQUESTS 1
#else
#define ENABLE_VALGRIND_REQUESTS 0
#endif

#if ENABLE_VALGRIND_REQUESTS
#   include <util/system/valgrind.h>
#   include <valgrind/memcheck.h>
#   define REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(data, size) VALGRIND_CHECK_MEM_IS_DEFINED(data, size)
#   define REQUEST_VALGRIND_CHECK_MEM_IS_ADDRESSABLE(data, size) VALGRIND_CHECK_MEM_IS_ADDRESSABLE(data, size)
#   define REQUEST_VALGRIND_MAKE_MEM_NOACCESS(data, size) VALGRIND_MAKE_MEM_NOACCESS(data, size)
#   define REQUEST_VALGRIND_MALLOCLIKE_BLOCK(data, size, rz, iz) VALGRIND_MALLOCLIKE_BLOCK(data, size, rz, iz)
#   define REQUEST_VALGRIND_MAKE_MEM_UNDEFINED(data, size) VALGRIND_MAKE_MEM_UNDEFINED(data, size)
#   define REQUEST_VALGRIND_MAKE_MEM_DEFINED(data, size) VALGRIND_MAKE_MEM_DEFINED(data, size)
#   define REQUEST_VALGRIND_FREELIKE_BLOCK(data, rz) VALGRIND_FREELIKE_BLOCK(data, rz)
#else
#   define REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(data, size) while (false) {}
#   define REQUEST_VALGRIND_CHECK_MEM_IS_ADDRESSABLE(data, size) while (false) {}
#   define REQUEST_VALGRIND_MAKE_MEM_NOACCESS(data, size) while (false) {}
#   define REQUEST_VALGRIND_MALLOCLIKE_BLOCK(data, size, rz, iz) while (false) {}
#   define REQUEST_VALGRIND_MAKE_MEM_UNDEFINED(data, size) while (false) {}
#   define REQUEST_VALGRIND_MAKE_MEM_DEFINED(data, size) while (false) {}
#   define REQUEST_VALGRIND_FREELIKE_BLOCK(data, size) while (false) {}
#endif //ENABLE_VALGRIND_REQUESTS
