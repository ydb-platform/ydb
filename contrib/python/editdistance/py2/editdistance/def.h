#ifndef ___DEF__H__
#define ___DEF__H__

#if defined(_WIN32) && _MSC_VER <= 1600
typedef signed __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef signed __int64 int64_t;
typedef unsigned __int64 uint64_t;
#else
#include <stdint.h>
#endif

#endif
