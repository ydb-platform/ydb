#pragma once

/* required to prevent double definition of struct stat on WIN64 */
#if defined _VCRT_BUILD || defined _CORECRT_BUILD
#include <sys/stat.h>
#else

#define _CRT_NO_TIME_T
#include <sys/stat.h>
#undef _CRT_NO_TIME_T

/* copied from sys/stat.h
   need'em here due to disabled _CRT_NO_TIME_T */
#ifdef _USE_32BIT_TIME_T
    typedef __time32_t time_t;
#else
    typedef __time64_t time_t;
#endif

#endif

#undef fstat
#undef stat
