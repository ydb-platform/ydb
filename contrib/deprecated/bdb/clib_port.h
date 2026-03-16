#ifndef clib_port_h_sdf75s67df5
#define clib_port_h_sdf75s67df5

#include <util/system/platform.h>

#if defined(_unix_)
    #include "clib_port_unix.h"
#elif defined(_win_)
    #include "clib_port_win.h"
#else
    #error todo
#endif

#endif
