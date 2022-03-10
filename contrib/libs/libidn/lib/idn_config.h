#ifndef _IDN_CONFIG_H_
#define _IDN_CONFIG_H_

#ifdef _WIN32
    #include "win/config.h"
    #include "win/stdbool.h"
    #include "win/unistd.h"
#else
    #include "unix/config.h"
#endif

#endif//_IDN_CONFIG_H_
