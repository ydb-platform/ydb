#include "config-linux.h"

#define ssize_t int
#undef HAVE_ALLOCA_H
#undef HAVE_DIRENT_H
#undef HAVE_DLFCN_H
#undef HAVE_EXECINFO_H
#undef HAVE_FTW_H
#undef HAVE_GETRLIMIT
#undef HAVE_MODE_T
#undef HAVE_MKSTEMP
#undef HAVE_STRINGS_H
#undef HAVE_SYS_PARAM_H
#undef HAVE_SYS_RESOURCE_H
#undef HAVE_SYS_TIME_H
#undef HAVE_SYS_XATTR_H
#undef HAVE_UINT
#undef HAVE_UNISTD_H
#undef HAVE_USHORT
#undef NCCONFIGURE_H
#undef USE_MMAP

#include "ncconfigure.h"
