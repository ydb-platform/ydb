#pragma once

#include "hook.h"
#include "libgreenify.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef enum
{
    FN_CONNECT,
    FN_READ,
    FN_WRITE,
    FN_PREAD,
    FN_PWRITE,
    FN_READV,
    FN_WRITEV,
    FN_RECV,
    FN_SEND,
    FN_RECVMSG,
    FN_SENDMSG,
    FN_RECVFROM,
    FN_SENDTO,
    FN_SELECT,
    FN_POLL,
} greenified_function_t;

void* greenify_patch_lib(const char* library_filename, greenified_function_t fn);

#ifdef __cplusplus
}
#endif

