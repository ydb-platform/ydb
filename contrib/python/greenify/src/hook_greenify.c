#include "hook_greenify.h"

#define _GREENIFY_PATCH_EXPAND(LIBPATH, FN) (hook((LIBPATH), #FN, (void*)green_##FN))

void* greenify_patch_lib(const char* library_filename, greenified_function_t fn)
{
    switch (fn)
    {
        case FN_CONNECT:
            return _GREENIFY_PATCH_EXPAND(library_filename, connect);
        case FN_READ:
            return _GREENIFY_PATCH_EXPAND(library_filename, read);
        case FN_WRITE:
            return _GREENIFY_PATCH_EXPAND(library_filename, write);
        case FN_PREAD:
            return _GREENIFY_PATCH_EXPAND(library_filename, pread);
        case FN_PWRITE:
            return _GREENIFY_PATCH_EXPAND(library_filename, pwrite);
        case FN_READV:
            return _GREENIFY_PATCH_EXPAND(library_filename, readv);
        case FN_WRITEV:
            return _GREENIFY_PATCH_EXPAND(library_filename, writev);
        case FN_RECV:
            return _GREENIFY_PATCH_EXPAND(library_filename, recv);
        case FN_SEND:
            return _GREENIFY_PATCH_EXPAND(library_filename, send);
        case FN_RECVMSG:
            return _GREENIFY_PATCH_EXPAND(library_filename, recvmsg);
        case FN_SENDMSG:
            return _GREENIFY_PATCH_EXPAND(library_filename, sendmsg);
        case FN_RECVFROM:
            return _GREENIFY_PATCH_EXPAND(library_filename, recvfrom);
        case FN_SENDTO:
            return _GREENIFY_PATCH_EXPAND(library_filename, sendto);
        case FN_SELECT:
            return _GREENIFY_PATCH_EXPAND(library_filename, select);
#ifndef NO_POLL
        case FN_POLL:
            return _GREENIFY_PATCH_EXPAND(library_filename, poll);
#endif
        default:
            return NULL;
    }
}

#undef _GREENIFY_PATCH_EXPAND
