#include "hook.h"
#include <stddef.h>

#ifdef __ELF__

#include "elf_hook.h"
#include <dlfcn.h>

#ifdef __cplusplus
extern "C"
{
#endif

void* hook(const char* library_filename, const char* function_name, const void* substitution_address)
{
    void *handle, *address;
    if (NULL == library_filename)
        return NULL;

    handle = dlopen(library_filename, RTLD_LAZY);
    if (NULL == handle)
        return NULL;

    address = LIBRARY_ADDRESS_BY_HANDLE(handle);
    return elf_hook(library_filename, address, function_name, substitution_address);
}

#ifdef __cplusplus
}
#endif

#else
/*
 * Only ELF hook is supported now.
 */

#ifdef __cplusplus
extern "C"
{
#endif

void* hook(const char* library_filename, const char* function_name, const void* substitution_address)
{
    return NULL;
}

#ifdef __cplusplus
}
#endif


#endif
