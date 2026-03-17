/*
 * Based on Anthony Shoumikhin's article: http://www.codeproject.com/Articles/70302/Redirecting-functions-in-shared-ELF-libraries
 */
#pragma once

#define LIBRARY_ADDRESS_BY_HANDLE(x) ((NULL == x) ? NULL : (void *)*(size_t const *)(x))  //undocumented hack to get shared library's address in memory by its handle

#ifdef __cplusplus
extern "C"
{
#endif

void *elf_hook(char const *library_filename, void const *library_address, char const *function_name, void const *substitution_address);

#ifdef __cplusplus
}
#endif
