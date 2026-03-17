#pragma once

#ifdef __cplusplus
extern "C"
{
#endif

void* hook(const char* library_filename, const char* function_name, const void* substitution_address);

#ifdef __cplusplus
}
#endif

