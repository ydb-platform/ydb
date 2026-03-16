#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

void* OMPIResourceGet(const char* name, const char** data, size_t* size);
void OMPIResourceFree(void* handle);

#ifdef __cplusplus
}
#endif
