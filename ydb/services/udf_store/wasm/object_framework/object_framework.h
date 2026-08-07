#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t TObjectHandle;

typedef struct TObjectType {
    const char* Name;
    size_t InstanceSize;
    void (*Init)(void* self, const void* blob, size_t blobLen);
    void (*Destroy)(void* self);
} TObjectType;

//! Allocates instance, calls Init, registers in the module-local registry.
//! Returns 0 on failure.
TObjectHandle ObjectFrameworkCreate(const TObjectType* type, const void* blob, size_t blobLen);

//! Looks up handle; returns nullptr if missing or type mismatch.
void* ObjectFrameworkGet(TObjectHandle handle, const TObjectType* expectedType);

//! Unregisters and destroys instance. No-op for unknown handle.
void ObjectFrameworkDestroy(TObjectHandle handle);

#ifdef __cplusplus
} // extern "C"
#endif
