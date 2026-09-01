#include "object_framework.h"

#include <stdlib.h>
#include <string.h>

enum {
    ObjectFrameworkMaxObjects = 1024,
};

typedef struct TObjectSlot {
    TObjectHandle Handle;
    const TObjectType* Type;
    void* Instance;
    int Used;
} TObjectSlot;

static TObjectSlot Registry[ObjectFrameworkMaxObjects];
static uint64_t NextHandle = 1;

static TObjectSlot* FindSlot(TObjectHandle handle) {
    if (handle == 0) {
        return NULL;
    }
    for (size_t i = 0; i < ObjectFrameworkMaxObjects; ++i) {
        if (Registry[i].Used && Registry[i].Handle == handle) {
            return &Registry[i];
        }
    }
    return NULL;
}

static TObjectSlot* FindFreeSlot(void) {
    for (size_t i = 0; i < ObjectFrameworkMaxObjects; ++i) {
        if (!Registry[i].Used) {
            return &Registry[i];
        }
    }
    return NULL;
}

TObjectHandle ObjectFrameworkCreate(const TObjectType* type, const void* blob, size_t blobLen) {
    if (!type || type->InstanceSize == 0 || !type->Init) {
        return 0;
    }
    TObjectSlot* slot = FindFreeSlot();
    if (!slot) {
        return 0;
    }
    void* instance = malloc(type->InstanceSize);
    if (!instance) {
        return 0;
    }
    memset(instance, 0, type->InstanceSize);
    type->Init(instance, blob, blobLen);

    const TObjectHandle handle = NextHandle++;
    if (NextHandle == 0) {
        NextHandle = 1;
    }
    slot->Handle = handle;
    slot->Type = type;
    slot->Instance = instance;
    slot->Used = 1;
    return handle;
}

void* ObjectFrameworkGet(TObjectHandle handle, const TObjectType* expectedType) {
    TObjectSlot* slot = FindSlot(handle);
    if (!slot) {
        return NULL;
    }
    if (expectedType && slot->Type != expectedType) {
        return NULL;
    }
    return slot->Instance;
}

void ObjectFrameworkDestroy(TObjectHandle handle) {
    TObjectSlot* slot = FindSlot(handle);
    if (!slot) {
        return;
    }
    if (slot->Type && slot->Type->Destroy && slot->Instance) {
        slot->Type->Destroy(slot->Instance);
    }
    free(slot->Instance);
    memset(slot, 0, sizeof(*slot));
}
