#include "resource.h"

#include <library/cpp/resource/resource.h>

#include <memory>

extern "C" void* OMPIResourceGet(const char* name, const char** data, size_t* size) {
    auto out = std::make_unique<TString>();
    if (!NResource::FindExact(TString("openmpi/") + name, out.get()) &&
        !NResource::FindExact(TString("openmpi/") + name + ".txt", out.get()))
        return nullptr;
    *data = out->data();
    *size = out->size();
    return out.release();
}

extern "C" void OMPIResourceFree(void* handle) {
    delete static_cast<TString*>(handle);
}
