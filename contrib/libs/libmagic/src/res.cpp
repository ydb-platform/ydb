#include <library/cpp/resource/resource.h>

extern "C" void _magic_read_res(const char* res, void** out, size_t* size) {
    TString s;
    if (NResource::FindExact(res, &s) && (*size = s.size()) && (*out = malloc(*size)))
        memcpy(*out, s.data(), *size);
}
