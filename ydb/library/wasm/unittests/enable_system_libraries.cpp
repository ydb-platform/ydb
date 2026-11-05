#include <ydb/library/wasm/engine/system_libraries.h>

#include <library/cpp/resource/resource.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

bool EnableSystemLibraries()
{
    constexpr auto resource = "libemscripten-system-libraries-dll.so";
    return NResource::Has(resource) && !NResource::Find(resource).empty();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NWasm
