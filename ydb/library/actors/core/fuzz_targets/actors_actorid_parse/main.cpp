// Fuzzer for TActorId::Parse (binary actor address parser).
// ActorIds are serialized over interconnect and embedded in protobuf messages
// exchanged between nodes. A malformed actor ID in a received message can
// trigger parsing bugs. Parse reads raw bytes and assembles a 16-byte structure.
#include <ydb/library/actors/core/actorid.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0 || size > 4096) return 0;
    try {
        NActors::TActorId actorId;
        bool ok = actorId.Parse(reinterpret_cast<const char*>(data), static_cast<ui32>(size));
        if (ok) {
            // Exercise ToString to cover serialisation path too
            auto str = actorId.ToString();
            (void)str;
        }
    } catch (...) {}
    return 0;
}
