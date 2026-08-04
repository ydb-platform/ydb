// Fuzzer for NACLib::TSecurityObject::MergeWithParent — ACL inheritance merge.
// MergeWithParent combines a child's ACL with a parent's inherited ACL entries,
// filtering by InheritContainer/InheritObject flags and access type ordering.
// This runs on every schema tree traversal when computing effective permissions.
// We use two proto-encoded TSecurityObjects split at a fuzz-controlled offset.
#include <ydb/library/aclib/aclib.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 4) return 0;
    // First 2 bytes select the split point between parent and child protos
    uint32_t raw_split = (uint32_t)data[0] | ((uint32_t)data[1] << 8);
    size_t remaining = size - 2;
    size_t split = 2 + (remaining > 0 ? raw_split % remaining : 0);

    TString parent_bytes(reinterpret_cast<const char*>(data + 2), split - 2);
    TString child_bytes(reinterpret_cast<const char*>(data + split), size - split);

    NACLibProto::TSecurityObject parent_proto;
    if (!parent_proto.ParseFromString(parent_bytes)) return 0;

    NACLibProto::TSecurityObject child_proto;
    if (!child_proto.ParseFromString(child_bytes)) return 0;

    try {
        NACLib::TSecurityObject child(child_proto, /*isContainer=*/false);
        auto result = child.MergeWithParent(parent_proto);
        (void)result.ToString();
    } catch (...) {}
    return 0;
}
