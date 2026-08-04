// Fuzzer for TDiffACL::ApplyDiff (apply a diff ACL serialized as protobuf to a security object).
// TSecurityObject::ApplyDiff is called on admin-supplied proto payloads that arrive
// over gRPC SchemeService / ModifyACL. The proto binary is attacker-controlled.
#include <ydb/library/aclib/aclib.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input(reinterpret_cast<const char*>(data), size);
    // Use ParseFromString directly: TDiffACL(string) calls Y_ABORT_UNLESS internally
    // which aborts on any non-proto bytes — that's a fuzzer design issue, not a real bug.
    // ApplyDiff takes NACLibProto::TDiffACL& so we can feed it directly.
    NACLibProto::TDiffACL proto;
    if (!proto.ParseFromString(input)) {
        return 0;
    }
    try {
        NACLib::TSecurityObject obj("owner@builtin", /*isContainer=*/true);
        obj.ApplyDiff(proto);
    } catch (...) {}
    return 0;
}
