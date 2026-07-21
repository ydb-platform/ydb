// Fuzzer for NACLib::TSecurityObject::CheckGrantAccess — grant permission check.
// CheckGrantAccess verifies whether a user's current effective rights are a
// superset of the rights a DiffACL is trying to grant/revoke. This is a
// distinct, complex code path from ApplyDiff: it calls GetEffectiveAccessRights
// internally and then simulates the diff on a copy of the ACL.
// Both DiffACL and UserToken come from attacker-controlled protobuf inputs.
#include <ydb/library/aclib/aclib.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 2) return 0;
    uint32_t raw_split = (uint32_t)data[0] | ((uint32_t)data[1] << 8);
    size_t remaining = size - 2;
    size_t split = 2 + (remaining > 0 ? raw_split % remaining : 0);

    TString diff_bytes(reinterpret_cast<const char*>(data + 2), split - 2);
    TString user_bytes(reinterpret_cast<const char*>(data + split), size - split);

    NACLibProto::TDiffACL diff_proto;
    if (!diff_proto.ParseFromString(diff_bytes)) return 0;

    NACLibProto::TUserToken user_proto;
    if (!user_proto.ParseFromString(user_bytes)) return 0;

    try {
        NACLib::TSecurityObject obj("owner@builtin", /*isContainer=*/true);
        NACLib::TUserToken user(user_proto);
        bool granted = obj.CheckGrantAccess(diff_proto, user);
        (void)granted;
    } catch (...) {}
    return 0;
}
