#include <ydb/library/aclib/aclib.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);

    // TDiffACL constructor uses Y_VERIFY(ParseFromString(...))
    // which aborts on invalid protobuf. Pre-validate first.
    NACLibProto::TDiffACL proto;
    if (!proto.ParseFromString(input)) {
        return 0;
    }

    try {
        NACLib::TDiffACL diffACL(input);
        NACLib::TACL acl;
        acl.ApplyDiff(diffACL);
    } catch (...) {}

    try {
        NACLib::TSecurityObject secObj;
        NACLib::TDiffACL diffACL2(input);
        secObj.ApplyDiff(diffACL2);
    } catch (...) {}
    return 0;
}
