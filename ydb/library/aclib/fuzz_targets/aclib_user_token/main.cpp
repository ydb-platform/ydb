// Fuzzer for NACLib::TUserToken protobuf deserialization.
// TUserToken is used throughout YDB authorization; a malformed token in a gRPC
// request header can reach this constructor. We parse the proto directly to
// avoid the Y_ABORT_UNLESS in the string constructor and fuzz the proto payload.
#include <ydb/library/aclib/aclib.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input(reinterpret_cast<const char*>(data), size);
    // Parse proto directly — the TUserToken(string) ctor does Y_ABORT_UNLESS which
    // would abort on non-proto bytes, masking real bugs deeper in the code.
    NACLibProto::TUserToken proto;
    if (!proto.ParseFromString(input)) {
        return 0;
    }
    try {
        NACLib::TUserToken token(proto);
        (void)token.GetUserSID();
        (void)token.GetGroupSIDs();
        (void)token.SerializeAsString();
    } catch (...) {}
    return 0;
}
