#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ymq/actor/proxy_actor.h>

using namespace NKikimr::NSQS;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    // Use input string for parsing
    TString token;
    if (!input.parse_string().empty()) {
        token = input.parse_string();
    } else if (!input.http_header().empty()) {
        token = input.http_header();
    } else {
        return; // No input to parse
    }

    try {
        auto [userName, folderId, userSID] = ParseCloudSecurityToken(token);
        (void)userName;
        (void)folderId;
        (void)userSID;
    } catch (...) {
    }
}
