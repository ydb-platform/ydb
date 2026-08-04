#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ymq/base/utils.h>

using namespace NKikimr::NSQS;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    if (input.parse_string().empty()) {
        return;
    }

    try {
        bool isPrivateRequest = input.is_container();
        TString accountName = ExtractAccountNameFromPath(input.parse_string(), isPrivateRequest);
        (void)accountName;
    } catch (...) {
    }
}
