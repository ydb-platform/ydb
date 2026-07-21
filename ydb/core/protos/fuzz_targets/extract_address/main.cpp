#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/util/address_classifier.h>

using namespace NKikimr::NAddressClassifier;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    // Use input string for parsing
    TString peer;
    if (!input.parse_string().empty()) {
        peer = input.parse_string();
    } else if (!input.owner().empty()) {
        peer = input.owner();
    } else {
        return; // No input to parse
    }

    try {
        TString result = ExtractAddress(peer);
        (void)result;
    } catch (...) {
    }
}
