#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ymq/base/utils.h>

using namespace NKikimr::NSQS;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    try {
        bool isPrivate = IsPrivateRequest(input.parse_string());
        (void)isPrivate;
    } catch (...) {
    }
}
