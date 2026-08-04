#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ymq/base/dlq_helpers.h>
#include <ydb/core/protos/config.pb.h>

using namespace NKikimr::NSQS;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    try {
        NKikimrConfig::TSqsConfig config; // Default config
        TRedrivePolicy policy = TRedrivePolicy::FromJson(input.parse_string(), config);
        (void)policy.IsValid();
        (void)policy.GetErrorText();
        (void)policy.ToJson();
    } catch (...) {
    }
}
