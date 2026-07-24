#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

using namespace NKikimr;
using namespace NFuzzHelpers;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    if (!input.has_mini_kql_type() || !input.has_mini_kql_value()) {
        return;
    }
    if (!IsSimpleDataType(input.mini_kql_type())) {
        return;
    }

    Ydb::Value out;
    try {
        ConvertMiniKQLValueToYdbValue(input.mini_kql_type(), input.mini_kql_value(), out);
    } catch (...) {
    }
    (void)out;
}
