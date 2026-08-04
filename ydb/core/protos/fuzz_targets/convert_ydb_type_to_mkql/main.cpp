#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

using namespace NKikimr;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    if (!input.has_ydb_type()) {
        return;
    }

    NKikimrMiniKQL::TType out;
    try {
        ConvertYdbTypeToMiniKQLType(input.ydb_type(), out);
    } catch (...) {
    }
    (void)out;
}
