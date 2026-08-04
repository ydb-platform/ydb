#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

using namespace NKikimrTxDataShard;

static TString MakeTokenBytes(const NKikimrFuzz::TFuzzInput& input) {
    if (input.has_ydb_value()) {
        return input.ydb_value().SerializeAsString();
    }
    if (input.has_result_set()) {
        return input.result_set().SerializeAsString();
    }
    return input.parse_string();
}

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    try {
        const TString data = MakeTokenBytes(input);
        TReadContinuationToken token;
        const bool ok = token.ParseFromString(data);
        if (ok) {
            Y_UNUSED(token.GetFirstUnprocessedQuery());
        }
    } catch (...) {
    }
}
