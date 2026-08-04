#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/table_description.h>

using namespace NKikimr;
using namespace NFuzzHelpers;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    if (!input.has_ydb_type()) {
        return;
    }

    NScheme::TTypeInfo outTypeInfo;
    TString outTypeMod;
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
    TString error;

    try {
        ExtractColumnTypeInfo(outTypeInfo, outTypeMod, input.ydb_type(), status, error);
    } catch (...) {
    }
    (void)outTypeInfo;
    (void)outTypeMod;
    (void)status;
    (void)error;
}
