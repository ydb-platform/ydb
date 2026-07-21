#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

using namespace NKikimr;
using namespace NFuzzHelpers;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    Ydb::Table::DescribeTableResult out;
    NKikimrSchemeOp::TTableDescription in;

    // Populate from input if available
    if (input.path_description().has_table()) {
        in = input.path_description().table();
    }

    try {
        FillStorageSettings(out, in);
    } catch (...) {
    }
    (void)out;
}
