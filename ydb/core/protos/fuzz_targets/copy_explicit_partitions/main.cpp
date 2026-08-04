#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

using namespace NKikimr;
using namespace NFuzzHelpers;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    NKikimrSchemeOp::TTableDescription out;
    Ydb::Table::ExplicitPartitions in;

    // Create minimal explicit partitions
    if (input.has_tuple_value() && input.tuple_value().itemsSize() > 0) {
        for (const auto& item : input.tuple_value().items()) {
            auto* splitPoint = in.add_split_points();
            if (item.has_text_value()) {
                splitPoint->mutable_value()->set_text_value(item.text_value());
            }
        }
    }

    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
    TString error;

    try {
        CopyExplicitPartitions(out, in, status, error);
    } catch (...) {
    }
    (void)out;
    (void)status;
    (void)error;
}
