#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <util/generic/map.h>

using namespace NKikimr;
using namespace NFuzzHelpers;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    Ydb::Table::DescribeTableResult out;
    NKikimrSchemeOp::TPathDescription in;
    TMap<ui64, ui64> nodeMap;

    // Populate from input if available
    if (input.has_path_description()) {
        in = input.path_description();
    }

    // Create minimal node map
    if (input.type_id_hint() != 0) {
        nodeMap[input.type_id_hint()] = input.type_id_hint();
    }

    try {
        FillTableStats(out, in, false, nodeMap);
    } catch (...) {
    }
    (void)out;
}
