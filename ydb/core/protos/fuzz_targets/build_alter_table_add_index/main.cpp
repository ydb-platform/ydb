#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/protos/index_builder.pb.h>

using namespace NKikimr;
using namespace NFuzzHelpers;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    Ydb::Table::AlterTableRequest req;
    NKikimrIndexBuilder::TIndexBuildSettings settings;

    // Use input data
    if (!input.owner().empty()) {
        req.set_path(input.owner());
    } else {
        req.set_path("/test");
    }

    auto* index = req.add_add_indexes();
    if (!input.permission_name().empty()) {
        index->set_name(input.permission_name());
    } else {
        index->set_name("idx1");
    }
    index->add_index_columns("key");

    ui64 flags = 0;
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
    TString error;

    try {
        BuildAlterTableAddIndexRequest(&req, &settings, flags, status, error);
    } catch (...) {
    }
    (void)settings;
    (void)status;
    (void)error;
}
