#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/table_settings.h>
#include <util/generic/list.h>

using namespace NKikimr;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    NKikimrSchemeOp::TTableDescription out;
    Ydb::Table::CreateTableRequest in;

    // Try to parse proto payload from fuzzed bytes
    if (!input.parse_string().empty()) {
        (void)in.ParseFromString(input.parse_string());
    }

    // If not provided, synthesize minimal partitioning settings from fuzz fields
    auto* partSettings = in.mutable_partitioning_settings();
    if (!partSettings->partitioning_by_size()) {
        auto flag = static_cast<Ydb::FeatureFlag::Status>(input.type_id_hint() % 3);
        partSettings->set_partitioning_by_size(flag);
        partSettings->set_partition_size_mb((input.acl_mask() % 32) + 1);
    }
    if (!partSettings->partitioning_by_load()) {
        auto flag = static_cast<Ydb::FeatureFlag::Status>((input.type_id_hint() / 3) % 3);
        partSettings->set_partitioning_by_load(flag);
    }
    if (!partSettings->min_partitions_count()) {
        partSettings->set_min_partitions_count((input.acl_mask() % 4) + 1);
    }
    if (!partSettings->max_partitions_count()) {
        partSettings->set_max_partitions_count(partSettings->min_partitions_count() + 3);
    }

    // Ensure partitions field exists to exercise FillPartitions path
    if (!in.has_uniform_partitions() && !in.has_partition_at_keys()) {
        in.set_uniform_partitions((input.type_id_hint() % 5) + 1);
    }

    Ydb::StatusIds::StatusCode code = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TString error;
    TList<TString> warnings;

    try {
        FillCreateTableSettingsDesc(out, in, code, error, warnings, /*tableProfileSet=*/false);
    } catch (...) {
    }
    (void)out;
    (void)code;
    (void)error;
}
