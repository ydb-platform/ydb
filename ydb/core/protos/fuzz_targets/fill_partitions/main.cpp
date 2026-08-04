#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <ydb/core/ydb_convert/table_settings.h>
#include <util/generic/list.h>

using namespace NKikimr;

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    NKikimrSchemeOp::TTableDescription out;
    Ydb::Table::CreateTableRequest in;

    // Populate from fuzzed bytes if available
    if (!input.parse_string().empty()) {
        (void)in.ParseFromString(input.parse_string());
    }

    // Exercise both uniform and explicit partitions
    if (!in.has_uniform_partitions() && !in.has_partition_at_keys()) {
        if (input.acl_mask() % 2) {
            in.set_uniform_partitions((input.type_id_hint() % 8) + 1);
        } else {
            auto* keys = in.mutable_partition_at_keys();
            const TString& blob = input.http_header().empty() ? input.parse_string() : input.http_header();
            auto addPoint = [&](TStringBuf chunk) {
                auto* point = keys->add_split_points();
                point->mutable_value()->set_text_value(TString(chunk));
            };
            if (!blob.empty()) {
                for (size_t i = 0; i + 4 <= blob.size() && keys->split_points_size() < 4; i += 4) {
                    addPoint(TStringBuf(blob.data() + i, 4));
                }
            } else {
                addPoint("a");
                addPoint("b");
            }
        }
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
