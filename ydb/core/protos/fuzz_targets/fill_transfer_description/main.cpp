#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/ydb_convert/replication_description.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

Y_DECLARE_OUT_SPEC(, NKikimrReplication::TReplicationConfig::TargetCase, stream, value) {
    stream << static_cast<int>(value);
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    // Variant 1: from TEvDescribeReplicationResult
    if (input.has_ev_describe_replication_result()) {
        Ydb::Replication::DescribeTransferResult result;
        try {
            NKikimr::FillTransferDescription(
                result,
                input.ev_describe_replication_result()
            );
        } catch (...) {
        }
    }

    // Variant 2: from TReplicationDescription and TDirEntry
    if (input.has_replication_description() && input.has_dir_entry()) {
        Ydb::Replication::DescribeTransferResult result;
        Ydb::StatusIds::StatusCode status;
        TString error;
        try {
            NKikimr::FillTransferDescription(
                result,
                input.replication_description(),
                input.dir_entry(),
                status,
                error
            );
        } catch (...) {
        }
    }

    return 0;
}
