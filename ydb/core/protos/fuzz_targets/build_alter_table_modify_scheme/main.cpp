#include <ydb/core/protos/fuzz_targets/fuzz_actor_utils.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/ydb_convert/table_description.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::AlterTableRequest request;
    if (!request.ParseFromArray(data, size)) {
        return 0;
    }

    RunWithMockedActorSystem([request = std::move(request)]() mutable {
        NKikimrSchemeOp::TModifyScheme modifyScheme;
        NKikimr::TTableProfiles profiles;
        NKikimr::TPathId pathId(1, 1);
        Ydb::StatusIds::StatusCode status;
        TString error;

        try {
            NKikimr::BuildAlterTableModifyScheme(
                "/Root", &request, &modifyScheme, profiles, pathId, status, error);
        } catch (...) {
        }
    });

    return 0;
}
