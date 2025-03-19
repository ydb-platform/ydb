#include "schemeshard_export_scheme_uploader.h"

#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

using namespace NActors;

namespace NKikimr::NSchemeShard {

class TSchemeUploaderFallback: public TActorBootstrapped<TSchemeUploaderFallback> {
public:
    explicit TSchemeUploaderFallback(TActorId schemeShard, ui64 exportId, ui32 itemIdx)
        : SchemeShard(schemeShard)
        , ExportId(exportId)
        , ItemIdx(itemIdx)
    {
    }

    void Bootstrap() {
        Send(SchemeShard, new TEvPrivate::TEvExportSchemeUploadResult(ExportId, ItemIdx, false,
            "Exports to S3 are disabled"
        ));
        PassAway();
    }

private:
    TActorId SchemeShard;
    ui64 ExportId;
    ui32 ItemIdx;
};


IActor* CreateSchemeUploader(TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions
) {
    Y_UNUSED(sourcePathId, settings, databaseRoot, metadata, enablePermissions);
    return new TSchemeUploaderFallback(schemeShard, exportId, itemIdx);
}

} // NKikimr::NSchemeShard
