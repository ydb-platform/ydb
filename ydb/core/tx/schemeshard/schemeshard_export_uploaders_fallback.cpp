#include "schemeshard_export_uploaders.h"

#include <ydb/core/backup/common/encryption.h>
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

class TExportMetadataUploaderFallback: public TActorBootstrapped<TExportMetadataUploaderFallback> {
public:
    TExportMetadataUploaderFallback(
        TActorId schemeShard,
        ui64 exportId
    )
        : SchemeShard(schemeShard)
        , ExportId(exportId)
    {
    }

    void Bootstrap() {
        Send(SchemeShard, new TEvPrivate::TEvExportUploadMetadataResult(ExportId, false, "Exports to S3 are disabled"));
        PassAway();
    }

private:
    TActorId SchemeShard;
    ui64 ExportId;
};


IActor* CreateSchemeUploader(TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, const TMaybe<NBackup::TEncryptionIV>& iv
) {
    Y_UNUSED(sourcePathId, settings, databaseRoot, metadata, enablePermissions, iv);
    return new TSchemeUploaderFallback(schemeShard, exportId, itemIdx);
}

NActors::IActor* CreateExportMetadataUploader(NActors::TActorId schemeShard, ui64 exportId,
    const Ydb::Export::ExportToS3Settings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
) {
    Y_UNUSED(settings, exportMetadata, enableChecksums);
    return new TExportMetadataUploaderFallback(schemeShard, exportId);
}

} // NKikimr::NSchemeShard
