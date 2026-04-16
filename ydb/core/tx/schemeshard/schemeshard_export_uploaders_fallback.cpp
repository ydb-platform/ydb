#include "schemeshard_export_uploaders.h"

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <type_traits>

using namespace NActors;

namespace NKikimr::NSchemeShard {

template <typename TSettings>
TString GetExportDisabledMessage() {
    return TStringBuilder() << "Exports to " << (std::is_same_v<TSettings, Ydb::Export::ExportToFsSettings> ? "FS" : "S3") << " are disabled";
}

template <typename TSettings>
class TSchemeUploaderFallback: public TActorBootstrapped<TSchemeUploaderFallback<TSettings>> {
public:
    explicit TSchemeUploaderFallback(TActorId schemeShard, ui64 exportId, ui32 itemIdx)
        : SchemeShard(schemeShard)
        , ExportId(exportId)
        , ItemIdx(itemIdx)
    {
    }

    void Bootstrap() {
        this->Send(SchemeShard, new TEvPrivate::TEvExportSchemeUploadResult(ExportId, ItemIdx, false,
            GetExportDisabledMessage<TSettings>()
        ));
        this->PassAway();
    }

private:
    TActorId SchemeShard;
    ui64 ExportId;
    ui32 ItemIdx;
};

template <typename TSettings>
class TExportMetadataUploaderFallback: public TActorBootstrapped<TExportMetadataUploaderFallback<TSettings>> {
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
        this->Send(SchemeShard, new TEvPrivate::TEvExportUploadMetadataResult(ExportId, false,
            GetExportDisabledMessage<TSettings>()
        ));
        this->PassAway();
    }

private:
    TActorId SchemeShard;
    ui64 ExportId;
};

template <typename TSettings>
IActor* CreateSchemeUploader(TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const TSettings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
) {
    Y_UNUSED(sourcePathId, settings, databaseRoot, metadata, enablePermissions, enableChecksums, iv);
    return new TSchemeUploaderFallback<TSettings>(schemeShard, exportId, itemIdx);
}

template <typename TSettings>
NActors::IActor* CreateExportMetadataUploader(NActors::TActorId schemeShard, ui64 exportId,
    const TSettings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
) {
    Y_UNUSED(settings, exportMetadata, enableChecksums);
    return new TExportMetadataUploaderFallback<TSettings>(schemeShard, exportId);
}

template IActor* CreateSchemeUploader<Ydb::Export::ExportToS3Settings>(
    TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
);

template IActor* CreateSchemeUploader<Ydb::Export::ExportToFsSettings>(
    TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToFsSettings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
);

template NActors::IActor* CreateExportMetadataUploader<Ydb::Export::ExportToS3Settings>(
    NActors::TActorId schemeShard, ui64 exportId,
    const Ydb::Export::ExportToS3Settings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
);

template NActors::IActor* CreateExportMetadataUploader<Ydb::Export::ExportToFsSettings>(
    NActors::TActorId schemeShard, ui64 exportId,
    const Ydb::Export::ExportToFsSettings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
);

} // NKikimr::NSchemeShard
