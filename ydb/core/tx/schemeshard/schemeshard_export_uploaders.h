#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimrSchemeOp {
    class TExportMetadata;
}

namespace NBackup {
    class TEncryptionIV;
}

namespace NKikimr::NSchemeShard {

template <typename TSettings>
NActors::IActor* CreateSchemeUploader(NActors::TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const TSettings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
);

template <typename TSettings>
NActors::IActor* CreateExportMetadataUploader(NActors::TActorId schemeShard, ui64 exportId,
    const TSettings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
);

}
