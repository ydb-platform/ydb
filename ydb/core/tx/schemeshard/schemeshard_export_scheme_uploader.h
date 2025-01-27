#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/actors/core/actor.h>

namespace Ydb::Export {
    class ExportToS3Settings;
}

namespace NKikimr::NSchemeShard {

NActors::IActor* CreateSchemeUploader(NActors::TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata
);

}
