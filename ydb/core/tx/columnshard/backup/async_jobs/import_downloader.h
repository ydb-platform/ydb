#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NDataShard {
class TTableInfo;
}

namespace NKikimr::NScheme {
class TTypeInfo;
}

namespace NKikimr::NColumnShard::NBackup {
    
TConclusion<std::unique_ptr<NActors::IActor>> CreateAsyncJobImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo);

std::unique_ptr<NActors::IActor> CreateImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo, const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema);

}