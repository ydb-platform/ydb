#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/tx/datashard/import_common.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/tx/datashard/import_s3.h>

namespace NKikimr::NColumnShard::NBackup {
    
TConclusion<std::unique_ptr<NActors::IActor>> CreateAsyncJobImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo);

std::unique_ptr<NActors::IActor> CreateImportDownloaderImport(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo);

}