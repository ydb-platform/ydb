#pragma once

#include "manifest.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NColumnShard::NBackup {

// Uploads data_NN.manifest.json (and its checksum) for the shard of the backup task.
// Replies with TEvPrivate::TEvBackupManifestWriteResult.
std::unique_ptr<NActors::IActor> CreateDataManifestWriter(
    const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask, const TDataManifest& manifest);

// Downloads data_NN.manifest.json for the shard of the restore task.
// Replies with TEvPrivate::TEvBackupManifestReadResult, FilesCount == 0 if there is no manifest.
std::unique_ptr<NActors::IActor> CreateDataManifestReader(
    const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TRestoreTask& restoreTask);

}   // namespace NKikimr::NColumnShard::NBackup
