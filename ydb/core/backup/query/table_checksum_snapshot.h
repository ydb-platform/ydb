#pragma once

#include <ydb/core/backup/common/table_checksum_snapshot.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/base/row_version.h>

namespace NKikimr::NBackup {

// A zero step discovers the exact version of the already completed transfer transaction
NActors::IActor* CreateTableChecksumSnapshotReader(NActors::TActorId replyTo, const TTableId& tableId,
    ui64 schemeShardId, TRowVersion snapshot, bool checkCopyCapture = false);

NActors::IActor* CreateTableChecksumSnapshotReleaser(const TTableId& tableId, ui64 schemeShardId, TRowVersion snapshot);

} // namespace NKikimr::NBackup
