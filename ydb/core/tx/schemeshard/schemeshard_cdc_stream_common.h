#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

namespace NKikimr {

struct TPathId;

namespace NSchemeShard {

struct TOperationContext;
struct TTxState;

} // namespace NSchemeShard

} // namespace NKikimr

namespace NKikimrTxDataShard {

class TCreateCdcStreamNotice;

} // namespace NKikimrTxDataShard

namespace NKikimr::NSchemeShard::NCdcStreamAtTable {

void FillNotice(const TPathId& pathId, TOperationContext& context, NKikimrTxDataShard::TCreateCdcStreamNotice& notice);

void CheckWorkingDirOnPropose(const TPath::TChecker& checks, bool isTableIndex);
void CheckSrcDirOnPropose(
    const TPath::TChecker& checks,
    bool isInsideTableIndexPath,
    TTxId op = InvalidTxId);

} // namespace NKikimr::NSchemeShard::NCdcStreamAtTable

namespace NKikimr::NSchemeShard::NCdcStreamState {

// Note: Sync functions (SyncIndexEntityVersion, SyncChildIndexes, HelpSyncSiblingVersions)
// have been removed. Version coordination is now done upfront via CoordinatedSchemaVersion
// proto field, which is calculated at the Propose phase and used by all SubOps.

} // namespace NKikimr::NSchemeShard::NCdcStreamState
