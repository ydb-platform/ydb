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

// Synchronize child index versions when parent table version is updated for continuous backup
void SyncIndexEntityVersion(
    const TPathId& indexPathId,
    ui64 targetVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db);

void SyncChildIndexes(
    TPathElement::TPtr parentPath,
    ui64 targetVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db);

} // namespace NKikimr::NSchemeShard::NCdcStreamState
