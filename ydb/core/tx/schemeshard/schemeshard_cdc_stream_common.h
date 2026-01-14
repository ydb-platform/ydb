#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

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

namespace NKikimr::NSchemeShard::NTableIndexVersion {

// Syncs child index versions with the parent table's AlterVersion.
// This ensures all indexes are at least at the table's version before publishing.
// Parameters:
//   - path: the parent table path element
//   - table: the parent table info
//   - targetVersion: the version to sync indexes to
//   - operationId: for PublishToSchemeBoard
//   - context: operation context
//   - db: database for persistence
//   - skipPlannedToDrop: if true, skip indexes that are PlannedToDrop (used by drop index)
// Returns: vector of index PathIds that were published
TVector<TPathId> SyncChildIndexVersions(
    TPathElement::TPtr path,
    TTableInfo::TPtr table,
    ui64 targetVersion,
    TOperationId operationId,
    TOperationContext& context,
    NIceDb::TNiceDb& db,
    bool skipPlannedToDrop = false);

} // namespace NKikimr::NSchemeShard::NTableIndexVersion
