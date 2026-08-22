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
