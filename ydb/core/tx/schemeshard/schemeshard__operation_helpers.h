#pragma once

#include "schemeshard__operation_part.h"

namespace NKikimr::NIceDb {
class TNiceDb;
}

namespace NKikimr::NSchemeShard::NOperationHelpers {

TTabletId GetTabletId(const TSchemeShard& ss);
TString GetRootPath(const TSchemeShard& ss);

bool CheckApplyIf(
    TSchemeShard& ss,
    const TTxTransaction& transaction,
    TString& error,
    TPathElement::EPathType pathType);

bool IsStrictAclCheckEnabled();
bool SidExists(const TSchemeShard& ss, const TString& sid);

THashSet<TPathId> ListSubTree(TSchemeShard& ss, TPathId pathId, const TActorContext& ctx);
TPathElement::TPtr FindPathElement(const TSchemeShard& ss, TPathId pathId);

void PersistACL(TSchemeShard& ss, NIceDb::TNiceDb& db, const TPathElement::TPtr& path);
void PersistOwner(TSchemeShard& ss, NIceDb::TNiceDb& db, const TPathElement::TPtr& path);
void PersistPathDirAlterVersion(TSchemeShard& ss, NIceDb::TNiceDb& db, const TPathElement::TPtr& path);
void ClearDescribePathCaches(TSchemeShard& ss, const TPathElement::TPtr& path);

} // namespace NKikimr::NSchemeShard::NOperationHelpers
