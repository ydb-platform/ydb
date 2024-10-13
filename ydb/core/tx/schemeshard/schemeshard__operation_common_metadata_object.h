#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) \
    if (!(x)) return result;

namespace NKikimr::NSchemeShard::NMetadataObject {

TPath::TChecker IsParentPathValid(const TPath& parentPath);

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath);

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context);

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& objectPathId, TTxState::ETxType txType);

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath);

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db);

void PersistOperation(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db,
    const TPathElement::TPtr& objectPath, const TString& acl, const bool created);

TString GetDestinationPath(const NKikimrSchemeOp::TModifyScheme& transaction);

}   // namespace NKikimr::NSchemeShard::NMetadataObject
