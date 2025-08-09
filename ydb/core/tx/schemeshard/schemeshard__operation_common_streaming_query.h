#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;

namespace NKikimr::NSchemeShard::NStreamingQuery {

TPath::TChecker IsParentPathValid(const TPath& parentPath, bool isCreate);

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath, bool isCreate);

bool IsApplyIfChecksPassed(const THolder<TProposeResponse>& result, const TTxTransaction& transaction, const TOperationContext& context);

bool IsDescriptionValid(const THolder<TProposeResponse>& result, const NKikimrSchemeOp::TStreamingQueryDescription& description);

TStreamingQueryInfo::TPtr CreateNewStreamingQuery(const NKikimrSchemeOp::TStreamingQueryDescription& description, ui64 alterVersion);

TStreamingQueryInfo::TPtr CreateModifyStreamingQuery(const NKikimrSchemeOp::TStreamingQueryDescription& description, const TStreamingQueryInfo::TPtr oldStreamingQueryInfo);

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& streamingQueryPathId, TTxState::ETxType txType);

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath);

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db);

void PersistStreamingQuery(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& streamingQueryPath, const TStreamingQueryInfo::TPtr& streamingQueryInfo, const TString& acl);

}  // namespace NKikimr::NSchemeShard::NStreamingQuery
