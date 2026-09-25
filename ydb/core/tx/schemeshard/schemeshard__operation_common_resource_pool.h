#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;


namespace NKikimr::NSchemeShard::NResourcePool {

TPath::TChecker IsParentPathValid(const TPath& parentPath);

bool IsParentPathValid(const std::unique_ptr<TProposeResponse>& result, const TPath& parentPath);

bool Validate(const NKikimrSchemeOp::TResourcePoolDescription& description, TString& errorStr);

TResourcePoolInfo::TPtr CreateResourcePool(const NKikimrSchemeOp::TResourcePoolDescription& description, ui64 alterVersion);

TResourcePoolInfo::TPtr ModifyResourcePool(const NKikimrSchemeOp::TResourcePoolDescription& description, const TResourcePoolInfo::TPtr oldResourcePoolInfo);

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const std::unique_ptr<TProposeResponse>& result, const TOperationContext& context);

bool IsDescriptionValid(const std::unique_ptr<TProposeResponse>& result, const NKikimrSchemeOp::TResourcePoolDescription& description);

bool IsResourcePoolInfoValid(const std::unique_ptr<TProposeResponse>& result, const TResourcePoolInfo::TPtr& info);

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& resourcePoolPathId, TTxState::ETxType txType);

}  // namespace NKikimr::NSchemeShard::NResourcePool
