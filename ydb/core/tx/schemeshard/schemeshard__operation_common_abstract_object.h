#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#include <ydb/services/metadata/manager/abstract.h>

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) \
    if (!(x)) return result;

namespace NKikimr::NSchemeShard::NAbstractObject {

TPath::TChecker IsParentPathValid(const TPath& parentPath);

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath, const TString& typeId);

TConclusion<NMetadata::NModifications::TBaseObject::TPtr> BuildObjectMetadata(const NKikimrSchemeOp::TModifyAbstractObject& description,
    TSchemeShard& context, const NMetadata::NModifications::TBaseObject::TPtr& oldMetadata);

TConclusionStatus ValidateOperation(const TString& name, const NMetadata::NModifications::TBaseObject::TPtr& object,
    const NMetadata::NModifications::IOperationsManager::EActivityType activity, TSchemeShard& context);

TAbstractObjectInfo::TPtr CreateAbstractObject(const NMetadata::NModifications::TBaseObject::TPtr& metadata, const ui64 alterVersion);

TAbstractObjectInfo::TPtr ModifyAbstractObject(
    const NMetadata::NModifications::TBaseObject::TPtr& metadata, const TAbstractObjectInfo::TPtr oldAbstractObjectInfo);

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context);

TTxState& CreateTransaction(
    const TOperationId& operationId, const TOperationContext& context, const TPathId& abstractObjectPathId, TTxState::ETxType txType);

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath);

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db);

void PersistAbstractObject(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db,
    const TPathElement::TPtr& abstractObjectPath, const TAbstractObjectInfo::TPtr& abstractObjectInfo, const TString& acll);

}   // namespace NKikimr::NSchemeShard::NAbstractObject
