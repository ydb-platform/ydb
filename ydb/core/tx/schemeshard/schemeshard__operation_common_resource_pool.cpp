#include "schemeshard__operation_common_resource_pool.h"
#include "schemeshard_impl.h"


namespace NKikimr::NSchemeShard::NResourcePool {

namespace {

constexpr uint32_t MAX_PROTOBUF_SIZE = 2 * 1024 * 1024; // 2 MiB


bool ValidateProperties(const NKikimrSchemeOp::TResourcePoolProperties& properties, TString& errorStr) {
    const ui64 propertiesSize = properties.ByteSizeLong();
    if (propertiesSize > MAX_PROTOBUF_SIZE) {
        errorStr = TStringBuilder() << "Maximum size of properties must be less or equal equal to " << MAX_PROTOBUF_SIZE << " but got " << propertiesSize;
        return false;
    }
    return true;
}

}  // anonymous namespace

TPath::TChecker IsParentPathValid(const TPath& parentPath) {
    auto checks = parentPath.Check();
    checks.NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .NotUnderDeleting()
        .IsCommonSensePath()
        .IsLikeDirectory();

    return std::move(checks);
}

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath) {
    const TString& resourcePoolsDir = JoinPath({parentPath.GetDomainPathString(), ".metadata/workload_manager/pools"});
    if (parentPath.PathString() != resourcePoolsDir) {
        result->SetError(NKikimrScheme::EStatus::StatusSchemeError, TStringBuilder() << "Resource pools shoud be placed in " << resourcePoolsDir);
        return false;
    }

    const auto checks = IsParentPathValid(parentPath);
    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
    }

    return static_cast<bool>(checks);
}

bool Validate(const NKikimrSchemeOp::TResourcePoolDescription& description, TString& errorStr) {
    return ValidateProperties(description.GetProperties(), errorStr);
}

TResourcePoolInfo::TPtr CreateResourcePool(const NKikimrSchemeOp::TResourcePoolDescription& description, ui64 alterVersion) {
    auto resourcePoolInfo = MakeIntrusive<TResourcePoolInfo>();
    resourcePoolInfo->AlterVersion = alterVersion;
    resourcePoolInfo->Properties.CopyFrom(description.GetProperties());
    return resourcePoolInfo;
}

TResourcePoolInfo::TPtr ModifyResourcePool(const NKikimrSchemeOp::TResourcePoolDescription& description, const TResourcePoolInfo::TPtr oldResourcePoolInfo) {
    TResourcePoolInfo::TPtr resourcePoolInfo = CreateResourcePool(description, oldResourcePoolInfo->AlterVersion + 1);

    auto& properties = *resourcePoolInfo->Properties.MutableProperties();
    for (const auto& [property, value] : oldResourcePoolInfo->Properties.GetProperties()) {
        if (!properties.contains(property)) {
            properties.insert({property, value});
        }
    }

    return resourcePoolInfo;
}

bool IsApplyIfChecksPassed(const TTxTransaction& transaction, const THolder<TProposeResponse>& result, const TOperationContext& context) {
    TString errorStr;
    if (!context.SS->CheckApplyIf(transaction, errorStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
        return false;
    }
    return true;
}

bool IsDescriptionValid(const THolder<TProposeResponse>& result, const NKikimrSchemeOp::TResourcePoolDescription& description) {
    TString errorStr;
    if (!NResourcePool::Validate(description, errorStr)) {
        result->SetError(NKikimrScheme::StatusSchemeError, errorStr);
        return false;
    }
    return true;
}

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& resourcePoolPathId, TTxState::ETxType txType) {
    Y_ABORT_UNLESS(!context.SS->FindTx(operationId));
    TTxState& txState = context.SS->CreateTx(operationId, txType, resourcePoolPathId);
    txState.Shards.clear();
    return txState;
}

void RegisterParentPathDependencies(const TOperationId& operationId, const TOperationContext& context, const TPath& parentPath) {
    if (parentPath.Base()->HasActiveChanges()) {
        const TTxId parentTxId = parentPath.Base()->PlannedToCreate()
                                    ? parentPath.Base()->CreateTxId
                                    : parentPath.Base()->LastTxId;
        context.OnComplete.Dependence(parentTxId, operationId.GetTxId());
    }
}

void AdvanceTransactionStateToPropose(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db) {
    context.SS->ChangeTxState(db, operationId, TTxState::Propose);
    context.OnComplete.ActivateTx(operationId);
}

void PersistResourcePool(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& resourcePoolPath, const TResourcePoolInfo::TPtr& resourcePoolInfo, const TString& acl) {
    const auto& resourcePoolPathId = resourcePoolPath->PathId;

    if (!context.SS->ResourcePools.contains(resourcePoolPathId)) {
        context.SS->IncrementPathDbRefCount(resourcePoolPathId);
    }
    context.SS->ResourcePools[resourcePoolPathId] = resourcePoolInfo;

    if (!acl.empty()) {
        resourcePoolPath->ApplyACL(acl);
    }

    context.SS->PersistPath(db, resourcePoolPathId);
    context.SS->PersistResourcePool(db, resourcePoolPathId, resourcePoolInfo);
    context.SS->PersistTxState(db, operationId);
}

}  // namespace NKikimr::NSchemeShard::NResourcePool
