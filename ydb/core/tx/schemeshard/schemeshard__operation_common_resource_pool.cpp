#include "schemeshard__operation_common_resource_pool.h"
#include "schemeshard__operation_common.h"


namespace NKikimr::NSchemeShard {

namespace NResourcePool {

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
    const TString& resourcePoolsDir = JoinPath({parentPath.GetDomainPathString(), ".resource_pools"});
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
    auto externalDataSoureInfo = MakeIntrusive<TResourcePoolInfo>();
    externalDataSoureInfo->AlterVersion = alterVersion;
    externalDataSoureInfo->Properties.CopyFrom(description.GetProperties());
    return externalDataSoureInfo;
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

}  // namespace NResourcePool

//// TResourcePoolSubOperation

TTxState::ETxState TResourcePoolSubOperation::NextState() {
    return TTxState::Propose;
}

TTxState::ETxState TResourcePoolSubOperation::NextState(TTxState::ETxState state) const {
    switch (state) {
    case TTxState::Waiting:
    case TTxState::Propose:
        return TTxState::Done;
    default:
        return TTxState::Invalid;
    }
}

TSubOperationState::TPtr TResourcePoolSubOperation::SelectStateFunc(TTxState::ETxState state) {
    switch (state) {
    case TTxState::Waiting:
    case TTxState::Propose:
        return GetProposeOperationState();
    case TTxState::Done:
        return MakeHolder<TDone>(OperationId);
    default:
        return nullptr;
    }
}

bool TResourcePoolSubOperation::IsApplyIfChecksPassed(const THolder<TProposeResponse>& result, const TOperationContext& context) const {
    TString errorStr;
    if (!context.SS->CheckApplyIf(Transaction, errorStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
        return false;
    }
    return true;
}

bool TResourcePoolSubOperation::IsDescriptionValid(const THolder<TProposeResponse>& result, const NKikimrSchemeOp::TResourcePoolDescription& description) {
    TString errorStr;
    if (!NResourcePool::Validate(description, errorStr)) {
        result->SetError(NKikimrScheme::StatusSchemeError, errorStr);
        return false;
    }
    return true;
}

void TResourcePoolSubOperation::AddPathInSchemeShard(const THolder<TProposeResponse>& result, const TPath& dstPath) {
    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
}

TTxState& TResourcePoolSubOperation::CreateTransaction(const TOperationContext& context, const TPathId& resourcePoolPathId, TTxState::ETxType txType) const {
    Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
    TTxState& txState = context.SS->CreateTx(OperationId, txType, resourcePoolPathId);
    txState.Shards.clear();
    return txState;
}

void TResourcePoolSubOperation::RegisterParentPathDependencies(const TOperationContext& context, const TPath& parentPath) const {
    if (parentPath.Base()->HasActiveChanges()) {
        const TTxId parentTxId = parentPath.Base()->PlannedToCreate()
                                    ? parentPath.Base()->CreateTxId
                                    : parentPath.Base()->LastTxId;
        context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
    }
}

void TResourcePoolSubOperation::AdvanceTransactionStateToPropose(const TOperationContext& context, NIceDb::TNiceDb& db) const {
    context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
    context.OnComplete.ActivateTx(OperationId);
}

void TResourcePoolSubOperation::PersistResourcePool(const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& resourcePoolPath, const TResourcePoolInfo::TPtr& resourcePoolInfo, const TString& acl) const {
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
    context.SS->PersistTxState(db, OperationId);
}

}  // namespace NKikimr::NSchemeShard
