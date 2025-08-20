#include "schemeshard__operation_common_streaming_query.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard::NStreamingQuery {

namespace {

constexpr ui64 MAX_PROTOBUF_SIZE = 2_MB;

bool ValidateProperties(const NKikimrSchemeOp::TStreamingQueryProperties& properties, TString& errorStr) {
    const ui64 propertiesSize = properties.ByteSizeLong();
    if (propertiesSize > MAX_PROTOBUF_SIZE) {
        errorStr = TStringBuilder() << "Maximum size of properties must be less or equal equal to " << MAX_PROTOBUF_SIZE << " but got " << propertiesSize;
        return false;
    }

    return true;
}

}  // anonymous namespace

TPath::TChecker IsParentPathValid(const TPath& parentPath, bool isCreate) {
    auto checks = parentPath.Check();

    checks.NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .NotUnderDeleting()
        .IsCommonSensePath()
        .IsLikeDirectory();

    if (isCreate) {
        checks.FailOnRestrictedCreateInTempZone();
    }

    return checks;
}

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath, bool isCreate) {
    const auto checks = IsParentPathValid(parentPath, isCreate);
    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
    }

    return static_cast<bool>(checks);
}

bool IsApplyIfChecksPassed(const THolder<TProposeResponse>& result, const TTxTransaction& transaction, const TOperationContext& context) {
    TString errorStr;
    if (!context.SS->CheckApplyIf(transaction, errorStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errorStr);
        return false;
    }

    return true;
}

bool IsDescriptionValid(const THolder<TProposeResponse>& result, const NKikimrSchemeOp::TStreamingQueryDescription& description) {
    TString errorStr;
    if (!ValidateProperties(description.GetProperties(), errorStr)) {
        result->SetError(NKikimrScheme::StatusSchemeError, errorStr);
        return false;
    }

    return true;
}

TStreamingQueryInfo::TPtr CreateNewStreamingQuery(const NKikimrSchemeOp::TStreamingQueryDescription& description, ui64 alterVersion) {
    auto streamingQueryInfo = MakeIntrusive<TStreamingQueryInfo>();
    streamingQueryInfo->AlterVersion = alterVersion;
    streamingQueryInfo->Properties = description.GetProperties();
    return streamingQueryInfo;
}

TStreamingQueryInfo::TPtr CreateModifyStreamingQuery(const NKikimrSchemeOp::TStreamingQueryDescription& description, const TStreamingQueryInfo::TPtr oldStreamingQueryInfo) {
    auto streamingQueryInfo = CreateNewStreamingQuery(description, oldStreamingQueryInfo->AlterVersion + 1);

    if (!description.GetReplaceIfExists()) {
        auto& properties = *streamingQueryInfo->Properties.MutableProperties();
        for (const auto& [property, value] : oldStreamingQueryInfo->Properties.GetProperties()) {
            properties.emplace(property, value);
        }
    }

    return streamingQueryInfo;
}

TTxState& CreateTransaction(const TOperationId& operationId, const TOperationContext& context, const TPathId& streamingQueryPathId, TTxState::ETxType txType) {
    Y_ABORT_UNLESS(!context.SS->FindTx(operationId));

    TTxState& txState = context.SS->CreateTx(operationId, txType, streamingQueryPathId);
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

void PersistStreamingQuery(const TOperationId& operationId, const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& streamingQueryPath, const TStreamingQueryInfo::TPtr& streamingQueryInfo, const TString& acl) {
    const auto& streamingQueryPathId = streamingQueryPath->PathId;

    const auto [it, inserted] = context.SS->StreamingQueries.emplace(streamingQueryPathId, streamingQueryInfo);
    if (inserted) {
        context.SS->IncrementPathDbRefCount(streamingQueryPathId);
    } else {
        it->second = streamingQueryInfo;
    }

    if (!acl.empty()) {
        streamingQueryPath->ApplyACL(acl);
    }

    context.SS->PersistPath(db, streamingQueryPathId);
    context.SS->PersistStreamingQuery(db, streamingQueryPathId, streamingQueryInfo);
    context.SS->PersistTxState(db, operationId);
}

}  // namespace NKikimr::NSchemeShard::NStreamingQuery
