#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

constexpr uint32_t MAX_FIELD_SIZE = 1000;
constexpr uint32_t MAX_PROTOBUF_SIZE = 2 * 1024 * 1024; // 2 MiB

bool ValidateLocationAndInstallation(const TString& location, const TString& installation, TString& errStr) {
    if (!location && !installation) {
        errStr = "Location or installation must not be empty";
        return false;
    }
    if (location.Size() > MAX_FIELD_SIZE) {
        errStr = Sprintf("Maximum length of location must be less or equal equal to %u but got %lu", MAX_FIELD_SIZE, location.Size());
        return false;
    }
    if (installation.Size() > MAX_FIELD_SIZE) {
        errStr = Sprintf("Maximum length of installation must be less or equal equal to %u but got %lu", MAX_FIELD_SIZE, installation.Size());
        return false;
    }
    return true;
}

bool CheckAuth(const TString& authMethod, const TVector<TString>& availableAuthMethods, TString& errStr) {
    if (Find(availableAuthMethods, authMethod) == availableAuthMethods.end()) {
        errStr = TStringBuilder{} << authMethod << " isn't supported for this source type";
        return false;
    }

    return true;
}

bool ValidateProperties(const NKikimrSchemeOp::TExternalDataSourceProperties& properties, TString& errStr) {
    if (properties.ByteSizeLong() > MAX_PROTOBUF_SIZE) {
        errStr = Sprintf("Maximum size of properties must be less or equal equal to %u but got %lu", MAX_PROTOBUF_SIZE, properties.ByteSizeLong());
        return false;
    }
    return true;
}

bool ValidateAuth(const NKikimrSchemeOp::TAuth& auth, const NKikimr::NExternalSource::IExternalSource::TPtr& source, TString& errStr) {
    if (auth.ByteSizeLong() > MAX_PROTOBUF_SIZE) {
        errStr = Sprintf("Maximum size of authorization information must be less or equal equal to %u but got %lu", MAX_PROTOBUF_SIZE, auth.ByteSizeLong());
        return false;
    }
    const auto availableAuthMethods = source->GetAuthMethods();
    switch (auth.identity_case()) {
        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET: {
            errStr = "Authorization method isn't specified";
            return false;
        }
        case NKikimrSchemeOp::TAuth::kServiceAccount:
            return CheckAuth("SERVICE_ACCOUNT", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kMdbBasic:
            return CheckAuth("MDB_BASIC", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kBasic:
            return CheckAuth("BASIC", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kAws:
            return CheckAuth("AWS", availableAuthMethods, errStr);
        case NKikimrSchemeOp::TAuth::kNone:
            return CheckAuth("NONE", availableAuthMethods, errStr);
    }
    return false;
}

bool Validate(const NKikimrSchemeOp::TExternalDataSourceDescription& desc, const NKikimr::NExternalSource::IExternalSourceFactory::TPtr& factory, TString& errStr) {
    try {
        auto source = factory->GetOrCreate(desc.GetSourceType());
        source->ValidateProperties(desc.GetProperties().SerializeAsString());
        return ValidateLocationAndInstallation(desc.GetLocation(), desc.GetInstallation(), errStr)
            && ValidateAuth(desc.GetAuth(), source, errStr)
            && ValidateProperties(desc.GetProperties(), errStr);
    } catch (...) {
        errStr = CurrentExceptionMessage();
        return false;
    }
}

TExternalDataSourceInfo::TPtr CreateExternalDataSource(const NKikimrSchemeOp::TExternalDataSourceDescription& desc, const NKikimr::NExternalSource::IExternalSourceFactory::TPtr& factory, TString& errStr) {
    if (!Validate(desc, factory, errStr)) {
        return nullptr;
    }
    TExternalDataSourceInfo::TPtr externalDataSoureInfo = new TExternalDataSourceInfo;
    externalDataSoureInfo->SourceType = desc.GetSourceType();
    externalDataSoureInfo->Location = desc.GetLocation();
    externalDataSoureInfo->Installation = desc.GetInstallation();
    externalDataSoureInfo->AlterVersion = 1;
    externalDataSoureInfo->Auth.CopyFrom(desc.GetAuth());
    externalDataSoureInfo->Properties.CopyFrom(desc.GetProperties());
    return externalDataSoureInfo;
}

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateExternalDataSource TPropose"
            << ", operationId: " << OperationId;
    }

public:
    explicit TPropose(TOperationId id)
        : OperationId(id)
    {
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateExternalDataSource);

        auto pathId = txState->TargetPathId;
        auto path = TPath::Init(pathId, context.SS);
        TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        context.SS->TabletCounters->Simple()[COUNTER_EXTERNAL_DATA_SOURCE_COUNT].Add(1);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ClearDescribePathCaches(pathPtr);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateExternalDataSource);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};


class TCreateExternalDataSource: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const auto ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& externalDataSoureDescription = Transaction.GetCreateExternalDataSource();
        const TString& name = externalDataSoureDescription.GetName();


        LOG_N("TCreateExternalDataSource Propose"
            << ": opId# " << OperationId
            << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath parentPath = TPath::Resolve(parentPathStr, context.SS);
        {
            auto checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        TPath dstPath = parentPath.Child(name);
        {
            auto checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeExternalDataSource, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName()
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TExternalDataSourceInfo::TPtr externalDataSoureInfo = CreateExternalDataSource(externalDataSoureDescription, context.SS->ExternalSourceFactory, errStr);
        if (!externalDataSoureInfo) {
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TPathElement::TPtr externalDataSoure = dstPath.Base();
        externalDataSoure->CreateTxId = OperationId.GetTxId();
        externalDataSoure->LastTxId = OperationId.GetTxId();
        externalDataSoure->PathState = TPathElement::EPathState::EPathStateCreate;
        externalDataSoure->PathType = TPathElement::EPathType::EPathTypeExternalDataSource;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateExternalDataSource, externalDataSoure->PathId);
        txState.Shards.clear();

        NIceDb::TNiceDb db(context.GetDB());

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->ExternalDataSources[externalDataSoure->PathId] = externalDataSoureInfo;
        context.SS->TabletCounters->Simple()[COUNTER_EXTERNAL_DATA_SOURCE_COUNT].Add(1);
        context.SS->IncrementPathDbRefCount(externalDataSoure->PathId);

        context.SS->PersistPath(db, externalDataSoure->PathId);

        if (!acl.empty()) {
            externalDataSoure->ApplyACL(acl);
            context.SS->PersistACL(db, externalDataSoure);
        }

        context.SS->PersistExternalDataSource(db, externalDataSoure->PathId, externalDataSoureInfo);
        context.SS->PersistTxState(db, OperationId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateExternalDataSource AbortPropose"
            << ": opId# " << OperationId);
        Y_FAIL("no AbortPropose for TCreateExternalDataSource");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateExternalDataSource AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewExternalDataSource(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateExternalDataSource>(id, tx);
}

ISubOperation::TPtr CreateNewExternalDataSource(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateExternalDataSource>(id, state);
}

}
