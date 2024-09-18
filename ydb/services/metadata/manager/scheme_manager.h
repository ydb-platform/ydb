#pragma once
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

#include <ydb/services/metadata/container/snapshot.h>
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

class TSchemeOperationsController: public IAlterController {
private:
    using TSideEffectPtr = std::shared_ptr<NContainer::TObjectSnapshotBase>;

    YDB_READONLY_DEF(TActorId, Recipient);
    YDB_READONLY_DEF(TString, TypeId);
    YDB_READONLY_DEF(TString, ObjectId);
    YDB_READONLY_DEF(ui64, OriginalCookie);
    YDB_READONLY_DEF(TSideEffectPtr, SideEffect);
    const TActorContext& Ctx;

private:
    void Reply(TConclusion<TSideEffectPtr> result) {
        Ctx.Send(
            Recipient, MakeHolder<NSchemeShard::TEvPrivate::TEvObjectModificationResult>(TypeId, ObjectId, std::move(result)), 0, OriginalCookie);
    }

public:
    TSchemeOperationsController(const TActorId& recipient, const NYql::TObjectSettingsImpl& operation, ui64 originalCookie,
        TSideEffectPtr sideEffect, const TActorContext& ctx)
        : Recipient(recipient)
        , TypeId(operation.GetTypeId())
        , ObjectId(operation.GetObjectId())
        , OriginalCookie(originalCookie)
        , SideEffect(std::move(sideEffect))
        , Ctx(ctx) {
    }

    virtual void OnAlteringProblem(const TString& errorMessage) override {
        Reply(TConclusionStatus::Fail(errorMessage));
    }

    virtual void OnAlteringFinished(TInstant historyInstant) override {
        AFL_VERIFY(SideEffect);
        // TODO: Consider renaming LastHistoryInstant -> HistoryInstant
        SideEffect->SetLastHistoryInstant(historyInstant);
        Reply(std::move(SideEffect));
    }
};

class TSchemeOperationsManagerBase {
protected:
    // TODO: fix terminology: abstract, extended, history, container
    // TODO: what should this method do? unclear. fix terminology: abstract
    virtual NFetcher::ISnapshotsFetcher::TPtr DoGetExtendedSnapshotFetcher() const = 0;

    virtual TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        IOperationsManager::TInternalModificationContext& context, NSchemeShard::TSchemeShard& ss) const = 0;

    virtual TConclusion<std::shared_ptr<IObjectModificationCommand>> DoBuildModificationCommand(
        NSchemeShard::TEvSchemeShard::TEvModifyObject::TPtr request, NSchemeShard::TSchemeShard& ss, const TActorContext& ctx) const = 0;

    static NThreading::TFuture<IOperationsManager::TYqlConclusionStatus> StartSchemeOperation(
        NKikimrSchemeOp::TModifyObjectDescription description, TActorSystem& actorSystem, TDuration livetime);

protected:
    class TPatchBuilder: public TPatchBuilderBase {
    private:
        using TSelf = TSchemeOperationsManagerBase;

        const TSelf& Owner;
        IOperationsManager::TInternalModificationContext& Context;
        NSchemeShard::TSchemeShard& SS;

    public:
        TPatchBuilder(const TSelf& owner, IOperationsManager::TInternalModificationContext& context, NSchemeShard::TSchemeShard& ss)
            : Owner(owner)
            , Context(context)
            , SS(ss) {
        }

    protected:
        TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings) const override {
            return Owner.DoBuildPatchFromSettings(settings, Context, SS);
        }
    };

public:
    TConclusion<std::shared_ptr<IObjectModificationCommand>> BuildModificationCommand(
        NSchemeShard::TEvSchemeShard::TEvModifyObject::TPtr request, NSchemeShard::TSchemeShard& ss, const TActorContext& ctx) const {
        return DoBuildModificationCommand(request, ss, ctx);
    }

    NFetcher::ISnapshotsFetcher::TPtr GetExtendedSnapshotFetcher() const {
        return DoGetExtendedSnapshotFetcher();
    }
};

template <MetadataObject TObject>
class TSchemeOperationsManager: public IObjectOperationsManager<TObject>, public TSchemeOperationsManagerBase {
private:
    using TBase = IObjectOperationsManager<TObject>;
    using IOperationsManager::TYqlConclusionStatus;

public:
    using TInternalModificationContext = typename TBase::TInternalModificationContext;
    using TExternalModificationContext = typename TBase::TExternalModificationContext;
    using EActivityType = typename IOperationsManager::EActivityType;

protected:
    virtual NFetcher::ISnapshotsFetcher::TPtr DoGetExtendedSnapshotFetcher() const override {
        return std::make_shared<NContainer::TSnapshotsFetcher<TObject>>();
    }

    // TODO: Consider putting nodeId to LocalContext. Why is it not in the context?
    virtual NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 /*nodeId*/,
        const IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override {
        if (!manager) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("modification object behaviour not initialized"));
        }
        if (!manager->GetOperationsManager()) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(
                TYqlConclusionStatus::Fail("modification is unavailable for " + manager->GetTypeId()));
        }

        TActorSystem* actorSystem = context.GetExternalData().GetLocalData().GetActorSystem();
        if (!actorSystem) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(
                TYqlConclusionStatus::Fail("This place needs an actor system. Please contact internal support"));
        }

        NKikimrSchemeOp::TModifyObjectDescription request;
        *request.MutableSettings() = settings.SerializeToProto();
        *request.MutableContext() = context.SerializeToProto();
        return StartSchemeOperation(std::move(request), *actorSystem, TDuration::Seconds(10));
    }

    virtual TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const NYql::TObjectSettingsImpl& /*settings*/,
        const IClassBehaviour::TPtr& /*manager*/, TInternalModificationContext& /*context*/) const override {
        return TYqlConclusionStatus::Fail("Preparation is not supported for this operation.");
    }

    virtual NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& /*schemeOperation*/,
        const ui32 /*nodeId*/, const IClassBehaviour::TPtr& /*manager*/, const TExternalModificationContext& /*context*/) const override {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("Preparation is not supported for this operation."));
    }

public:
    TConclusion<std::shared_ptr<IObjectModificationCommand>> DoBuildModificationCommand(
        NSchemeShard::TEvSchemeShard::TEvModifyObject::TPtr request, NSchemeShard::TSchemeShard& ss, const TActorContext& ctx) const override {
        NYql::TObjectSettingsImpl settings;
        settings.DeserializeFromProto(request->Get()->Record.GetSettings());

        std::shared_ptr<IClassBehaviour> manager =
            NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(settings.GetTypeId()));
        AFL_VERIFY(manager)("type_id", settings.GetTypeId());

        NMetadata::NModifications::IOperationsManager::TInternalModificationContext modificationCtx;
        modificationCtx.DeserializeFromProto(request->Get()->Record.GetContext());
        modificationCtx.MutableExternalData().MutableLocalData().SetActorSystem(ctx.ActorSystem());

        const TPatchBuilder patchBuilder(*this, modificationCtx, ss);
        TOperationParsingResult patch = patchBuilder.BuildPatchFromSettings(settings);
        if (!patch.IsSuccess()) {
            return TConclusionStatus::Fail(patch.GetErrorMessage());
        }

        std::shared_ptr<NContainer::TObjectSnapshotBase> resultObject;
        if (modificationCtx.GetActivityType() != EActivityType::Drop) {
            Ydb::ResultSet records = patch.GetResult().BuildRecordSet();
            AFL_VERIFY(records.rowsSize() == 1);
            typename TObject::TDecoder decoder(records);
            TObject object;
            if (!object.DeserializeFromRecord(decoder, records.Getrows(0))) {
                return TConclusionStatus::Fail("Internal error: can't deserialize object.");
            }
            resultObject = std::make_shared<NContainer::TObjectSnapshot<TObject>>(std::move(object), TInstant::Zero());
        }

        IObjectModificationCommand::TPtr modifyObjectCommand;
        auto controller = std::make_shared<TSchemeOperationsController>(ss.ActorContext().SelfID, settings, request->Cookie, resultObject, ctx);
        switch (modificationCtx.GetActivityType()) {
            case EActivityType::Upsert:
                return std::make_shared<TUpsertObjectCommand<TObject>>(patch.GetResult(), manager, std::move(controller), modificationCtx);
            case EActivityType::Create:
                return std::make_shared<TCreateObjectCommand<TObject>>(
                    patch.GetResult(), manager, std::move(controller), modificationCtx, settings.GetExistingOk());
            case EActivityType::Alter:
                return std::make_shared<TUpdateObjectCommand<TObject>>(patch.GetResult(), manager, std::move(controller), modificationCtx);
            case EActivityType::Drop:
                return std::make_shared<TDeleteObjectCommand<TObject>>(
                    patch.GetResult(), manager, std::move(controller), modificationCtx, settings.GetMissingOk());
            case EActivityType::Undefined:
                return TConclusionStatus::Fail("undefined action type");
        }
    }
};

}   // namespace NKikimr::NMetadata::NModifications
