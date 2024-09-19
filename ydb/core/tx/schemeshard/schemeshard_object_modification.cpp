#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/services/metadata/container/snapshot.h>
#include <ydb/services/metadata/ds_table/accessor_snapshot_simple.h>
#include <ydb/services/metadata/manager/abstract.h>
#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NSchemeShard {

void TSchemeShard::Handle(const TEvSchemeShard::TEvModifyObject::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvModifyObject"
            << ", at schemeshard: " << TabletID() << ", message: " << ev->Get()->Record.ShortDebugString());

    Execute(CreateTxStartObjectModification(ev, ctx));
}

void TSchemeShard::Handle(const TEvPrivate::TEvCommitObjectModification::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvCommitObjectModification"
            << ", at schemeshard: " << TabletID());   // TODO: add message info

    Execute(CreateTxCommitObjectModification(ev, ctx));
}

void TSchemeShard::Handle(const TEvPrivate::TEvObjectModificationResult::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvObjectModificationResult"
            << ", at schemeshard: " << TabletID() << ", typeId: " << ev->Get()->TypeId << ", objectId: " << ev->Get()->ObjectId);

    Execute(CreateTxCompleteObjectModification(ev, ctx));
}

void TSchemeShard::Handle(const TEvPrivate::TEvInitializeObjectMetadata::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvInitializeObjectMetadata"
            << ", at schemeshard: " << TabletID() << ", typeId: " << ev->Get()->TypeId << ", snapshot: " << ev->Get()->Snapshot->DebugString());

    Execute(CreateTxInitializeObjectMetadata(ev, ctx));
}

void TSchemeShard::Handle(const TEvPrivate::TEvSubscribeToMetadataInitialization::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvSubscribeToMetadataInitialization"
            << ", at schemeshard: " << TabletID() << ", typeId: " << ev->Get()->TypeId);

    const TString& typeId = ev->Get()->TypeId;

    if (Objects.IsInitialized(typeId)) {
        ctx.Send(ev->Sender, new TEvPrivate::TEvNotifyMetadataInitialized(typeId), 0, ev->Cookie);
    } else {
        Objects.SubscribeToInitialization(typeId, TObjectsInfo::TSubscriber(ev->Sender, ev->Cookie));
    }
}

// TODO: Rewrite transactions such that in-mem side-effects are applied only on complete

struct TTxStartObjectModification: public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvModifyObject::TPtr Request;

    TTxStartObjectModification(TSelf* self, const TEvSchemeShard::TEvModifyObject::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_START_OBJECT_MODIFICATION;
    }

    void ReplyError(TString errorMessage, const TActorContext& ctx) {
        ctx.Send(Request->Sender, MakeHolder<TEvSchemeShard::TEvModifyObjectResult>(std::move(errorMessage)), 0, Request->Cookie);
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxStartObjectModification DoExecute"
                << ", record: " << Request->Get()->Record.ShortDebugString() << ", at schemeshard: " << Self->TabletID());

        const TString& typeId = Request->Get()->Record.GetSettings().GetType();
        const TString& objectId = Request->Get()->Record.GetSettings().GetObject();

        if (!Self->Objects.IsInitialized(typeId)) {
            ReplyError(TStringBuilder() << typeId << " metadata hasn't been initialized on scheme shard yet.", ctx);
            return;
        }

        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(typeId)));
        if (!cBehaviour) {
            ReplyError(TStringBuilder() << "Incorrect object type: \"" << typeId << "\"", ctx);
            return;
        }
        if (!cBehaviour->GetOperationsManager()) {
            ReplyError(TStringBuilder() << "Object type \"" << typeId << "\" does not have manager for operations", ctx);
            return;
        }
        std::shared_ptr<NMetadata::NModifications::TSchemeOperationsManagerBase> manager =
            std::dynamic_pointer_cast<NMetadata::NModifications::TSchemeOperationsManagerBase>(cBehaviour->GetOperationsManager());
        if (!manager) {
            ReplyError(TStringBuilder() << "Object type \"" << typeId << "\" does not have manager for scheme operations", ctx);
            return;
        }

        auto buildResult = manager->BuildModificationCommand(Request, *Self, ctx);
        if (buildResult.IsFail()) {
            ReplyError(buildResult.GetErrorMessage(), ctx);
            return;
        }
        std::shared_ptr<NMetadata::NModifications::IObjectModificationCommand> modifyObjectCommand = std::move(buildResult.GetResult());

        const auto& modification = Self->Objects.OnModificationStarted(typeId, objectId, Request->Sender);

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistObjectModificationStarted(db, typeId, objectId, modification.PreviousHistoryInstant, modification.Sender);

        // TODO: Delay commit of TX that updates the .metadata table:
        //     1. On Metadata service, create TX
        //     2. Return TEvCommitObjectModification to SS
        //     3. On SS, request commit from Metadata service
        // The purpose is to guarantee that the SS was active between start and commit of TX that update metadata;
        // it will allow invalidating all TXs that may update metadata in future
        ctx.Send(NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
            new NMetadata::NProvider::TEvObjectsOperation(std::move(modifyObjectCommand)));
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }
};

struct TTxCommitObjectModification: public TSchemeShard::TRwTxBase {
    TEvPrivate::TEvCommitObjectModification::TPtr Request;

    TTxCommitObjectModification(TSelf* self, const TEvPrivate::TEvCommitObjectModification::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_COMMIT_OBJECT_MODIFICATION;
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxStartObjectModification DoExecute");   // TODO: Add message info

        // Not implemented
        Y_UNUSED(txc);
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }
};

struct TTxCompleteObjectModification: public TSchemeShard::TRwTxBase {
    TEvPrivate::TEvObjectModificationResult::TPtr Request;

    TTxCompleteObjectModification(TSelf* self, const TEvPrivate::TEvObjectModificationResult::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_COMPLETE_OBJECT_MODIFICATION;
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Handle TTxCompleteObjectModification"
                << ", at schemeshard: " << Self->TabletID() << ", typeId: " << Request->Get()->TypeId
                << ", objectId: " << Request->Get()->ObjectId);

        const TString& typeId = Request->Get()->TypeId;
        const TString& objectId = Request->Get()->ObjectId;
        const auto& result = Request->Get()->Result;

        const auto* modification = Self->Objects.FindModification(typeId, objectId);
        AFL_VERIFY(modification)("type_id", typeId)("object_id", objectId);

        if (result.IsSuccess()) {
            auto objectsMetadata = Self->Objects.GetMetadataVerified(typeId);
            if (result.GetResult()) {
                objectsMetadata->EmplaceAbstractVerified(objectId, result.GetResult());
            } else {
                objectsMetadata->Erase(objectId);
            }
        }

        if (result.IsSuccess()) {
            // TODO: Consider returning a status code
            ctx.Send(modification->Sender, MakeHolder<TEvSchemeShard::TEvModifyObjectResult>(), 0, Request->Cookie);
        } else {
            // TODO: Consider returning a status code
            ctx.Send(modification->Sender, MakeHolder<TEvSchemeShard::TEvModifyObjectResult>(result.GetErrorMessage()), 0, Request->Cookie);
        }

        Self->Objects.OnModificationFinished(typeId, objectId);

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistObjectModificationFinished(db, typeId, objectId);
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }
};

struct TTxInitializeObjectMetadata: public TSchemeShard::TRwTxBase {
    TEvPrivate::TEvInitializeObjectMetadata::TPtr Request;

    TTxInitializeObjectMetadata(TSelf* self, const TEvPrivate::TEvInitializeObjectMetadata::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INITIALIZE_OBJECT_METADATA;
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxInitializeObjectMetadata DoExecute"
                << ", at schemeshard: " << Self->TabletID() << ", typeId: " << Request->Get()->TypeId
                << ", snapshot: " << Request->Get()->Snapshot->DebugString());

        TString typeId = Request->Get()->TypeId;
        auto currentObjects = *Request->Get()->Snapshot;

        NIceDb::TNiceDb db(txc.DB);

        for (const TString& objectId : Self->Objects.ListObjectsUnderModification(typeId)) {
            const TObjectsInfo::TModificationInfo* modification = Self->Objects.FindModification(typeId, objectId);
            AFL_VERIFY(modification);
            if (modification->PreviousHistoryInstant == currentObjects.FindObjectAbstract(objectId)->GetLastHistoryInstant()) {
                ctx.Send(modification->Sender, new TEvSchemeShard::TEvModifyObjectResult("Transaction was invalidated."));
            } else {
                ctx.Send(modification->Sender, new TEvSchemeShard::TEvModifyObjectResult());
            }

            Self->Objects.OnModificationFinished(typeId, objectId);
            Self->PersistObjectModificationFinished(db, typeId, objectId);
        }

        auto subscribers = Self->Objects.InitializeAndGetSubscribers(typeId, std::move(currentObjects));
        for (auto&& subscriber : subscribers) {
            ctx.Send(subscriber.Actor, new TEvPrivate::TEvNotifyMetadataInitialized(typeId), 0, subscriber.Cookie);
        }
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxStartObjectModification(
    const TEvSchemeShard::TEvModifyObject::TPtr& ev, const TActorContext& /*ctx*/) {
    return new TTxStartObjectModification(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCommitObjectModification(
    const TEvPrivate::TEvCommitObjectModification::TPtr& ev, const TActorContext& /*ctx*/) {
    return new TTxCommitObjectModification(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteObjectModification(
    const TEvPrivate::TEvObjectModificationResult::TPtr& ev, const TActorContext& /*ctx*/) {
    return new TTxCompleteObjectModification(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInitializeObjectMetadata(
    const TEvPrivate::TEvInitializeObjectMetadata::TPtr& ev, const TActorContext& /*ctx*/) {
    return new TTxInitializeObjectMetadata(this, ev);
}

void TSchemeShard::PersistObjectModificationStarted(NIceDb::TNiceDb& db, const TString& typeId, const TString& objectId, TInstant prevHistoryInstant, const TActorId& sender) const {
    db.Table<Schema::ObjectModificationsInFly>()
        .Key(typeId, objectId)
        .Update(NIceDb::TUpdate<Schema::ObjectModificationsInFly::PreviousHistoryInstant>(prevHistoryInstant.GetValue()))
        .Update(NIceDb::TUpdate<Schema::ObjectModificationsInFly::SenderActorId>(sender.ToString()));
}

void TSchemeShard::PersistObjectModificationFinished(NIceDb::TNiceDb& db, const TString& typeId, const TString& objectId) const {
    db.Table<Schema::ObjectModificationsInFly>().Key(typeId, objectId).Delete();
}

class TMetadataInitializer: public TActorBootstrapped<TMetadataInitializer> {
private:
    constexpr static const TInstant RetryDelay = TInstant::Seconds(1);

    TString TypeId;
    std::shared_ptr<NMetadata::NModifications::TSchemeOperationsManagerBase> Manager;
    NActors::TActorId Owner;

    void StartFetching() const {
        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
            MakeHolder<NMetadata::NProvider::TEvAskExtendedSnapshot>(Manager->GetExtendedSnapshotFetcher()));
    }

    void ScheduleRetry() const {
        this->Schedule(RetryDelay, new TEvents::TEvWakeup());
    }

public:
    TMetadataInitializer(
        const TString& typeId, const std::shared_ptr<NMetadata::NModifications::TSchemeOperationsManagerBase>& manager, const TActorId& owner)
        : TypeId(typeId)
        , Manager(manager)
        , Owner(owner) {
    }

    void Handle(NMetadata::NProvider::TDSAccessorSimple::TEvController::TEvResult::TPtr& ev) {
        const auto& snapshot = ev->Get()->GetResult();
        std::shared_ptr<NMetadata::NContainer::TSnapshotBase> abstractSnapshot =
            std::dynamic_pointer_cast<NMetadata::NContainer::TSnapshotBase>(snapshot);
        AFL_VERIFY(abstractSnapshot)("snapshot", snapshot->SerializeToString());
        Send(Owner, MakeHolder<TEvPrivate::TEvInitializeObjectMetadata>(
                        TypeId, std::make_shared<NMetadata::NContainer::TAbstractObjectContainer>(abstractSnapshot->GetObjects())));
    }

    void Handle(NMetadata::NProvider::TDSAccessorSimple::TEvController::TEvError::TPtr& ev, const TActorContext& ctx) {
        LOG_WARN_S(
            ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Can't fetch metadata for objectType=" << TypeId << ": " << ev->Get()->GetErrorMessage());
        // TODO: Consider using sime kind of background task manager for potentially infinite retries
        ScheduleRetry();
    }

    void Handle(NMetadata::NProvider::TDSAccessorSimple::TEvController::TEvTableAbsent::TPtr& /*ev*/) {
        Send(Owner,
            MakeHolder<TEvPrivate::TEvInitializeObjectMetadata>(TypeId, std::make_shared<NMetadata::NContainer::TAbstractObjectContainer>()));
    }

    void Handle(TEvents::TEvWakeup::TPtr& /*ev*/) {
        StartFetching();
    }

    void Bootstrap() {
        // TODO:
        // 1. Request from Metadata service:
        //     1.1. to invalidate all running transactions in the .metadata table
        //     1.2. return current metadata snapshot with history instants
        // 2. Apply this snapshot to Objects
        // 3. For each transaction in fly:
        //     * if it has been completed (current history instant != one in modification info), then reply success to KQP
        //     * otherwise reply failure
        //     then remove TX in fly
        StartFetching();
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TDSAccessorSimple::TEvController::TEvResult, Handle);
            HFunc(NMetadata::NProvider::TDSAccessorSimple::TEvController::TEvError, Handle);
            hFunc(NMetadata::NProvider::TDSAccessorSimple::TEvController::TEvTableAbsent, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

void TSchemeShard::InitializeObjects(const TActorContext& ctx) const {
    for (const TString& typeId : NMetadata::IClassBehaviour::TFactory::GetRegisteredKeys()) {
        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(typeId)));
        if (cBehaviour && cBehaviour->GetOperationsManager()) {
            if (auto manager =
                    dynamic_pointer_cast<NMetadata::NModifications::TSchemeOperationsManagerBase>(cBehaviour->GetOperationsManager())) {
                InitializeObjectMetadata(typeId, manager, ctx);
            }
        }
    }
}

void TSchemeShard::InitializeObjectMetadata(const TString& typeId,
    const std::shared_ptr<NMetadata::NModifications::TSchemeOperationsManagerBase>& manager, const TActorContext& ctx) const {
    ctx.Register(new TMetadataInitializer(typeId, manager, ctx.SelfID));
}

}   // namespace NKikimr::NSchemeShard
