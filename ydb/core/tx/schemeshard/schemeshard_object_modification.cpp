#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/services/metadata/manager/scheme_operations_manager.h>

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

struct TTxStartObjectModification: public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvModifyObject::TPtr Request;
    std::shared_ptr<NMetadata::NModifications::IObjectModificationCommand> ModifyObjectCommand;

    TTxStartObjectModification(TSelf* self, const TEvSchemeShard::TEvModifyObject::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_START_OBJECT_MODIFICATION;
    }

    void ReplyWithError(TString errorMessage, const TActorContext& ctx) {
        ctx.Send(Request->Sender, MakeHolder<TEvSchemeShard::TEvModifyObjectResult>(std::move(errorMessage)), 0, Request->Cookie);
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxStartObjectModification DoExecute"
                << ", record: " << Request->Get()->Record.ShortDebugString() << ", at schemeshard: " << Self->TabletID());

        const TString& typeId = Request->Get()->Record.GetSettings().GetType();
        const TString& objectId = Request->Get()->Record.GetSettings().GetObject();

        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(typeId)));
        if (!cBehaviour) {
            ReplyWithError(TStringBuilder() << "Incorrect object type: \"" << typeId << "\"", ctx);
        }
        if (!cBehaviour->GetOperationsManager()) {
            ReplyWithError(TStringBuilder() << "Object type \"" << typeId << "\" does not have manager for operations", ctx);
        }
        std::shared_ptr<NMetadata::NModifications::TSchemeOperationsManagerBase> manager =
            std::dynamic_pointer_cast<NMetadata::NModifications::TSchemeOperationsManagerBase>(cBehaviour->GetOperationsManager());
        if (!manager) {
            ReplyWithError(TStringBuilder() << "Object type \"" << typeId << "\" does not have manager for scheme operations", ctx);
        }

        auto buildResult = manager->BuildModificationCommand(Request, *Self, ctx);
        if (buildResult.IsFail()) {
            ReplyWithError(buildResult.GetErrorMessage(), ctx);
        }
        ModifyObjectCommand = std::move(buildResult.GetResult());

        const auto& modification = Self->Objects.OnModificationStarted(typeId, objectId, Request->Sender);

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistObjectModificationStarted(db, TObjectId(typeId, objectId), modification.PreviousHistoryInstant, modification.Sender);
    }

    void DoComplete(const TActorContext& ctx) override {
        // TODO: Delay commit of TX that updates the .metadata table:
        //     1. On Metadata service, create TX
        //     2. Return TEvCommitObjectModification to SS
        //     3. On SS, request commit from Metadata service
        // The purpose is to guarantee that the SS was active between start and commit of TX that update metadata;
        // it will allow invalidating all TXs that may update metadata in future
        TActivationContext::Send(new IEventHandle(NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()), {},
            new NMetadata::NProvider::TEvObjectsOperation(std::move(ModifyObjectCommand))));
    }
};

struct TTxCommitObjectModification: public TSchemeShard::TRwTxBase {
    TEvPrivate::TEvCommitObjectModification::TPtr Request;

    TTxCommitObjectModification(TSelf* self, const TEvPrivate::TEvCommitObjectModification::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_START_OBJECT_MODIFICATION;
    }

    void ReplyWithError(TString errorMessage, const TActorContext& ctx) {
        ctx.Send(Request->Sender, MakeHolder<TEvSchemeShard::TEvModifyObjectResult>(std::move(errorMessage)), 0, Request->Cookie);
    }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxStartObjectModification DoExecute");   // TODO: Add message info

        // Not implemented
    }

    void DoComplete(const TActorContext& ctx) override {
        // Not implemented
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

        const auto* modification = Self->Objects.FindModificationInfo(typeId, objectId);
        AFL_VERIFY(modification)("type_id", typeId)("object_id", objectId);

        if (result.IsSuccess()) {
            Self->Objects.GetMetadataVerified(typeId)->EmplaceAbstractVerified(objectId, result.GetResult());
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
        Self->PersistObjectModificationFinished(db, { typeId, objectId });
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

void TSchemeShard::PersistObjectModificationStarted(
    NIceDb::TNiceDb& db, const TObjectId& objectId, TInstant prevHistoryInstant, const TActorId& sender) const {
    db.Table<Schema::ObjectModificationsInFly>()
        .Key(objectId.GetType(), objectId.GetLocalId())
        .Update(NIceDb::TUpdate<Schema::ObjectModificationsInFly::PreviousHistoryInstant>(prevHistoryInstant.GetValue()))
        .Update(NIceDb::TUpdate<Schema::ObjectModificationsInFly::SenderActorId>(sender.ToString()));
}

void TSchemeShard::PersistObjectModificationFinished(NIceDb::TNiceDb& db, const TObjectId& objectId) const {
    db.Table<Schema::ObjectModificationsInFly>().Key(objectId.GetType(), objectId.GetLocalId()).Delete();
}

void TSchemeShard::InitializeObjects(const TActorContext& ctx) {
    for (const TString& typeId : NMetadata::IClassBehaviour::TFactory::GetRegisteredKeys()) {
        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(typeId)));
        if (cBehaviour && cBehaviour->GetOperationsManager() &&
            !!dynamic_pointer_cast<NMetadata::NModifications::TSchemeOperationsManagerBase>(cBehaviour->GetOperationsManager())) {
            InitializeObjectMetadata(typeId, ctx);
        }
    }
}

void TSchemeShard::InitializeObjectMetadata(TString typeId, const TActorContext& ctx) {
    // TODO:
    // 1. Request from Metadata service:
    //     1.1. to invalidate all running transactions in the .metadata table
    //     1.2. return current metadata snapshot with history instants
    // 2. Apply this snapshot to Objects
    // 3. For each transaction in fly:
    //     * if it has been completed (current history instant != one in modification info), then reply success to KQP
    //     * otherwise reply failure
    //     then remove TX in fly
}

}   // namespace NKikimr::NSchemeShard
