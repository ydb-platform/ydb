#include "console_tenants_manager.h"
#include "console_impl.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/unknown_field_set.h>

namespace NKikimr::NConsole {

namespace {

class TDatabaseFreezeRequest : public TActorBootstrapped<TDatabaseFreezeRequest> {
    const TActorId Owner;
    const NKikimrConsole::TDatabaseFreezeOperation Operation;
    ui64 SchemeShardId = 0;
    ui64 PathId = 0;
    TActorId Pipe;
    bool Resolving = false;

    void Fail(ui32 status, const TString& issue) {
        auto result = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult>();
        result->Record.SetEpoch(Operation.GetEpoch());
        result->Record.SetReady(true);
        result->Record.SetStatus(status);
        result->Record.SetIssue(issue);
        Send(Owner, result.Release());
        PassAway();
    }

    void Progress() {
        if (!SchemeShardId) {
            if (!Resolving) {
                Resolving = true;
                auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
                request->DatabaseName = Operation.GetPath();
                auto& entry = request->ResultSet.emplace_back();
                entry.Path = SplitPath(Operation.GetPath());
                entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
                entry.SyncVersion = true;
                Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
            }
            return;
        }
        if (!Pipe) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = 3};
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), SchemeShardId, config));
        }
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDatabaseFreeze>();
        request->Record.SetPathId(PathId);
        request->Record.SetEpoch(Operation.GetEpoch());
        request->Record.SetFreeze(Operation.GetFreeze());
        NTabletPipe::SendData(SelfId(), Pipe, request.Release());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        Resolving = false;
        const auto& navigation = *ev->Get()->Request;
        if (navigation.ResultSet.size() == 1
            && navigation.ResultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown)
        {
            return Fail(Ydb::StatusIds::NOT_FOUND, "Database disappeared while applying freeze");
        }
        if (navigation.ErrorCount || navigation.ResultSet.size() != 1
            || !navigation.ResultSet.front().DomainInfo)
        {
            return; // Resolution can be temporarily unavailable during restart.
        }
        const auto& domain = *navigation.ResultSet.front().DomainInfo;
        if (domain.DomainKey != TPathId(Operation.GetDomainOwnerId(), Operation.GetDomainLocalPathId())) {
            return Fail(Ydb::StatusIds::ABORTED, "Database incarnation changed while applying freeze");
        }
        SchemeShardId = domain.Params.HasSchemeShard()
            ? domain.Params.GetSchemeShard() : domain.DomainKey.OwnerId;
        PathId = SchemeShardId == domain.DomainKey.OwnerId
            ? ui64(domain.DomainKey.LocalPathId) : NSchemeShard::RootPathId;
        Progress();
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult::TPtr& ev) {
        const auto& result = ev->Get()->Record;
        if (result.GetEpoch() != Operation.GetEpoch() || !result.GetReady()) {
            return;
        }
        auto resultEvent = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult>();
        resultEvent->Record.CopyFrom(result);
        Send(Owner, resultEvent.Release());
        PassAway();
    }

    void Reconnect() {
        NTabletPipe::CloseAndForgetClient(SelfId(), Pipe);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK && ev->Get()->ClientId == Pipe) {
            Reconnect();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == Pipe) {
            Reconnect();
        }
    }

    void Wakeup() {
        // Navigation may have been lost while a local service was restarting.
        Resolving = false;
        Progress();
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    void PassAway() override {
        Reconnect();
        TActorBootstrapped::PassAway();
    }

public:
    TDatabaseFreezeRequest(TActorId owner, NKikimrConsole::TDatabaseFreezeOperation operation)
        : Owner(owner), Operation(std::move(operation))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Wakeup();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TEvWakeup::EventType, Wakeup);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // anonymous namespace

bool TTenantsManager::IsDatabaseFreezePending(TTenant::TPtr tenant) const {
    auto it = DatabaseFreezeOperations.find(tenant->FreezeEpoch);
    return it != DatabaseFreezeOperations.end() && !it->second.GetReady();
}

void TTenantsManager::FillDatabaseFreezeOperation(ui64 epoch, Ydb::Operations::Operation& result) {
    const auto& operation = DatabaseFreezeOperations.at(epoch);
    result.set_id(NOperationId::ProtoToString(MakeOperationId(operation.GetPath(), epoch, TTenant::SET_FREEZE)));
    result.set_ready(operation.GetReady());
    if (operation.GetReady()) {
        result.set_status(static_cast<Ydb::StatusIds::StatusCode>(operation.GetStatus()));
        if (operation.GetIssue()) {
            auto* issue = result.add_issues();
            issue->set_message(operation.GetIssue());
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
        }
    }
}

void TTenantsManager::NotifyDatabaseFreeze(ui64 epoch, const TActorContext& ctx) {
    auto it = DatabaseFreezeSubscribers.find(epoch);
    if (it == DatabaseFreezeSubscribers.end()) {
        return;
    }
    for (const auto subscriber : it->second) {
        auto notification = MakeHolder<TEvConsole::TEvOperationCompletionNotification>();
        FillDatabaseFreezeOperation(epoch, *notification->Record.MutableResponse()->mutable_operation());
        ctx.Send(subscriber, notification.Release());
    }
    DatabaseFreezeSubscribers.erase(it);
}

void TTenantsManager::StartDatabaseFreeze(ui64 epoch, const TActorContext& ctx) {
    const auto& operation = DatabaseFreezeOperations.at(epoch);
    if (!operation.GetReady() && !DatabaseFreezeWorkers.contains(epoch)) {
        DatabaseFreezeWorkers[epoch] = ctx.Register(new TDatabaseFreezeRequest(SelfId(), operation));
    }
}

class TTenantsManager::TTxFreezeTenant : public TTransactionBase<TTenantsManager> {
    TEvConsole::TEvAlterTenantRequest::TPtr Request;
    THolder<TEvConsole::TEvAlterTenantResponse> Response;
    ui64 Epoch = 0;
    ui64 CancelledEpoch = 0;
    bool Start = false;

public:
    TTxFreezeTenant(TTenantsManager* self, TEvConsole::TEvAlterTenantRequest::TPtr ev)
        : TBase(self), Request(std::move(ev))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Response = MakeHolder<TEvConsole::TEvAlterTenantResponse>();
        auto& result = *Response->Record.MutableResponse()->mutable_operation();
        auto fail = [&](Ydb::StatusIds::StatusCode code, const TString& issue) {
            result.set_ready(true);
            result.set_status(code);
            result.add_issues()->set_message(issue);
            return true;
        };
        Ydb::StatusIds::StatusCode status;
        TString issue;
        if (!Self->CheckAccess(Request->Get()->Record.GetUserToken(), status, issue, ctx)) {
            return fail(status, issue);
        }
        const auto& request = Request->Get()->Record.GetRequest();
        // A whitelist makes a future AlterDatabase field incompatible with
        // freeze by default, including explicitly present false-valued fields.
        std::vector<const google::protobuf::FieldDescriptor*> fields;
        if (!request.GetReflection()->GetUnknownFields(request).empty()) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "freeze cannot be combined with unknown database settings");
        }
        request.GetReflection()->ListFields(request, &fields);
        for (const auto* field : fields) {
            switch (field->number()) {
                case 1: case 7: case 8: case 10: case 15:
                    break;
                default:
                    return fail(Ydb::StatusIds::BAD_REQUEST, "freeze cannot be combined with other database changes");
            }
        }
        if (request.operation_params().has_cancel_after() || request.operation_params().has_forget_after()) {
            return fail(Ydb::StatusIds::UNSUPPORTED, "cancel_after and forget_after are not supported for database freeze");
        }
        auto tenant = Self->GetTenant(CanonizePath(request.path()));
        if (!tenant) {
            return fail(Ydb::StatusIds::NOT_FOUND, "Database does not exist");
        }
        if (!tenant->IsRunning() || !tenant->HasSubDomainKey()) {
            return fail(Ydb::StatusIds::UNAVAILABLE, "Database is not running");
        }
        if (request.idempotency_key()) {
            for (const auto& [epoch, operation] : Self->DatabaseFreezeOperations) {
                if (operation.GetPath() == tenant->Path && operation.GetTenantTxId() == tenant->TxId
                    && operation.GetDomainOwnerId() == tenant->DomainId.OwnerId
                    && operation.GetDomainLocalPathId() == tenant->DomainId.LocalPathId
                    && operation.GetIdempotencyKey() == request.idempotency_key())
                {
                    if (operation.GetFreeze() != request.freeze()) {
                        return fail(Ydb::StatusIds::BAD_REQUEST, "Idempotency key has a different freeze value");
                    }
                    Self->FillDatabaseFreezeOperation(epoch, result);
                    return true;
                }
            }
        }
        if (request.generation() && request.generation() != tenant->Generation) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Tenant generation does not match requested generation");
        }
        if (!request.idempotency_key() && Self->IsDatabaseFreezePending(tenant)
            && Self->DatabaseFreezeOperations.at(tenant->FreezeEpoch).GetFreeze() == request.freeze())
        {
            Self->FillDatabaseFreezeOperation(tenant->FreezeEpoch, result);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        if (Self->IsDatabaseFreezePending(tenant)) {
            CancelledEpoch = tenant->FreezeEpoch;
            auto& previous = Self->DatabaseFreezeOperations.at(CancelledEpoch);
            previous.SetReady(true);
            previous.SetStatus(Ydb::StatusIds::CANCELLED);
            previous.SetIssue("Superseded by another database freeze request");
            db.Table<Schema::DatabaseFreezeOperations>().Key(CancelledEpoch)
                .Update<Schema::DatabaseFreezeOperations::State>(previous.SerializeAsString());
        }
        Epoch = Self->NextDatabaseFreezeEpoch++;
        Y_ENSURE(Epoch && Self->NextDatabaseFreezeEpoch);
        auto& operation = Self->DatabaseFreezeOperations[Epoch];
        operation.SetPath(tenant->Path);
        operation.SetTenantTxId(tenant->TxId);
        operation.SetDomainOwnerId(tenant->DomainId.OwnerId);
        operation.SetDomainLocalPathId(tenant->DomainId.LocalPathId);
        operation.SetEpoch(Epoch);
        operation.SetFreeze(request.freeze());
        operation.SetIdempotencyKey(request.idempotency_key());
        db.Table<Schema::DatabaseFreezeOperations>().Key(Epoch)
            .Update<Schema::DatabaseFreezeOperations::State>(operation.SerializeAsString());
        tenant->FreezeEpoch = Epoch;
        Self->DbUpdateTenantGeneration(tenant, tenant->Generation + 1, txc, ctx);
        ++tenant->Generation;
        Self->FillDatabaseFreezeOperation(Epoch, result);
        Start = true;
        return true;
    }

    void Complete(const TActorContext& executorCtx) override {
        const auto ctx = executorCtx.MakeFor(Self->SelfId());
        if (CancelledEpoch) {
            auto it = Self->DatabaseFreezeWorkers.find(CancelledEpoch);
            if (it != Self->DatabaseFreezeWorkers.end()) {
                ctx.Send(it->second, new TEvents::TEvPoisonPill);
                Self->DatabaseFreezeWorkers.erase(it);
            }
            Self->NotifyDatabaseFreeze(CancelledEpoch, ctx);
        }
        if (Start) {
            Self->StartDatabaseFreeze(Epoch, ctx);
        }
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
        Self->TxProcessor->TxCompleted(this, ctx);
    }
};

class TTenantsManager::TTxFreezeTenantDone : public TTransactionBase<TTenantsManager> {
    const ui64 Epoch;
    const ui32 Status;
    const TString Issue;
    NKikimrConsole::TDatabaseFreezeOperation Completed;
    bool Changed = false;

public:
    TTxFreezeTenantDone(TTenantsManager* self, ui64 epoch, ui32 status, TString issue)
        : TBase(self), Epoch(epoch), Status(status), Issue(std::move(issue))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& operation = Self->DatabaseFreezeOperations.at(Epoch);
        if (!operation.GetReady()) {
            Completed.CopyFrom(operation);
            Completed.SetReady(true);
            Completed.SetStatus(Status);
            Completed.SetIssue(Issue);
            NIceDb::TNiceDb(txc.DB).Table<Schema::DatabaseFreezeOperations>().Key(Epoch)
                .Update<Schema::DatabaseFreezeOperations::State>(Completed.SerializeAsString());
            Changed = true;
        }
        return true;
    }

    void Complete(const TActorContext& executorCtx) override {
        const auto ctx = executorCtx.MakeFor(Self->SelfId());
        if (Changed && !Self->DatabaseFreezeOperations.at(Epoch).GetReady()) {
            Self->DatabaseFreezeOperations.at(Epoch).CopyFrom(Completed);
        }
        Self->DatabaseFreezeWorkers.erase(Epoch);
        Self->NotifyDatabaseFreeze(Epoch, ctx);
        Self->TxProcessor->TxCompleted(this, ctx);
    }
};

ITransaction* TTenantsManager::CreateTxFreezeTenant(TEvConsole::TEvAlterTenantRequest::TPtr& ev) {
    return new TTxFreezeTenant(this, ev);
}

ITransaction* TTenantsManager::CreateTxFreezeTenantDone(ui64 epoch, ui32 status, const TString& issue) {
    return new TTxFreezeTenantDone(this, epoch, status, issue);
}

void TTenantsManager::Handle(NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult::TPtr& ev, const TActorContext& ctx) {
    const auto epoch = ev->Get()->Record.GetEpoch();
    auto worker = DatabaseFreezeWorkers.find(epoch);
    if (worker != DatabaseFreezeWorkers.end() && worker->second == ev->Sender) {
        TxProcessor->ProcessTx(CreateTxFreezeTenantDone(epoch, ev->Get()->Record.GetStatus(), ev->Get()->Record.GetIssue()), ctx);
    }
}

} // namespace NKikimr::NConsole
