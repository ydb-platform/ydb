#include "registry_actor.h"

#include "insert_publication_query.h"
#include "tables_creator.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

struct TPendingBeginPublication {
    TString Database;
    TString ExtPublicationId;
    TMaybe<TString> WriterIdentity;
    TString CreatedBy;
    NActors::TActorId ReplyTo;
};

class TDeferredPublishRegistryActor : public NActors::TActorBootstrapped<TDeferredPublishRegistryActor> {
    enum class ETablesStatus {
        NotReady,
        Pending,
        Ready,
    };

    struct TDatabaseState {
        ETablesStatus TablesStatus = ETablesStatus::NotReady;
        TVector<TPendingBeginPublication> PendingRequests;
    };

public:
    void Bootstrap() {
        Become(&TDeferredPublishRegistryActor::StateFunc);
    }

    void Handle(TEvBeginPublicationRequest::TPtr& ev) {
        if (ShuttingDown) {
            ReplyAborted(ev->Sender);
            return;
        }

        const auto& request = *ev->Get();
        auto& state = Databases[request.Database];

        switch (state.TablesStatus) {
            case ETablesStatus::Ready:
                StartInsert(request.Database, request.ExtPublicationId, request.WriterIdentity,
                    request.CreatedBy, ev->Sender);
                return;
            case ETablesStatus::Pending:
                state.PendingRequests.emplace_back(TPendingBeginPublication{
                    .Database = request.Database,
                    .ExtPublicationId = request.ExtPublicationId,
                    .WriterIdentity = request.WriterIdentity,
                    .CreatedBy = request.CreatedBy,
                    .ReplyTo = ev->Sender,
                });
                return;
            case ETablesStatus::NotReady:
                state.PendingRequests.emplace_back(TPendingBeginPublication{
                    .Database = request.Database,
                    .ExtPublicationId = request.ExtPublicationId,
                    .WriterIdentity = request.WriterIdentity,
                    .CreatedBy = request.CreatedBy,
                    .ReplyTo = ev->Sender,
                });
                state.TablesStatus = ETablesStatus::Pending;
                Register(CreateDeferredPublishTablesCreator(request.Database));
                return;
        }
    }

    void Handle(TEvTablesCreationFinished::TPtr& ev) {
        auto& state = Databases[ev->Get()->Database];
        Y_ABORT_UNLESS(state.TablesStatus == ETablesStatus::Pending);

        TVector<TPendingBeginPublication> pending = std::move(state.PendingRequests);
        state.PendingRequests.clear();

        if (ShuttingDown) {
            state.TablesStatus = ETablesStatus::NotReady;
            for (const auto& request : pending) {
                ReplyAborted(request.ReplyTo);
            }
            TryPassAway();
            return;
        }

        if (ev->Get()->Success) {
            state.TablesStatus = ETablesStatus::Ready;
            for (const auto& request : pending) {
                StartInsert(request.Database, request.ExtPublicationId, request.WriterIdentity,
                    request.CreatedBy, request.ReplyTo);
            }
            return;
        }

        state.TablesStatus = ETablesStatus::NotReady;

        NYql::TIssues issues = ev->Get()->Issues;
        if (issues.Empty()) {
            issues.AddIssue("Failed to create deferred publish registry tables");
        }

        for (const auto& request : pending) {
            auto* response = new TEvBeginPublicationResponse;
            response->Status = Ydb::StatusIds::INTERNAL_ERROR;
            response->Issues = issues;
            Send(request.ReplyTo, response);
        }
    }

    void Handle(TEvInsertPublicationFinished::TPtr& ev) {
        if (InFlightInserts == 0) {
            return;
        }
        --InFlightInserts;

        auto* response = new TEvBeginPublicationResponse;
        response->Status = ev->Get()->Status;
        response->Issues = ev->Get()->Issues;
        response->IntPublicationId = ev->Get()->IntPublicationId;
        Send(ev->Get()->ReplyTo, response);

        TryPassAway();
    }

    void HandlePoison() {
        ShuttingDown = true;

        for (auto& [database, state] : Databases) {
            Y_UNUSED(database);
            for (const auto& request : state.PendingRequests) {
                ReplyAborted(request.ReplyTo);
            }
            state.PendingRequests.clear();
            state.TablesStatus = ETablesStatus::NotReady;
        }

        TryPassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBeginPublicationRequest, Handle);
            hFunc(TEvTablesCreationFinished, Handle);
            hFunc(TEvInsertPublicationFinished, Handle);
            sFunc(TEvents::TEvPoison, HandlePoison);
            default:
                break;
        }
    }

private:
    void ReplyAborted(const NActors::TActorId& replyTo) {
        NYql::TIssues issues;
        issues.AddIssue("Deferred publish registry is shutting down");

        auto* response = new TEvBeginPublicationResponse;
        response->Status = Ydb::StatusIds::ABORTED;
        response->Issues = issues;
        Send(replyTo, response);
    }

    void TryPassAway() {
        if (ShuttingDown && InFlightInserts == 0) {
            PassAway();
        }
    }

    void StartInsert(
        const TString& database,
        const TString& extPublicationId,
        const TMaybe<TString>& writerIdentity,
        const TString& createdBy,
        const NActors::TActorId& replyTo)
    {
        Y_ABORT_UNLESS(!ShuttingDown);
        ++InFlightInserts;
        Register(CreateInsertPublicationQueryActor(replyTo, database, extPublicationId, writerIdentity, createdBy));
    }

    bool ShuttingDown = false;
    ui32 InFlightInserts = 0;
    THashMap<TString, TDatabaseState> Databases;
};

} // namespace

NActors::IActor* CreateDeferredPublishRegistryActor() {
    return new TDeferredPublishRegistryActor();
}

} // namespace NKikimr::NPQ::NDeferredPublish
