#pragma once
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/initializer/accessor_init.h>
#include <ydb/services/metadata/request/request_actor.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NMetadata::NProvider {

class TDSAccessorRefresher;

class TEvRefresh: public NActors::TEventLocal<TEvRefresh, EEvSubscribe::EvRefresh> {
public:
};

class TEvYQLResponse: public NActors::TEventLocal<TEvYQLResponse, EEvSubscribe::EvYQLResponse> {
private:
    YDB_READONLY_DEF(NRequest::TDialogYQLRequest::TResponse, Response);
public:
    TEvYQLResponse(const NRequest::TDialogYQLRequest::TResponse& r)
        : Response(r)
    {

    }
};

class TEvEnrichSnapshotResult: public NActors::TEventLocal<TEvEnrichSnapshotResult, EEvSubscribe::EvEnrichSnapshotResult> {
private:
    YDB_READONLY_DEF(NFetcher::ISnapshot::TPtr, EnrichedSnapshot);
public:
    TEvEnrichSnapshotResult(NFetcher::ISnapshot::TPtr snapshot)
        : EnrichedSnapshot(snapshot) {

    }
};

class TEvEnrichSnapshotProblem: public NActors::TEventLocal<TEvEnrichSnapshotProblem, EEvSubscribe::EvEnrichSnapshotProblem> {
private:
    YDB_READONLY_DEF(TString, ErrorText);
public:
    TEvEnrichSnapshotProblem(const TString& errorText)
        : ErrorText(errorText) {

    }
};

class TRefreshInternalController: public NFetcher::ISnapshotAcceptorController, public NRequest::IQueryOutput {
private:
    const TActorIdentity ActorId;
public:
    TRefreshInternalController(const TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void EnrichProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotProblem(errorMessage));
    }

    virtual void Enriched(NFetcher::ISnapshot::TPtr enrichedSnapshot) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotResult(enrichedSnapshot));
    }

    virtual void OnReply(const NRequest::TDialogYQLRequest::TResponse& response) override {
        ActorId.Send(ActorId, new TEvYQLResponse(response));
    }
};

class TDSAccessorRefresher: public NActors::TActorBootstrapped<TDSAccessorRefresher> {
private:
    using TBase = NActors::TActorBootstrapped<TDSAccessorRefresher>;
    NFetcher::ISnapshotsFetcher::TPtr SnapshotConstructor;
    std::shared_ptr<TRefreshInternalController> InternalController;
    YDB_READONLY_DEF(NFetcher::ISnapshot::TPtr, CurrentSnapshot);
    YDB_READONLY_DEF(Ydb::Table::ExecuteQueryResult, CurrentSelection);
    Ydb::Table::ExecuteQueryResult ProposedProto;
    TInstant RequestedActuality = TInstant::Zero();
    const TConfig Config;
protected:
    virtual void RegisterState() {
        Become(&TDSAccessorRefresher::StateMain);
    }
    bool IsReady() const {
        return !!CurrentSnapshot;
    }
    virtual void OnSnapshotModified() = 0;
    virtual void OnSnapshotRefresh() = 0;
public:
    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYQLResponse, Handle);
            hFunc(TEvRefresh, Handle);
            hFunc(TEvEnrichSnapshotResult, Handle);
            hFunc(TEvEnrichSnapshotProblem, Handle);
            default:
                break;
        }
    }

    TDSAccessorRefresher(const TConfig& config, NFetcher::ISnapshotsFetcher::TPtr snapshotConstructor);

    void Handle(TEvEnrichSnapshotResult::TPtr& ev);
    void Handle(TEvEnrichSnapshotProblem::TPtr& ev);
    void Handle(TEvYQLResponse::TPtr& ev);
    void Handle(TEvRefresh::TPtr& ev);
};

}
