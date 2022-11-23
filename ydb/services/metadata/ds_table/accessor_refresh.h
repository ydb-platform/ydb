#pragma once
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/initializer/accessor_init.h>
#include <ydb/services/metadata/request/request_actor.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NMetadataProvider {

class TDSAccessorRefresher;

class TEvRefresh: public NActors::TEventLocal<TEvRefresh, EEvSubscribe::EvRefresh> {
public:
};

class TEvYQLResponse: public NActors::TEventLocal<TEvYQLResponse, EEvSubscribe::EvYQLResponse> {
private:
    YDB_READONLY_DEF(NInternal::NRequest::TDialogYQLRequest::TResponse, Response);
public:
    TEvYQLResponse(const NInternal::NRequest::TDialogYQLRequest::TResponse& r)
        : Response(r)
    {

    }
};

class TEvEnrichSnapshotResult: public NActors::TEventLocal<TEvEnrichSnapshotResult, EEvSubscribe::EvEnrichSnapshotResult> {
private:
    YDB_READONLY_DEF(ISnapshot::TPtr, EnrichedSnapshot);
public:
    TEvEnrichSnapshotResult(ISnapshot::TPtr snapshot)
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

class TRefreshInternalController: public ISnapshotAcceptorController, public NInternal::NRequest::IQueryOutput {
private:
    const TActorIdentity ActorId;
public:
    TRefreshInternalController(const TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void EnrichProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotProblem(errorMessage));
    }

    virtual void Enriched(ISnapshot::TPtr enrichedSnapshot) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotResult(enrichedSnapshot));
    }

    virtual void OnReply(const NInternal::NRequest::TDialogYQLRequest::TResponse& response) override {
        ActorId.Send(ActorId, new TEvYQLResponse(response));
    }
};

class TDSAccessorRefresher: public NActors::TActorBootstrapped<TDSAccessorRefresher> {
private:
    using TBase = NActors::TActorBootstrapped<TDSAccessorRefresher>;
    ISnapshotParser::TPtr SnapshotConstructor;
    std::shared_ptr<TRefreshInternalController> InternalController;
    YDB_READONLY_DEF(ISnapshot::TPtr, CurrentSnapshot);
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

    TDSAccessorRefresher(const TConfig& config, ISnapshotParser::TPtr snapshotConstructor);

    void Handle(TEvEnrichSnapshotResult::TPtr& ev);
    void Handle(TEvEnrichSnapshotProblem::TPtr& ev);
    void Handle(TEvYQLResponse::TPtr& ev);
    void Handle(TEvRefresh::TPtr& ev);
};

}
