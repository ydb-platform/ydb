#pragma once
#include "table_exists.h"
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/initializer/accessor_init.h>
#include <ydb/services/metadata/request/request_actor_cb.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NMetadata::NProvider {

class TEvRecheckExistence: public NActors::TEventLocal<TEvRecheckExistence, EEvents::EvRecheckExistence> {
private:
    YDB_READONLY_DEF(TString, Path);
public:
    TEvRecheckExistence(const TString& path)
        : Path(path)
    {

    }
};

class TEvRefresh: public NActors::TEventLocal<TEvRefresh, EEvents::EvRefresh> {
public:
};

class TEvEnrichSnapshotResult: public NActors::TEventLocal<TEvEnrichSnapshotResult, EEvents::EvEnrichSnapshotResult> {
private:
    YDB_READONLY_DEF(NFetcher::ISnapshot::TPtr, EnrichedSnapshot);
public:
    TEvEnrichSnapshotResult(NFetcher::ISnapshot::TPtr snapshot)
        : EnrichedSnapshot(snapshot) {

    }
};

class TEvEnrichSnapshotProblem: public NActors::TEventLocal<TEvEnrichSnapshotProblem, EEvents::EvEnrichSnapshotProblem> {
private:
    YDB_READONLY_DEF(TString, ErrorText);
public:
    TEvEnrichSnapshotProblem(const TString& errorText)
        : ErrorText(errorText) {

    }
};

class TRefreshInternalController: public NFetcher::ISnapshotAcceptorController,
    public NRequest::TNaiveExternalController<NRequest::TDialogYQLRequest>,
    public TTableExistsActor::TEvController {
private:
    using TBaseRequestController = NRequest::TNaiveExternalController<NRequest::TDialogYQLRequest>;
    const TActorIdentity ActorId;
public:
    TRefreshInternalController(const TActorIdentity& actorId)
        : TBaseRequestController(actorId)
        , TTableExistsActor::TEvController(actorId)
        , ActorId(actorId) {

    }

    virtual void OnSnapshotEnrichError(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotProblem(errorMessage));
    }

    virtual void OnSnapshotEnriched(NFetcher::ISnapshot::TPtr enrichedSnapshot) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotResult(enrichedSnapshot));
    }
};

class TDSAccessorBase: public NActors::TActorBootstrapped<TDSAccessorBase> {
private:
    using TBase = NActors::TActorBootstrapped<TDSAccessorBase>;
    YDB_READONLY(TInstant, RequestedActuality, TInstant::Zero());
    const NRequest::TConfig Config;
    std::map<TString, i32> ExistenceChecks;
    std::map<TString, i32> CurrentExistence;
    void StartSnapshotsFetchingImpl();
protected:
    std::shared_ptr<TRefreshInternalController> InternalController;
    NFetcher::ISnapshotsFetcher::TPtr SnapshotConstructor;

    virtual void OnBootstrap() {
        Become(&TDSAccessorBase::StateMain);
    }
    virtual void OnNewEnrichedSnapshot(NFetcher::ISnapshot::TPtr snapshot) = 0;
    virtual void OnNewParsedSnapshot(Ydb::Table::ExecuteQueryResult&& qResult, NFetcher::ISnapshot::TPtr snapshot);
    virtual void OnConstructSnapshotError(const TString& errorMessage);
    void StartSnapshotsFetching();

    void Handle(TEvRecheckExistence::TPtr& ev);
    void Handle(TEvEnrichSnapshotResult::TPtr& ev);
    void Handle(TEvEnrichSnapshotProblem::TPtr& ev);
    void Handle(NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>::TPtr& ev);
    void Handle(NRequest::TEvRequestFailed::TPtr& ev);
    void Handle(TTableExistsActor::TEvController::TEvError::TPtr& ev);
    void Handle(TTableExistsActor::TEvController::TEvResult::TPtr& ev);
public:
    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>, Handle);
            hFunc(NRequest::TEvRequestFailed, Handle);
            hFunc(TEvEnrichSnapshotResult, Handle);
            hFunc(TEvEnrichSnapshotProblem, Handle);
            hFunc(TTableExistsActor::TEvController::TEvError, Handle);
            hFunc(TTableExistsActor::TEvController::TEvResult, Handle);
            hFunc(TEvRecheckExistence, Handle);
            
            default:
                break;
        }
    }

    TDSAccessorBase(const NRequest::TConfig& config, NFetcher::ISnapshotsFetcher::TPtr snapshotConstructor);
};

}
