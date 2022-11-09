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

class TSnapshotAcceptorController: public ISnapshotAcceptorController {
private:
    const TActorIdentity ActorId;
public:
    TSnapshotAcceptorController(const TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void EnrichProblem(const TString& errorMessage) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotProblem(errorMessage));
    }

    virtual void Enriched(ISnapshot::TPtr enrichedSnapshot) override {
        ActorId.Send(ActorId, new TEvEnrichSnapshotResult(enrichedSnapshot));
    }
};

class TDSAccessorRefresher: public NMetadataInitializer::TDSAccessorInitialized {
private:
    using TBase = NMetadataInitializer::TDSAccessorInitialized;
    ISnapshotParser::TPtr SnapshotConstructor;
    YDB_READONLY_DEF(ISnapshot::TPtr, CurrentSnapshot);
    YDB_READONLY_DEF(Ydb::Table::ExecuteQueryResult, CurrentSelection);
    TInstant RequestedActuality = TInstant::Zero();
    const TConfig Config;

    mutable ISnapshotAcceptorController::TPtr ControllerImpl;

    ISnapshotAcceptorController::TPtr GetController() const;
protected:
    virtual void Prepare(NMetadataInitializer::IController::TPtr controller) override {
        SnapshotConstructor->Prepare(controller);
    }
    bool IsReady() const {
        return !!CurrentSnapshot;
    }
    virtual void OnInitialized() override;
    virtual void OnSnapshotModified() = 0;
public:
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogSelect>, Handle);
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogCreateSession>, Handle);
            hFunc(TEvRefresh, Handle);
            hFunc(TEvEnrichSnapshotResult, Handle);
            hFunc(TEvEnrichSnapshotProblem, Handle);
            default:
                TBase::StateMain(ev, ctx);
        }
    }

    TDSAccessorRefresher(const TConfig& config, ISnapshotParser::TPtr snapshotConstructor);

    void Handle(TEvEnrichSnapshotResult::TPtr& ev);
    void Handle(TEvEnrichSnapshotProblem::TPtr& ev);
    void Handle(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogSelect>::TPtr& ev);
    void Handle(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogCreateSession>::TPtr& ev);
    void Handle(TEvRefresh::TPtr& ev);
};

}
