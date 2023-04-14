#pragma once
#include "accessor_snapshot_base.h"
#include "table_exists.h"

namespace NKikimr::NMetadata::NProvider {

class ISnapshotConstructorController {
public:
    using TPtr = std::shared_ptr<ISnapshotConstructorController>;
    virtual ~ISnapshotConstructorController() = default;
    virtual void OnSnapshotConstructionResult(NFetcher::ISnapshot::TPtr snapshot) const = 0;
    virtual void OnSnapshotConstructionError(const TString& errorMessage) const = 0;
    virtual void OnSnapshotConstructionTableAbsent(const TString& path) const = 0;
};

class TDSAccessorSimple: public TDSAccessorBase {
private:
    using TBase = TDSAccessorBase;
    std::set<TString> PathesInCheck;

    class TInputController: public TTableExistsActor::TEvController {
    public:
        TInputController(const TActorIdentity& actorId)
            : TTableExistsActor::TEvController(actorId) {

        }
    };

    std::shared_ptr<TInputController> InputController;
    ISnapshotConstructorController::TPtr OutputController;
protected:
    virtual void OnNewEnrichedSnapshot(NFetcher::ISnapshot::TPtr snapshot) final;
    virtual void OnConstructSnapshotError(const TString& errorMessage) final;

    virtual void OnBootstrap() override;

    void Handle(TTableExistsActor::TEvController::TEvError::TPtr& ev);

    void Handle(TTableExistsActor::TEvController::TEvResult::TPtr& ev);

public:
    class TEvController: public ISnapshotConstructorController {
    private:
        const TActorIdentity ActorId;
    public:
        class TEvResult: public TEventLocal<TEvResult, EEvents::EvAccessorSimpleResult> {
        private:
            YDB_READONLY_DEF(NFetcher::ISnapshot::TPtr, Result);
        public:
            TEvResult(NFetcher::ISnapshot::TPtr snapshot)
                : Result(snapshot) {

            }
        };

        class TEvError: public TEventLocal<TEvError, EEvents::EvAccessorSimpleError> {
        private:
            YDB_READONLY_DEF(TString, ErrorMessage);
        public:
            TEvError(const TString& errorMessage)
                : ErrorMessage(errorMessage) {

            }
        };

        class TEvTableAbsent: public TEventLocal<TEvTableAbsent, EEvents::EvAccessorSimpleTableAbsent> {
        private:
            YDB_READONLY_DEF(TString, Path);
        public:
            TEvTableAbsent(const TString& path)
                : Path(path) {

            }
        };

        virtual void OnSnapshotConstructionResult(NFetcher::ISnapshot::TPtr snapshot) const override {
            ActorId.Send(ActorId, new TEvResult(snapshot));
        }

        virtual void OnSnapshotConstructionError(const TString& errorMessage) const override {
            ActorId.Send(ActorId, new TEvError(errorMessage));
        }

        virtual void OnSnapshotConstructionTableAbsent(const TString& path) const override {
            ActorId.Send(ActorId, new TEvTableAbsent(path));
        }

        TEvController(const TActorIdentity& actorId)
            : ActorId(actorId)
        {

        }
    };

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TTableExistsActor::TEvController::TEvError, Handle);
            hFunc(TTableExistsActor::TEvController::TEvResult, Handle);
            default:
                TBase::StateMain(ev);
        }
    }
    TDSAccessorSimple(const NRequest::TConfig& config, ISnapshotConstructorController::TPtr outputController,
        NFetcher::ISnapshotsFetcher::TPtr snapshotConstructor)
        : TBase(config, snapshotConstructor)
        , OutputController(outputController){

    }
};

}
