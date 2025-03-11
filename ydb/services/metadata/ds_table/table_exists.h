#pragma once

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/services/metadata/common/ss_dialog.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NProvider {

class ITableExistsController {
public:
    using TPtr = std::shared_ptr<ITableExistsController>;
    virtual ~ITableExistsController() = default;
    virtual void OnPathExistsCheckFailed(const TString& errorMessage, const TString& path) const = 0;
    virtual void OnPathExistsCheckResult(const bool exists, const TString& path) const = 0;
};

class TTableExistsActor: public NInternal::TTimeoutActor<TTableExistsActor> {
private:
    using TBase = NInternal::TTimeoutActor<TTableExistsActor>;
    ITableExistsController::TPtr OutputController;
    const TString Path;
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
    virtual void OnBootstrap() override;
    virtual void OnTimeout() override;
public:

    class TEvController: public ITableExistsController {
    private:
        const TActorIdentity ActorId;
    public:
        class TEvError: public TEventLocal<TEvError, EvPathExistsCheckFailed> {
        private:
            YDB_READONLY_DEF(TString, ErrorMessage);
            YDB_READONLY_DEF(TString, Path);
        public:
            TEvError(const TString& errorMessage, const TString& path)
                : ErrorMessage(errorMessage)
                , Path(path)
            {

            }
        };

        class TEvResult: public TEventLocal<TEvResult, EvPathExistsCheckResult> {
        private:
            YDB_READONLY_FLAG(TableExists, false);
            YDB_READONLY_DEF(TString, TablePath);
        public:
            TEvResult(const bool exists, const TString& path)
                : TableExistsFlag(exists)
                , TablePath(path) {

            }
        };

        virtual void OnPathExistsCheckFailed(const TString& errorMessage, const TString& path) const override {
            ActorId.Send(ActorId, new TEvError(errorMessage, path));
        }
        virtual void OnPathExistsCheckResult(const bool exists, const TString& path) const override {
            ActorId.Send(ActorId, new TEvResult(exists, path));
        }

        TEvController(const TActorIdentity& actorId)
            : ActorId(actorId) {

        }
    };

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            default:
                TBase::StateMain(ev);
        }
    }
    TTableExistsActor(ITableExistsController::TPtr controller, const TString& path, const TDuration d)
        : TBase(d)
        , OutputController(controller)
        , Path(path)
    {

    }
};

}
