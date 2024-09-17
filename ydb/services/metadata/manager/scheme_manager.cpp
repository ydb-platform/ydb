#include "scheme_manager.h"

#include <ydb/services/metadata/common/ss_dialog.h>

namespace NKikimr::NMetadata::NModifications {

namespace {

class TSchemeObjectModificationHandler: public NInternal::TSSDialogActor {
private:
    using TBase = NInternal::TSSDialogActor;
    using TResult = IOperationsManager::TYqlConclusionStatus;

    const NKikimrSchemeOp::TModifyObjectDescription Request;
    NThreading::TPromise<TResult> Promise;

private:
    void Reply(TResult status) {
        auto g = PassAwayGuard();
        Promise.SetValue(std::move(status));
    }

protected:
    virtual void OnBootstrap() override {
        UnsafeBecome(&TSchemeObjectModificationHandler::StateMain);
        TBase::OnBootstrap();
    }

    virtual void OnFail(const TString& errorMessage) override {
        Reply(TResult::Fail(errorMessage));
    }

    virtual void Execute() override {
        auto req = std::make_unique<NSchemeShard::TEvSchemeShard::TEvModifyObject>(Request);
        Send(SchemeShardPipe, new TEvPipeCache::TEvForward(req.release(), SchemeShardId, false));
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvModifyObjectResult::TPtr& ev) {
        std::optional<TString> errorMessage = ev->Get()->Record.HasError() ? std::optional<TString>(ev->Get()->Record.GetError()) : std::nullopt;

        if (errorMessage) {
            Reply(TResult::Fail(*errorMessage));
        } else {
            Reply(TResult::Success());
        }
    }

public:
    explicit TSchemeObjectModificationHandler(
        NKikimrSchemeOp::TModifyObjectDescription request, NThreading::TPromise<TResult> promise, TDuration livetime)
        : TBase(livetime)
        , Request(std::move(request))
        , Promise(std::move(promise)) {
    }

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvModifyObjectResult, Handle);
            default:
                TBase::StateMain(ev);
        }
    }
};

}   // namespace

NThreading::TFuture<IOperationsManager::TYqlConclusionStatus> TSchemeOperationsManagerBase::StartSchemeOperation(
    NKikimrSchemeOp::TModifyObjectDescription description, TActorSystem& actorSystem, TDuration livetime) {
    auto promise = NThreading::NewPromise<IOperationsManager::TYqlConclusionStatus>();
    actorSystem.Register(new TSchemeObjectModificationHandler(std::move(description), promise, livetime));
    return promise.GetFuture();
}

}   // namespace NKikimr::NMetadata::NModifications
