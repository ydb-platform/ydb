#pragma once
#include "object.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/services/metadata/common/ss_dialog.h>

namespace NKikimr::NColumnShard::NTiers {

class ISSFetchingController {
public:
    using TPtr = std::shared_ptr<ISSFetchingController>;
    virtual ~ISSFetchingController() = default;
    virtual void FetchingProblem(const TString& errorMessage) const = 0;
    virtual void FetchingResult(const NKikimrScheme::TEvProcessingResponse& result) const = 0;
};

class TSSFetchingController: public ISSFetchingController {
private:
    const TActorIdentity ActorId;
public:
    TSSFetchingController(const TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void FetchingProblem(const TString& errorMessage) const override {
        ActorId.Send(ActorId, new NSchemeShard::TEvSchemeShard::TEvProcessingResponse(errorMessage));
    }
    virtual void FetchingResult(const NKikimrScheme::TEvProcessingResponse& result) const override {
        ActorId.Send(ActorId, new NSchemeShard::TEvSchemeShard::TEvProcessingResponse(result));
    }
};

class TSSFetchingActor: public NMetadata::NInternal::TSSDialogActor {
private:
    using TBase = NMetadata::NInternal::TSSDialogActor;
    NSchemeShard::ISSDataProcessor::TPtr Processor;
    ISSFetchingController::TPtr Controller;
    void Handle(NSchemeShard::TEvSchemeShard::TEvProcessingResponse::TPtr& ev);
protected:
    virtual void OnBootstrap() override {
        UnsafeBecome(&TSSFetchingActor::StateMain);
        TBase::OnBootstrap();
    }
    virtual void OnFail(const TString& errorMessage) override {
        Controller->FetchingProblem(errorMessage);
    }
    virtual void Execute() override {
        auto req = std::make_unique<NSchemeShard::TEvSchemeShard::TEvProcessingRequest>(*Processor);
        Send(SchemeShardPipe, new TEvPipeCache::TEvForward(req.release(), SchemeShardId, false));
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType();
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvProcessingResponse, Handle);
            default:
                TBase::StateMain(ev);
        }
    }
    TSSFetchingActor(NSchemeShard::ISSDataProcessor::TPtr processor, ISSFetchingController::TPtr controller, const TDuration livetime);
};

}
