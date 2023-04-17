#pragma once
#include "timeout.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/tablet_pipecache.h>

namespace NKikimr::NMetadata::NInternal {

class TSSDialogActor: public TTimeoutActor<TSSDialogActor> {
private:
    using TBase = TTimeoutActor<TSSDialogActor>;
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
protected:
    virtual void OnFail(const TString& errorMessage) = 0;
    virtual void OnTimeout() override;
    virtual void OnBootstrap() override;
    virtual void Execute() = 0;
    virtual void Die(const TActorContext& ctx) override;
protected:
    ui64 SchemeShardId = 0;
    TActorId SchemeShardPipe;
public:
    using TBase::TBase;

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            default:
                TBase::StateMain(ev);
        }
    }
};

}
