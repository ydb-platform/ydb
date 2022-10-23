#pragma once
#include "config.h"
#include "request_actor.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadataProvider {

class TDSAccessorInitialized;

class TDSAccessorInitialized: public NActors::TActorBootstrapped<TDSAccessorInitialized> {
private:
    YDB_READONLY_FLAG(Initialized, false);
protected:
    const TConfig& Config;
    virtual Ydb::Table::CreateTableRequest GetTableSchema() const = 0;
    virtual void RegisterState() = 0;
public:
    TDSAccessorInitialized(const TConfig& config)
        : Config(config) {

    }
    void Bootstrap(const NActors::TActorContext& /*ctx*/);
    virtual bool Handle(TEvRequestResult<TDialogCreateTable>::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestResult<TDialogCreateTable>, Handle);
            default:
                break;
        }
    }
};

}
