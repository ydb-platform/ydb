#pragma once
#include <ydb/services/ext_index/common/config.h>

#include <ydb/services/metadata/initializer/accessor_init.h>
#include <ydb/services/metadata/ds_table/service.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/ext_index/metadata/snapshot.h>
#include <ydb/services/ext_index/common/service.h>

namespace NKikimr::NCSIndex {

class TExecutor: public NActors::TActorBootstrapped<TExecutor> {
private:
    using TBase = NActors::TActorBootstrapped<TExecutor>;
    TString TableName;
    const TString ExecutorId = TGUID::CreateTimebased().AsUuidString();
    const TConfig Config;
    std::set<TString> CurrentTaskIds;
    NMetadata::NProvider::TEventsWaiter DeferredEventsOnIntialization;
    std::shared_ptr<NMetadata::NCSIndex::TSnapshot> IndexesSnapshot;

    enum class EActivity {
        Created,
        Preparation,
        Active
    };

    EActivity ActivityState = EActivity::Created;

    bool CheckActivity();

protected:
    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr& ev);
    void Handle(TEvAddData::TPtr& ev);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            hFunc(TEvAddData, Handle);
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }

public:
    void Bootstrap();

    TExecutor(const TConfig& config)
        : Config(config) {
        TServiceOperator::Register(Config);
    }
};

IActor* CreateService(const TConfig& config);

}
