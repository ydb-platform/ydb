#include "kqp_vector_index_levels_cache.h"
#include "kqp_read_iterator_common.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/scheme_board/subscriber.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

namespace NKikimr::NKqp {

namespace {

constexpr ui64 LevelCacheTxId = std::numeric_limits<ui64>::max() - 1;

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "VectorIndexLevelsCacheMaintainer: " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "VectorIndexLevelsCacheMaintainer: " << stream)


class TVectorIndexLevelsCacheMaintainer
    : public TActorBootstrapped<TVectorIndexLevelsCacheMaintainer>
{
    using TBase = TActorBootstrapped<TVectorIndexLevelsCacheMaintainer>;

public:
    explicit TVectorIndexLevelsCacheMaintainer(
        TIntrusivePtr<TVectorIndexLevelsCache> cache,
        std::shared_ptr<NRm::IKqpResourceManager> rm,
        const NKikimrConfig::TTableServiceConfig::TResourceManager& initialConfig)
        : Cache_(std::move(cache))
        , ResourceManager(rm)
        , Tx(MakeIntrusive<NRm::TTxState>(ResourceManager, LevelCacheTxId, TInstant::Now(), NKikimr::NResourcePool::DEFAULT_POOL_ID, 100.0, AppData()->TenantName, false))
        , RmConfig(initialConfig)
    {}

    struct TEvPrivate {
        enum EEv {
            EvIncreaseCacheSize = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvIncreaseCacheSize: public TEventLocal<TEvIncreaseCacheSize, EEv::EvIncreaseCacheSize> {};
    };

    void Bootstrap() {
        Become(&TThis::StateWork);
        UpdateCacheSize();

        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
            IEventHandle::FlagTrackDelivery);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
            cFunc(TEvPrivate::EvIncreaseCacheSize, UpdateCacheSize);
        }
    }

    void PassAway() override {
        TBase::PassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_E("Failed to deliver subscription request to config dispatcher");
                break;
            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
                break;
            default:
                LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;

        auto newConfig = event.GetConfig().GetTableServiceConfig();

        RmConfig.Swap(newConfig.MutableResourceManager());
        UpdateCacheSize();
    }

    void UpdateCacheSize() {
        i64 maxCurrentSizeBytes = Cache_->MaxBytes();
        i64 leftBytes = static_cast<i64>(maxCurrentSizeBytes) - static_cast<i64>(Cache_->Bytes());
        ui64 increaseBatchSize = RmConfig.GetKqpLevelCacheIncreaseBatchSizeBytes();

        ui64 maxAllowedSizeBytes = RmConfig.GetKqpLevelCacheMaxSizeBytes();

        if ((maxCurrentSizeBytes == 0 || leftBytes < static_cast<i64>(increaseBatchSize))
            && static_cast<ui64>(maxCurrentSizeBytes) + increaseBatchSize <= maxAllowedSizeBytes) {

            auto res = ResourceManager->AllocateResources(*Tx, 1, NRm::TKqpResourcesRequest{.Memory=increaseBatchSize});
            if (res) {
                Cache_->SetMaxBytes(maxCurrentSizeBytes + increaseBatchSize);
                LOG_N("Altered max bytes to " << HumanReadableSize(maxCurrentSizeBytes + increaseBatchSize, ESizeFormat::SF_BYTES)
                    << ", prev size " << HumanReadableSize(maxCurrentSizeBytes, ESizeFormat::SF_BYTES));
            }

        } else if (maxAllowedSizeBytes < static_cast<ui64>(maxCurrentSizeBytes)) {
            ui64 deficit = static_cast<ui64>(maxCurrentSizeBytes) - maxAllowedSizeBytes;
            ui64 change = std::min(increaseBatchSize, deficit);
            ResourceManager->FreeResources(*Tx, 1, NRm::TKqpResourcesRequest{.Memory=change});
            i64 newSize = maxCurrentSizeBytes - static_cast<i64>(change);
            Cache_->SetMaxBytes(newSize);
            LOG_N("Altered max bytes to " << HumanReadableSize(newSize, ESizeFormat::SF_BYTES)
                << ", prev size " << HumanReadableSize(maxCurrentSizeBytes, ESizeFormat::SF_BYTES));
        }

        Schedule(TDuration::Seconds(1), new TEvPrivate::TEvIncreaseCacheSize);
    }

private:
    TIntrusivePtr<TVectorIndexLevelsCache> Cache_;

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    TIntrusivePtr<NRm::TTxState> Tx;
    NKikimrConfig::TTableServiceConfig::TResourceManager RmConfig;
};

#undef LOG_E
#undef LOG_N

} // anonymous namespace

IActor* CreateVectorIndexLevelsCacheMaintainer(
    TIntrusivePtr<TVectorIndexLevelsCache> cache,
    std::shared_ptr<NRm::IKqpResourceManager> rm,
    const NKikimrConfig::TTableServiceConfig::TResourceManager& initialConfig)
{
    return new TVectorIndexLevelsCacheMaintainer(std::move(cache), std::move(rm), initialConfig);
}


} // namespace NKikimr::NKqp
