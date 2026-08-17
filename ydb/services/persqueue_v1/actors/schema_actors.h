#pragma once

#include "events.h"
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/util/backoff.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <optional>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

class TPQDescribeTopicActor : public TPQGrpcSchemaBase<TPQDescribeTopicActor, NKikimr::NGRpcService::TEvPQDescribeTopicRequest>
                            , public TCdcStreamCompatible
{
using TBase = TPQGrpcSchemaBase<TPQDescribeTopicActor, TEvPQDescribeTopicRequest>;

public:
     TPQDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request);
    ~TPQDescribeTopicActor() = default;

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
};

struct TDescribeTopicActorSettings {
    enum class EMode {
        DescribeTopic,
        DescribeConsumer,
        DescribePartitions,
    };
    EMode Mode;
    TString Consumer;
    TSet<TString> Consumers;
    TVector<ui32> Partitions;
    bool RequireStats = false;
    bool RequireLocation = false;

    TDescribeTopicActorSettings(EMode mode, bool requireStats, bool requireLocation)
        : Mode(mode)
        , RequireStats(requireStats)
        , RequireLocation(requireLocation)
    {}

    static TDescribeTopicActorSettings DescribeTopic(bool requireStats, bool requireLocation, const TVector<ui32>& partitions = {}) {
        TDescribeTopicActorSettings res{EMode::DescribeTopic, requireStats, requireLocation};
        res.Partitions = partitions;
        return res;
    }

    static TDescribeTopicActorSettings DescribeConsumer(const TString& consumer, bool requireStats, bool requireLocation)
    {
        TDescribeTopicActorSettings res{EMode::DescribeConsumer, requireStats, requireLocation};
        res.Consumer = consumer;
        return res;
    }

    static TDescribeTopicActorSettings GetPartitionsLocation(const TVector<ui32>& partitions) {
        TDescribeTopicActorSettings res{EMode::DescribePartitions, false, true};
        res.Partitions = partitions;
        return res;
    }

    static TDescribeTopicActorSettings DescribePartitionSettings(ui32 partition, bool stats, bool location) {
        TDescribeTopicActorSettings res{EMode::DescribePartitions, stats, location};
        res.Partitions = {partition};
        return res;
    }

};

class TDescribeTopicActorImpl
{
protected:
    static constexpr TDuration RequestTimeout = TDuration::Seconds(30);

    struct TTabletInfo {
        ui64 TabletId = 0;
        std::vector<ui32> Partitions;
        TActorId Pipe;
        ui32 NodeId = 0;
        ui32 RetriesLeft = 3;
        bool ResultRecived = false;
        ui64 Generation = 0;
        TTabletInfo() = default;
        TTabletInfo(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

public:
    TDescribeTopicActorImpl(const TDescribeTopicActorSettings& settings);
    virtual ~TDescribeTopicActorImpl() = default;

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);

    void Handle(NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvRequestTablet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    bool ProcessTablets(const NKikimrSchemeOp::TPersQueueGroupDescription& description, const TActorContext& ctx);

    void RequestTablet(TTabletInfo& tablet, const TActorContext& ctx);
    void RequestTablet(ui64 tabletId, const TActorContext& ctx);
    void RestartTablet(ui64 tabletId, const TActorContext& ctx, TActorId pipe = {}, const TDuration& delay = TDuration::Zero());
    void RequestBalancer(const TActorContext& ctx);
    void RequestPartitionStatus(const TTabletInfo& tablet, const TActorContext& ctx);
    void RequestPartitionsLocation(const TActorContext& ctx);
    void RequestReadSessionsInfo(const TActorContext& ctx);
    void CheckCloseBalancerPipe(const TActorContext& ctx);

    bool StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    virtual void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) = 0;

    virtual void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
                            const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) = 0;
    virtual void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev,
                               const TActorContext& ctx) = 0;
    virtual void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev,
                               const TActorContext& ctx) = 0;
    virtual bool ApplyResponse(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr&, const TActorContext&) = 0;

    virtual void Reply(const TActorContext& ctx) = 0;

    void PassAway(const TActorContext& ctx);

private:
    void CancelRequestTimeout(const TActorContext& ctx);

    std::map<ui64, TTabletInfo> Tablets;
    ui32 RequestsInfly = 0;

    bool GotLocation = false;
    bool GotReadSessions = false;
    TBackoff LocationsBackoff = TBackoff(25, TDuration::MilliSeconds(10), TDuration::MilliSeconds(100));
    TActorId TimeoutTimerActorId;
    std::optional<TInstant> RequestStartTime;

    TDuration RemainingRequestTimeout() const;

protected:
    ui64 BalancerTabletId = 0;
    ui32 TotalPartitions = 0;
    TDescribeTopicActorSettings Settings;
};

} // namespace NKikimr::NGRpcProxy::V1
