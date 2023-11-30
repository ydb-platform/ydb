#pragma once
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/kesus.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/containers/ring_buffer/ring_buffer.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NQuoter {

struct ITabletPipeFactory {
    virtual NActors::IActor* CreateTabletPipe(const NActors::TActorId& owner, ui64 tabletId, const NKikimr::NTabletPipe::TClientConfig& config = NKikimr::NTabletPipe::TClientConfig()) = 0;

    virtual ~ITabletPipeFactory() = default;

    static THolder<ITabletPipeFactory> GetDefaultFactory();
};

NActors::IActor* CreateKesusQuoterProxy(ui64 quoterId, const NSchemeCache::TSchemeCacheNavigate::TEntry& navEntry, const NActors::TActorId& quoterServiceId, THolder<ITabletPipeFactory> tabletPipeFactory = ITabletPipeFactory::GetDefaultFactory());

class TKesusResourceAllocationStatistics {
public:
    explicit TKesusResourceAllocationStatistics(size_t windowSize = 100);

    void SetProps(const NKikimrKesus::TStreamingQuoterResource& props);

    void OnConnected();
    void OnResourceAllocated(TInstant now, double amount);

    std::pair<TDuration, double> GetAverageAllocationParams() const;

private:
    struct TStatItem {
        TInstant Time;
        double Amount;
    };

private:
    static std::pair<TDuration, double> GetAverageAllocationParams(const TSimpleRingBuffer<TStatItem>& stat);

private:
    TSimpleRingBuffer<TStatItem> BestPrevStat; // Full stat that was made before current connection
    TSimpleRingBuffer<TStatItem> Stat;
    TDuration DefaultAllocationDelta;
    double DefaultAllocationAmount = 0;
};

} // namespace NQuoter
} // namespace NKikimr
