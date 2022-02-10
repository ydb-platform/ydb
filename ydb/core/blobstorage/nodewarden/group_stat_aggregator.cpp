#include "group_stat_aggregator.h"
#include <ydb/core/base/group_stat.h>

using namespace NKikimr;
using namespace NActors;

namespace {

    class TGroupStatAggregatorActor : public TActorBootstrapped<TGroupStatAggregatorActor> {
        static constexpr TDuration NodeStatKeepingTime = TDuration::Seconds(20);

        struct TNodeStat {
            TInstant Timestamp; // when the last stats was reported
            TGroupStat Stat; // the reported stat
        };

        const ui32 GroupId;
        const TActorId VDiskServiceId;
        const TDuration ReportPeriod;
        TGroupStat Accum;
        THashMap<ui32, TNodeStat> PerNodeStat;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODE_WARDEN_STATAGGR_ACTOR;
        }

        TGroupStatAggregatorActor(ui32 groupId, const TActorId& vdiskServiceId, TDuration reportPeriod = TDuration::Seconds(10))
            : GroupId(groupId)
            , VDiskServiceId(vdiskServiceId)
            , ReportPeriod(reportPeriod)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc, ReportPeriod, new TEvents::TEvWakeup);
        }

        void Handle(TEvGroupStatReport::TPtr& ev) {
            TEvGroupStatReport *msg = ev->Get();
            TGroupStat stat;
            if (msg->GetGroupId() == GroupId && msg->GetStat(stat)) {
                const TActorId& sender = ev->Sender;
                const ui32 nodeId = sender.NodeId();
                auto it = PerNodeStat.find(nodeId);
                if (it != PerNodeStat.end()) {
                    TNodeStat& current = it->second;
                    Accum.Replace(stat, current.Stat);
                    current.Stat = stat;
                    current.Timestamp = TActivationContext::Now();
                } else {
                    Accum.Add(stat);
                    TNodeStat current;
                    current.Stat = stat;
                    current.Timestamp = TActivationContext::Now();
                    PerNodeStat.emplace(nodeId, std::move(current));
                }
            }
        }

        void CleanupExpiredPerNodeStat() {
            const TInstant now = TActivationContext::Now();
            for (auto it = PerNodeStat.begin(), next = it; it != PerNodeStat.end(); it = next) {
                next = std::next(it);
                const TNodeStat& stat = it->second;
                const TInstant expirationTimestamp = stat.Timestamp + NodeStatKeepingTime;
                if (now >= expirationTimestamp) {
                    Accum.Subtract(stat.Stat);
                    PerNodeStat.erase(it);
                }
            }
        }

        void HandleWakeup() {
            CleanupExpiredPerNodeStat();
            const TActorId nodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
            Send(nodeWardenId, new TEvGroupStatReport(VDiskServiceId, GroupId, Accum));
            Schedule(ReportPeriod, new TEvents::TEvWakeup);
        }

        STRICT_STFUNC(StateFunc, {
            hFunc(TEvGroupStatReport, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
        })
    };

} // anon

namespace NKikimr {

    IActor *CreateGroupStatAggregatorActor(ui32 groupId, const TActorId& vdiskServiceId) {
        return new TGroupStatAggregatorActor(groupId, vdiskServiceId);
    }

} // NKikimr
