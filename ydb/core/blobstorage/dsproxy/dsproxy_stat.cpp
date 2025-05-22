#include "dsproxy_impl.h"

namespace NKikimr {

    void TBlobStorageGroupProxy::HandleUpdateResponsiveness() {
        PerDiskStats = ResponsivenessTracker.Update(TActivationContext::Now());

        auto formatResponsiveness = [&] {
            TStringStream str;
            str << "{";
            bool first = true;
            for (const auto &kv : PerDiskStats->DiskData) {
                if (first) {
                    first = false;
                } else {
                    str << " ";
                }
                str << kv.first << ":" << kv.second.ToString();
            }
            str << "}";
            return str.Str();
        };
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId << " Responsiveness# " << formatResponsiveness());

        if (!ResponsivenessTracker.IsEmpty()) {
            ScheduleUpdateResponsiveness();
        }
    }

    void TBlobStorageGroupProxy::ScheduleUpdateResponsiveness() {
        Schedule(UpdateResponsivenessTimeout, new TEvUpdateResponsiveness);
    }

    void TBlobStorageGroupProxy::HandleUpdateGroupStat() {
        Y_ABORT_UNLESS(GroupStatUpdateScheduled);
        GroupStatUpdateScheduled = false;
        if (Info) {
            Stat.Fadeout(TActivationContext::Now());
            for (ui32 i = 0, num = Info->GetTotalVDisksNum(); i < num; ++i) {
                const TActorId vdiskServiceId = Info->GetActorId(i);
                const TActorId groupStatAggregatorId = MakeGroupStatAggregatorId(vdiskServiceId);
                Send(groupStatAggregatorId, new TEvGroupStatReport(TActorId(), GroupId.GetRawId(), Stat));
            }
        }
        ScheduleUpdateGroupStat();
    }

    void TBlobStorageGroupProxy::ScheduleUpdateGroupStat() {
        if (!std::exchange(GroupStatUpdateScheduled, true)) {
            Schedule(GroupStatUpdateInterval, new TEvUpdateGroupStat);
        }
    }

    void TBlobStorageGroupProxy::Handle(TEvLatencyReport::TPtr& ev) {
        TEvLatencyReport *msg = ev->Get();
        Stat.Update(msg->Kind, msg->Sample, TActivationContext::Now());
    }

    void TBlobStorageGroupProxy::Handle(TEvTimeStats::TPtr& ev) {
        // handle TEvBlobStorage::TEvPut first
        Mon->TimeStats.MergeIn(std::move(ev->Get()->TimeStats));
    }

} // NKikimr
