#include "stat_processor.h"

namespace NKikimr::NBsController {

    class TStatProcessorActor : public TActorBootstrapped<TStatProcessorActor> {
        using TGroupCleanupSchedule = TMultiMap<TInstant, std::pair<TGroupId, TActorId>>;

        struct TPerGroupRecord {
            struct TPerAggregatorInfo {
                TGroupStat Stat;
                TGroupCleanupSchedule::iterator ScheduleIter;
            };

            TGroupStat Accum;
            THashMap<TActorId, TPerAggregatorInfo> PerAggegatorInfo;
            TGroupLatencyStats Stats;

            void Update(const NKikimrBlobStorage::TEvGroupStatReport& report, TInstant now, TGroupId groupId, TGroupCleanupSchedule& schedule) {
                TGroupStat stat;
                if (stat.Deserialize(report)) {
                    const TActorId vdiskServiceId = ActorIdFromProto(report.GetVDiskServiceId());
                    auto& item = PerAggegatorInfo[vdiskServiceId];
                    Accum.Replace(stat, item.Stat);
                    item.Stat = std::move(stat);

                    const TInstant barrier = now + TDuration::Seconds(30);
                    if (item.ScheduleIter != TGroupCleanupSchedule::iterator()) {
                        schedule.erase(item.ScheduleIter);
                    }
                    item.ScheduleIter = schedule.emplace(barrier, std::make_pair(groupId, vdiskServiceId));
                }
            }

            void Cleanup(const TActorId& vdiskServiceId) {
                auto it = PerAggegatorInfo.find(vdiskServiceId);
                Y_ABORT_UNLESS(it != PerAggegatorInfo.end());
                auto& item = it->second;
                Accum.Subtract(item.Stat);
                PerAggegatorInfo.erase(it);
            }

            void RecalculatePercentiles() {
                Stats.PutTabletLog = Accum.GetPercentile(TGroupStat::EKind::PUT_TABLET_LOG, 0.90);
                Stats.PutUserData = Accum.GetPercentile(TGroupStat::EKind::PUT_USER_DATA, 0.90);
                Stats.GetFast = Accum.GetPercentile(TGroupStat::EKind::GET_FAST, 0.90);
            }
        };

        static constexpr TDuration UpdatePeriod = TDuration::Seconds(10);

        TActorId ParentActorId;
        TMap<TGroupId, TPerGroupRecord> Groups;
        TSet<TGroupId> UpdatedGroupIds;
        TGroupCleanupSchedule GroupCleanupSchedule;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BSC_STAT_PROCESSOR;
        }

        void Bootstrap(const TActorId& parentActorId) {
            ParentActorId = parentActorId;
            Become(&TThis::StateFunc, UpdatePeriod, new TEvents::TEvWakeup);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvControllerUpdateGroupStat, Handle);
            hFunc(TEvControllerNotifyGroupChange, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )

        void Handle(TEvBlobStorage::TEvControllerUpdateGroupStat::TPtr& ev) {
            TSet<TGroupId> ids;
            const TInstant now = TActivationContext::Now();

            // apply new report
            auto& record = ev->Get()->Record;
            for (const NKikimrBlobStorage::TEvGroupStatReport& item : record.GetPerGroupReport()) {
                const TGroupId groupId = TGroupId::FromProto(&item, &NKikimrBlobStorage::TEvGroupStatReport::GetGroupId);
                auto it = Groups.find(groupId);
                if (it != Groups.end()) {
                    it->second.Update(item, now, groupId, GroupCleanupSchedule);
                    ids.insert(groupId);
                }
            }

            // cleanup obsolete items from the schedule
            TGroupCleanupSchedule::iterator it;
            for (it = GroupCleanupSchedule.begin(); it != GroupCleanupSchedule.end() && it->first <= now; ++it) {
                TGroupId groupId;
                TActorId vdiskServiceId;
                std::tie(groupId, vdiskServiceId) = it->second;
                auto groupIt = Groups.find(groupId);
                if (groupIt != Groups.end()) {
                    groupIt->second.Cleanup(vdiskServiceId);
                    ids.insert(groupId);
                }
            }
            GroupCleanupSchedule.erase(GroupCleanupSchedule.begin(), it);

            // recalculate percentiles for changed groups
            for (const TGroupId& groupId : ids) {
                Groups[groupId].RecalculatePercentiles();
                UpdatedGroupIds.insert(groupId);
            }
        }

        void Handle(TEvControllerNotifyGroupChange::TPtr& ev) {
            for (TGroupId groupId : ev->Get()->Created) {
                Groups.emplace(groupId, TPerGroupRecord());
                UpdatedGroupIds.insert(groupId);
            }
            for (TGroupId groupId : ev->Get()->Deleted) {
                Groups.erase(groupId);
                UpdatedGroupIds.erase(groupId);
            }
        }

        void HandleWakeup() {
            if (UpdatedGroupIds) {
                auto ev = MakeHolder<TEvControllerCommitGroupLatencies>();

                const bool scanLowerBound = UpdatedGroupIds.size() <= Groups.size() / 10;
                auto it = Groups.begin();
                for (TGroupId groupId : UpdatedGroupIds) {
                    if (scanLowerBound) {
                        // use lower bound logic as the number of updated groups is not so big
                        it = Groups.lower_bound(groupId);
                    } else {
                        // advance second iterator to the corresponding group -- it must exist
                        for (; it != Groups.end() && it->first < groupId; ++it)
                        {}
                    }
                    Y_ABORT_UNLESS(it != Groups.end() && it->first == groupId);

                    // report this group to the event
                    ev->Updates.emplace_hint(ev->Updates.end(), groupId, it->second.Stats);
                }

                // report updates to the controller
                Send(ParentActorId, ev.Release());

                // reset updates
                UpdatedGroupIds.clear();
            }

            Schedule(UpdatePeriod, new TEvents::TEvWakeup);
        }
    };

    IActor *CreateStatProcessorActor() {
        return new TStatProcessorActor;
    }

} // NKikimr::NBsController
