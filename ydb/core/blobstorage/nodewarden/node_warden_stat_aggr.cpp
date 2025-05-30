#include "node_warden_impl.h"

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::ReportLatencies() {
    std::unique_ptr<TEvBlobStorage::TEvControllerUpdateGroupStat> ev;

    for (const auto& kv : PerAggregatorInfo) {
        const TAggregatorInfo& info = kv.second;
        const TGroupID groupId(info.GroupId);
        if (!ev) {
            ev.reset(new TEvBlobStorage::TEvControllerUpdateGroupStat);
        }
        auto& record = ev->Record;
        auto *pb = record.AddPerGroupReport();
        pb->SetGroupId(info.GroupId);
        ActorIdToProto(kv.first, pb->MutableVDiskServiceId());
        info.Stat.Serialize(pb);
    }

    if (ev) {
        SendToController(std::move(ev));
    }
}

void TNodeWarden::Handle(TEvGroupStatReport::TPtr ev) {
    TEvGroupStatReport *msg = ev->Get();
    TActorId vdiskServiceId = msg->GetVDiskServiceId();

    TGroupStat stat;
    if (RunningVDiskServiceIds.count(vdiskServiceId) && msg->GetStat(stat)) {
        PerAggregatorInfo[vdiskServiceId] = TAggregatorInfo{
            msg->GetGroupId(),
            std::move(stat)
        };
    }
}

void TNodeWarden::StartAggregator(const TActorId& vdiskServiceId, ui32 groupId) {
    if (RunningVDiskServiceIds.emplace(vdiskServiceId).second) {
        const TActorId groupStatAggregatorId = MakeGroupStatAggregatorId(vdiskServiceId);
        const TActorId actorId = Register(CreateGroupStatAggregatorActor(groupId, vdiskServiceId),
            TMailboxType::Revolving, AppData()->SystemPoolId);
        TActivationContext::ActorSystem()->RegisterLocalService(groupStatAggregatorId, actorId);
    }
}

void TNodeWarden::StopAggregator(const TActorId& vdiskServiceId) {
    if (RunningVDiskServiceIds.erase(vdiskServiceId)) {
        const TActorId groupStatAggregatorId = MakeGroupStatAggregatorId(vdiskServiceId);
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, groupStatAggregatorId, {}, nullptr, 0));
        PerAggregatorInfo.erase(groupStatAggregatorId);
    }
}
