#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/pdisk/mock/pdisk_mock.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/testactorsys.h>
#include <ydb/core/base/blobstorage_common.h>
#include <util/system/env.h>
#include <random>

namespace NKikimr {
namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

template<typename TTransformItem, typename TBaseContainer>
class TFilteredContainer {
    TBaseContainer& Base;
    TTransformItem Callback;

    template<typename TBaseIterator>
    class TIterator : public TBaseIterator {
        const TTransformItem& Callback;

    public:
        template<typename... TArgs>
        TIterator(const TTransformItem& callback, TArgs&&... args)
            : TBaseIterator(std::forward<TArgs>(args)...)
            , Callback(callback)
        {}

        std::invoke_result_t<TTransformItem, decltype(*std::declval<TBaseIterator>())> operator *() {
            return std::invoke(Callback, *static_cast<TBaseIterator&>(*this));
        }

        std::invoke_result_t<TTransformItem, decltype(*std::declval<const TBaseIterator>())> operator *() const {
            return std::invoke(Callback, *static_cast<const TBaseIterator&>(*this));
        }
    };

public:
    template<typename... TArgs>
    TFilteredContainer(TBaseContainer& base, TArgs&&... args)
        : Base(base)
        , Callback(std::forward<TArgs>(args)...)
    {}

    TIterator<decltype(std::declval<TBaseContainer>().begin())> begin() { return {Callback, Base.begin()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().begin())> begin() const { return {Callback, Base.begin()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().cbegin())> cbegin() const { return {Callback, Base.cbegin()}; }

    TIterator<decltype(std::declval<TBaseContainer>().rbegin())> rbegin() { return {Callback, Base.rbegin()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().rbegin())> rbegin() const { return {Callback, Base.rbegin()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().crbegin())> crbegin() const { return {Callback, Base.crbegin()}; }

    TIterator<decltype(std::declval<TBaseContainer>().end())> end() { return {Callback, Base.end()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().end())> end() const { return {Callback, Base.end()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().cend())> cend() const { return {Callback, Base.cend()}; }

    TIterator<decltype(std::declval<TBaseContainer>().rend())> rend() { return {Callback, Base.rend()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().rend())> rend() const { return {Callback, Base.rend()}; }
    TIterator<decltype(std::declval<const TBaseContainer>().crend())> crend() const { return {Callback, Base.crend()}; }
};

template<typename TTransformItem, typename TCont>
TFilteredContainer<TTransformItem, TCont> FilterContainer(TCont& container, TTransformItem&& callback) {
    return {container, std::forward<TTransformItem>(callback)};
}

using namespace NKikimr;

template<typename TEvent> struct TResultForImpl;
template<> struct TResultForImpl<TEvBlobStorage::TEvPut> { using Type = TEvBlobStorage::TEvPutResult; };
template<> struct TResultForImpl<TEvBlobStorage::TEvGet> { using Type = TEvBlobStorage::TEvGetResult; };
template<> struct TResultForImpl<TEvBlobStorage::TEvBlock> { using Type = TEvBlobStorage::TEvBlockResult; };
template<> struct TResultForImpl<TEvBlobStorage::TEvDiscover> { using Type = TEvBlobStorage::TEvDiscoverResult; };
template<> struct TResultForImpl<TEvBlobStorage::TEvRange> { using Type = TEvBlobStorage::TEvRangeResult; };
template<> struct TResultForImpl<TEvBlobStorage::TEvCollectGarbage> { using Type = TEvBlobStorage::TEvCollectGarbageResult; };
template<> struct TResultForImpl<TEvBlobStorage::TEvStatus> { using Type = TEvBlobStorage::TEvStatusResult; };
template<typename TEvent> using TResultFor = typename TResultForImpl<TEvent>::Type;

TString GenerateRandomBuffer(std::unordered_map<size_t, TString>& cache) {
    const size_t len = TAppData::RandomProvider->Uniform(100, 201);
    if (const auto it = cache.find(len); it != cache.end()) {
        return it->second;
    }
    TString buffer = TString::Uninitialized(len);
    std::mt19937 gen(TAppData::RandomProvider->GenRand64());
    char *p = buffer.Detach();
    char *end = p + len;
    for (; p != end; ++p) {
        *p = gen();
    }
    cache.emplace(len, buffer);
    return buffer;
}

class TNodeWardenMockActor : public TActor<TNodeWardenMockActor> {
    std::map<std::tuple<ui32, ui32, ui32>, NKikimrBlobStorage::EVDiskStatus*>& StatusMap;

public:
    TNodeWardenMockActor(std::map<std::tuple<ui32, ui32, ui32>, NKikimrBlobStorage::EVDiskStatus*>& statusMap)
        : TActor(&TThis::StateFunc)
        , StatusMap(statusMap)
    {}

    void Ignore() {}

    void ForwardToProxy(TAutoPtr<IEventHandle> ev) {
        const TGroupID groupId(GroupIDFromBlobStorageProxyID(ev->GetForwardOnNondeliveryRecipient()));
        const ui32 id = groupId.GetRaw();
        Forward(ev, MakeBlobStorageProxyID(id));
    }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvBlobStorage::EvUpdateGroupInfo, Ignore);
        cFunc(TEvBlobStorage::EvControllerUpdateDiskStatus, Ignore);
        cFunc(TEvBlobStorage::EvDropDonor, Ignore);
        cFunc(TEvBlobStorage::EvGroupStatReport, Ignore);
        cFunc(TEvBlobStorage::EvNotifyVDiskGenerationChange, Ignore);
        cFunc(TEvents::TSystem::Gone, Ignore);

        fFunc(TEvBlobStorage::EvPut, ForwardToProxy);
        fFunc(TEvBlobStorage::EvGet, ForwardToProxy);
        fFunc(TEvBlobStorage::EvBlock, ForwardToProxy);
        fFunc(TEvBlobStorage::EvDiscover, ForwardToProxy);
        fFunc(TEvBlobStorage::EvRange, ForwardToProxy);
        fFunc(TEvBlobStorage::EvCollectGarbage, ForwardToProxy);
        fFunc(TEvBlobStorage::EvStatus, ForwardToProxy);
        fFunc(TEvBlobStorage::EvBunchOfEvents, ForwardToProxy);

        hFunc(TEvStatusUpdate, Handle);

        IgnoreFunc(TEvBlobStorage::TEvControllerScrubQueryStartQuantum);
        IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate);
    )

    void Handle(TEvStatusUpdate::TPtr ev) {
        auto *msg = ev->Get();
        if (const auto it = StatusMap.find({msg->NodeId, msg->PDiskId, msg->VSlotId}); it != StatusMap.end()) {
            *it->second = msg->Status;
        }
    }
};

class TTestEnv {
public:
    struct TDiskRecord {
        TVDiskID VDiskId;
        TActorId PDiskActorId;
        TActorId VDiskActorId;
        ui32 NodeId;
        ui32 PDiskId;
        ui32 VDiskSlotId;
        ui64 PDiskGuid;
        NKikimrBlobStorage::EVDiskStatus Status;
    };

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const NPDisk::TMainKey MainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence } };
    const ui32 NodeCount;
    const ui32 GroupId = 0;
    std::vector<TDiskRecord> Disks;
    std::map<std::tuple<ui32, ui32, ui32>, NKikimrBlobStorage::EVDiskStatus*> StatusMap;
    NPDisk::TOwnerRound Round = 1;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    std::map<std::tuple<ui32, ui32>, ui32> NextVDiskSlotId;
    std::multimap<ui32, std::tuple<ui32, ui32, ui64>> NodePDiskIds;
    std::unique_ptr<TAllVDiskKinds> AllVDiskKinds = std::make_unique<TAllVDiskKinds>();
    TIntrusivePtr<TStoragePoolCounters> StoragePoolCounters;

    TTestEnv(ui32 nodeCount)
        : Counters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
        , NodeCount(nodeCount)
    {}

    void Run(std::function<void(TTestActorSystem&)> fn) {
        TTestActorSystem runtime(NodeCount);
        SetupLogging(runtime);
        runtime.Start();
        SetupStorage(runtime);
        fn(runtime);
        runtime.Stop();
    }

    ui32 GetNumReadyDisks() {
        ui32 numReady = 0;
        for (const auto& disk : Disks) {
            numReady += disk.Status == NKikimrBlobStorage::EVDiskStatus::READY;
        }
        return numReady;
    }

    void WaitReady(TTestActorSystem& runtime) {
        while (GetNumReadyDisks() < Info->GetTotalVDisksNum()) {
            TInstant clock = runtime.GetClock();
            runtime.Sim([&] {
                return runtime.GetClock() < clock + TDuration::MilliSeconds(100);
            });
        }
    }

    bool DoObscenity(TTestActorSystem& runtime, ui32 *allowReformatCounter) {
        const auto *top = &Info->GetTopology();
        TBlobStorageGroupInfo::TGroupVDisks failedDisks(top);
        for (const auto& disk : Disks) {
            if (disk.Status != NKikimrBlobStorage::EVDiskStatus::READY) {
                failedDisks |= {top, disk.VDiskId};
            }
        }
        auto& checker = Info->GetQuorumChecker();
        std::vector<ui32> options;
        const bool reformat = *allowReformatCounter < 3;
        for (ui32 index = 0; index < Disks.size(); ++index) {
            auto item = TBlobStorageGroupInfo::TGroupVDisks(top, Disks[index].VDiskId);
            if ((reformat || !(failedDisks & item)) && checker.CheckFailModelForGroup(failedDisks | item)) {
                options.push_back(index);
            }
        }
        if (options.size()) {
            const size_t index = TAppData::RandomProvider->Uniform(options.size());
            const ui32 diskIdx = options[index];
            const ui32 decision = TAppData::RandomProvider->Uniform(100);
            TDiskRecord& disk = Disks[diskIdx];
            if (decision < 95) {
                RestartDisk(runtime, disk);
            } else if (decision < 98) {
                FormatDisk(runtime, disk);
            } else {
//                ReassignDisk(runtime, disk);
            }
            ++*allowReformatCounter;
            *allowReformatCounter %= 15;
            return true;
        }
        return false;
    }

    TString GetDiskStatusMap() const {
        TStringBuilder s;
        for (auto& disk : Disks) {
            switch (disk.Status) {
                case NKikimrBlobStorage::EVDiskStatus::ERROR: s << 'E'; break;
                case NKikimrBlobStorage::EVDiskStatus::INIT_PENDING: s << 'I'; break;
                case NKikimrBlobStorage::EVDiskStatus::REPLICATING: s << 'R'; break;
                case NKikimrBlobStorage::EVDiskStatus::READY: s << 'Y'; break;
                default: s << '?'; break;
            }
        }
        return s;
    }

    void RestartDisk(TTestActorSystem& runtime, TDiskRecord& disk) {
        LOG_NOTICE_S(runtime, NActorsServices::TEST, "Restarting " << disk.VDiskId << " over NodeId# " << disk.NodeId
            << " PDiskId# " << disk.PDiskId);
        runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, disk.VDiskActorId, TActorId(), nullptr, 0), disk.NodeId);
        StartVDisk(runtime, disk);
    }

    void FormatDisk(TTestActorSystem& runtime, TDiskRecord& disk) {
        LOG_NOTICE_S(runtime, NActorsServices::TEST, "Formatting " << disk.VDiskId << " over NodeId# " << disk.NodeId
            << " PDiskId# " << disk.PDiskId << " DiskStatus# " << GetDiskStatusMap());
        runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, disk.VDiskActorId, TActorId(), nullptr, 0), disk.NodeId);
        Slay(runtime, disk);
        StartVDisk(runtime, disk);
    }

    void ReassignDisk(TTestActorSystem& runtime, TDiskRecord& disk) {
        // create set of nodes suitable as target ones
        std::set<ui32> nodes = runtime.GetNodes();
        for (const auto& actorId : Info->GetDynamicInfo().ServiceIdForOrderNumber) {
            const ui32 num = nodes.erase(actorId.NodeId());
            Y_ABORT_UNLESS(num);
        }
        Y_ABORT_UNLESS(nodes.size() == 1);
        const ui32 targetNode = *nodes.begin();

        // pick random target pdisk
        const auto range = NodePDiskIds.equal_range(targetNode);
        auto it = range.first;
        std::advance(it, TAppData::RandomProvider->Uniform(std::distance(range.first, range.second)));
        ui32 nodeId, pdiskId;
        ui64 pdiskGuid;
        std::tie(nodeId, pdiskId, pdiskGuid) = it->second;

        LOG_NOTICE_S(runtime, NActorsServices::TEST, "Reassigning " << disk.VDiskId
            << " from NodeId# " << disk.NodeId << " PDiskId# " << disk.PDiskId << " to NodeId# " << nodeId
            << " PDiskId# " << pdiskId << " PDiskGuid# " << pdiskGuid);

        // slay it over PDisk
        Slay(runtime, disk);

        // terminate running VDisk directly
        runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, disk.VDiskActorId, TActorId(), nullptr, 0), disk.NodeId);

        // fill in new VDisk params
        const ui32 vdiskSlotId = ++NextVDiskSlotId[std::make_tuple(nodeId, pdiskId)];
        disk.VDiskId = TVDiskID(Info->GroupID, Info->GroupGeneration + 1, disk.VDiskId);
        disk.PDiskActorId = MakeBlobStoragePDiskID(nodeId, pdiskId);
        disk.VDiskActorId = MakeBlobStorageVDiskID(nodeId, pdiskId, vdiskSlotId);
        disk.NodeId = nodeId;
        disk.PDiskId = pdiskId;
        disk.VDiskSlotId = vdiskSlotId;
        disk.PDiskGuid = pdiskGuid;

        // update group info
        TBlobStorageGroupInfo::TTopology topology(Info->Type);
        topology.FailRealms = Info->GetTopology().FailRealms;
        TBlobStorageGroupInfo::TDynamicInfo dynamic(Info->GroupID, Info->GroupGeneration + 1);
        dynamic.ServiceIdForOrderNumber = Info->GetDynamicInfo().ServiceIdForOrderNumber;
        dynamic.ServiceIdForOrderNumber[topology.GetOrderNumber(disk.VDiskId)] = disk.VDiskActorId;
        Info = MakeIntrusive<TBlobStorageGroupInfo>(std::move(topology), std::move(dynamic), TString(), Nothing(),
            NPDisk::DEVICE_TYPE_SSD);

        StartVDisk(runtime, disk);

        // update group generation for other disks
        for (TDiskRecord& disk : Disks) {
            if (disk.VDiskId.GroupGeneration != Info->GroupGeneration) {
                disk.VDiskId = TVDiskID(Info->GroupID, Info->GroupGeneration, disk.VDiskId);
                runtime.Send(new IEventHandle(disk.VDiskActorId, {}, new TEvVGenerationChange(disk.VDiskId, Info)),
                    disk.VDiskActorId.NodeId());
            }
        }

        // update group info for proxy
        runtime.Send(new IEventHandle(MakeBlobStorageProxyID(Info->GroupID), TActorId(),
            new TEvBlobStorage::TEvConfigureProxy(Info, StoragePoolCounters)), 1);
    }

    void Slay(TTestActorSystem& runtime, TDiskRecord& disk) {
        LOG_INFO_S(runtime, NActorsServices::TEST, "Slaying VDiskId# " << disk.VDiskId);
        const TActorId& edge = runtime.AllocateEdgeActor(disk.NodeId);
        auto slay = std::make_unique<NPDisk::TEvSlay>(disk.VDiskId, ++Round, disk.PDiskId, disk.VDiskSlotId);
        runtime.Send(new IEventHandle(disk.PDiskActorId, edge, slay.release()), disk.NodeId);
        auto res = runtime.WaitForEdgeActorEvent({edge});
        if (const auto *ev = res->CastAsLocal<NPDisk::TEvSlayResult>()) {
            Y_VERIFY_S(ev->Status == NKikimrProto::OK || ev->Status == NKikimrProto::ALREADY, "TEvSlayResult# " << ev->ToString());
            LOG_INFO_S(runtime, NActorsServices::TEST, "Slayed VDiskId# " << disk.VDiskId);
        } else {
            Y_ABORT("unexpected event to edge actor");
        }
    }

private:
    void SetupLogging(TTestActorSystem& runtime) {
        runtime.SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_DEBUG);
//        runtime.SetLogPriority(NKikimrServices::BS_SKELETON, NLog::PRI_DEBUG);
//        runtime.SetLogPriority(NKikimrServices::BS_REPL, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BS_SYNCLOG, NLog::PRI_ERROR);
        runtime.SetLogPriority(NKikimrServices::BS_SYNCER, NLog::PRI_CRIT);
//        runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_DEBUG);
//        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_DEBUG);
//        runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NActorsServices::TEST, NLog::PRI_INFO);
        runtime.SetLogPriority(NActorsServices::TEST, NLog::PRI_DEBUG);
    }

    void SetupStorage(TTestActorSystem& runtime) {
        TVector<TActorId> vdiskActorIds;

        Disks.reserve(8 * NodeCount);

        for (ui32 nodeId = 1, i = 0; nodeId <= NodeCount; ++nodeId, ++i) {
            const ui32 pdiskId = 1;
            const ui64 pdiskGuid = TAppData::RandomProvider->GenRand64();
            const TPDiskCategory category(NPDisk::DEVICE_TYPE_SSD, 0);
            const TActorId& actorId = runtime.Register(CreatePDiskMockActor(MakeIntrusive<TPDiskMockState>(nodeId,
                pdiskId, pdiskGuid, ui64(1000) << 30)), TActorId(), 0, std::nullopt, nodeId);
            const TActorId& serviceId = MakeBlobStoragePDiskID(nodeId, pdiskId);
            runtime.RegisterService(serviceId, actorId);
            NodePDiskIds.emplace(nodeId, std::make_tuple(nodeId, pdiskId, pdiskGuid));
            if (i < 8) {
                const ui32 vdiskSlotId = ++NextVDiskSlotId[std::make_tuple(nodeId, pdiskId)];
                const TActorId& vdiskActorId = MakeBlobStorageVDiskID(nodeId, pdiskId, vdiskSlotId);
                vdiskActorIds.push_back(vdiskActorId);
                const TVDiskID vdiskId(TGroupId::FromValue(GroupId), 1, 0, i, 0);
                Disks.push_back(TDiskRecord{
                    vdiskId,
                    serviceId,
                    vdiskActorId,
                    nodeId,
                    pdiskId,
                    vdiskSlotId,
                    pdiskGuid,
                    NKikimrBlobStorage::EVDiskStatus::INIT_PENDING,
                });
                StatusMap.emplace(std::make_tuple(nodeId, pdiskId, vdiskSlotId), &Disks.back().Status);
            }
            runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), runtime.Register(new TNodeWardenMockActor(StatusMap),
                TActorId(), 0, std::nullopt, nodeId));
        }

        Info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block, 1u, 0u, 1u,
            &vdiskActorIds, TBlobStorageGroupInfo::EEM_NONE);

        for (TDiskRecord& disk : Disks) {
            StartVDisk(runtime, disk);
        }

        auto proxy = Counters->GetSubgroup("subsystem", "proxy");
        TIntrusivePtr<TDsProxyNodeMon> mon = MakeIntrusive<TDsProxyNodeMon>(proxy, true);
        StoragePoolCounters = MakeIntrusive<TStoragePoolCounters>(proxy, TString(), NPDisk::DEVICE_TYPE_SSD);
        std::unique_ptr<IActor> proxyActor{CreateBlobStorageGroupProxyConfigured(TIntrusivePtr(Info), false, false, mon,
                TIntrusivePtr(StoragePoolCounters), DefaultEnablePutBatching, DefaultEnableVPatch,
                DefaultSlowDiskThreshold)};
        const TActorId& actorId = runtime.Register(proxyActor.release(), TActorId(), 0, std::nullopt, 1);
        runtime.RegisterService(MakeBlobStorageProxyID(GroupId), actorId);
    }

    void StartVDisk(TTestActorSystem& runtime, TDiskRecord& disk) {
        TVDiskConfig::TBaseInfo baseInfo(disk.VDiskId, disk.PDiskActorId, disk.PDiskGuid, disk.PDiskId,
            NPDisk::DEVICE_TYPE_SSD, disk.VDiskSlotId, NKikimrBlobStorage::TVDiskKind::Default, ++Round,
            TString());
        auto vdiskConfig = AllVDiskKinds->MakeVDiskConfig(baseInfo);
        vdiskConfig->EnableVDiskCooldownTimeout = true;
        vdiskConfig->UseCostTracker = false;
        auto counters = Counters->GetSubgroup("node", ToString(disk.NodeId))->GetSubgroup("vdisk", disk.VDiskId.ToString());
        const TActorId& actorId = runtime.Register(CreateVDisk(vdiskConfig, Info, counters), TActorId(), 0, std::nullopt, disk.NodeId);
        runtime.RegisterService(disk.VDiskActorId, actorId);
        disk.Status = NKikimrBlobStorage::EVDiskStatus::INIT_PENDING;
    }
};

class TActivityActorImpl : public TActorCoroImpl {
    const ui64 TabletId;
    const ui32 GroupId;
    const ui32 MaxGen;
    TString Prefix;
    const TActorId ProxyId;
    ui32* const DoneCounter;
    ui32* const Counter;
    std::map<TLogoBlobID, TString> Committed;
    std::unordered_map<size_t, TString> Cache;

public:
    TActivityActorImpl(ui64 tabletId, ui32 groupId, ui32 *doneCounter, ui32 *counter, ui32 maxGen)
        : TActorCoroImpl(65536, true)
        , TabletId(tabletId)
        , GroupId(groupId)
        , MaxGen(maxGen)
        , ProxyId(MakeBlobStorageProxyID(GroupId))
        , DoneCounter(doneCounter)
        , Counter(counter)
    {}

    void Run() override {
        for (ui32 generation = 1; generation <= MaxGen && generation; ++generation) {
            Prefix = TStringBuilder() << "[" << TabletId << ":" << generation << "@" << GroupId << "]";
            LOG_INFO_S(GetActorContext(), NActorsServices::TEST, Prefix << " running generation# " << generation);
            RunCycle(generation);
        }
        LOG_INFO_S(GetActorContext(), NActorsServices::TEST, Prefix << " done");
        ++*DoneCounter;
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
        Y_ABORT("unexpected event Type# 0x%08" PRIx32, ev->GetTypeRewrite());
    }

    template<typename TEvent, typename... TArgs>
    TAutoPtr<TEventHandle<TResultFor<TEvent>>> Query(TArgs&&... args) {
        auto q = std::make_unique<TEvent>(std::forward<TArgs>(args)...);
        LOG_DEBUG_S(GetActorContext(), NActorsServices::TEST, Prefix << " sending " << TypeName<TEvent>() << "# "
            << q->Print(false));
        GetActorSystem()->Schedule(TDuration::MicroSeconds(TAppData::RandomProvider->Uniform(10, 100)),
            new IEventHandle(ProxyId, SelfActorId, q.release()));
        ++*Counter;
        auto ev = WaitForSpecificEvent<TResultFor<TEvent>>(&TActivityActorImpl::ProcessUnexpectedEvent);
        LOG_DEBUG_S(GetActorContext(), NActorsServices::TEST, Prefix << " received "
            << TypeName<TResultFor<TEvent>>() << "# " << ev->Get()->Print(false));
        return ev;
    }

    void RunCycle(ui32 generation) {
        // wait for disks to get ready
        while (auto ev = Query<TEvBlobStorage::TEvStatus>(TInstant::Max())) {
            if (ev->Get()->Status == NKikimrProto::OK) {
                break;
            }
        }

        // set a block for current generation
        if (auto ev = Query<TEvBlobStorage::TEvBlock>(TabletId, generation - 1, TInstant::Max())) {
            const auto& status = ev->Get()->Status;
            Y_VERIFY_S(status == NKikimrProto::OK || status == NKikimrProto::RACE,
                "TEvBlockResult# " << ev->Get()->Print(false));
        }

        // discover previously written data
        if (auto ev = Query<TEvBlobStorage::TEvDiscover>(TabletId, 0, true, false, TInstant::Max(), 0, true)) {
            Y_ABORT_UNLESS(ev->Get()->Status == (Committed.empty() ? NKikimrProto::NODATA : NKikimrProto::OK));
            if (ev->Get()->Status == NKikimrProto::OK) {
                Y_ABORT_UNLESS(ev->Get()->Buffer == Committed.rbegin()->second);
            }
        }

        // issue range read
        const TLogoBlobID from(TabletId, generation - 1, 0, 0, 0, 0);
        const TLogoBlobID to(TabletId, generation - 1, Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
        std::deque<TLogoBlobID> readQueue;
        if (auto ev = Query<TEvBlobStorage::TEvRange>(TabletId, from, to, true, TInstant::Max(), true)) {
            Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK);
            for (const auto& response : ev->Get()->Responses) {
                readQueue.push_back(response.Id);
            }
        }

        // issue gets from the read queue
        ui32 readsInFlight = 0;
        const ui32 maxReadsInFlight = 3;
        TMonotonic nextSendTimestamp;
        while (readsInFlight || !readQueue.empty()) {
            const TMonotonic now = TActorCoroImpl::Monotonic();
            if (readQueue.size() && now >= nextSendTimestamp && readsInFlight < maxReadsInFlight) {
                auto ev = std::make_unique<TEvBlobStorage::TEvGet>(readQueue.front(), 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, true, false);
                LOG_DEBUG_S(GetActorContext(), NActorsServices::TEST, Prefix << " sending TEvGet# " << ev->Print(false));
                nextSendTimestamp = now + TDuration::MicroSeconds(TAppData::RandomProvider->Uniform(10, 100));
                Send(ProxyId, ev.release());
                ++*Counter;
                readQueue.pop_front();
                ++readsInFlight;
            } else if (auto ev = WaitForSpecificEvent<TEvBlobStorage::TEvGetResult>(&TActivityActorImpl::ProcessUnexpectedEvent,
                    nextSendTimestamp)) {
                LOG_DEBUG_S(GetActorContext(), NActorsServices::TEST, Prefix << " received TEvGetResult# " << ev->Get()->Print(false));
                Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK);
                for (ui32 i = 0; i < ev->Get()->ResponseSz; ++i) {
                    const auto& response = ev->Get()->Responses[i];
                    Y_ABORT_UNLESS(response.Status == NKikimrProto::OK);
                    const auto it = Committed.find(response.Id);
                    Y_ABORT_UNLESS(it != Committed.end());
                    Y_ABORT_UNLESS(it->second == response.Buffer.ConvertToString());
                    Committed.erase(it);
                }
                --readsInFlight;
            }
        }
        auto filter = [](const auto& item) { return item.first.ToString(); };
        Y_VERIFY_S(Committed.empty(), Prefix << " missing blobs# " << FormatList(FilterContainer(Committed, filter)));

        // set up a barrier
        if (generation != 1) {
            if (auto ev = Query<TEvBlobStorage::TEvCollectGarbage>(TabletId, generation, 0, 0, true, generation - 1,
                    Max<ui32>(), nullptr, nullptr, TInstant::Max(), false)) {
                Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK);
            }
        }

        // begin putting
        std::map<TLogoBlobID, TString> writesInFlight;
        const ui32 maxWritesInFlight = 3;
        ui32 numWritesRemain = TAppData::RandomProvider->Uniform(100, 201);
        ui32 step = 1;
        while (writesInFlight.size() || numWritesRemain) {
            const TMonotonic now = TActorCoroImpl::Monotonic();
            if (numWritesRemain && now >= nextSendTimestamp && writesInFlight.size() < maxWritesInFlight) {
                TString buffer = GenerateRandomBuffer(Cache);
                const TLogoBlobID id(TabletId, generation, step++, 0, buffer.size(), 0);
                auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, buffer, TInstant::Max());
                LOG_DEBUG_S(GetActorContext(), NActorsServices::TEST, Prefix << " sending TEvPut# " << ev->Print(false));
                nextSendTimestamp = now + TDuration::MicroSeconds(TAppData::RandomProvider->Uniform(10, 100));
                Send(ProxyId, ev.release());
                ++*Counter;
                writesInFlight.emplace(id, std::move(buffer));
                --numWritesRemain;
            } else if (auto ev = WaitForSpecificEvent<TEvBlobStorage::TEvPutResult>(&TActivityActorImpl::ProcessUnexpectedEvent,
                    nextSendTimestamp)) {
                LOG_DEBUG_S(GetActorContext(), NActorsServices::TEST, Prefix << " received TEvPutResult# " << ev->Get()->Print(false)
                    << " writesInFlight.size# " << writesInFlight.size());
                Y_VERIFY_S(ev->Get()->Status == NKikimrProto::OK, "TEvPutResult# " << ev->Get()->Print(false));
                const auto it = writesInFlight.find(ev->Get()->Id);
                Y_ABORT_UNLESS(it != writesInFlight.end());
                Committed.insert(writesInFlight.extract(it));
            }
        }
    }
};

Y_UNIT_TEST_SUITE(GroupStress) {
    Y_UNIT_TEST(Test) {
        return;

        THPTimer timer;
        TAppData::RandomProvider = CreateDeterministicRandomProvider(1);
        SetRandomSeed(1);
        TTestEnv env(9);
        env.Run([&](TTestActorSystem& runtime) {
            env.WaitReady(runtime); // wait for all disks to become ready

            const bool infinite = GetEnv("INFINITE", "no") == "yes";

            ui64 tabletId = 1;
            ui32 numTablets = 125;
            ui32 done = 0;
            ui32 counter = 0;
            for (ui32 i = 0; i < numTablets; ++i) {
                auto actor = MakeHolder<TActivityActorImpl>(tabletId++, env.GroupId, &done, &counter,
                    infinite ? Max<ui32>() : 3);
                runtime.Register(new TActorCoro(std::move(actor)), TActorId(), 0, std::nullopt, 1);
            }

            ui32 obscenityCounter = 0;
            ui32 backoff = 128;

            using TCondition = std::function<bool()>;
            using TAction = std::function<void()>;
            std::deque<std::pair<TCondition, TAction>> map;
            bool exit = false;
            ui32 allowReformatCounter = 0;
            map.emplace_back([&] { return done == numTablets; }, [&] { exit = true; });
            map.emplace_back([&] { return counter > obscenityCounter + backoff; }, [&] {
                if (env.DoObscenity(runtime, &allowReformatCounter)) {
                    backoff = 128;
                } else {
                    backoff = Min<ui32>(1024, backoff * 2);
                }
                obscenityCounter = counter;
            });

            ui64 lastEventsProcessed = 0;

            auto dump = [&] {
                Cerr << Endl;
                Cerr << "Actor count @ EventsProcessed# " << runtime.GetEventsProcessed() << Endl;
                runtime.DumpActorCount(Cerr, "    ", "\n");
                Cerr << Endl;
            };

            do {
                if (TDuration::Seconds(timer.Passed()) >= TDuration::Minutes(5)) {
                    break;
                }
                runtime.Sim([&] {
                    for (auto& [condition, action] : map) {
                        if (condition()) {
                            return false;
                        }
                    }
                    return true;
                });
                if (infinite && runtime.GetEventsProcessed() >= lastEventsProcessed + 100000) {
                    lastEventsProcessed = runtime.GetEventsProcessed();
                    dump();
                }
                for (auto& [condition, action] : map) {
                    if (condition()) {
                        action();
                    }
                }
            } while (!exit);
        });
    }
}
