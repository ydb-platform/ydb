#include "../ut/dsproxy_vdisk_mock_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy_nodemon.h>
#include <ydb/core/blobstorage/dsproxy/group_sessions.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

namespace NKikimr {
namespace {

struct TStalledDataReadScenario {
    TTestActorSystem Runtime{1};
    TActorId Edge;
    TActorId Proxy;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TGroupQueues> Queues;
    std::unique_ptr<TGroupMock> Group;
    TControlWrapper MaxGetAccelerations{DefaultMaxGetAccelerations, 0, MaxGetAccelerationsLimit};
    TLogoBlobID Blob{1, 1, 1, 0, 262144, 0};
    TString Data = TString::Uninitialized(Blob.BlobSize());
    std::vector<std::unique_ptr<IEventHandle>> Pending;
    std::unique_ptr<IEventHandle> Result;

    TStalledDataReadScenario() {
        Runtime.Start();
        Edge = Runtime.AllocateEdgeActor(1);
        TVector<TActorId> disks;
        for (ui32 disk = 0; disk < 8; ++disk) {
            const auto service = MakeBlobStorageVDiskID(1, disk + 1, 1);
            Runtime.RegisterService(service, Runtime.AllocateEdgeActor(1));
            disks.push_back(service);
        }
        Info = new TBlobStorageGroupInfo(TErasureType::Erasure4Plus2Block, 1, 8, 1,
            &disks, TBlobStorageGroupInfo::EEM_NONE);
        Group = std::make_unique<TGroupMock>(0, Info);
        for (size_t i = 0; i < Data.size(); ++i) {
            Data[i] = static_cast<char>(i * 37 + i / 257);
        }
        for (ui32 part = 1; part <= 6; ++part) {
            if (part != 5) {
                Group->PutPartAtSlot(Blob, part - 1, part, Data);
            }
        }

        Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVCheckReadiness::EventType) {
                Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                    new TEvBlobStorage::TEvVCheckReadinessResult(NKikimrProto::OK, false), 0, ev->Cookie), 1);
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGet::EventType) {
                Pending.push_back(std::move(ev));
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvGetResult::EventType && ev->Recipient == Edge) {
                Result = std::move(ev);
                return false;
            }
            return true;
        };

        auto icb = Runtime.GetNode(1)->AppData->Icb;
        TControlBoard::RegisterSharedControl(MaxGetAccelerations, icb->DSProxyControls.MaxGetAccelerations);
        auto counters = Runtime.GetNode(1)->AppData->Counters;
        TIntrusivePtr<TDsProxyNodeMon> nodeMon = new TDsProxyNodeMon(counters, true);
        TDsProxyPerPoolCounters pools(counters);
        Proxy = Runtime.Register(CreateBlobStorageGroupProxyConfigured(TIntrusivePtr(Info), nullptr,
            true, nodeMon, pools.GetPoolCounters("test"), TBlobStorageProxyParameters{
                .UseActorSystemTimeInBSQueue = true,
                .Controls = TBlobStorageProxyControlWrappers{
                    .EnablePutBatching = TControlWrapper(DefaultEnablePutBatching),
                    .EnableVPatch = TControlWrapper(DefaultEnableVPatch),
                    .MaxGetAccelerations = MaxGetAccelerations,
                },
            }), {}, 0, std::nullopt, 1);
        Runtime.Send(new IEventHandle(Proxy, Edge, new TEvRequestProxySessionsState), 1);
        Queues = Runtime.WaitForEdgeActorEvent<TEvProxySessionsState>(Edge, false)->Get()->GroupQueues;
        SimFor(TDuration::Seconds(1));
    }

    ~TStalledDataReadScenario() {
        Runtime.FilterFunction = {};
        Runtime.Stop();
    }

    void SimFor(TDuration duration) {
        const TInstant barrier = Runtime.GetClock() + duration;
        Runtime.Schedule(barrier, nullptr, new TFakeSchedulerCookie, 1);
        Runtime.Sim([&] { return Runtime.GetClock() < barrier; });
    }

    ui32 Disk(IEventHandle& ev) const {
        const auto& request = ev.Get<TEvBlobStorage::TEvVGet>()->Record;
        return Info->GetIdxInSubgroup(VDiskIDFromVDiskID(request.GetVDiskID()), Blob.Hash());
    }

    bool HasRead(ui32 disk, ui32 part) const {
        for (const auto& ev : Pending) {
            if (Disk(*ev) == disk) {
                for (const auto& query : ev->Get<TEvBlobStorage::TEvVGet>()->Record.GetExtremeQueries()) {
                    if (LogoBlobIDFromLogoBlobID(query.GetId()).PartId() == part) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    void Reply(ui32 disk) {
        auto it = std::find_if(Pending.begin(), Pending.end(), [&](const auto& ev) {
            return Disk(*ev) == disk;
        });
        UNIT_ASSERT_C(it != Pending.end(), "No pending VGet on subgroup disk " << disk);
        auto request = std::move(*it);
        Pending.erase(it);
        auto reply = std::make_unique<TEvBlobStorage::TEvVGetResult>();
        Group->OnVGet(*request->Get<TEvBlobStorage::TEvVGet>(), *reply);
        Runtime.Send(new IEventHandle(request->Sender, request->GetRecipientRewrite(),
            reply.release(), 0, request->Cookie), 1);
    }

    void Run(ui32 limit) {
        UNIT_ASSERT(Pending.empty());
        Result.reset();
        auto icb = Runtime.GetNode(1)->AppData->Icb;
        TControlBoard::SetValue(limit, icb->DSProxyControls.MaxGetAccelerations);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(MaxGetAccelerations), limit);
        // Exercise updates to the already created proxy, including its cache.
        SimFor(TDuration::Seconds(16));
        for (ui32 disk = 0; disk < 8; ++disk) {
            const ui32 order = Info->GetOrderNumber(Group->GetVDiskInSubgroup(Blob, disk).GetVDiskId());
            auto flow = Queues->DisksByOrderNumber[order]->Queues.FlowRecordForQueueId(NKikimrBlobStorage::GetFastRead);
            UNIT_ASSERT(flow);
            flow->SetPredictedDelayNs(disk == 1 ? 10'000'000 : 100'000);
        }
        Runtime.Send(new IEventHandle(Proxy, Edge, new TEvBlobStorage::TEvGet(
            Blob, 0, Blob.BlobSize(), TInstant::Max(), NKikimrBlobStorage::FastRead)), 1);
        SimFor(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(Pending.size(), 5);
        for (ui32 disk : {2u, 3u, 0u}) {
            Reply(disk);
        }
        SimFor(TDuration::MilliSeconds(1));
        UNIT_ASSERT(HasRead(1, 2)); // First acceleration bypasses both handoffs.
        Reply(6); // Initial handoff: NODATA for parts 1-4.
        SimFor(TDuration::MilliSeconds(1));
        if (limit == 1) {
            UNIT_ASSERT(!HasRead(4, 5));
        } else {
            UNIT_ASSERT(HasRead(4, 5));
            UNIT_ASSERT(HasRead(6, 5));
            Reply(4);
            SimFor(TDuration::MilliSeconds(1));
            UNIT_ASSERT(!HasRead(5, 6)); // Three disks still outstanding.
            Reply(6);
            SimFor(TDuration::MilliSeconds(1));
        }
        if (limit == 3) {
            UNIT_ASSERT_C(HasRead(5, 6), "Third actor acceleration must request part 6 after both part-5 NODATA replies");
            Reply(5);
        } else {
            SimFor(TDuration::MilliSeconds(100));
            UNIT_ASSERT(!HasRead(5, 6));
            UNIT_ASSERT(!Result);
            Reply(1); // Legacy budget still needs the original part-2 read.
        }
        SimFor(TDuration::MilliSeconds(1));
        UNIT_ASSERT(Result);
        const auto* result = Result->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Responses[0].Buffer.ConvertToString(), Data);
        // Replies to the obsolete duplicate reads are not needed for completion.
        Pending.clear();
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TGetAccelerationLimit) {
    Y_UNIT_TEST(IcbUpdatesAffectExistingProxy) {
        TStalledDataReadScenario scenario;
        for (ui32 limit : {0u, 2u, 3u, 2u, 1u}) {
            scenario.Run(limit);
        }
    }
}

} // NKikimr
