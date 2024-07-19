#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/core/util/testactorsys.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/pdisk/mock/pdisk_mock.h>
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

using namespace NActors;
using namespace NKikimr;

class TTestEnv {
public:
    struct TPDisk {
        ui32 NodeId;
        ui32 PDiskId;
        ui64 PDiskGuid;
        TPDiskMockState::TPtr State;
    };

    const TBlobStorageGroupType GType;
    const ui32 NodeCount;
    std::vector<TPDisk> PDisks;

    TIntrusivePtr<TBlobStorageGroupInfo> CreateGroupInfo(const std::set<ui32>& nodeIds) const {
        UNIT_ASSERT_VALUES_EQUAL(nodeIds.size(), GType.BlobSubgroupSize());
        TVector<TActorId> vdiskIds;
        for (ui32 nodeId : nodeIds) {
            vdiskIds.push_back(MakeBlobStorageVDiskID(nodeId, 1, 0));
        }
        return MakeIntrusive<TBlobStorageGroupInfo>(GType, 1U, 0U, 1U, &vdiskIds);
    }

    void FormatDisk(ui32 index) {
        TPDisk& pdisk = PDisks[index];
        pdisk.State = MakeIntrusive<TPDiskMockState>(pdisk.NodeId, pdisk.PDiskId, pdisk.PDiskGuid, ui64(10) << 40);
    }

    TTestEnv(TBlobStorageGroupType gtype)
        : GType(gtype)
        , NodeCount(gtype.BlobSubgroupSize())
        , PDisks(NodeCount)
        , Counters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
    {
        for (ui32 i = 0; i < PDisks.size(); ++i) {
            PDisks[i] = {i + 1, 1, RandomNumber<ui64>(), nullptr};
            FormatDisk(i);
        }
    }

    class TCoro;

    enum {
        EvTestFinished = EventSpaceBegin(TEvents::ES_PRIVATE)
    };
    struct TEvTestFinished : TEventLocal<TEvTestFinished, EvTestFinished> {
        std::exception_ptr Exception;

        TEvTestFinished(std::exception_ptr exception)
            : Exception(std::move(exception))
        {}
    };

    void Run(std::function<void(TCoro*)> fn) {
        TTestActorSystem runtime(NodeCount);
        SetupLogging(runtime);
        runtime.Start();
        SetupStorage(runtime);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        auto coro = MakeHolder<TCoro>(*this, std::move(fn), edge);
        UNIT_ASSERT(!CurrentCoro);
        CurrentCoro = coro.Get();
        runtime.Register(new TActorCoro(std::move(coro)), TActorId(), 0, std::nullopt, 1);
        auto ev = runtime.WaitForEdgeActorEvent({edge});
        CurrentCoro = nullptr;

        runtime.Stop();

        if (const auto& exception = ev->CastAsLocal<TEvTestFinished>()->Exception) {
            std::rethrow_exception(exception);
        }
    }

    class TCoro : public TActorCoroImpl {
        TTestEnv& Env;
        std::function<void(TCoro*)> Fn;
        const TActorId Edge;
        std::map<ui32, TActorId> Backpressure;

    public:
        TIntrusivePtr<TBlobStorageGroupInfo> Info;

    public:
        TCoro(TTestEnv& env, std::function<void(TCoro*)>&& fn, const TActorId& edge)
            : TActorCoroImpl(65536)
            , Env(env)
            , Fn(std::move(fn))
            , Edge(edge)
            , Info(env.Info)
        {}

        void Run() override {
            std::exception_ptr exception;
            try {
                Fn(this);
            } catch (const TDtorException&) {
                return; // coroutine terminated from outside
            } catch (...) {
                exception = std::current_exception();
            }
            Send(Edge, new TEvTestFinished(exception));
        }

        void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> /*ev*/) {
            UNIT_ASSERT(false);
        }

        TActorId GetBackpressureFor(ui32 diskOrderNum) {
            auto& res = Backpressure[diskOrderNum];
            if (res == TActorId()) {
                // create BS_QUEUE to this disk
                auto counters = Env.Counters->GetSubgroup("bsqueue", ToString(diskOrderNum));
                auto bspctx = MakeIntrusive<TBSProxyContext>(counters->GetSubgroup("subsystem", "memory"));
                auto flowRecord = MakeIntrusive<NBackpressure::TFlowRecord>();
                res = Register(CreateVDiskBackpressureClient(Info, Info->GetVDiskId(diskOrderNum),
                    NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, counters, bspctx,
                    NBackpressure::TQueueClientId(NBackpressure::EQueueClientType::VDiskLoad, 0), "queue", 0, false,
                    TDuration::Seconds(60), flowRecord, NMonitoring::TCountableBase::EVisibility::Public));
            }
            return res;
        }

        std::set<TVDiskID> GetVDiskSet() const {
            std::set<TVDiskID> vdisks;
            for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
                vdisks.insert(Info->GetVDiskId(i));
            }
            return vdisks;
        }

        void CreateQueuesAndWaitForReady() {
            for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
                GetBackpressureFor(i);
            }
            auto vdisks = GetVDiskSet();
            while (!vdisks.empty()) {
                auto ev = WaitForSpecificEvent<TEvProxyQueueState>(&TCoro::ProcessUnexpectedEvent);
                auto& msg = *ev->Get();
                UNIT_ASSERT(msg.IsConnected);
                const bool erased = vdisks.erase(msg.VDiskId);
                UNIT_ASSERT(erased);
            }
        }

        void WaitForRepl() {
            auto vdisks = GetVDiskSet();
            while (!vdisks.empty()) {
                for (const TVDiskID& vdiskId : vdisks) {
                    Send(Info->GetActorId(Info->GetOrderNumber(vdiskId)), new TEvBlobStorage::TEvVStatus(vdiskId));
                }
                for (size_t num = vdisks.size(); num; --num) {
                    auto ev = WaitForSpecificEvent<TEvBlobStorage::TEvVStatusResult>(&TCoro::ProcessUnexpectedEvent);
                    auto& record = ev->Get()->Record;
                    if (record.GetStatus() == NKikimrProto::OK && record.GetReplicated()) {
                        const bool erased = vdisks.erase(VDiskIDFromVDiskID(record.GetVDiskID()));
                        UNIT_ASSERT(erased);
                    }
                }
                if (!vdisks.empty()) {
                    Sleep(TDuration::Seconds(15));
                }
            }
        }

        void Sleep(TDuration timeout) {
            Schedule(timeout, new TEvents::TEvWakeup);
            WaitForSpecificEvent<TEvents::TEvWakeup>(&TCoro::ProcessUnexpectedEvent);
        }

        NKikimrProto::EReplyStatus Put(const TVDiskID& vdiskId, const TLogoBlobID& blobId, const TString& data) {
            TRcBuf dataWithHeadroom(TRcBuf::Uninitialized(data.size(), 32));
            std::memcpy(dataWithHeadroom.UnsafeGetDataMut(), data.data(), data.size());
            Send(GetBackpressureFor(Info->GetOrderNumber(vdiskId)), new TEvBlobStorage::TEvVPut(blobId, TRope(dataWithHeadroom), vdiskId,
                false, nullptr, TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog));
            auto ev = WaitForSpecificEvent<TEvBlobStorage::TEvVPutResult>(&TCoro::ProcessUnexpectedEvent);
            auto& record = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(vdiskId, VDiskIDFromVDiskID(record.GetVDiskID()));
            return record.GetStatus();
        }

        struct TGotItem {
            NKikimrProto::EReplyStatus Status;
            std::optional<TIngress> Ingress;
            std::optional<TString> Data;

            TGotItem(const NKikimrBlobStorage::TQueryResult& pb, TEvBlobStorage::TEvVGetResult& ev)
                : Status(pb.GetStatus())
                , Ingress(pb.HasIngress() ? std::make_optional(TIngress(pb.GetIngress())) : std::nullopt)
                , Data(ev.HasBlob(pb) ? std::make_optional(ev.GetBlobData(pb).ConvertToString()) : std::nullopt)
            {
                UNIT_ASSERT(pb.HasStatus());
            }
        };
        using TGetResult = std::map<TLogoBlobID, TGotItem>;

        TGetResult GetRange(const TVDiskID& vdiskId, const TLogoBlobID& from, const TLogoBlobID& to) {
            return Get(vdiskId, TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::ShowInternals,
                Nothing(), from, to));
        }

        TGetResult GetExtr(const TVDiskID& vdiskId, const TLogoBlobID& blobId) {
            return Get(vdiskId, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::ShowInternals,
                Nothing(), {{blobId}}));
        }

        TGetResult Get(const TVDiskID& vdiskId, std::unique_ptr<TEvBlobStorage::TEvVGet>&& query) {
            Send(GetBackpressureFor(Info->GetOrderNumber(vdiskId)), query.release());
            auto ev = WaitForSpecificEvent<TEvBlobStorage::TEvVGetResult>(&TCoro::ProcessUnexpectedEvent);
            auto& record = ev->Get()->Record;
            TGetResult res;
            for (const auto& item : record.GetResult()) {
                const bool inserted = res.try_emplace(LogoBlobIDFromLogoBlobID(item.GetBlobID()), item, *ev->Get()).second;
                UNIT_ASSERT(inserted); // blob ids should not repeat
            }
            return res;
        }
    };

public:
    TCoro *CurrentCoro = nullptr;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;

private:
    void SetupLogging(TTestActorSystem& runtime) {
        runtime.SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_SKELETON, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BS_LOCALRECOVERY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_VDISK_PUT, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BS_VDISK_GET, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_REPL, NLog::PRI_DEBUG);
//        runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_DEBUG);
//        runtime.SetLogPriority(NKikimrServices::BS_SYNCER, NLog::PRI_DEBUG);
//        runtime.SetLogPriority(NKikimrServices::BS_SYNCLOG, NLog::PRI_DEBUG);
    }

    void SetupStorage(TTestActorSystem& runtime) {
        Info = CreateGroupInfo(runtime.GetNodes());
        const TBlobStorageGroupInfo& info = *Info;

        for (ui32 i = 0; i < info.GetTotalVDisksNum(); ++i) {
            // create pdisk
            TPDisk& pdisk = PDisks[i];
            const TActorId pdiskActorId = MakeBlobStoragePDiskID(pdisk.NodeId, pdisk.PDiskId);
            runtime.RegisterService(pdiskActorId, runtime.Register(CreatePDiskMockActor(pdisk.State), TActorId(), 0,
                std::nullopt, pdisk.NodeId));

            // create vdisk
            TVDiskConfig::TBaseInfo baseInfo(
                info.GetVDiskId(i),
                pdiskActorId,
                pdisk.PDiskGuid,
                pdisk.PDiskId,
                NPDisk::DEVICE_TYPE_SSD,
                0,
                NKikimrBlobStorage::TVDiskKind::Default,
                2, // InitOwnerRound
                ""
            );
            const auto vcfg = MakeIntrusive<TVDiskConfig>(baseInfo);
            vcfg->UseCostTracker = false;
            auto vdiskCounters = Counters->GetSubgroup("vdisk", ToString(i));
            runtime.RegisterService(info.GetActorId(i), runtime.Register(CreateVDisk(vcfg, Info, vdiskCounters),
                TActorId(), 0, std::nullopt, pdisk.NodeId));
        }
    }

private:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
};

void ReplicationTest(const TString datum) {
    TTestEnv env(TBlobStorageGroupType::ErasureMirror3of4);
    const TLogoBlobID blobId(1, 1, 1, 0, datum.size(), 0);
    std::map<ui32, std::vector<ui8>> diskToParts = {{0, {1}}, {1, {2}}, {2, {1}}, {3, {3}}, {4, {3}}, {5, {2, 3}}};
    auto checkPartsInPlace = [&](auto *coro) {
        for (ui32 i = 0; i < coro->Info->GetTotalVDisksNum(); ++i) {
            auto res = coro->GetExtr(coro->Info->GetVDiskId(i), blobId);
            if (const auto it = diskToParts.find(i); it != diskToParts.end()) {
                const std::vector<ui8>& parts = it->second;
                std::set<ui8> m(parts.begin(), parts.end());
                for (const auto& [key, value] : res) {
                    UNIT_ASSERT_VALUES_EQUAL(key.FullID(), blobId);
                    UNIT_ASSERT(m.count(key.PartId()));
                    m.erase(key.PartId());
                    UNIT_ASSERT_VALUES_EQUAL(value.Status, NKikimrProto::OK);
                    UNIT_ASSERT(value.Data);
                    UNIT_ASSERT_VALUES_EQUAL(*value.Data, key.PartId() < 3 ? datum : TString());
                }
                UNIT_ASSERT(m.empty());
            } else {
                auto& [key, value] = *res.begin();
                UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(key, blobId);
                UNIT_ASSERT_VALUES_EQUAL(value.Status, NKikimrProto::NODATA);
            }
        }
    };
    env.Run([&](auto *coro) {
        coro->CreateQueuesAndWaitForReady();
        for (const auto& [diskOrderNum, partIds] : diskToParts) {
            for (const ui8 partId : partIds) {
                UNIT_ASSERT_VALUES_EQUAL(coro->Put(coro->Info->GetVDiskId(diskOrderNum), TLogoBlobID(blobId, partId),
                    partId < 3 ? datum : TString()), NKikimrProto::OK);
            }
        }
        TIngress required;
        for (const auto& [diskOrderNum, partIds] : diskToParts) {
            for (const ui8 partId : partIds) {
                const TVDiskID& vdiskId = coro->Info->GetVDiskId(diskOrderNum);
                const TLogoBlobID id(blobId, partId);
                required.Merge(*TIngress::CreateIngressWOLocal(&coro->Info->GetTopology(), vdiskId, id));
            }
        }
        for (;;) {
            bool done = true;
            TStringStream msgs;
            for (const auto& [diskOrderNum, partIds] : diskToParts) {
                for (const ui8 partId : partIds) {
                    const TVDiskID& vdiskId = coro->Info->GetVDiskId(diskOrderNum);
                    const TLogoBlobID id(blobId, partId);
                    auto allres = coro->GetExtr(vdiskId, id);
                    UNIT_ASSERT_VALUES_EQUAL(allres.size(), 1);
                    const auto& res = allres.begin()->second;
                    UNIT_ASSERT(res.Ingress);
                    TIngress ingr(*res.Ingress);
                    ingr = ingr.CopyWithoutLocal(coro->Info->Type);
                    if (ingr.Raw() != required.Raw()) {
                        done = false;
                    }
                    msgs << "*** " << vdiskId << " " << id << " "
                        << ingr.ToString(&coro->Info->GetTopology(), vdiskId, id) << " <> "
                        << required.ToString(&coro->Info->GetTopology(), vdiskId, id)
                        << Endl;
                }
            }
            Cerr << Endl << msgs.Str() << Endl;
            if (done) {
                break;
            } else {
                coro->Sleep(TDuration::Seconds(10));
            }
        }
        checkPartsInPlace(coro);
    });
    Cerr << Endl << "*** rechecking data after restart" << Endl << Endl;
    env.Run([&](auto *coro) {
        coro->CreateQueuesAndWaitForReady();
        checkPartsInPlace(coro);
    });
    for (ui32 diskIdx = 0; diskIdx < env.GType.BlobSubgroupSize(); ++diskIdx) {
        Cerr << Endl << "*** formatting disk " << diskIdx << Endl << Endl;
        env.FormatDisk(diskIdx);
        env.Run([&](auto *coro) {
            coro->CreateQueuesAndWaitForReady();
            coro->WaitForRepl();
            checkPartsInPlace(coro);
        });
    }
    for (ui32 diskIdx = 0; diskIdx < env.GType.BlobSubgroupSize(); ++diskIdx) {
        for (ui32 diskIdx1 = 0; diskIdx1 < diskIdx; ++diskIdx1) {
            Cerr << Endl << "*** formatting disks " << diskIdx << ", " << diskIdx1 << Endl << Endl;
            env.FormatDisk(diskIdx);
            env.FormatDisk(diskIdx1);
            env.Run([&](auto *coro) {
                coro->CreateQueuesAndWaitForReady();
                coro->WaitForRepl();
                checkPartsInPlace(coro);
            });
        }
    }
}

Y_UNIT_TEST_SUITE(Mirror3of4) {

    Y_UNIT_TEST(ReplicationSmall) {
        ReplicationTest("hello, world!");
    }

    Y_UNIT_TEST(ReplicationHuge) {
        ui32 size = 1 << 20;
        TString datum = TString::Uninitialized(size);
        char *p = datum.Detach();
        for (ui32 i = 0; i < size; ++i) {
            p[i] = i;
        }
        ReplicationTest(datum);
    }

}
