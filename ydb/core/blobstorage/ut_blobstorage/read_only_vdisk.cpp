#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
// #include <ydb/core/load_test/events.h>
#include <ydb/core/load_test/service_actor.h>

#include <library/cpp/protobuf/util/pb_io.h>


struct TTetsEnv {
    TTetsEnv()
    : Env({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    })
    , Counters(new ::NMonitoring::TDynamicCounters())
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        VDiskActorId = GroupInfo->GetActorId(0);

        DataArr = {
            PrepareData(128 * 1024, 0),
            PrepareData(32 * 1024, 3),
        };
    }

    static TString PrepareData(const ui32 dataLen, const ui32 start) {
        TString data(Reserve(dataLen));
        for (ui32 i = 0; i < dataLen; ++i) {
            data.push_back('a' + (start + i) % 26);
        }
        return data;
    };

    void SendPut(ui32 step, NKikimrProto::EReplyStatus expectedStatus) {
        const TString& data = DataArr[step % 2];
        const TLogoBlobID id(1, 1, step, 0, data.size(), 0);
        Cerr << "SEND TEvPut with key " << id.ToString() << Endl;
        const TActorId sender = Env.Runtime->AllocateEdgeActor(VDiskActorId.NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
        Env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, expectedStatus);
        Cerr << "TEvPutResult: " << res->Get()->ToString() << Endl;
    };

    auto SendGet(ui32 step, ui32 dataSize, bool mustRestoreFirst=false) {
        const TLogoBlobID blobId(1, 1, step, 0, dataSize, 0);
        Cerr << "SEND TEvGet with key " << blobId.ToString() << Endl;
        const TActorId sender = Env.Runtime->AllocateEdgeActor(VDiskActorId.NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            blobId,
            /* shift */ 0,
            /* size */ dataSize,
            TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead,
            mustRestoreFirst
        );
        Env.Runtime->WrapInActorContext(sender, [&] () {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        TInstant getDeadline = Env.Now() + TDuration::Seconds(30);
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, /* termOnCapture */ false, getDeadline);
        Cerr << "TEvGetResult: " << res->Get()->ToString() << Endl;
        return res;
    };

    auto SendGetWithChecks(ui32 step) {
        const TString& data = DataArr[step % 2];
        auto res = SendGet(step, data.size());
        Y_ABORT_UNLESS(res->Get()->Status == NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.size(), data.size());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), data);
        return res;
    }

    void ReadAllBlobs(ui32 steps) {
        Cerr << "=== Read all " << steps << " blob(s) ===" << Endl;
        for (ui32 step = 0; step < steps; ++step) {
            SendGetWithChecks(step);
        }
    }

    void SetVDiskReadOnly(ui32 position, bool value) {
        const TVDiskID& someVDisk = GroupInfo->GetVDiskId(position);
        auto baseConfig = Env.FetchBaseConfig();

        const auto& somePDisk = baseConfig.GetPDisk(position);
        const auto& someVSlot = baseConfig.GetVSlot(position);
        Cerr << "Setting VDisk read-only to " << value << " for position " << position << Endl;
        if (!value) {
            Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->SetReadOnly(someVDisk, value);
        }
        Env.SetVDiskReadOnly(somePDisk.GetNodeId(), somePDisk.GetPDiskId(), someVSlot.GetVSlotId().GetVSlotId(), someVDisk, value);
        Env.Sim(TDuration::Seconds(30));
        if (value) {
            Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->SetReadOnly(someVDisk, value);
        }
    }

    void SendCutLog(ui32 position) {
        auto baseConfig = Env.FetchBaseConfig();
        const auto& somePDisk = baseConfig.GetPDisk(position);
        const TActorId sender = Env.Runtime->AllocateEdgeActor(somePDisk.GetNodeId(), __FILE__, __LINE__);
        Env.Runtime->WrapInActorContext(sender, [&] () {
            Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->TrimQuery();
        });
        Env.Sim(TDuration::Minutes(1));
    }

    auto SendCollectGarbage(ui32 step) {
        const TActorId sender = Env.Runtime->AllocateEdgeActor(VDiskActorId.NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
            1, ++CollectGeneration, 0,
            true, 1, step,
            nullptr, nullptr, TInstant::Max()
        );
        Env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(sender, false);
        Env.Sim(TDuration::Seconds(10));
        return res;
    }

    auto SendDiscover() {
        const TActorId sender = Env.Runtime->AllocateEdgeActor(VDiskActorId.NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvDiscover>(
            1, 1, false, true, TInstant::Max(), 0, false
        );
        Env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(sender, false);
        return res;
    }

    auto StartStorageLoadActor() {
        StorageLoadActorId = Env.Runtime->Register(NKikimr::CreateLoadTestActor(Counters), TActorId(), 0, std::nullopt, 1);
    }

    auto RunStorageLoad() {
        if (!StorageLoadActorId) {
            StartStorageLoadActor();
        }
        const TActorId sender = Env.Runtime->AllocateEdgeActor(VDiskActorId.NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvLoad::TEvLoadTestRequest>();

        TString conf("StorageLoad: {\n"
            "DurationSeconds: 8\n"
            "Tablets: {\n"
                "Tablets: { TabletId: 1 Channel: 0 GroupId: " + ToString(GroupInfo->GroupID) + " Generation: 1 }\n"
                "WriteSizes: { Weight: 1.0 Min: 1000000 Max: 4000000 }\n"
                "WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }\n"
                "MaxInFlightWriteRequests: 10\n"
                "FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }\n"
                "PutHandleClass: TabletLog\n"
            "}\n"
        "}");
        auto constStream = TStringInput(conf);
        ev->Record = ParseFromTextFormat<NKikimr::TEvLoadTestRequest>(constStream);
        Env.Runtime->WrapInActorContext(sender, [&] {
            Env.Runtime->Send(new IEventHandle(StorageLoadActorId, sender, ev.release()));
        });
        {
            auto res = Env.WaitForEdgeActorEvent<TEvLoad::TEvLoadTestResponse>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), 1);
        }
        {
            auto res = Env.WaitForEdgeActorEvent<TEvLoad::TEvNodeFinishResponse>(sender, false);
            UNIT_ASSERT(res->Get()->Record.GetSuccess());
        }
        Env.Sim(TDuration::Seconds(60));
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    TVector<TString> DataArr;
    ui32 CollectGeneration = 0;
    TActorId StorageLoadActorId;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
};


Y_UNIT_TEST_SUITE(ReadOnlyVDisk) {

    Y_UNIT_TEST(TestReads) {
        TTetsEnv env;

        Cerr << "=== Trying to put and get a blob ===" << Endl;
        ui32 step = 0;
        env.SendPut(step, NKikimrProto::OK);
        ++step;
        env.ReadAllBlobs(step);


        for (ui32 i = 0; i < 7; ++i) {
            Cerr << "=== Putting VDisk #" << i << " to read-only ===" << Endl;
            env.SetVDiskReadOnly(i, true);
            env.ReadAllBlobs(step);
            env.SendCutLog(i);
            env.SendCutLog((i + 1) % 7);
        }

        for (ui32 i = 0; i < 7; ++i) {
            Cerr << "=== Restoring to normal VDisk #" << i << " ===" << Endl;
            env.SetVDiskReadOnly(i, false);
            env.ReadAllBlobs(step);
        }
    }

    Y_UNIT_TEST(TestWrites) {
        TTetsEnv env;

        Cerr << "=== Trying to put and get a blob ===" << Endl;
        ui32 step = 0;
        env.SendPut(step, NKikimrProto::OK);
        ++step;
        env.ReadAllBlobs(step);

        Cerr << "=== Putting VDisk #0 to read-only ===" << Endl;
        env.SetVDiskReadOnly(0, true);

        Cerr << "=== Write 10 blobs, expect some VDisks refuse parts but writes go through ===" << Endl;
        for (ui32 i = 0; i < 10; ++i) {
            env.SendPut(step, NKikimrProto::OK);
            ++step;
        }

        env.SendCutLog(0);
        env.ReadAllBlobs(step);

        Cerr << "=== Put 2 more VDisks to read-only ===" << Endl;
        env.SetVDiskReadOnly(1, true);
        env.SetVDiskReadOnly(2, true);

        Cerr << "=== Write 10 more blobs, expect errors ===" << Endl;
        for (ui32 i = 0; i < 10; ++i) {
            env.SendPut(step, NKikimrProto::ERROR);
            ++step;
        }
        env.SendCutLog(0);
        env.SendCutLog(1);
        env.SendCutLog(2);
        env.SendCutLog(3);
        // Even though previous writes were not successfull, some parts were written which is enough to read the blobs back, at least before GC happens.
        env.ReadAllBlobs(step);

        Cerr << "=== Restoring to normal VDisk #0 ===" << Endl;
        env.SetVDiskReadOnly(0, false);

        Cerr << "=== Write 10 blobs, expect some VDisks refuse parts but the writes still go through ===" << Endl;
        for (ui32 i = 0; i < 10; ++i) {
            env.SendPut(step, NKikimrProto::OK);
            ++step;
        }

        env.ReadAllBlobs(step);
    }

    Y_UNIT_TEST(TestGetWithMustRestoreFirst) {
        TTetsEnv env;

        Cerr << "=== Trying to put and get a blob ===" << Endl;
        ui32 step = 0;
        env.SendPut(step, NKikimrProto::OK);
        ++step;
        env.ReadAllBlobs(step);

        Cerr << "=== Putting VDisk #0 to read-only ===" << Endl;
        env.SetVDiskReadOnly(0, true);

        Cerr << "=== Write 10 blobs, expect some VDisks refuse parts but writes go through ===" << Endl;
        for (ui32 i = 0; i < 10; ++i) {
            env.SendPut(step, NKikimrProto::OK);
            ++step;
        }

        env.ReadAllBlobs(step);

        Cerr << "=== Put 2 more VDisks to read-only ===" << Endl;
        env.SetVDiskReadOnly(1, true);
        env.SetVDiskReadOnly(2, true);

        env.SendCutLog(1);
        env.SendCutLog(2);
        env.SendCutLog(3);

        ui32 stoppedStep = step;

        Cerr << "=== Write 10 more blobs, expect errors ===" << Endl;
        for (ui32 i = 0; i < 10; ++i) {
            env.SendPut(step, NKikimrProto::ERROR);
            ++step;
        }
        // Even though previous writes were not successfull, some parts were written which is enough to read the blobs back, at least before GC happens.
        // env.SendGetWithChecks(stoppedStep);
        auto res = env.SendGet(stoppedStep, env.DataArr[stoppedStep % 2].size(), true);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
    }

    Y_UNIT_TEST(TestGarbageCollect) {
        TTetsEnv env;
        ui32 step = 0;
        auto checkGarbageCollectErr = [&]() {
            UNIT_ASSERT_VALUES_EQUAL(env.SendCollectGarbage(step)->Get()->Status, NKikimrProto::ERROR);
        };
        auto checkGarbageCollectOk = [&]() {
            UNIT_ASSERT_VALUES_EQUAL(env.SendCollectGarbage(step)->Get()->Status, NKikimrProto::OK);
            auto res = env.SendGet(step - 1, 1);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_UNEQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
        };

        env.SendPut(step++, NKikimrProto::OK);
        env.SendPut(step++, NKikimrProto::OK);
        env.ReadAllBlobs(step);
        checkGarbageCollectOk();

        env.SetVDiskReadOnly(0, true);
        env.SendPut(step++, NKikimrProto::OK);
        checkGarbageCollectOk();
        env.SendCutLog(0);
        env.SetVDiskReadOnly(1, true);
        env.SendPut(step++, NKikimrProto::OK);
        env.SendCutLog(1);
        checkGarbageCollectOk();
        env.SetVDiskReadOnly(2, true);
        env.SendPut(step, NKikimrProto::ERROR);
        env.SendCutLog(2);
        checkGarbageCollectErr();
        env.SendCutLog(2);

        for (ui32 i = 3; i < 7; ++i) {
            Cerr << "=== Putting VDisk #" << i << " to read-only ===" << Endl;
            env.SetVDiskReadOnly(i, true);
            checkGarbageCollectErr();
        }

        for (ui32 i = 0; i < 7 - 3; ++i) {
            Cerr << "=== Putting VDisk #" << i << " to normal ===" << Endl;
            env.SetVDiskReadOnly(i, false);
            checkGarbageCollectErr();
        }

        env.SetVDiskReadOnly(4, false);
        env.SendCutLog(4);
        checkGarbageCollectOk();
        env.SetVDiskReadOnly(5, false);
        checkGarbageCollectOk();
        env.SendCutLog(5);
        env.SetVDiskReadOnly(6, false);
        checkGarbageCollectOk();

        env.SendPut(step++, NKikimrProto::OK);
        checkGarbageCollectOk();
    }

    Y_UNIT_TEST(TestDiscover) {
        TTetsEnv env;
        ui32 step = 0;

        env.SendPut(step++, NKikimrProto::OK);
        env.SendPut(step++, NKikimrProto::OK);
        env.SendPut(step++, NKikimrProto::OK);
        env.ReadAllBlobs(step);

        auto checkDiscoverOk = [&]() {
            auto res = env.SendDiscover();
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            auto logoBlobId = res->Get()->Id;
            UNIT_ASSERT_VALUES_EQUAL(logoBlobId.TabletID(), 1);
            UNIT_ASSERT_VALUES_EQUAL(logoBlobId.Step(), step - 1);
        };

        env.SetVDiskReadOnly(0, true);
        env.SendPut(step++, NKikimrProto::OK);
        checkDiscoverOk();
        env.SetVDiskReadOnly(1, true);
        env.SendPut(step++, NKikimrProto::OK);
        checkDiscoverOk();
        env.SetVDiskReadOnly(2, true);
        env.SendPut(step++, NKikimrProto::ERROR);
        checkDiscoverOk();

        for (ui32 i = 3; i < 7; ++i) {
            Cerr << "=== Putting VDisk #" << i << " to read-only ===" << Endl;
            env.SetVDiskReadOnly(i, true);
            env.ReadAllBlobs(step);
            checkDiscoverOk();
        }

        for (ui32 i = 0; i < 7; ++i) {
            Cerr << "=== Putting VDisk #" << i << " to normal ===" << Endl;
            env.SetVDiskReadOnly(i, false);
            checkDiscoverOk();
        }

        env.SendPut(step++, NKikimrProto::OK);
        checkDiscoverOk();
    }

    Y_UNIT_TEST(TestSync) {
        TTetsEnv env;
        ui32 step = 0;

        for (ui32 i = 0; i < 7; ++i) {
            env.SetVDiskReadOnly(i, true);
            env.SetVDiskReadOnly((i + 1) % 7, true);
            env.SendPut(step++, NKikimrProto::OK);
            env.SendCutLog(i);
            env.SendCutLog((i + 1) % 7);
            env.SetVDiskReadOnly(i, false);
            env.SetVDiskReadOnly((i + 1) % 7, false);
        }

        env.ReadAllBlobs(step);
    }

    Y_UNIT_TEST(TestStorageLoad) {
        TTetsEnv env;

        env.RunStorageLoad();

        env.SetVDiskReadOnly(0, true);
        env.RunStorageLoad();
        env.SendCutLog(0);

        env.SetVDiskReadOnly(1, true);
        env.RunStorageLoad();
        env.SendCutLog(1);

        for (ui32 i = 2; i < 8; ++i) {
            env.SetVDiskReadOnly(i, true);
        }
        env.RunStorageLoad();
        for (ui32 i = 2; i < 8; ++i) {
            env.SendCutLog(i);
        }

        for (ui32 i = 0; i < 7; ++i) {
            env.SetVDiskReadOnly(i, false);
        }
        env.RunStorageLoad();
        for (ui32 i = 0; i < 8; ++i) {
            env.SendCutLog(i);
        }
    }
}
