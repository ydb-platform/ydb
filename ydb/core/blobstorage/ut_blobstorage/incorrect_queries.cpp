#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/protos/blobstorage.pb.h>

Y_UNIT_TEST_SUITE(IncorrectQueries) {
    const TVector<TString> erasureTypes = {"none", "block-4-2", "mirror-3", "mirror-3of4", "mirror-3-dc"};

    void SendPut(TEnvironmentSetup& env, TTestInfo& test,
                const TLogoBlobID& blobId, NKikimrProto::EReplyStatus status, ui32 blob_size, bool isEmptyObject = false, bool isEmptyMeta = false) {
        const TString data(blob_size, 'a');
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvVPut>(blobId, TRope(data), test.Info->GetVDiskInSubgroup(0, blobId.Hash()),
                                 false, nullptr, TInstant::Max(), NKikimrBlobStorage::AsyncBlob);

        if (isEmptyObject) {
            if (isEmptyMeta) {
                ev = std::make_unique<TEvBlobStorage::TEvVPut>();
            } else {
                NKikimrBlobStorage::TEvVPut protoQuery;
                static_cast<TEvBlobStorage::TEvVPut*>(ev.get())->Record = protoQuery;
            }
        } else if (isEmptyMeta) {
            static_cast<TEvBlobStorage::TEvVPut*>(ev.get())->StripPayload();
        }

        env.WithQueueId(test.Info->GetVDiskInSubgroup(0, blobId.Hash()), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            test.Edge = test.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            test.Runtime->Send(new IEventHandle(queueId, test.Edge, ev.release()), queueId.NodeId());
            auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
            UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvVPutResult);
            TEvBlobStorage::TEvVPutResult *putResult = handle->Get<TEvBlobStorage::TEvVPutResult>();
            UNIT_ASSERT_VALUES_EQUAL(putResult->Record.GetStatus(), status);
        });
    }

    void SendGet(TEnvironmentSetup& env, TTestInfo& test, const TVDiskID& vdiskId, const TLogoBlobID& blobId, const TString& part,
             NKikimrProto::EReplyStatus status = NKikimrProto::OK, bool isEmptyObject = false, bool isEmptyMeta = false) {
        std::unique_ptr<IEventBase> ev = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId,
            TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
            Nothing(), {{blobId, 0u, ui32(part.size())}});

        if (isEmptyObject) {
            if (isEmptyMeta) {
                ev = std::make_unique<TEvBlobStorage::TEvVGet>();
            } else {
                NKikimrBlobStorage::TEvVGet protoQuery;
                static_cast<TEvBlobStorage::TEvVGet*>(ev.get())->Record = protoQuery;
            }
        } else if (isEmptyMeta) {
            static_cast<TEvBlobStorage::TEvVGet*>(ev.get())->StripPayload();
        }

        env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
            test.Edge = test.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            test.Runtime->Send(new IEventHandle(queueId, test.Edge, ev.release()), queueId.NodeId());
            auto r = test.Runtime->WaitForEdgeActorEvent({test.Edge});

            UNIT_ASSERT_EQUAL(r->Type, TEvBlobStorage::EvVGetResult);
            TEvBlobStorage::TEvVGetResult *getResult = r->Get<TEvBlobStorage::TEvVGetResult>();
            UNIT_ASSERT_VALUES_EQUAL(getResult->Record.GetStatus(), status);
            if (status == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(getResult->GetBlobData(getResult->Record.GetResult(0)).ConvertToString(), part);
            }
        });
    }

    struct TBlobInfo {
        TLogoBlobID BlobId;
        TString Data;
        NKikimrProto::EReplyStatus ExpectedStatus;
    };

    void SendEmptyMultiPut(TEnvironmentSetup& env, TTestInfo& test, const std::vector<TBlobInfo>& blobs, NKikimrProto::EReplyStatus status,
                           NKikimrBlobStorage::TEvVMultiPut proto) {

        std::unique_ptr<IEventBase> ev(new TEvBlobStorage::TEvVMultiPut(test.Info->GetVDiskId(0),
                                TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog, false, nullptr));

        for(auto [blob, data, status] : blobs) {
            static_cast<TEvBlobStorage::TEvVMultiPut*>(ev.get())->AddVPut(blob, TRcBuf(data), nullptr, nullptr, NWilson::TTraceId());
        }

        static_cast<TEvBlobStorage::TEvVMultiPut*>(ev.get())->Record = proto;

        env.WithQueueId(test.Info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            test.Edge = test.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            test.Runtime->Send(new IEventHandle(queueId, test.Edge, ev.release()), queueId.NodeId());
            auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
            UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvVMultiPutResult);
            TEvBlobStorage::TEvVMultiPutResult *putResult = handle->Get<TEvBlobStorage::TEvVMultiPutResult>();
            UNIT_ASSERT_VALUES_EQUAL(putResult->Record.GetStatus(), status);

            auto results = putResult->Record.GetItems();
            for(int i = 0; i < results.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(results[i].GetStatus(), blobs[i].ExpectedStatus);
            }
        });
    }

    void SendMultiPut(TEnvironmentSetup& env, TTestInfo& test, NKikimrProto::EReplyStatus status, const std::vector<TBlobInfo>& blobs) {
        std::unique_ptr<IEventBase> ev(new TEvBlobStorage::TEvVMultiPut(test.Info->GetVDiskInSubgroup(0, blobs[0].BlobId.Hash()),
                                TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog, false, nullptr));

        for(auto [blob, data, status] : blobs) {
            static_cast<TEvBlobStorage::TEvVMultiPut*>(ev.get())->AddVPut(blob, TRcBuf(data), nullptr, nullptr, NWilson::TTraceId());
        }

        env.WithQueueId(test.Info->GetVDiskInSubgroup(0, blobs[0].BlobId.Hash()), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            test.Edge = test.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            test.Runtime->Send(new IEventHandle(queueId, test.Edge, ev.release()), queueId.NodeId());
            auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
            UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvVMultiPutResult);
            TEvBlobStorage::TEvVMultiPutResult *putResult = handle->Get<TEvBlobStorage::TEvVMultiPutResult>();
            UNIT_ASSERT_VALUES_EQUAL(putResult->Record.GetStatus(), status);

            auto results = putResult->Record.GetItems();
            for(size_t i = 0; i < blobs.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(results[i].GetStatus(), blobs[i].ExpectedStatus);
            }
        });
    }

    void MakeCrcTest(const TString& erasure, ui64 crc) {
        TEnvironmentSetup env(true, GetErasureTypeByString(erasure));
        env.SetupLogging();
        TTestInfo test = InitTest(env);

        NKikimrProto::TLogoBlobID pBlobId;
        auto blobId = LogoBlobIDFromLogoBlobID(pBlobId);
        SendPut(env,test, blobId, NKikimrProto::ERROR, 0);

        pBlobId.set_rawx1(0xABC);
        pBlobId.set_rawx2(0);
        pBlobId.set_rawx3(crc);

        blobId = LogoBlobIDFromLogoBlobID(pBlobId);
        SendPut(env,test, blobId, NKikimrProto::ERROR, 0);
    }

    Y_UNIT_TEST(InvalidPartID) {
        for(const auto& erasure : erasureTypes) {
            TEnvironmentSetup env(true, GetErasureTypeByString(erasure));
            TTestInfo test = InitTest(env);

            constexpr ui32 size = 100;
            TLogoBlobID blobId(1, 1, 0, 0, size, 0, 13);
            SendPut(env, test, blobId, NKikimrProto::ERROR, size);
        }
    }

    Y_UNIT_TEST(VeryBigBlob) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = (10 << 21);
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);

        SendPut(env, test, blobId, NKikimrProto::ERROR, size);
    }

    Y_UNIT_TEST(Incompatible) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);

        SendPut(env, test, blobId, NKikimrProto::ERROR, size - 42);
        SendPut(env, test, blobId, NKikimrProto::ERROR, size + 42);
        SendPut(env, test, blobId, NKikimrProto::ERROR, 0);


        SendPut(env, test, blobId, NKikimrProto::ERROR, size - 42, true);
        SendPut(env, test, blobId, NKikimrProto::ERROR, size + 42, true);
        SendPut(env, test, blobId, NKikimrProto::ERROR, 0, true);

        return;

        SendPut(env, test, blobId, NKikimrProto::ERROR, size - 42, false, true);
        SendPut(env, test, blobId, NKikimrProto::ERROR, size + 42, false, true);
        SendPut(env, test, blobId, NKikimrProto::ERROR, 0, false, true);

        SendPut(env, test, blobId, NKikimrProto::ERROR, size - 42, true, true);
        SendPut(env, test, blobId, NKikimrProto::ERROR, size + 42, true, true);
        SendPut(env, test, blobId, NKikimrProto::ERROR, 0, true, true);
    }

    Y_UNIT_TEST(Proto) {
       for(const auto& erasure : erasureTypes) {
            TEnvironmentSetup env(true, GetErasureTypeByString(erasure));
            TTestInfo test = InitTest(env);

            NKikimrProto::TLogoBlobID protoBlobId;

            auto blobId = LogoBlobIDFromLogoBlobID(protoBlobId);
            SendPut(env, test, blobId, NKikimrProto::ERROR, 0);
            SendPut(env, test, blobId, NKikimrProto::ERROR, 42);

            protoBlobId.set_rawx1(0xABC);
            protoBlobId.set_rawx2(0xFFFFFF);
            protoBlobId.set_rawx3(0xFF);
            blobId = LogoBlobIDFromLogoBlobID(protoBlobId);
            SendPut(env, test, blobId, NKikimrProto::ERROR, 42);

            protoBlobId.clear_rawx1();
            blobId = LogoBlobIDFromLogoBlobID(protoBlobId);
            SendPut(env, test, blobId, NKikimrProto::ERROR, 42);
        }
    }

    Y_UNIT_TEST(BaseReadingTest) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 10;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        SendPut(env, test, blobId, NKikimrProto::OK, size);
        const TString data(size, 'a');
        auto vdiskId = test.Info->GetVDiskId(0);
        SendGet(env, test, vdiskId, blobId, data);
    }

    Y_UNIT_TEST(EmptyGetTest) {
        return;
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 10;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        SendPut(env, test, blobId, NKikimrProto::OK, size);
        const TString data(size, 'a');
        auto vdiskId = test.Info->GetVDiskId(0);

        SendGet(env, test, vdiskId, blobId, data, NKikimrProto::ERROR, true);
        SendGet(env, test, vdiskId, blobId, data, NKikimrProto::OK, false, true);
        SendGet(env, test, vdiskId, blobId, data, NKikimrProto::ERROR, true, true);
    }

    Y_UNIT_TEST(WrongDataSize) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        SendPut(env, test, blobId, NKikimrProto::ERROR, size - 1);
        auto vdiskId = test.Info->GetVDiskInSubgroup(0, blobId.Hash());
        SendGet(env, test, vdiskId, blobId, "", NKikimrProto::OK);
    }

    Y_UNIT_TEST(WrongVDiskID) {
        TEnvironmentSetup env(true, GetErasureTypeByString("block-4-2"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        SendPut(env, test, blobId, NKikimrProto::OK, 32);

        auto vdiskId = test.Info->GetVDiskInSubgroup(1, blobId.Hash());
        SendGet(env, test, vdiskId, blobId, "", NKikimrProto::OK);
    }

    Y_UNIT_TEST(ProtoBlobGet) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        NKikimrProto::TLogoBlobID protoBlobId;
        protoBlobId.set_rawx1(0xABC);
        protoBlobId.set_rawx2(std::numeric_limits<uint64_t>::max());
        protoBlobId.set_rawx3(17);

        TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(protoBlobId);
        SendPut(env, test, blobId, NKikimrProto::OK, 1);
        auto vdiskId = test.Info->GetVDiskInSubgroup(0, blobId.Hash());
        SendGet(env, test, vdiskId, blobId, "a");
    }

    Y_UNIT_TEST(ProtoQueryGet) {
        return;
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        SendPut(env, test, blobId, NKikimrProto::OK, size);
        const TString data(size, 'a');
        auto vdiskId = test.Info->GetVDiskId(0);
        SendGet(env, test, vdiskId, blobId, data, NKikimrProto::ERROR, true);
    }


    Y_UNIT_TEST(WrongPartId) {
        TEnvironmentSetup env(true, GetErasureTypeByString("block-4-2"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 2);
        SendPut(env, test, blobId, NKikimrProto::ERROR, 32);

        auto vdiskId = test.Info->GetVDiskInSubgroup(0, blobId.Hash());
        SendGet(env, test, vdiskId, blobId, "");
    }

    Y_UNIT_TEST(EmptyTest) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 0;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        SendPut(env, test, blobId, NKikimrProto::OK, size);
        const TString data("");
        auto vdiskId = test.Info->GetVDiskId(0);
        SendGet(env, test, vdiskId, blobId, data);
    }


    Y_UNIT_TEST(ProtobufBlob) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        NKikimrProto::TLogoBlobID pBlobId;
        auto blobId = LogoBlobIDFromLogoBlobID(pBlobId);
        SendPut(env,test, blobId, NKikimrProto::ERROR, 0);

        pBlobId.set_rawx1(std::numeric_limits<ui64>::max());
        pBlobId.set_rawx2(std::numeric_limits<ui64>::max());
        pBlobId.set_rawx3(std::numeric_limits<ui64>::max());

        blobId = LogoBlobIDFromLogoBlobID(pBlobId);
        SendPut(env,test, blobId, NKikimrProto::ERROR, 0);
    }

    Y_UNIT_TEST(SameBlob) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 10;
        TLogoBlobID BlobId(1, 1, 0, 0, size, 0, 1);
        TLogoBlobID BlobId2(1, 1, 0, 0, size+100, 0, 1);
        SendPut(env, test, BlobId, NKikimrProto::OK, size);
        SendPut(env, test, BlobId2, NKikimrProto::OK, size+100);
        const TString data(size - 1, 'a');
        auto vdiskId = test.Info->GetVDiskId(0);
        SendGet(env, test, vdiskId, BlobId, data);
        SendGet(env, test, vdiskId, BlobId2, data);

    }

    Y_UNIT_TEST(WrongCrc) {
        for(const auto& erasure : erasureTypes) {
            MakeCrcTest(erasure, (1ull << 31) + 1);
            MakeCrcTest(erasure, (1ull << 30) + (1ull << 31) + 1);
        }
    }


    Y_UNIT_TEST(BasePutTest) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 10;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        SendPut(env, test, blobId, NKikimrProto::OK, size);
    }

    Y_UNIT_TEST(MultiPutBaseTest) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);
        constexpr ui32 size = 1000;
        TLogoBlobID blobId(1, 1, 0, 0, size, 0, 1);
        TString data(size, 'a');
        std::vector<TBlobInfo> blobs;
        blobs.push_back({blobId, data, NKikimrProto::OK});
        SendMultiPut(env, test, NKikimrProto::OK, blobs);
    }

    Y_UNIT_TEST(MultiPutCrcTest) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        NKikimrProto::TLogoBlobID pBlobId;
        auto blobId = LogoBlobIDFromLogoBlobID(pBlobId);

        std::vector<TBlobInfo> blobs;
        blobs.push_back({blobId, "", NKikimrProto::ERROR});

        SendMultiPut(env, test, NKikimrProto::OK, blobs);

        pBlobId.set_rawx1(0xABC);
        pBlobId.set_rawx2(0);
        pBlobId.set_rawx3((1ull << 30) + (1ull << 31) + 1);

        blobId = LogoBlobIDFromLogoBlobID(pBlobId);
        blobs.push_back({blobId, "", NKikimrProto::ERROR});

        pBlobId.set_rawx3((1ull << 31) + 1);

        blobId = LogoBlobIDFromLogoBlobID(pBlobId);
        blobs.push_back({blobId, "", NKikimrProto::ERROR});

        SendMultiPut(env, test, NKikimrProto::OK, blobs);

        blobs.clear();

        constexpr ui32 size = 1000;
        TLogoBlobID niceBlobId(1, 1, 0, 0, size, 0, 1);
        TString data(size, 'a');

        blobs.push_back({niceBlobId, data, NKikimrProto::OK});
        SendMultiPut(env, test, NKikimrProto::OK, blobs);
    }


    Y_UNIT_TEST(MultiPutWithoutBlobs) {
        return;
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);
        std::unique_ptr<IEventBase> ev(new TEvBlobStorage::TEvVMultiPut(test.Info->GetVDiskId(0),
                                TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog, false, nullptr));

        env.WithQueueId(test.Info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            test.Edge = test.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            test.Runtime->Send(new IEventHandle(queueId, test.Edge, ev.release()), queueId.NodeId());
            auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
            UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvVMultiPutResult);
            TEvBlobStorage::TEvVMultiPutResult *putResult = handle->Get<TEvBlobStorage::TEvVMultiPutResult>();
            UNIT_ASSERT_VALUES_EQUAL(putResult->Record.GetStatus(), NKikimrProto::ERROR);
        });
    }

    Y_UNIT_TEST(ProtoHasOnlyVDiskId) {
        return;
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);
        NKikimrBlobStorage::TVDiskID VDiskId;
        VDiskIDFromVDiskID(test.Info->GetVDiskId(0), &VDiskId);

        NKikimrBlobStorage::TEvVMultiPut query;
        auto VDisk = query.MutableVDiskID();
        *VDisk = VDiskId;

        SendEmptyMultiPut(env, test, {}, NKikimrProto::ERROR, query);

        constexpr int blobCount = 1000;
        constexpr int blobSize = 10;
        TString data(blobSize, 'a');

        std::vector<TBlobInfo> blobs(blobCount);
        for(int i = 0; i < blobCount; ++i) {
            TLogoBlobID blob(1, 1, i, 0, blobSize, 0, 1);
            blobs[i] = {blob, data, NKikimrProto::OK};
        }

        SendEmptyMultiPut(env, test, blobs, NKikimrProto::ERROR, query);
    }

    Y_UNIT_TEST(ProtoHasVDiskAndExtQueue) {
        return;
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);
        NKikimrBlobStorage::TVDiskID VDiskId;
        VDiskIDFromVDiskID(test.Info->GetVDiskId(0), &VDiskId);

        NKikimrBlobStorage::TEvVMultiPut query;
        auto VDisk = query.MutableVDiskID();
        *VDisk = VDiskId;
        auto msg = query.MutableMsgQoS();
        msg->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);

        SendEmptyMultiPut(env, test, {}, NKikimrProto::ERROR, query);

        constexpr int blobCount = 1000;
        constexpr int blobSize = 10;
        TString data(blobSize, 'a');

        std::vector<TBlobInfo> blobs(blobCount);
        for(int i = 0; i < blobCount; ++i) {
            TLogoBlobID blob(1, 1, i, 0, blobSize, 0, 1);
            blobs[i] = {blob, data, NKikimrProto::OK};
            auto protoBlob = query.AddItems()->MutableBlobID();
            LogoBlobIDFromLogoBlobID(blob, protoBlob);
        }

        SendEmptyMultiPut(env, test, blobs, NKikimrProto::OK, query);

        for(int i = 0; i < 1000 * blobCount; ++i) {
            TLogoBlobID blob(1, 1, i, 0, blobSize, 0, 1);
            blobs.push_back({blob, data, NKikimrProto::OK});
            auto protoBlob = query.AddItems()->MutableBlobID();
            LogoBlobIDFromLogoBlobID(blob, protoBlob);
        }

        SendEmptyMultiPut(env, test, blobs, NKikimrProto::OK, query);
    }

    Y_UNIT_TEST(EmptyProtoMultiPut) {
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr int blobCount = 1;
        constexpr int blobSize = 10;
        TString data(blobSize, 'a');

        std::vector<TBlobInfo> blobs(blobCount);
        for(int i = 0; i < blobCount; ++i) {
            TLogoBlobID blob(1, 1, i, 0, blobSize, 0, 1);
            blobs[i] = {blob, data, NKikimrProto::OK};
        }

        SendMultiPut(env, test, NKikimrProto::OK, blobs);
    }

    Y_UNIT_TEST(ManyQueriesThroughOneBSQueue) {
        return;
        TEnvironmentSetup env(true, GetErasureTypeByString("none"));
        TTestInfo test = InitTest(env);

        constexpr int eventsCount = 10000;
        constexpr int blobSize = 100;
        const TString data(blobSize, 'a');
        std::vector<std::unique_ptr<IEventBase>> events(eventsCount);

        int goodCount = 0;
        for(int i = 0; i < eventsCount; ++i) {
            events[i].reset(new TEvBlobStorage::TEvVMultiPut(test.Info->GetVDiskId(0),
                                TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog, false, nullptr));
            if (i % 19 != 18) {
                ++goodCount;
                TLogoBlobID blob(i, 1, 0, 0, blobSize, 0, 1);
                static_cast<TEvBlobStorage::TEvVMultiPut*>(events[i].get())->AddVPut(blob, TRcBuf(data), nullptr, nullptr, NWilson::TTraceId());
            }
        }

        env.WithQueueId(test.Info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            test.Edge = test.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            for(int i = 0; i < eventsCount; ++i) {
                test.Runtime->Send(new IEventHandle(queueId, test.Edge, events[i].release()), queueId.NodeId());
            }

            int okCount = 0;
            int errorCount = 0;
            for(int i = 0; i < eventsCount; ++i) {
                auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
                UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvVMultiPutResult);
                TEvBlobStorage::TEvVMultiPutResult *putResult = handle->Get<TEvBlobStorage::TEvVMultiPutResult>();
                if (putResult->Record.GetStatus() == NKikimrProto::OK) {
                    ++okCount;
                } else if (putResult->Record.GetStatus() == NKikimrProto::ERROR) {
                    ++errorCount;
                }
            }

                UNIT_ASSERT_VALUES_EQUAL(okCount, goodCount);
                UNIT_ASSERT_VALUES_EQUAL(errorCount, eventsCount - goodCount);
        });
    }
}
