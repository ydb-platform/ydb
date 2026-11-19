#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

Y_UNIT_TEST_SUITE(Get) {

    void SendGet(
        const TTestInfo &test,
        const TLogoBlobID &blobId,
        const TString &data,
        NKikimrProto::EReplyStatus status,
        std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> readerTabletData = {},
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData = {},
        bool mustRestoreFirst = false,
        bool isIndexOnly = false,
        bool checkResultData = true)
    {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> getQueries{new TEvBlobStorage::TEvGet::TQuery[1]};
        getQueries[0].Id = blobId;
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(getQueries, 1, TInstant::Max(),
                NKikimrBlobStorage::AsyncRead, mustRestoreFirst, isIndexOnly, forceBlockTabletData);
        ev->ReaderTabletData = readerTabletData;
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvGetResult);
        TEvBlobStorage::TEvGetResult *getResult = handle->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Status, status);
        if (checkResultData && status == NKikimrProto::EReplyStatus::OK) {
            UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Buffer.ConvertToString(), data);
        }
    }

    void SendPut(const TTestInfo &test, const TLogoBlobID &blobId, const TString &data,
            NKikimrProto::EReplyStatus status)
    {
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvPut>(blobId, data, TInstant::Max());

        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});

        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvPutResult);
        TEvBlobStorage::TEvPutResult *putResult = handle->Get<TEvBlobStorage::TEvPutResult>();
        UNIT_ASSERT_EQUAL(putResult->Status, status);
    }

    void TestUnrecoverableGet(TBlobStorageGroupType type) {
        for (bool mustRestoreFirst : {false, true}) {
            TEnvironmentSetup env({
                .NodeCount = type.BlobSubgroupSize(),
                .VDiskReplPausedAtStart = true,
                .Erasure = type,
            });
            const TTestInfo test = InitTest(env);
            const TString data(4097, 'x');
            const TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
            SendPut(test, id, data, NKikimrProto::OK);
            SendGet(test, id, data, NKikimrProto::OK);

            TBlobStorageGroupInfo::TVDiskIds subgroup;
            test.Info->PickSubgroup(id.Hash(), &subgroup, nullptr);
            THashSet<TVDiskID> unavailable;
            // Leave K-1 readable data parts: the last data part and both parity parts are unavailable.
            for (ui32 disk = type.DataParts() - 1; disk < type.TotalPartCount(); ++disk) {
                unavailable.insert(subgroup[disk]);
            }
            env.Runtime->FilterFunction = [&, unavailable](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGet) {
                    const TVDiskID disk = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVGet>()->Record.GetVDiskID());
                    if (unavailable.contains(disk)) {
                        auto result = std::make_unique<TEvBlobStorage::TEvVGetResult>();
                        result->MakeError(NKikimrProto::ERROR, "part is unavailable",
                            ev->Get<TEvBlobStorage::TEvVGet>()->Record);
                        env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie),
                            ev->Sender.NodeId());
                        return false;
                    }
                }
                return true;
            };

            const TInstant deadline = env.Now() + TDuration::Seconds(10);
            env.Runtime->WrapInActorContext(test.Edge, [&] {
                SendToBSProxy(test.Edge, test.Info->GroupID, new TEvBlobStorage::TEvGet(
                    id, 0, 0, deadline, NKikimrBlobStorage::FastRead, mustRestoreFirst));
            });
            auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(
                test.Edge, false, deadline + TDuration::Seconds(10));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->ResponseSz, 1, result->Get()->ToString());
            const auto& response = result->Get()->Responses[0];
            UNIT_ASSERT_VALUES_EQUAL(response.Id, id);
            UNIT_ASSERT_VALUES_EQUAL_C(response.Status, NKikimrProto::ERROR, result->Get()->ToString());
            UNIT_ASSERT(response.Buffer.empty());
            UNIT_ASSERT(env.Now() < deadline);
            env.Runtime->FilterFunction = {};
        }
    }

    Y_UNIT_TEST(UnrecoverableGetBlock42) {
        TestUnrecoverableGet(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(UnrecoverableGetBlock82) {
        TestUnrecoverableGet(TBlobStorageGroupType::Erasure8Plus2Block);
    }

    Y_UNIT_TEST(TestBlockedEvGetRequest) {
        TEnvironmentSetup env(true);
        TTestInfo test = InitTest(env);

        ui64 tabletId = 1;
        ui32 tabletGeneration = 1;

        constexpr ui32 size = 100;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(tabletId, tabletGeneration, 0, 0, size, 0);

        // check that TEvGet returns OK without reader params
        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        // check that TEvGet returns OK with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));

        // block tablet generation
        ui32 blockedTabletGeneration = tabletGeneration + 1;
        auto ev = std::make_unique<TEvBlobStorage::TEvBlock>(tabletId, blockedTabletGeneration, TInstant::Max());

        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });

        auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvBlockResult);
        auto blockResult = handle->Get<TEvBlobStorage::TEvBlockResult>();
        UNIT_ASSERT(blockResult);

        // check that TEvGet still returns OK for blocked generation without reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK);
        // check that now TEvGet returns BLOCKED for blocked generation with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::BLOCKED, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));
    }

    Y_UNIT_TEST(TestForceBlockTabletDataWithIndexRestoreGetRequest) {
        TEnvironmentSetup env(true);
        TTestInfo test = InitTest(env);

        ui64 tabletId = 1;
        ui32 tabletGeneration = 1;

        constexpr ui32 size = 100;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(tabletId, tabletGeneration, 0, 0, size, 0);

        // check that TEvGet returns OK without reader params
        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        // check that TEvGet returns OK with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));

        // block tablet generation
        ui32 blockedTabletGeneration = tabletGeneration + 1;
        SendGet(test, originalBlobId, data, NKikimrProto::OK, std::nullopt, TEvBlobStorage::TEvGet::TForceBlockTabletData(tabletId, blockedTabletGeneration), false, true, false);

        // check that TEvGet still returns OK for blocked generation without reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK);
        // check that now TEvGet returns BLOCKED for blocked generation with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::BLOCKED, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));
    }

    Y_UNIT_TEST(TestRdmaRegiseredMemory) {
#if defined(_msan_enabled_)
            return;
#endif
        TEnvironmentSetup env(true);
        TTestInfo test = InitTest(env);

        auto groups = env.GetGroups();
        auto groupInfo = env.GetGroupInfo(groups.front());

        ui64 tabletId = 1;
        ui32 tabletGeneration = 1;

        constexpr ui32 size = 1000;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(tabletId, tabletGeneration, 0, 0, size, 0);

        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        env.CompactVDisk(groupInfo->GetActorId(0));

        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvChunkReadResult) {
                Cerr << "TEvChunkReadResult: " << ev->ToString() << Endl;
                auto* res = ev->CastAsLocal<NPDisk::TEvChunkReadResult>();
                auto buf = res->Data.ToString();
                auto newBuf = memPool->AllocRcBuf(buf.size(), NInterconnect::NRdma::IMemPool::EMPTY);
                UNIT_ASSERT(newBuf);
                std::memcpy(newBuf->UnsafeGetDataMut(), buf.GetData(), buf.GetSize());
                res->Data = TBufferWithGaps(0, std::move(*newBuf));
                res->Data.Commit();
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult) {
                Cerr << "TEvVGetResult: " << ev->ToString() << Endl;
                auto* res = ev->CastAsLocal<TEvBlobStorage::TEvVGetResult>();
                auto payload = res->GetPayload();
                UNIT_ASSERT_VALUES_EQUAL(payload.size(), 1);
                for (auto& rope : payload) {
                    for (auto it = rope.Begin(); it != rope.End(); ++it) {
                        const TRcBuf& chunk = it.GetChunk();
                        auto memReg = NInterconnect::NRdma::TryExtractFromRcBuf(chunk);
                        UNIT_ASSERT_C(!memReg.Empty(), "unable to extract mem region from chunk");
                        UNIT_ASSERT_VALUES_EQUAL_C(memReg.GetSize(), size, "invalid size for memReg");
                    }
                }
            }
            return true;
        };

        SendGet(test, originalBlobId, data, NKikimrProto::OK, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));
    }
}
