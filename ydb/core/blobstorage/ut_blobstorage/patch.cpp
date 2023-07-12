#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

Y_UNIT_TEST_SUITE(BlobPatching) {

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
    };

    void SendGet(const TTestInfo &test, const TLogoBlobID &blobId, const TString &data,
            NKikimrProto::EReplyStatus status)
    {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> getQueries{new TEvBlobStorage::TEvGet::TQuery[1]};
        getQueries[0].Id = blobId;
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvGet>(getQueries, 1, TInstant::Max(),
                NKikimrBlobStorage::AsyncRead);
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvGetResult);
        TEvBlobStorage::TEvGetResult *getResult = handle->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Status, status);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Buffer.ConvertToString(), data);
    };

    void SendPatch(const TTestInfo &test, const TLogoBlobID &originalBlobId, const TLogoBlobID &patchedBlobId, ui32 mask,
            const TVector<TEvBlobStorage::TEvPatch::TDiff> &diffs, NKikimrProto::EReplyStatus status)
    {
        TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffArr{new TEvBlobStorage::TEvPatch::TDiff[diffs.size()]};
        for (ui32 idx = 0; idx < diffs.size(); ++idx) {
            diffArr[idx] = diffs[idx];
        }
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvPatch>(test.Info->GroupID, originalBlobId, patchedBlobId,
                mask, std::move(diffArr), diffs.size(), TInstant::Max());
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvPatchResult);
        TEvBlobStorage::TEvPatchResult *patchResult = handle->Get<TEvBlobStorage::TEvPatchResult>();
        UNIT_ASSERT_EQUAL(patchResult->Status, status);
    };

    TString ApplyDiffs(const TString &source, const TVector<TEvBlobStorage::TEvPatch::TDiff> &diffs) {
        TString result = TString::Uninitialized(source.size());
        Copy(source.begin(), source.end(), result.begin());
        for (auto &diff : diffs) {
            Copy(diff.Buffer.begin(), diff.Buffer.end(), result.begin() + diff.Offset);
        }
        return result;
    };

    void MakePatchingTest(TString erasure) {
        TEnvironmentSetup env(true, GetErasureTypeByString(erasure));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(1, 1, 0, 0, size, 0);
        std::unique_ptr<IEventHandle> handle;

        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        SendGet(test, originalBlobId, data, NKikimrProto::OK);

        TVector<TEvBlobStorage::TEvPatch::TDiff> diffs1(1);
        diffs1[0].Set("b", 0);
        TString patchedData1 = ApplyDiffs(data, diffs1);
        TLogoBlobID patchedBlobId1(1, 1, 1, 0, size, 0);
        TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &patchedBlobId1, 0,
                test.Info->GroupID, test.Info->GroupID);
        SendPatch(test, originalBlobId, patchedBlobId1, 0, diffs1, NKikimrProto::OK);
        SendGet(test, patchedBlobId1, patchedData1, NKikimrProto::OK);

        TVector<TEvBlobStorage::TEvPatch::TDiff> diffs2(2);
        diffs2[0].Set("b", 0);
        diffs2[1].Set("b", 99);
        TString patchedData2 = ApplyDiffs(data, diffs2);
        TLogoBlobID patchedBlobId2(1, 1, 2, 0, size, 0);
        TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &patchedBlobId2, 0,
                test.Info->GroupID, test.Info->GroupID);
        SendPatch(test, originalBlobId, patchedBlobId2, 0, diffs2, NKikimrProto::OK);
        SendGet(test, patchedBlobId2, patchedData2, NKikimrProto::OK);

        TLogoBlobID patchedBlobId3(1, 1, 3, 0, size, 0);
        TLogoBlobID truePatchedBlobId3(1, 1, 3, 0, size, 0);
        TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &truePatchedBlobId3, TLogoBlobID::MaxCookie,
                test.Info->GroupID, test.Info->GroupID);
        UNIT_ASSERT(patchedBlobId3 != truePatchedBlobId3);
        NKikimrProto::EReplyStatus statusWhenNotMatchingCookie = (erasure == "block-4-2" ? NKikimrProto::ERROR : NKikimrProto::OK);
        SendPatch(test, originalBlobId, patchedBlobId3, TLogoBlobID::MaxCookie, diffs2, statusWhenNotMatchingCookie);
        SendPatch(test, originalBlobId, truePatchedBlobId3, TLogoBlobID::MaxCookie, diffs2, NKikimrProto::OK);
        SendGet(test, truePatchedBlobId3, patchedData2, NKikimrProto::OK);
    }

    void MakeStressPatchingTest(TString erasure) {
        TEnvironmentSetup env(true, GetErasureTypeByString(erasure));
        TTestInfo test = InitTest(env);

        constexpr ui32 size = 100;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(1, 1, 0, 0, size, 0);
        std::unique_ptr<IEventHandle> handle;

        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        SendGet(test, originalBlobId, data, NKikimrProto::OK);

        TVector<TEvBlobStorage::TEvPatch::TDiff> diffs(50);
        for (ui32 idx = 0; idx < diffs.size(); ++idx) {
            diffs[idx].Set("b", idx * 2);
        }
        TString patchedData = ApplyDiffs(data, diffs);

        constexpr ui32 patchCount = 1'000;
        for (ui32 patchIdx = 0; patchIdx < patchCount; ++patchIdx) {
            TLogoBlobID patchedBlobId(1, 1, patchIdx + 1, 0, size, 0);
            TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(originalBlobId, &patchedBlobId, TLogoBlobID::MaxCookie,
                    test.Info->GroupID, test.Info->GroupID);
            SendPatch(test, originalBlobId, patchedBlobId, 0, diffs, NKikimrProto::OK);
            SendGet(test, patchedBlobId, patchedData, NKikimrProto::OK);
        }
    }


    Y_UNIT_TEST(Mirror3of4) {
        MakePatchingTest("mirror-3of4");
    }

    Y_UNIT_TEST(Mirror3dc) {
        MakePatchingTest("mirror-3-dc");
    }

    Y_UNIT_TEST(Mirror3) {
        MakePatchingTest("mirror-3");
    }

    Y_UNIT_TEST(Block42) {
        MakePatchingTest("block-4-2");
    }

    Y_UNIT_TEST(None) {
        MakePatchingTest("none");
    }


    Y_UNIT_TEST(StressMirror3of4) {
        MakeStressPatchingTest("mirror-3of4");
    }

    Y_UNIT_TEST(StressMirror3dc) {
        MakeStressPatchingTest("mirror-3-dc");
    }

    Y_UNIT_TEST(StressMirror3) {
        MakeStressPatchingTest("mirror-3");
    }

    Y_UNIT_TEST(StressBlock42) {
        MakeStressPatchingTest("block-4-2");
    }

    Y_UNIT_TEST(StressNone) {
        MakeStressPatchingTest("none");
    }

    Y_UNIT_TEST(DiffsWithIncorectPatchedBlobPartId) {
        TEnvironmentSetup env(true);
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        env.CommenceReplication();
        auto groups = env.GetGroups();
        auto info = env.GetGroupInfo(groups[0]);
        constexpr ui32 size = 100;
        TLogoBlobID originalBlobId(1, 1, 0, 0, size, 0);

        env.WithQueueId(info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob, [&](TActorId queueId) {
            TActorId edge = runtime->AllocateEdgeActor(queueId.NodeId());
            constexpr ui32 diffCount = 100;
            for (ui32 diffIdx = 0; diffIdx < diffCount; ++diffIdx) {
                TLogoBlobID patchedBlobId(1, 1, diffIdx + 1, 0, size, 0);
                std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> ev = std::make_unique<TEvBlobStorage::TEvVPatchDiff>(
                        originalBlobId, patchedBlobId, info->GetVDiskId(0), 0, TInstant::Max(), 0);
                runtime->Send(new IEventHandle(queueId, edge, ev.release()), queueId.NodeId());
            }
            for (ui32 diffIdx = 0; diffIdx < diffCount; ++diffIdx) {
                auto r = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPatchResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(r->Get()->Record.GetStatus(), NKikimrProto::ERROR);
            }
        });
    }
}

