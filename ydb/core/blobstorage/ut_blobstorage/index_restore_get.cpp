#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

Y_UNIT_TEST_SUITE(IndexRestoreGet) {
    Y_UNIT_TEST(DoNotKeepBeforeAndAfterSoftBarrier) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });
        env.CreateBoxAndPool(1, 1);
        const ui32 groupId = env.GetGroups().front();
        const TString data = "blob";
        const TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
        env.PutBlob(groupId, id, data);

        auto collect = [&](bool advanceBarrier) {
            const auto edge = env.Runtime->AllocateEdgeActor(1);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(),
                    advanceBarrier ? 2 : 1, id.Channel(), advanceBarrier, id.Generation(), id.Step(), nullptr,
                    advanceBarrier ? nullptr : new TVector<TLogoBlobID>{id}, TInstant::Max(), false));
            });
            auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, NKikimrProto::OK);
        };

        for (bool advanceBarrier : {false, true}) {
            collect(advanceBarrier);
            for (bool indexOnly : {false, true}) {
                const auto edge = env.Runtime->AllocateEdgeActor(1);
                env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::FastRead, false, indexOnly));
                });
                auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
                UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(reply->Get()->ResponseSz, 1);
                const auto& item = reply->Get()->Responses[0];
                UNIT_ASSERT_VALUES_EQUAL(item.Status, advanceBarrier ? NKikimrProto::NODATA : NKikimrProto::OK);
                UNIT_ASSERT(!item.Keep);
                if (!advanceBarrier) {
                    UNIT_ASSERT(item.DoNotKeep);
                }
                UNIT_ASSERT_VALUES_EQUAL(item.Buffer.ConvertToString(), !advanceBarrier && !indexOnly ? data : TString());
            }
        }
    }

    Y_UNIT_TEST(HandoffMarkedForDeletionStillReportsLocalPart) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        env.CreateBoxAndPool(1, 1);
        auto info = env.GetGroupInfo(env.GetGroups().front());
        const TLogoBlobID id(1, 1, 1, 0, 4096, 0);
        const TLogoBlobID partId(id, 1);
        const auto handoff = info->GetVDiskInSubgroup(info->Type.TotalPartCount(), id.Hash());
        const TString data(info->Type.PartSize(partId), 'x');
        env.PutBlob(handoff, partId, data);

        // Balancing drops the handoff obligation before compaction removes the local bytes.
        TIngress deletion;
        deletion.DeleteHandoff(&info->GetTopology(), handoff, partId);
        const auto vdiskActor = info->GetActorId(handoff);
        const auto edge = env.Runtime->AllocateEdgeActor(vdiskActor.NodeId());
        auto ev = std::make_unique<IEventHandle>(vdiskActor, edge, new TEvDelLogoBlobDataSyncLog(id, deletion, 0));
        ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, vdiskActor);
        env.Runtime->Send(ev.release(), edge.NodeId());
        env.WaitForEdgeActorEvent<TEvDelLogoBlobDataSyncLogResult>(edge);

        env.WithQueueId(handoff, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queue) {
            using TVGet = TEvBlobStorage::TEvVGet;
            using TVGetResult = TEvBlobStorage::TEvVGetResult;
            auto index = env.SyncQueryFactory<TVGetResult>(queue, [&] {
                return TVGet::CreateExtremeIndexQuery(handoff, TInstant::Max(), NKikimrBlobStorage::FastRead,
                    TVGet::EFlags::ShowInternals, {}, {id});
            });
            UNIT_ASSERT_VALUES_EQUAL(index->Record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(index->Record.ResultSize(), 1);
            const auto& item = index->Record.GetResult(0);
            UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::NODATA);
            UNIT_ASSERT(item.HasIngress());
            const TIngress ingress(item.GetIngress());
            UNIT_ASSERT(ingress.PartsWeMustHaveLocally(&info->GetTopology(), handoff, id).Empty());
            UNIT_ASSERT(ingress.LocalParts(info->Type).Get(0));
            UNIT_ASSERT_VALUES_EQUAL(item.PartsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(item.GetParts(0), 1);

            auto body = env.SyncQueryFactory<TVGetResult>(queue, [&] {
                return TVGet::CreateExtremeDataQuery(handoff, TInstant::Max(), NKikimrBlobStorage::FastRead,
                    TVGet::EFlags::None, {}, {partId});
            });
            UNIT_ASSERT_VALUES_EQUAL(body->Record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(body->Record.ResultSize(), 1);
            const auto& part = body->Record.GetResult(0);
            UNIT_ASSERT_VALUES_EQUAL(part.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(body->GetBlobData(part).ConvertToString(), data);
        });
    }

    Y_UNIT_TEST(BlobRecovery) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        TLogoBlobID id;
        TString error;
        const bool success = TLogoBlobID::Parse(id, "[72075186270680851:57:3905:6:786432:4194304:0]", error);
        UNIT_ASSERT(success);

        TString data = TString(id.BlobSize(), 'X');

        TDataPartSet parts;
        info->Type.SplitData((TErasureType::ECrcMode)id.CrcMode(), data, parts);

        TBlobStorageGroupInfo::TOrderNums nums{5, 6, 7, 0, 1, 2, 3, 4};

        for (ui32 i = 0; i < 6; ++i) {
            const ui32 partId = i + 1;
            const ui32 orderNumber = nums[i];
            if (i != 4) {
                env.PutBlob(info->GetVDiskId(orderNumber), TLogoBlobID(id, partId), parts.Parts[i].OwnedString.ConvertToString());
            }
        }

        {
            const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(),
                    1, id.Channel(), true, id.Generation(), id.Step(), new TVector<TLogoBlobID>{id}, nullptr,
                    TInstant::Max(), true, TWriteSource::Unknown, false));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        env.Sim(TDuration::Minutes(1));

        for (ui32 idx = 0; idx < 8; ++idx) {
            const ui32 orderNumber = nums[idx];
            Cerr << idx << ' ' << orderNumber << Endl;

            std::vector<ui32> v;
            if (idx < 6) {
                v.push_back(idx + 1);
            } else {
                v.push_back(1);
                v.push_back(2);
                v.push_back(3);
                v.push_back(4);
                v.push_back(5);
                v.push_back(6);
            }

            for (ui32 partId : v) {
                env.CheckBlob(info->GetActorId(orderNumber), info->GetVDiskId(orderNumber), TLogoBlobID(id, partId),
                    parts.Parts[partId - 1].OwnedString.ConvertToString(),
                    idx == 4 || idx == 6 || idx == 7 ? NKikimrProto::NODATA : NKikimrProto::OK);
            }
        }

        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
//            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
//                NKikimrBlobStorage::EGetHandleClass::FastRead, true, true));
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvRange(id.TabletID(), id, id, true, TInstant::Max(), true));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses.size(), 1);
//        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
//        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
//        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
//        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);

        for (ui32 idx = 0; idx < 8; ++idx) {
            const ui32 orderNumber = nums[idx];
            Cerr << idx << ' ' << orderNumber << Endl;

            std::vector<ui32> v;
            if (idx < 6) {
                v.push_back(idx + 1);
            } else {
                v.push_back(1);
                v.push_back(2);
                v.push_back(3);
                v.push_back(4);
                v.push_back(5);
                v.push_back(6);
            }

            for (ui32 partId : v) {
                env.CheckBlob(info->GetActorId(orderNumber), info->GetVDiskId(orderNumber), TLogoBlobID(id, partId),
                    parts.Parts[partId - 1].OwnedString.ConvertToString(),
                    idx == 6 || idx == 7 ? NKikimrProto::NODATA : NKikimrProto::OK);
            }
        }
    }
}
