#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>

Y_UNIT_TEST_SUITE(GarbageCollection) {
    Y_UNIT_TEST(EmptyGcCmd) {
        TEnvironmentSetup env({
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto info = env.GetGroupInfo(env.GetGroups().front());

        auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(1u, 1u, 1u, 0u, false, 0u, 0u, nullptr, nullptr,
            TInstant::Max(), true);
        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, ev.release());
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
    }

    Y_UNIT_TEST(PutWithKeep) {
        TEnvironmentSetup env({
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto info = env.GetGroupInfo(env.GetGroups().front());

        const ui64 tabletId = 1;
        const ui32 recordGeneration = 1;
        const ui32 perGenerationCounter = 1;
        const ui8 channel = 0;

        const ui32 collectGeneration = 1;
        const ui32 collectStep = 1;

        const ui32 generation = collectGeneration;
        const ui32 step = collectStep;

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(tabletId, recordGeneration,
                perGenerationCounter, channel, true, collectGeneration, collectStep, nullptr, nullptr, TInstant::Max(),
                false, false));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        // ensure it gets spread and synced
        env.Sim(TDuration::Seconds(10));

        ui32 cookie = 1;

        auto check = [&](ui32 numBlobs, ui32 dataSize, bool issueKeepFlag, bool restart) {
            Cerr << "check: numBlobs=" << numBlobs << " dataSize=" << dataSize << " issueKeepFlag=" << issueKeepFlag
                << " restart=" << restart << Endl;
            const TString data = FastGenDataForLZ4(dataSize);
            std::vector<TLogoBlobID> ids;
            const TActorId writer = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            for (ui32 i = 0; i < numBlobs; ++i) {
                const TLogoBlobID id(tabletId, generation, step, channel, data.size(), cookie++);
                runtime->WrapInActorContext(writer, [&] {
                    SendToBSProxy(writer, info->GroupID, new TEvBlobStorage::TEvPut(id, data, TInstant::Max(),
                        NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault, issueKeepFlag));
                    ids.push_back(id);
                });
            }
            for (ui32 i = 0; i < numBlobs; ++i) {
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(writer, i + 1 == numBlobs);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }
            if (restart) {
                for (const auto& vdisk : info->GetVDisks()) {
                    const auto& actorId = info->GetActorId(vdisk.VDiskIdShort);
                    env.RestartNode(actorId.NodeId());
                }
                env.Sim(TDuration::Seconds(10));
            }
            for (const auto& vdisk : info->GetVDisks()) {
                const auto& actorId = info->GetActorId(vdisk.VDiskIdShort);
                const auto& sender = env.Runtime->AllocateEdgeActor(actorId.NodeId());
                auto ev = std::make_unique<IEventHandle>(actorId, sender, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
                ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
                env.Runtime->Send(ev.release(), sender.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(sender);
            }

            const TActorId reader = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            runtime->WrapInActorContext(reader, [&] {
                TArrayHolder<TEvBlobStorage::TEvGet::TQuery> q(new TEvBlobStorage::TEvGet::TQuery[numBlobs]);
                for (ui32 i = 0; i < numBlobs; ++i) {
                    q[i].Set(ids[i]);
                }
                SendToBSProxy(reader, info->GroupID, new TEvBlobStorage::TEvGet(q, numBlobs, TInstant::Max(),
                    NKikimrBlobStorage::FastRead));
            });
            {
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(reader);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, numBlobs);
                for (ui32 i = 0; i < numBlobs; ++i) {
                    auto& r = res->Get()->Responses[i];
                    UNIT_ASSERT_VALUES_EQUAL(r.Status, issueKeepFlag ? NKikimrProto::OK : NKikimrProto::NODATA);
                    if (issueKeepFlag) {
                        UNIT_ASSERT_VALUES_EQUAL(r.Id, ids[i]);
                        UNIT_ASSERT_VALUES_EQUAL(r.Buffer.ConvertToString(), data);
                        UNIT_ASSERT(r.Keep);
                        UNIT_ASSERT(!r.DoNotKeep);
                    }
                }
            }
        };

        for (bool restart : {false, true}) {
            for (bool issueKeepFlag : {false, true}) {
                for (ui32 dataSize : {100, 1048576}) {
                    for (ui32 numBlobs : {1, 100}) {
                        check(numBlobs, dataSize, issueKeepFlag, restart);
                    }
                }
            }
        }
    }
}
