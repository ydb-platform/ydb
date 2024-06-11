#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(VDiskAssimilation) {
    Y_UNIT_TEST(Test) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        auto edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        THashMap<ui64, ui32> blocks;

        using TBarrier = std::tuple<ui32, ui32, ui32, ui32>;
        THashMap<std::pair<ui64, ui8>, std::pair<TBarrier, TBarrier>> barriers;

        THashSet<TLogoBlobID> blobs;

        size_t numBlocks = 1000 + RandomNumber(100'000u);
        size_t numBarriers = 1000 + RandomNumber(100'000u);
        size_t numBlobs = 1000 + RandomNumber(100'000u);

        runtime->WrapInActorContext(edge, [&] {
            for (size_t i = 0; i < numBlocks; ++i) {
                const ui64 tabletId = 1 + i;
                const ui32 generation = 1;
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvBlock(tabletId, generation, TInstant::Max()));
                blocks.emplace(tabletId, generation);
            }

            for (size_t i = 0; i < numBlobs; ++i) {
                const ui64 tabletId = 1 + RandomNumber(10u);
                TString data = TStringBuilder() << i;
                TLogoBlobID id(tabletId, 2, 2, 0, data.size(), i);
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
                blobs.emplace(id);
            }

            for (size_t i = 0; i < numBarriers; ++i) {
                const ui64 tabletId = 11 + RandomNumber(100u);
                const ui8 channel = 1;
                const ui32 recordGen = 2;
                const ui32 recordGenCounter = i;
                const ui32 collectGen = 1 + i;
                const ui32 collectStep = 1 + i;
                const bool hard = RandomNumber(2u);
                SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(tabletId, recordGen,
                    recordGenCounter, channel, true, collectGen, collectStep, nullptr, nullptr, TInstant::Max(),
                    false, hard));

                auto& x = barriers[std::make_pair(tabletId, channel)];
                (hard ? x.first : x.second) = {recordGen, recordGenCounter, collectGen, collectStep};
            }
        });

        while (numBlocks || numBarriers || numBlobs) {
            Cerr << numBlocks << "/" << numBarriers << "/" << numBlobs << Endl;
            auto ev = runtime->WaitForEdgeActorEvent({edge});
            if (auto *p = ev->CastAsLocal<TEvBlobStorage::TEvBlockResult>()) {
                UNIT_ASSERT_VALUES_EQUAL(p->Status, NKikimrProto::OK);
                UNIT_ASSERT(numBlocks);
                --numBlocks;
            } else if (auto *p = ev->CastAsLocal<TEvBlobStorage::TEvCollectGarbageResult>()) {
                UNIT_ASSERT_VALUES_EQUAL(p->Status, NKikimrProto::OK);
                UNIT_ASSERT(numBarriers);
                --numBarriers;
            } else if (auto *p = ev->CastAsLocal<TEvBlobStorage::TEvPutResult>()) {
                UNIT_ASSERT_VALUES_EQUAL(p->Status, NKikimrProto::OK);
                UNIT_ASSERT(numBlobs);
                --numBlobs;
            } else {
                UNIT_ASSERT(false);
            }
        }

        THashMap<ui64, ui32> aBlocks;
        THashMap<std::pair<ui64, ui8>, std::pair<TBarrier, TBarrier>> aBarriers;
        THashSet<TLogoBlobID> aBlobs;

        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            std::optional<ui64> lastBlock;
            std::optional<std::pair<ui64, ui8>> lastBarrier;
            std::optional<TLogoBlobID> lastBlob;

            for (;;) {
                const TActorId vdiskId = info->GetActorId(i);
                const TActorId client = runtime->AllocateEdgeActor(vdiskId.NodeId(), __FILE__, __LINE__);
                auto ev = std::make_unique<TEvBlobStorage::TEvVAssimilate>(info->GetVDiskId(i), lastBlock, lastBarrier,
                    lastBlob);
                ev->Record.SetIgnoreDecommitState(true);
                runtime->Send(new IEventHandle(vdiskId, client, ev.release()), vdiskId.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVAssimilateResult>(client);
                const auto& record = res->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                if (record.BlocksSize() + record.BarriersSize() + record.BlobsSize() == 0) {
                    break;
                }
                for (auto& item : record.GetBlocks()) {
                    UNIT_ASSERT(!lastBlock || *lastBlock < item.GetTabletId());
                    lastBlock.emplace(item.GetTabletId());
                    const auto [it, inserted] = aBlocks.emplace(item.GetTabletId(), item.GetBlockedGeneration());
                    UNIT_ASSERT_VALUES_EQUAL(it->second, item.GetBlockedGeneration());
                }
                for (auto& item : record.GetBarriers()) {
                    std::pair<ui64, ui8> key(item.GetTabletId(), item.GetChannel());
                    UNIT_ASSERT(!lastBarrier || *lastBarrier < key);
                    lastBarrier.emplace(key);
                    auto& b = aBarriers[key];
                    auto parse = [](const auto& what) -> TBarrier {
                        return {what.GetRecordGeneration(), what.GetPerGenerationCounter(), what.GetCollectGeneration(),
                            what.GetCollectStep()};
                    };
                    if (item.HasHard()) {
                        b.first = parse(item.GetHard());
                    }
                    if (item.HasSoft()) {
                        b.second = parse(item.GetSoft());
                    }
                }

                ui64 raw[3] = {0, 0, 0};
                for (auto& item : record.GetBlobs()) {
                    if (item.HasRawX1()) {
                        raw[0] = item.GetRawX1();
                    } else if (item.HasDiffX1()) {
                        raw[0] += item.GetDiffX1();
                    }
                    if (item.HasRawX2()) {
                        raw[1] = item.GetRawX2();
                    } else if (item.HasDiffX2()) {
                        raw[1] += item.GetDiffX2();
                    }
                    if (item.HasRawX3()) {
                        raw[2] = item.GetRawX3();
                    } else if (item.HasDiffX3()) {
                        raw[2] += item.GetDiffX3();
                    }

                    UNIT_ASSERT(!lastBlob || *lastBlob < TLogoBlobID(raw));
                    lastBlob.emplace(raw);

                    TIngress ingress(item.GetIngress());
                    if (const auto& p = ingress.LocalParts(info->Type); !p.Empty()) {
                        aBlobs.emplace(*lastBlob);
                    }
                }
            }
        }

        UNIT_ASSERT_EQUAL(blocks, aBlocks);
        UNIT_ASSERT_EQUAL(barriers, aBarriers);
        UNIT_ASSERT_EQUAL(blobs, aBlobs);
    }
}
