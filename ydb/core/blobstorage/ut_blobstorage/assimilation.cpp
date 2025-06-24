#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

void RunAssimilationTest(bool reverse) {
    TEnvironmentSetup env{{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::ErasureNone,
    }};
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(NKikimrServices::BS_PROXY_ASSIMILATE, NLog::PRI_DEBUG);

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
        //Cerr << numBlocks << "/" << numBarriers << "/" << numBlobs << Endl;
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
                lastBlob, true, reverse);
            runtime->Send(new IEventHandle(vdiskId, client, ev.release()), vdiskId.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVAssimilateResult>(client);
            const auto& record = res->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            if (record.BlocksSize() + record.BarriersSize() + record.BlobsSize() == 0) {
                break;
            }
            for (auto& item : record.GetBlocks()) {
                UNIT_ASSERT(!lastBlock || (reverse ? item.GetTabletId() < *lastBlock : *lastBlock < item.GetTabletId()));
                lastBlock.emplace(item.GetTabletId());
                const auto [it, inserted] = aBlocks.emplace(item.GetTabletId(), item.GetBlockedGeneration());
                UNIT_ASSERT_VALUES_EQUAL(it->second, item.GetBlockedGeneration());
            }
            for (auto& item : record.GetBarriers()) {
                std::pair<ui64, ui8> key(item.GetTabletId(), item.GetChannel());
                UNIT_ASSERT(!lastBarrier || (reverse ? key < *lastBarrier : *lastBarrier < key));
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
#define UNWRAP(X, INDEX) \
                if (item.HasRaw##X()) { \
                    raw[INDEX] = item.GetRaw##X(); \
                } else if (item.HasDiff##X()) { \
                    if (reverse) { \
                        raw[INDEX] -= item.GetDiff##X(); \
                    } else { \
                        raw[INDEX] += item.GetDiff##X(); \
                    } \
                }
                UNWRAP(X1, 0)
                UNWRAP(X2, 1)
                UNWRAP(X3, 2)
#undef UNWRAP

                UNIT_ASSERT(!lastBlob || (reverse ? TLogoBlobID(raw) < *lastBlob : *lastBlob < TLogoBlobID(raw)));
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

    std::optional<ui64> skipBlocksUpTo;
    std::optional<std::tuple<ui64, ui8>> skipBarriersUpTo;
    std::optional<TLogoBlobID> skipBlobsUpTo;

    auto updateKey = [&](auto& skipUpTo, const auto& key) {
        UNIT_ASSERT(!skipUpTo || (reverse ? key < *skipUpTo : *skipUpTo < key));
        skipUpTo.emplace(key);
    };

    for (;;) {
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvAssimilate(skipBlocksUpTo, skipBarriersUpTo,
                skipBlobsUpTo, true, reverse));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvAssimilateResult>(edge, false);
        auto& m = *res->Get();
        UNIT_ASSERT_VALUES_EQUAL(m.Status, NKikimrProto::OK);
        if (m.Blocks.empty() && m.Barriers.empty() && m.Blobs.empty()) {
            break;
        }
        for (const auto& item : m.Blocks) {
            const auto it = aBlocks.find(item.TabletId);
            UNIT_ASSERT(it != aBlocks.end());
            UNIT_ASSERT(it->second == item.BlockedGeneration);
            aBlocks.erase(it);
            updateKey(skipBlocksUpTo, item.TabletId);
        }
        for (const auto& item : m.Barriers) {
            const auto it = aBarriers.find(std::make_pair(item.TabletId, item.Channel));
            UNIT_ASSERT(it != aBarriers.end());
            UNIT_ASSERT(it->second == std::make_pair(
                std::make_tuple(item.Hard.RecordGeneration, item.Hard.PerGenerationCounter, item.Hard.CollectGeneration, item.Hard.CollectStep),
                std::make_tuple(item.Soft.RecordGeneration, item.Soft.PerGenerationCounter, item.Soft.CollectGeneration, item.Soft.CollectStep)
            ));
            aBarriers.erase(it);
            updateKey(skipBarriersUpTo, std::make_tuple(item.TabletId, item.Channel));
        }
        for (const auto& item : m.Blobs) {
            const auto it = aBlobs.find(item.Id);
            UNIT_ASSERT(it != aBlobs.end());
            aBlobs.erase(it);
            updateKey(skipBlobsUpTo, item.Id);
        }
    }

    UNIT_ASSERT(aBlocks.empty());
    UNIT_ASSERT(aBarriers.empty());
    UNIT_ASSERT(aBlobs.empty());
}

Y_UNIT_TEST_SUITE(VDiskAssimilation) {
    Y_UNIT_TEST(Test) {
        RunAssimilationTest(false);
    }
    Y_UNIT_TEST(TestReverse) {
        RunAssimilationTest(true);
    }
}
