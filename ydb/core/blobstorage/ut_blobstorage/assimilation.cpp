#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/util/lz4_data_generator.h>

#include <util/system/type_name.h>

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
        SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvStatus(TInstant::Max()));
    });
    auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);

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
                false, TWriteSource::Unknown, hard));

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

void RunVDiskIterationTest(bool reverse) {
    TEnvironmentSetup env{{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }};
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(NKikimrServices::BS_PROXY_ASSIMILATE, NLog::PRI_DEBUG);

    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
    const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

    auto edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const TVDiskID vdiskId = info->GetVDiskId(0);
    auto putQueueId = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, 0, 1);
    auto getQueueId = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::GetFastRead, 0, 1);

    THashMap<TLogoBlobID, ui64> blobs;
    ui64 tabletId = 1;
    ui32 gen = 1;
    ui32 step = 1;

    TString data = FastGenDataForLZ4(32);

    for (size_t iter = 0; iter < 10000; ++iter) {
        ui32 action = RandomNumber(100u);
        if (action < 95) {
            ui32 partId = 1 + RandomNumber(6u);
            TLogoBlobID id;
            if (RandomNumber(4u) || blobs.empty()) {
                for (;;) {
                    id = TLogoBlobID(tabletId, gen, step++, 0, 128, 0);
                    if (info->GetIdxInSubgroup(vdiskId, id.Hash()) >= 6) {
                        // this VDisk is a handoff for blob
                        break;
                    }
                }
            } else {
                std::vector<TLogoBlobID> ids;
                std::ranges::copy(blobs | std::views::keys, std::back_inserter(ids));
                id = ids[RandomNumber(ids.size())];
            }
            Cerr << "putting " << id << " partId# " << partId << Endl;
            runtime->Send(new IEventHandle(putQueueId, edge, new TEvBlobStorage::TEvVPut(TLogoBlobID(id, partId),
                TRope(data), vdiskId, false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)),
                edge.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), NKikimrProto::OK);
            blobs[id] |= TIngress::CreateIngressWithLocal(&info->GetTopology(), vdiskId, TLogoBlobID(id, partId))->Raw();
        } else {
            Cerr << "compacting" << Endl;
            const TActorId actorId = info->GetDynamicInfo().ServiceIdForOrderNumber.front();
            auto ev = std::make_unique<IEventHandle>(actorId, edge, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
            ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
            env.Runtime->Send(ev.release(), edge.NodeId());
            env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(edge, false);
        }

        std::optional<TLogoBlobID> from;
        THashMap<TLogoBlobID, ui64> m = blobs;

        for (;;) {
            Cerr << "starting assimilation from# " << (from ? from->ToString() : "<none>") << Endl;
            runtime->Send(new IEventHandle(getQueueId, edge, new TEvBlobStorage::TEvVAssimilate(vdiskId, std::nullopt,
                std::nullopt, from, true, reverse)), edge.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVAssimilateResult>(edge, false);
            if (!res->Get()->Record.BlobsSize()) {
                break;
            }

            ui64 raw[3] = {0, 0, 0};
            size_t n = res->Get()->Record.BlobsSize();
            for (const auto& item : res->Get()->Record.GetBlobs()) {
                if (item.HasRawX1()) {
                    raw[0] = item.GetRawX1();
                } else if (item.HasDiffX1()) {
                    if (reverse) {
                        raw[0] -= item.GetDiffX1();
                    } else {
                        raw[0] += item.GetDiffX1();
                    }
                }

                if (item.HasRawX2()) {
                    raw[1] = item.GetRawX2();
                } else if (item.HasDiffX2()) {
                    if (reverse) {
                        raw[1] -= item.GetDiffX2();
                    } else {
                        raw[1] += item.GetDiffX2();
                    }
                }

                if (item.HasRawX3()) {
                    raw[2] = item.GetRawX3();
                } else if (item.HasDiffX3()) {
                    if (reverse) {
                        raw[2] -= item.GetDiffX3();
                    } else {
                        raw[2] += item.GetDiffX3();
                    }
                }

                TLogoBlobID id(raw);

                UNIT_ASSERT(m.contains(id));
                UNIT_ASSERT_VALUES_EQUAL(m[id], item.GetIngress());
                m.erase(id);

                if (RandomNumber(n) == 0) {
                    from = id;
                    break;
                }

                --n;
            }
        }
        UNIT_ASSERT(m.empty());
    }
}

// DS proxy restarts the assimilation request when the group generation changes; the items it has merged by then are not
// returned to the caller, so the restarted request must start from the caller's cursors
void RunAssimilationRestartTest() {
    TEnvironmentSetup env{{
        .NodeCount = 9, // spare node for disk reassignment
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }};
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(NKikimrServices::BS_PROXY_ASSIMILATE, NLog::PRI_DEBUG);

    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    const auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
    const ui32 groupId = groups.front();
    const auto edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

    std::set<TLogoBlobID> blobs;
    for (ui32 step = 1; step <= 10; ++step) {
        const TString data = TStringBuilder() << "blob " << step;
        const TLogoBlobID id(1, 1, step, 0, data.size(), 0);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        blobs.insert(id);
    }

    // hold continuation requests of the DS proxy, which are issued with some items merged already
    bool holding = true;
    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
    runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (holding && ev->GetTypeRewrite() == TEvBlobStorage::EvVAssimilate) {
            const auto& record = ev->Get<TEvBlobStorage::TEvVAssimilate>()->Record;
            if (record.HasSkipBlocksUpTo() || record.HasSkipBarriersUpTo() || record.HasSkipBlobsUpTo()) {
                held.emplace_back(nodeId, std::move(ev));
                return false;
            }
        }
        return true;
    };

    runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvAssimilate(std::nullopt, std::nullopt, std::nullopt,
            /*ignoreDecommitState=*/ true, /*reverse=*/ false));
    });
    for (ui32 i = 0; i < 100 && held.empty(); ++i) {
        env.Sim(TDuration::MilliSeconds(10));
    }
    UNIT_ASSERT(!held.empty());

    // change the group generation; VDisks answer RACE to the continuations then
    const auto baseConfig = env.FetchBaseConfig();
    const NKikimrBlobStorage::TBaseConfig::TVSlot *target = nullptr;
    for (const auto& vslot : baseConfig.GetVSlot()) {
        if (vslot.GetGroupId() == groupId) {
            target = &vslot;
            break;
        }
    }
    UNIT_ASSERT(target);
    NKikimrBlobStorage::TConfigRequest request;
    auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
    reassign->SetGroupId(groupId);
    reassign->SetGroupGeneration(target->GetGroupGeneration());
    reassign->SetFailRealmIdx(target->GetFailRealmIdx());
    reassign->SetFailDomainIdx(target->GetFailDomainIdx());
    reassign->SetVDiskIdx(target->GetVDiskIdx());
    const auto response = env.Invoke(request);
    UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    env.Sim(TDuration::Seconds(10));

    holding = false;
    for (auto& [nodeId, ev] : std::exchange(held, {})) {
        const TActorId forwardOnNondelivery = ev->GetForwardOnNondeliveryRecipient();
        const TActorId *forward = forwardOnNondelivery ? &forwardOnNondelivery : nullptr;
        runtime->Send(ev->HasEvent()
            ? new IEventHandle(ev->Recipient, ev->Sender, ev->ReleaseBase().Release(), ev->Flags, ev->Cookie, forward)
            : new IEventHandle(ev->Type, ev->Flags, ev->Recipient, ev->Sender, ev->ReleaseChainBuffer(), ev->Cookie,
                forward), nodeId);
    }

    std::set<TLogoBlobID> assimilated;
    std::optional<TLogoBlobID> skipBlobsUpTo;
    for (;;) {
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvAssimilateResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ErrorReason);
        if (res->Get()->Blobs.empty()) {
            break;
        }
        for (const auto& blob : res->Get()->Blobs) {
            assimilated.insert(blob.Id);
            skipBlobsUpTo = blob.Id;
        }
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvAssimilate(Max<ui64>(),
                std::make_tuple(Max<ui64>(), Max<ui8>()), skipBlobsUpTo, true, false));
        });
    }
    UNIT_ASSERT_EQUAL(assimilated, blobs);
}

// the VDisk goes away (its skeleton is stopped, as the skeleton front does on PDisk error) while the assimilation
// request waits there for the log; the client can't learn of it from the connection, as the request goes without QoS,
// so the VDisk must answer it, and the rest of the group, being a quorum, completes the request
void RunVDiskGoesAwayTest() {
    TEnvironmentSetup env{{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }};
    auto& runtime = env.Runtime;
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    const auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
    const ui32 groupId = groups.front();

    const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const TString data = "assimilated blob";
    std::set<TLogoBlobID> written;
    for (ui32 step = 1; step <= 10; ++step) {
        const TLogoBlobID id(100500, 1, step, 0, data.size(), 0);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        written.insert(id);
    }

    // the skeleton of the VDisk on node 2 doesn't get its log written, so it has a write in flight
    const ui32 nodeId = 2;
    TActorId skeletonId;
    bool assimilationPending = false;
    std::unique_ptr<IEventHandle> result;
    runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetRecipientRewrite() == edge) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilateResult) {
                result = std::move(ev);
            }
            return false; // the result of the put below doesn't matter
        } else if (recipientNodeId != nodeId) {
            return true;
        } else if (IActor *actor = runtime->GetActor(ev->GetRecipientRewrite()); !actor ||
                !TypeName(*actor).EndsWith("::TSkeleton")) {
            return true;
        } else if (ev->GetTypeRewrite() == NPDisk::TEvLogResult::EventType) {
            skeletonId = ev->GetRecipientRewrite();
            return false;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVAssimilate) {
            assimilationPending = true;
        }
        return true;
    };
    runtime->WrapInActorContext(edge, [&] {
        const TLogoBlobID id(100500, 1, 100, 0, data.size(), 0);
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
    });
    for (ui32 i = 0; i < 100 && !skeletonId; ++i) {
        env.Sim(TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT(skeletonId);

    runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvAssimilate(std::nullopt, std::nullopt, std::nullopt, true,
            false));
    });
    for (ui32 i = 0; i < 100 && !assimilationPending; ++i) {
        env.Sim(TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT(assimilationPending);
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(!result);

    runtime->Send(new IEventHandle(skeletonId, {}, new TEvents::TEvPoisonPill), nodeId);
    for (ui32 i = 0; i < 60 && !result; ++i) {
        env.Sim(TDuration::Seconds(10));
    }
    runtime->FilterFunction = {};

    UNIT_ASSERT_C(result, "the request has got no reply from the VDisk gone away");
    auto *msg = result->Get<TEvBlobStorage::TEvAssimilateResult>();
    UNIT_ASSERT_VALUES_EQUAL_C(msg->Status, NKikimrProto::OK, msg->ErrorReason);
    std::set<TLogoBlobID> assimilated;
    for (const auto& blob : msg->Blobs) {
        assimilated.insert(blob.Id);
    }
    for (const auto& id : written) {
        UNIT_ASSERT_C(assimilated.contains(id), id);
    }
}

Y_UNIT_TEST_SUITE(VDiskAssimilation) {
    Y_UNIT_TEST(VDiskGoesAway) {
        RunVDiskGoesAwayTest();
    }
    Y_UNIT_TEST(RestartKeepsCallerCursors) {
        RunAssimilationRestartTest();
    }

    Y_UNIT_TEST(Test) {
        RunAssimilationTest(false);
    }
    Y_UNIT_TEST(TestReverse) {
        RunAssimilationTest(true);
    }
    Y_UNIT_TEST(Iteration) {
        RunVDiskIterationTest(false);
    }
    Y_UNIT_TEST(IterationReverse) {
        RunVDiskIterationTest(true);
    }
}
