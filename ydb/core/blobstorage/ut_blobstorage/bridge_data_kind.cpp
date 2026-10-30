#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <util/system/type_name.h>

using NBridgeTest::GetBridgedLocation;

namespace {

    // two-pile cluster with the interpile option on, warmed up so that writes to the remote pile take it
    std::unique_ptr<TEnvironmentSetup> MakeInterpileEnv(ui32& groupId) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableInterpileTrafficOptimization(true);
        auto env = std::make_unique<TEnvironmentSetup>(TEnvironmentSetup::TSettings{
            .NodeCount = 8 * 2,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        });
        groupId = NBridgeTest::CreatePoolAndGetGroup(*env);

        // the interpile option is only taken once the queues towards the remote pile have connected
        const TString data = "warm up";
        const TActorId sender = env->Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env->Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(TLogoBlobID(100500, 1, 1, 0, data.size(), 0), data,
                TInstant::Max()));
        });
        UNIT_ASSERT_VALUES_EQUAL(env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false)->Get()->Status,
            NKikimrProto::OK);
        env->Sim(TDuration::Seconds(5));
        return env;
    }

    // the put the remote pile's NodeWarden makes of the interpile one, as it goes to the DS proxy of the pile group
    struct TRebuiltPut {
        TLogoBlobID Id;
        NKikimrBlobStorage::EPutHandleClass HandleClass;
        TEvBlobStorage::TEvPut::ETactic Tactic;
        TWriteSource WriteSource;
        NKikimrBlobStorage::TDataKind::E DataKind;
        bool IssueKeepFlag;
        bool IgnoreBlock;
        bool AlreadyEncrypted;
        bool IsZeroEntry;
        std::vector<std::pair<ui64, ui32>> ExtraBlockChecks;
    };

    auto MakeRebuiltPutCatcher(TEnvironmentSetup& env, std::vector<TRebuiltPut>& rebuilt) {
        return [&env, &rebuilt](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvPut || !ev->HasEvent()) {
                return true;
            } else if (IActor *actor = env.Runtime->GetActor(ev->Sender); actor &&
                    TypeName(*actor).Contains("TNodeWarden")) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvPut>();
                rebuilt.push_back({msg->Id, msg->HandleClass, msg->Tactic, msg->WriteSource, msg->DataKind,
                    msg->IssueKeepFlag, msg->IgnoreBlock, msg->AlreadyEncrypted, msg->IsZeroEntry,
                    msg->ExtraBlockChecks});
            }
            return true;
        };
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeDataKind) {

    // the remote pile's answer to the interpile put tells everything the VDisks of the pile would, had the blob been
    // written to them from here: status flags and free space, the blob written beyond the barrier, and the generation
    // the put raced with (here the put is made with stale generation, as by the node not knowing the newer one)
    Y_UNIT_TEST(InterpilePutResultKeepsResultFields) {
        ui32 groupId;
        auto env = MakeInterpileEnv(groupId);
        const auto pileGroups = NBridgeTest::GetPileGroups(*env, groupId);
        const TString data = "hello";

        struct TResult {
            NKikimrProto::EReplyStatus Status;
            ui32 RacingGeneration;
            bool WrittenBeyondBarrier;
            ui32 StatusFlags;
            float ApproximateFreeSpaceShare;
        };
        std::optional<TResult> result;
        std::optional<TResult> localResult; // the one of pile 0, written to from here
        bool interpile = false;
        bool makeStale = false;
        env->Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut) {
                interpile = true;
                if (std::exchange(makeStale, false)) {
                    auto& record = ev->Get<TEvInterpilePut>()->Record;
                    record.SetGroupGeneration(record.GetGroupGeneration() - 1);
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult && ev->HasEvent()) {
                if (IActor *actor = env->Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                    const auto *msg = ev->Get<TEvBlobStorage::TEvPutResult>();
                    const TResult item{msg->Status, msg->RacingGeneration, msg->WrittenBeyondBarrier,
                        msg->StatusFlags.Raw, msg->ApproximateFreeSpaceShare};
                    if (msg->GroupId == pileGroups[1]) {
                        result.emplace(item);
                        return false; // the bridged put doesn't matter, only the answer of the pile does
                    } else if (msg->GroupId == pileGroups[0]) {
                        localResult.emplace(item);
                    }
                }
            }
            return true;
        };
        auto put = [&](const TLogoBlobID& id) {
            result.reset();
            localResult.reset();
            interpile = false;
            const TActorId sender = env->Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env->Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            NBridgeTest::WaitUntil(*env, [&] { return result && localResult; }, "answers of piles",
                TDuration::MilliSeconds(100), 300);
            UNIT_ASSERT_C(interpile, "no write reached a remote pile the interpile way");
            return *result;
        };

        // whatever the VDisks tell of their space comes from the remote pile the same way it comes from the local one
        auto res = put(TLogoBlobID(100500, 1, 2, 0, data.size(), 0));
        UNIT_ASSERT_VALUES_EQUAL(res.Status, NKikimrProto::OK);
        const ui32 valid = ui32(NKikimrBlobStorage::StatusIsValid);
        UNIT_ASSERT_VALUES_EQUAL(res.StatusFlags & valid, localResult->StatusFlags & valid);
        UNIT_ASSERT_VALUES_EQUAL(res.ApproximateFreeSpaceShare > 0, localResult->ApproximateFreeSpaceShare > 0);
        UNIT_ASSERT(!res.WrittenBeyondBarrier);

        makeStale = true;
        res = put(TLogoBlobID(100500, 1, 3, 0, data.size(), 0));
        UNIT_ASSERT_VALUES_EQUAL(res.Status, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(res.RacingGeneration, NBridgeTest::GetNodeGroupGeneration(*env, pileGroups[1], 9));

        // the barrier comes into effect in pile 1, and the blob is written behind it
        const TLogoBlobID probe(100600, 1, 1, 0, data.size(), 0);
        const auto probePut = NBridgeTest::QueryPileGroup<TEvBlobStorage::TEvPutResult>(*env, pileGroups[1], 9,
            new TEvBlobStorage::TEvPut(probe, data, TInstant::Max()));
        UNIT_ASSERT_VALUES_EQUAL(probePut->Status, NKikimrProto::OK);
        const auto gc = NBridgeTest::QueryPileGroup<TEvBlobStorage::TEvCollectGarbageResult>(*env, pileGroups[1], 9,
            new TEvBlobStorage::TEvCollectGarbage(probe.TabletID(), 1, 0, 0, true, 1, 10, nullptr, nullptr,
                TInstant::Max(), false));
        UNIT_ASSERT_VALUES_EQUAL(gc->Status, NKikimrProto::OK);
        NBridgeTest::WaitUntil(*env, [&] {
            return NBridgeTest::ReadFromPileGroup(*env, pileGroups[1], 9, probe, data) == NKikimrProto::NODATA;
        }, "barrier in effect", TDuration::Seconds(1), 300);
        res = put(TLogoBlobID(probe.TabletID(), 1, 2, 0, data.size(), 0));
        UNIT_ASSERT_VALUES_EQUAL(res.Status, NKikimrProto::OK);
        UNIT_ASSERT(res.WrittenBeyondBarrier);

        env->Runtime->FilterFunction = nullptr;
    }

    // the interpile put carries every parameter of the original put, which has meaning for the remote pile
    Y_UNIT_TEST(InterpilePutKeepsPutParameters) {
        ui32 groupId;
        auto env = MakeInterpileEnv(groupId);

        const TString data = "hello";
        const TLogoBlobID id(100500, 1, 2, 0, data.size(), 0);
        std::vector<TRebuiltPut> rebuilt;
        env->Runtime->FilterFunction = MakeRebuiltPutCatcher(*env, rebuilt);
        const TActorId sender = env->Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env->Runtime->WrapInActorContext(sender, [&] {
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(TEvBlobStorage::TEvPut::TParameters{
                .BlobId = id,
                .Buffer = TRope(data),
                .Deadline = TInstant::Max(),
                .HandleClass = NKikimrBlobStorage::UserData,
                .Tactic = TEvBlobStorage::TEvPut::TacticMaxThroughput,
                .WriteSource = TWriteSource::WriteLogEntry,
                .DataKind = NKikimrBlobStorage::TDataKind::SYSTEM,
                .IssueKeepFlag = true,
            });
            ev->ExtraBlockChecks = {{100600, 1}, {100700, 2}};
            SendToBSProxy(sender, groupId, ev.release());
        });
        auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
        env->Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());

        UNIT_ASSERT_C(!rebuilt.empty(), "no write reached a remote pile the interpile way");
        for (const auto& put : rebuilt) {
            UNIT_ASSERT_VALUES_EQUAL(put.Id, id);
            UNIT_ASSERT_EQUAL(put.HandleClass, NKikimrBlobStorage::UserData);
            UNIT_ASSERT_EQUAL(put.Tactic, TEvBlobStorage::TEvPut::TacticMaxThroughput);
            UNIT_ASSERT_EQUAL(put.WriteSource, TWriteSource::WriteLogEntry);
            UNIT_ASSERT_EQUAL(put.DataKind, NKikimrBlobStorage::TDataKind::SYSTEM);
            UNIT_ASSERT(put.IssueKeepFlag);
            UNIT_ASSERT(!put.IgnoreBlock);
            UNIT_ASSERT(!put.AlreadyEncrypted);
            UNIT_ASSERT(!put.IsZeroEntry);
            UNIT_ASSERT((put.ExtraBlockChecks == std::vector<std::pair<ui64, ui32>>{{100600, 1}, {100700, 2}}));
        }
    }

    // the tablet of the extra block check is blocked in the remote pile only, as after a block reaching some piles; the
    // put must be blocked there, as it would be without the interpile option
    Y_UNIT_TEST(InterpilePutChecksExtraBlocks) {
        ui32 groupId;
        auto env = MakeInterpileEnv(groupId);
        const auto pileGroups = NBridgeTest::GetPileGroups(*env, groupId);

        const auto block = NBridgeTest::QueryPileGroup<TEvBlobStorage::TEvBlockResult>(*env, pileGroups[1], 9,
            new TEvBlobStorage::TEvBlock(100600, 5, TInstant::Max()));
        UNIT_ASSERT_VALUES_EQUAL_C(block->Status, NKikimrProto::OK, block->ToString());

        const TString data = "hello";
        std::vector<TRebuiltPut> rebuilt;
        env->Runtime->FilterFunction = MakeRebuiltPutCatcher(*env, rebuilt);
        const TActorId sender = env->Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env->Runtime->WrapInActorContext(sender, [&] {
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(TLogoBlobID(100500, 1, 2, 0, data.size(), 0), data,
                TInstant::Max());
            ev->ExtraBlockChecks = {{100600, 5}};
            SendToBSProxy(sender, groupId, ev.release());
        });
        auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
        env->Runtime->FilterFunction = nullptr;
        UNIT_ASSERT_C(!rebuilt.empty(), "no write reached a remote pile the interpile way");
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::BLOCKED, res->Get()->ToString());
    }

    Y_UNIT_TEST(StorageInfoVersion) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        auto block = [&](ui32 generation, ui32 version) {
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvBlock(100500, generation,
                    TInstant::Max(), 1, TWriteSource::Unknown, version));
            });
            return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(sender, false);
        };

        auto result = block(10, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(20, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->IsTabletStorageInfoVersionObsolete);
    }

    Y_UNIT_TEST(EncryptionWorksWithInterpileTrafficOptimization) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableInterpileTrafficOptimization(true);

        const ui8 keyData[32] = "Hello, I'm your new key";
        const TEncryptionKey key{
            .Key = {keyData, sizeof(keyData)},
            .Version = 1,
            .Id = "tenant key",
        };

        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .Encryption = true,
            .ConfigPreprocessor = [key](ui32, TNodeWardenConfig& config) { config.TenantKey = key; },
            .LocationGenerator = GetBridgedLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();

        ui32 step = 1;
        auto put = [&](const TString& data) {
            const TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            return id;
        };

        put("warmup");
        env.Sim(TDuration::Seconds(5));

        size_t encryptedInterpilePuts = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut) {
                for (const auto& item : ev->Get<TEvInterpilePut>()->Record.GetItems()) {
                    encryptedInterpilePuts += item.GetAlreadyEncrypted();
                }
            }
            return true;
        };

        const TString data(1_MB + 1, 'x');
        const TLogoBlobID id = put(data);

        env.Runtime->FilterFunction = nullptr;

        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), data);
        UNIT_ASSERT_C(encryptedInterpilePuts, "no encrypted write reached a remote pile the interpile way");
    }

    // With interpile traffic optimization the bridge proxy hands the blob over to a node inside the
    // remote pile instead of writing it across the link itself. That leg rebuilds the message and
    // then passes it through a protobuf, so the data kind has to survive both hops -- otherwise a
    // system tablet writing to a bridged group is admitted as user data in every pile but its own.
    Y_UNIT_TEST(SurvivesInterpileTrafficOptimization) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableInterpileTrafficOptimization(true);

        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();

        ui32 step = 1;
        auto put = [&](NKikimrBlobStorage::TDataKind::E dataKind) {
            const TString data = "hello";
            const TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = id,
                    .Buffer = TRope(data),
                    .Deadline = TInstant::Max(),
                    .DataKind = dataKind,
                }));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        };

        // The interpile option is only taken once the queues towards the remote piles have
        // connected, so warm them up first -- otherwise the measured writes may quietly fall back
        // to going across the link and the test would prove nothing.
        put(NKikimrBlobStorage::TDataKind::USER);
        env.Sim(TDuration::Seconds(5));

        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut) {
                    for (const auto& item : ev->Get<TEvInterpilePut>()->Record.GetItems()) {
                        seen.push_back(item.GetDataKind());
                    }
                }
                return true;
            };

            put(dataKind);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(!seen.empty(), "no write reached a remote pile the interpile way");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

    // A MustRestoreFirst read of a blob that only one pile has makes the bridge proxy copy it into
    // the other piles. That copy is a write the reader cannot avoid, so it has to be admitted the
    // same way the read itself is -- otherwise a system tablet cannot boot off a bridged group that
    // is short of space.
    Y_UNIT_TEST(RestorePutInheritsTheKindOfTheRead) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);

        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "hello";

        ui32 step = 1;
        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            // Write straight into the group backing the first pile, leaving the other two without
            // the blob.
            const TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
            env.Runtime->WrapInActorContext(sender, [&] {
                auto put = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
                put->ForceGroupGeneration = 1;
                SendToBSProxy(sender, groupId + 1, put.release());
            });
            auto putRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                    const auto& put = *ev->Get<TEvBlobStorage::TEvPut>();
                    if (put.WriteSource == TWriteSource::BridgeProxyRestorePut) {
                        seen.push_back(put.DataKind);
                    }
                }
                return true;
            };

            env.Runtime->WrapInActorContext(sender, [&] {
                auto get = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::FastRead, true /*mustRestoreFirst*/);
                get->DataKind = dataKind;
                SendToBSProxy(sender, info->GroupID, get.release());
            });
            auto getRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->Responses[0].Status, NKikimrProto::OK);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(!seen.empty(), "the read restored nothing, so it proves nothing");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

}
