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
                rebuilt.push_back({msg->Id, msg->HandleClass, msg->Tactic, msg->WriteSource,
                    msg->IssueKeepFlag, msg->IgnoreBlock, msg->AlreadyEncrypted, msg->IsZeroEntry,
                    msg->ExtraBlockChecks});
            }
            return true;
        };
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeDataKind) {

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

}
