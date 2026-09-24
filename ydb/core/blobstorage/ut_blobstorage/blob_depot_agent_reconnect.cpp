#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

#include "blob_depot_agent_pipe_control.h"
#include "blob_depot_event_managers.h"
#include "blob_depot_test_helpers.h"
#include "blob_depot_test_env.h"

using namespace NKikimr;

namespace {

// The agent lives on the node the request is issued from, so every operation here goes through node AgentNodeId
// and it is that node's agent whose pipe the test breaks.
struct TAgentReconnectTest {
    static constexpr ui32 AgentNodeId = 1;
    static constexpr ui64 UserTabletId = 100;

    TBlobDepotTestEnvironment TEnv;
    TEnvironmentSetup& Env;
    const ui32 VirtualGroup;
    const ui64 BlobDepotTabletId;
    TAgentPipeControl Pipes;
    std::vector<TBlobInfo> Blobs;

    TAgentReconnectTest()
        : TEnv(1, 4)
        , Env(*TEnv.Env)
        , VirtualGroup(TEnv.BlobDepot)
        , BlobDepotTabletId(NBlobDepotTest::GetBlobDepotTabletId(Env, VirtualGroup))
        , Pipes(Env, BlobDepotTabletId)
    {
        Blobs.reserve(64);
        Env.Runtime->SetLogPriority(NKikimrServices::BLOB_DEPOT, NLog::PRI_DEBUG);
        Env.Runtime->SetLogPriority(NKikimrServices::BLOB_DEPOT_AGENT, NLog::PRI_DEBUG);
    }

    TBlobInfo& AddBlob(ui32 cookie, ui32 size = 1024) {
        Blobs.emplace_back(TEnv.DataGen(size), UserTabletId, cookie);
        return Blobs.back();
    }

    // Deliberately not VerifiedPut/VerifiedGet: those treat ERROR as "unknown" and then let the matching get come
    // back NODATA, so a test built on them passes just as happily when every write fails -- which is exactly the
    // regression these cases exist to catch. Here a put means OK, and a get means OK with the same bytes back.
    TActorId PutAsync(TBlobInfo& blob) {
        const TActorId sender = Env.Runtime->AllocateEdgeActor(AgentNodeId);
        SendTEvPut(Env, sender, VirtualGroup, blob.Id, blob.Data);
        return sender;
    }

    NKikimrProto::EReplyStatus AwaitPut(TActorId sender, TBlobInfo& blob) {
        auto res = CaptureTEvPutResult(Env, sender, true, false);
        UNIT_ASSERT_C(res, "no TEvPutResult for " << blob.Id.ToString());
        const auto status = res->Get()->Status;
        if (status == NKikimrProto::OK) {
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Id.ToString(), blob.Id.ToString());
            blob.Status = TBlobInfo::EStatus::WRITTEN;
        }
        return status;
    }

    void Put(TBlobInfo& blob) {
        const TActorId sender = PutAsync(blob);
        const auto status = AwaitPut(sender, blob);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::OK, "put of " << blob.Id.ToString() << " failed");
    }

    void Get(TBlobInfo& blob) {
        UNIT_ASSERT_C(blob.Status == TBlobInfo::EStatus::WRITTEN,
            "asked to read " << blob.Id.ToString() << " which was never written");
        const TActorId sender = Env.Runtime->AllocateEdgeActor(AgentNodeId);
        SendTEvGet(Env, sender, VirtualGroup, blob.Id);
        auto res = CaptureTEvGetResult(Env, sender, true, false);
        UNIT_ASSERT_C(res, "no TEvGetResult for " << blob.Id.ToString());
        auto& msg = *res->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(msg.Status, NKikimrProto::OK, "get of " << blob.Id.ToString() << " failed");
        UNIT_ASSERT_VALUES_EQUAL(msg.ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(msg.Responses[0].Status, NKikimrProto::OK,
            "get of " << blob.Id.ToString() << " came back " << NKikimrProto::EReplyStatus_Name(msg.Responses[0].Status));
        UNIT_ASSERT_VALUES_EQUAL(msg.Responses[0].Id.ToString(), blob.Id.ToString());
        UNIT_ASSERT_VALUES_EQUAL(msg.Responses[0].Buffer.ConvertToString(), blob.Data);
    }

    void GetAll() {
        for (auto& blob : Blobs) {
            Get(blob);
        }
    }

    // User-level collect through a (possibly different) node's agent. recordGeneration is one past the blob
    // generation: the issuing tablet has to be in a generation that is allowed to collect.
    void CollectHard(ui32 nodeId, ui32 collectGeneration, ui32 collectStep) {
        TBSState state;
        state[UserTabletId];
        VerifiedCollectGarbage(Env, nodeId, VirtualGroup, UserTabletId,
            collectGeneration + 1, 1, 0,
            true, collectGeneration, collectStep,
            nullptr, nullptr, false, true, Blobs, state);
    }

    ui32 DataChannelCollects() const {
        return Pipes.TabletCollectGarbageCount(true) + Pipes.TabletCollectGarbageCount(false);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobDepotAgentReconnect) {

    // Baseline: the agent survives losing its pipe mid-stream, without the tablet generation changing. Also the
    // smoke test for the harness itself -- if breaking the pipe did not really break it, everything below is
    // testing nothing.
    Y_UNIT_TEST(SurvivesPipeBreak) {
        TAgentReconnectTest test;

        test.Put(test.AddBlob(1));
        const ui32 connectsBefore = test.Pipes.ConnectCount(test.AgentNodeId);

        test.Pipes.Break(test.AgentNodeId);
        UNIT_ASSERT_C(test.Pipes.ConnectCount(test.AgentNodeId) > connectsBefore,
            "the agent did not reconnect after its pipe was broken");

        test.Put(test.AddBlob(2));
        test.GetAll();
    }

    // A put that fails at the blobstorage level must hand its blob sequence id back, or the tablet keeps the id
    // reserved and garbage collection on that channel never moves again. The id travels in TEvDiscardSpoiledBlobSeq,
    // whose blob-seq-id branch used to be dead code.
    Y_UNIT_TEST(FailedBackingPutReturnsBlobSeqId) {
        TAgentReconnectTest test;

        test.Put(test.AddBlob(1)); // warm up, so the agent holds a range
        UNIT_ASSERT_VALUES_EQUAL(test.Pipes.DiscardedBlobSeqIds(), 0);

        auto& doomed = test.AddBlob(2);
        test.Pipes.FailNextBackingPut(test.AgentNodeId);

        const TActorId sender = test.PutAsync(doomed);
        const auto status = test.AwaitPut(sender, doomed);
        UNIT_ASSERT_VALUES_UNEQUAL_C(status, NKikimrProto::OK, "the injected backing failure did not reach the user");
        test.Blobs.pop_back(); // never landed

        test.Env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_C(test.Pipes.DiscardedBlobSeqIds() > 0,
            "the failed put never returned its BlobSeqId to the tablet");

        test.Put(test.AddBlob(3));
        test.GetAll();
    }

    // Held down past ExpirationTimeout, the tablet reclaims the agent's id ranges so garbage collection can move
    // on. When the agent comes back it must be told what was taken and carry on rather than committing against
    // blobs that may already be gone.
    Y_UNIT_TEST(ExpiredAgentRecovers) {
        TAgentReconnectTest test;

        test.Put(test.AddBlob(1));
        UNIT_ASSERT_C(test.Pipes.InvalidatedSteps(test.AgentNodeId).empty(),
            "nothing should have been reclaimed from a healthy agent");

        test.Pipes.HoldDown(test.AgentNodeId);
        test.Env.Sim(TDuration::Minutes(2)); // ExpirationTimeout is one minute
        test.Pipes.Release(test.AgentNodeId);

        // TEvRegisterAgentResult.InvalidatedSteps is populated from TAgent::ExpiredSteps and nothing writes that
        // but ExpireAgent, so a watermark here is proof the sweep ran and reclaimed this agent's ranges. Without
        // it the rest of this test would pass just as well with expiry switched off entirely.
        const ui32 watermark = test.Pipes.MaxInvalidatedStep(test.AgentNodeId);
        UNIT_ASSERT_C(!test.Pipes.InvalidatedSteps(test.AgentNodeId).empty(),
            "the tablet never reclaimed the id ranges of an agent held down past its expiry");

        test.Put(test.AddBlob(2));

        // and the ids handed out afterwards have to sit above the watermark the agent was told to drop, otherwise
        // a fresh write would be condemned by the very trim the tablet just ordered
        UNIT_ASSERT_C(test.Pipes.LastCommitIsAboveWatermark(test.AgentNodeId),
            "blob seq id committed after reclamation (step " << test.Pipes.LastCommittedStep(test.AgentNodeId)
                << ") is not above its channel's invalidated step; max watermark was " << watermark);

        test.GetAll();
    }

    // A backing write that outlives the disconnect and lands after the tablet has reclaimed its id. It must fail
    // rather than commit against a blob the tablet may already have collected -- and, more to the point, it must
    // not take the tablet down on the way: before the ownership check in BeginCommittingBlobSeqId, this commit
    // tripped either a RemovePoint abort or the "committing trimmed BlobSeqId" verify.
    Y_UNIT_TEST(LateBackingPutAfterExpiryIsRejected) {
        TAgentReconnectTest test;

        test.Put(test.AddBlob(1));

        auto& doomed = test.AddBlob(2);
        test.Pipes.HoldNextBackingPut(test.AgentNodeId);
        const TActorId sender = test.PutAsync(doomed);
        test.Env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_C(test.Pipes.BackingPutHeld(test.AgentNodeId),
            "the agent's backing write was never withheld, so nothing outlives the disconnect");

        test.Pipes.HoldDown(test.AgentNodeId);
        test.Env.Sim(TDuration::Minutes(2));
        test.Pipes.Release(test.AgentNodeId);
        UNIT_ASSERT_C(!test.Pipes.InvalidatedSteps(test.AgentNodeId).empty(), "the agent was never expired");

        test.Pipes.DeliverHeldBackingPut(test.AgentNodeId);

        const auto status = test.AwaitPut(sender, doomed);
        UNIT_ASSERT_VALUES_UNEQUAL_C(status, NKikimrProto::OK,
            "a write whose blob seq id had been reclaimed was committed anyway");
        test.Blobs.pop_back(); // never landed

        // the tablet is still alive and serving
        test.Put(test.AddBlob(3));
        test.GetAll();
    }

    // The regression this suite exists for. Expiry watermarks are per tablet generation: steps start over low after
    // a restart, so a watermark carried across one condemns perfectly good fresh ids and every write on that
    // channel fails. The put after the restart is the assertion.
    Y_UNIT_TEST(ExpiredAgentWorksAfterTabletRestart) {
        TAgentReconnectTest test;

        test.Put(test.AddBlob(1));

        test.Pipes.HoldDown(test.AgentNodeId);
        test.Env.Sim(TDuration::Minutes(2));
        test.Pipes.Release(test.AgentNodeId);

        test.Put(test.AddBlob(2)); // agent has applied the watermark and works within this generation

        const ui32 watermark = test.Pipes.MaxInvalidatedStep(test.AgentNodeId);
        UNIT_ASSERT_C(!test.Pipes.InvalidatedSteps(test.AgentNodeId).empty(), "the agent was never expired");

        NBlobDepotTest::RestartTablet(test.TEnv, test.BlobDepotTabletId);
        test.Env.Sim(TDuration::Seconds(5));

        // A new generation starts its steps over from a low value, so these puts sit at or below the watermark
        // recorded in the previous one. If that watermark is still being applied they all fail.
        test.Put(test.AddBlob(3));
        UNIT_ASSERT_C(test.Pipes.LastCommittedStep(test.AgentNodeId) <= watermark,
            "post-restart steps (" << test.Pipes.LastCommittedStep(test.AgentNodeId) << ") did not fall back to or"
                " below the old watermark (" << watermark << "), so this case is no longer exercising the bug");
        test.Put(test.AddBlob(4));
        test.GetAll();
    }

    // Expiry twice over, with a restart in between, so the watermark is set in one generation and then again in
    // the next: the second must replace the first rather than max against it.
    Y_UNIT_TEST(ExpiredAgentAcrossTwoGenerations) {
        TAgentReconnectTest test;

        test.Put(test.AddBlob(1));

        test.Pipes.HoldDown(test.AgentNodeId);
        test.Env.Sim(TDuration::Minutes(2));
        test.Pipes.Release(test.AgentNodeId);
        test.Put(test.AddBlob(2));

        NBlobDepotTest::RestartTablet(test.TEnv, test.BlobDepotTabletId);
        test.Env.Sim(TDuration::Seconds(5));
        test.Put(test.AddBlob(3));

        test.Pipes.HoldDown(test.AgentNodeId);
        test.Env.Sim(TDuration::Minutes(2));
        test.Pipes.Release(test.AgentNodeId);

        const ui32 secondWatermark = test.Pipes.MaxInvalidatedStep(test.AgentNodeId);
        UNIT_ASSERT_C(!test.Pipes.InvalidatedSteps(test.AgentNodeId).empty(),
            "the agent was never expired in the second generation");

        test.Put(test.AddBlob(4));
        UNIT_ASSERT_C(test.Pipes.LastCommitIsAboveWatermark(test.AgentNodeId),
            "blob seq id committed after the second reclamation is not above its channel's watermark; max"
                " watermark was " << secondWatermark);

        test.GetAll();
    }

    // The reason expiry exists: leftover given ids of a disconnected agent pin GetLeastExpectedBlobId, so
    // HandleTrash cannot issue a barrier covering trash in that step. A second node's agent can still accept
    // a user collect (which creates the trash) but cannot trim the gone agent's ranges. After the 1-minute
    // timeout those ranges are reclaimed and a tablet-side collect must go out.
    Y_UNIT_TEST(ExpiredAgentUnblocksTabletGC) {
        TAgentReconnectTest test;
        constexpr ui32 otherNodeId = 2;

        auto& doomed = test.Blobs.emplace_back(test.TEnv.DataGen(1024), test.UserTabletId, 1, 1, 1, 0);
        auto& keep = test.Blobs.emplace_back(test.TEnv.DataGen(1024), test.UserTabletId, 2, 1, 2, 0);
        test.Put(doomed);
        test.Put(keep);
        test.Env.Sim(TDuration::Seconds(5));

        const ui32 collectsBefore = test.DataChannelCollects();

        test.Pipes.HoldDown(test.AgentNodeId);
        test.CollectHard(otherNodeId, 1, 1);
        UNIT_ASSERT_C(doomed.Status == TBlobInfo::EStatus::COLLECTED,
            "user collect of gen:step 1:1 did not mark the doomed blob collected");
        UNIT_ASSERT_C(keep.Status == TBlobInfo::EStatus::WRITTEN,
            "user collect of gen:step 1:1 must not collect the keep blob at step 2");

        // This case only isolates the held-down agent as the thing pinning the step if the second node's agent
        // holds no ranges of its own. It does not: a collect allocates no blob sequence numbers, and the
        // IssueAllocateIdsIfNeeded loop on the registration path is a no-op because IsConnected is still false
        // there. Assert it rather than rely on it, so that if that ever changes this fails saying why instead of
        // looking like expiry stopped working.
        UNIT_ASSERT_VALUES_EQUAL_C(test.Pipes.AllocateIdsCount(otherNodeId), 0,
            "node " << otherNodeId << "'s agent took id ranges of its own, so it is pinning the step too and this"
                " test can no longer attribute GC progress to the expiry of node " << test.AgentNodeId);

        test.Env.Sim(TDuration::Seconds(5));
        const ui32 collectsWhilePinned = test.DataChannelCollects();
        UNIT_ASSERT_VALUES_EQUAL_C(collectsWhilePinned, collectsBefore,
            "tablet-side collect moved while the expired agent's leftover ids should still have pinned the step; "
            "before " << collectsBefore << ", after user collect " << collectsWhilePinned);

        test.Env.Sim(TDuration::Minutes(2));
        const ui32 collectsAfterExpiry = test.DataChannelCollects();
        UNIT_ASSERT_C(collectsAfterExpiry > collectsWhilePinned,
            "expiry did not let the tablet collect the pinned trash; collects stayed at " << collectsWhilePinned);

        test.Pipes.Release(test.AgentNodeId);
        UNIT_ASSERT_C(!test.Pipes.InvalidatedSteps(test.AgentNodeId).empty(),
            "the tablet never reclaimed the id ranges of the agent that was pinning GC");

        test.Get(keep);
    }
}
