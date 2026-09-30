#include "../blob_depot.h"
#include "../agent/agent.h"
#include "../events.h"
#include "../s3_router_events.h"
#include "../types.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobDepot {
namespace {

struct TAgent {
    TActorId Edge;
    TActorId Pipe;
    ui32 NodeIndex;
};

struct TTestEnv {
    static constexpr ui64 TabletId = 72075186224000000;
    static constexpr ui32 VirtualGroupId = 0x80000000;
    TTestBasicRuntime Runtime{3};
    ui32 DataGroupId = 0;
    THashSet<ui64> PrepareResults;
    THashMap<TActorId, ui64> RequestIds;
    THashMap<std::pair<TActorId, ui64>, ui64> PrepareCookies;
    TTestActorRuntime::TEventObserverHolder Observer;

    TTestEnv(bool useS3 = true) {
        SetupTabletServices(Runtime, nullptr, true);
        Runtime.GetAppData().Icb->CreateConfigControls(true);
        TControlBoard::SetValue(1, Runtime.GetAppData().Icb->BlobDepotControls.S3MaxWritesInFlight);

        Runtime.RegisterService(MakeBlobDepotS3RouterID(TabletId), Runtime.AllocateEdgeActor());
        Observer = Runtime.AddObserver([this](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NStorage::TEvNodeWardenAcquireBlobDepotS3Router::EventType:
                case NStorage::TEvNodeWardenReleaseBlobDepotS3Router::EventType:
                    ev.Reset();
                    break;
                case TEvBlobDepot::TEvPrepareWriteS3Result::EventType:
                    PrepareResults.insert(PrepareCookies.at(std::make_pair(ev->Recipient, ev->Cookie)));
                    break;
            }
        });

        auto* info = CreateTestTabletInfo(TabletId, TTabletTypes::BlobDepot);
        info->Channels.resize(3); // two system channels and one data channel, matching the config below
        DataGroupId = info->GroupFor(2, 1);
        const auto bootstrapper = CreateTestBootstrapper(Runtime, info, CreateBlobDepot);
        Runtime.EnableScheduleForActor(bootstrapper);
        const auto edge = Runtime.AllocateEdgeActor();
        auto config = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
        auto* proto = config->Record.MutableConfig();
        proto->SetName("disconnect-test");
        auto* system = proto->AddChannelProfiles();
        system->SetCount(2);
        system->SetChannelKind(NKikimrBlobDepot::TChannelKind::System);
        proto->AddChannelProfiles()->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
        if (useS3) {
            auto* s3 = proto->MutableS3BackendSettings();
            s3->MutableSyncMode();
            s3->MutableSettings()->SetBucket("test-bucket");
        } else {
            proto->SetVirtualGroupId(VirtualGroupId);
        }
        Runtime.SendToPipe(TabletId, edge, config.release(), 0, GetPipeConfigWithRetries());
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvApplyConfigResult>(edge, TDuration::Seconds(5)), "BlobDepot did not apply the test configuration");
    }

    TAgent Connect(ui32 nodeIndex, ui64 instanceId = 1, bool supportsIdRangeExpiry = false) {
        const auto edge = Runtime.AllocateEdgeActor(nodeIndex);
        const auto pipe = Runtime.ConnectToPipe(TabletId, edge, nodeIndex, GetPipeConfigWithRetries());
        const TAgent agent{edge, pipe, nodeIndex};
        auto request = std::make_unique<TEvBlobDepot::TEvRegisterAgent>();
        request->Record.SetAgentInstanceId(instanceId);
        request->Record.SetSupportsIdRangeExpiry(supportsIdRangeExpiry);
        Send(agent, request.release());
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvRegisterAgentResult>(edge, TDuration::Seconds(1)), "BlobDepot did not register the test agent");
        return agent;
    }

    ui64 Send(const TAgent& agent, IEventBase* event) {
        const ui64 cookie = ++RequestIds[agent.Pipe];
        Runtime.SendToPipe(agent.Pipe, agent.Edge, event, agent.NodeIndex, cookie);
        return cookie;
    }

    void Disconnect(const TAgent& agent) {
        Runtime.ClosePipe(agent.Pipe, agent.Edge, agent.NodeIndex);
        Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }

    TBlobSeqId AllocateBlobSeqId(const TAgent& agent) {
        Send(agent, new TEvBlobDepot::TEvAllocateIds(NKikimrBlobDepot::TChannelKind::Data, 1));
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvAllocateIdsResult>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(response);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetGivenIdRange().ChannelRangesSize(), 1);
        const auto& range = record.GetGivenIdRange().GetChannelRanges(0);
        UNIT_ASSERT_VALUES_EQUAL(range.GetEnd(), range.GetBegin() + 1);
        return TBlobSeqId::FromSequentalNumber(range.GetChannel(), record.GetGeneration(), range.GetBegin());
    }

    void Commit(const TAgent& agent, TBlobSeqId blobSeqId, NKikimrProto::EReplyStatus expectedStatus) {
        if (expectedStatus == NKikimrProto::OK) {
            // GC may issue Keep for a committed blob, so its data must exist in the storage mock too.
            // Mock proxies have independent data on each node; use the tablet's node, where GC runs.
            const auto edge = Runtime.AllocateEdgeActor();
            const auto id = blobSeqId.MakeBlobId(TabletId, EBlobType::VG_DATA_BLOB, 0, 100);
            Runtime.SendAsync(CreateEventForBSProxy(edge, DataGroupId,
                new TEvBlobStorage::TEvPut(id, TString(100, 'x'), TInstant::Max()), 0));
            const auto result = Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(edge, TDuration::Seconds(1));
            UNIT_ASSERT_C(result, "storage Put did not complete for " << id << " in group " << DataGroupId);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        }
        auto request = std::make_unique<TEvBlobDepot::TEvCommitBlobSeq>();
        auto* item = request->Record.AddItems();
        item->SetKey(TLogoBlobID(12345, 1, blobSeqId.Step, 0, 100, blobSeqId.Index).AsBinaryString());
        auto* locator = item->MutableBlobLocator();
        blobSeqId.ToProto(locator->MutableBlobSeqId());
        locator->SetGroupId(DataGroupId);
        locator->SetTotalDataLen(100);
        Send(agent, request.release());
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvCommitBlobSeqResult>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.ItemsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetItems(0).GetStatus(), expectedStatus);
    }

    void CollectGarbage(const TAgent& agent, ui32 step, bool hard, std::optional<TBlobSeqId> keep = {}) {
        auto request = std::make_unique<TEvBlobDepot::TEvCollectGarbage>();
        auto& record = request->Record;
        record.SetTabletId(12345);
        record.SetGeneration(1);
        record.SetPerGenerationCounter(1);
        record.SetChannel(0);
        record.SetHard(hard);
        record.SetCollectGeneration(1);
        record.SetCollectStep(step);
        if (keep) {
            LogoBlobIDFromLogoBlobID(TLogoBlobID(12345, 1, keep->Step, 0, 100, keep->Index), record.AddKeep());
        }
        Send(agent, request.release());
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvCollectGarbageResult>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrProto::OK);
    }

    void Prepare(const TAgent& agent, ui64 cookie) {
        auto request = std::make_unique<TEvBlobDepot::TEvPrepareWriteS3>();
        auto* item = request->Record.AddItems();
        item->SetKey(TStringBuilder() << "key-" << cookie);
        item->SetLen(100);
        const ui64 requestId = Send(agent, request.release());
        PrepareCookies.emplace(std::make_pair(agent.Edge, requestId), cookie);
    }

    void ExpectPending(ui64 cookie) {
        Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(!PrepareResults.contains(cookie), "write must wait for the occupied S3 slot");
    }

    void ExpectPrepared(const TAgent& agent, ui64 cookie) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvPrepareWriteS3Result>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT_C(response, "S3 write queue did not resume after agent disconnect/reconnect");
        UNIT_ASSERT_VALUES_EQUAL(PrepareCookies.at(std::make_pair(agent.Edge, response->Cookie)), cookie);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.ItemsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(record.GetItems(0).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT(record.GetItems(0).HasS3Locator());
    }

    void SendQueryBlocks(const TAgent& agent, ui64 tabletId) {
        auto request = std::make_unique<TEvBlobDepot::TEvQueryBlocks>();
        request->Record.AddTabletIds(tabletId);
        Send(agent, request.release());
    }

    void ExpectQueryBlocks(const TAgent& agent, ui32 expectedGeneration = 0) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvQueryBlocksResult>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.BlockedGenerationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetBlockedGenerations(0), expectedGeneration);
    }

    void QueryBlocks(const TAgent& agent, ui64 tabletId) {
        SendQueryBlocks(agent, tabletId);
        ExpectQueryBlocks(agent);
    }

    void Block(const TAgent& agent, ui64 tabletId) {
        auto request = std::make_unique<TEvBlobDepot::TEvBlock>();
        request->Record.SetTabletId(tabletId);
        request->Record.SetBlockedGeneration(1);
        request->Record.SetIssuerGuid(123);
        Send(agent, request.release());
    }

    void ExpectBlocked(const TAgent& agent, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvBlockResult>(
            agent.Edge, TDuration::Seconds(2));
        UNIT_ASSERT_C(response, "block did not finish after the disconnected agent timed out");
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), status);
    }
};

void CheckReconnectReleasesWrites(ui64 newInstanceId) {
    TTestEnv env;
    const auto oldAgent = env.Connect(0);
    const auto otherAgent = env.Connect(1);
    env.Prepare(oldAgent, 1);
    env.ExpectPrepared(oldAgent, 1);
    env.Prepare(oldAgent, 2);
    env.Prepare(otherAgent, 3);
    env.ExpectPending(2);
    env.ExpectPending(3);

    const auto replacement = env.Connect(0, newInstanceId);
    env.ExpectPrepared(otherAgent, 3);
    UNIT_ASSERT(!env.PrepareResults.contains(2));

    env.Disconnect(oldAgent);
    env.Prepare(replacement, 4);
    env.ExpectPending(4);
    env.Disconnect(otherAgent);
    env.ExpectPrepared(replacement, 4);
}

void CheckSupersededInitialRegistration(bool disconnectReplacement) {
    TTestEnv env(false);
    const auto edge = env.Runtime.AllocateEdgeActor(1);
    const auto pipe = env.Runtime.ConnectToPipe(TTestEnv::TabletId, edge, 1, GetPipeConfigWithRetries());
    const TAgent oldAgent{edge, pipe, 1};

    // Delay the first registration and the request behind it before the tablet records the pipe's NodeId.
    TBlockEvents<TEvBlobDepot::TEvRegisterAgent> registrations(env.Runtime, [&](const auto& ev) {
        return ev->Sender == oldAgent.Edge;
    });
    TBlockEvents<TEvBlobDepot::TEvQueryBlocks> queries(env.Runtime, [&](const auto& ev) {
        return ev->Sender == oldAgent.Edge;
    });
    auto request = std::make_unique<TEvBlobDepot::TEvRegisterAgent>();
    request->Record.SetAgentInstanceId(1);
    env.Send(oldAgent, request.release());
    env.SendQueryBlocks(oldAgent, 12345);
    env.Runtime.WaitFor("old registration and query", [&] {
        return !registrations.empty() && !queries.empty();
    }, TDuration::Seconds(1));

    const auto oldServerId = registrations.front()->Recipient;
    TBlockEvents<TEvTabletPipe::TEvServerDisconnected> disconnects(env.Runtime, [&](const auto& ev) {
        return ev->Get()->ServerId == oldServerId;
    });
    env.Disconnect(oldAgent);
    UNIT_ASSERT_VALUES_EQUAL(disconnects.size(), 1);

    const auto replacement = env.Connect(1);
    env.QueryBlocks(replacement, 12345);
    if (disconnectReplacement) {
        env.Disconnect(replacement);
    }

    ui32 staleReplies = 0;
    auto replyObserver = env.Runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->Recipient == oldAgent.Edge && (ev->Type == TEvBlobDepot::EvRegisterAgentResult ||
                ev->Type == TEvBlobDepot::EvQueryBlocksResult)) {
            ++staleReplies;
        }
    });
    registrations.Stop().Unblock();
    queries.Stop().Unblock();
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    UNIT_ASSERT_VALUES_EQUAL_C(staleReplies, 0, "superseded requests must not be handled");

    const auto current = disconnectReplacement ? env.Connect(1) : replacement;
    env.QueryBlocks(current, 12345);
    disconnects.Stop().Unblock();
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    env.QueryBlocks(current, 12345);
}

void CheckQueryBlocksCommit(bool reboot) {
    TTestEnv env;
    constexpr ui64 tabletId = 12345;
    const auto lessee = env.Connect(0);
    const auto blocker = env.Connect(1);
    env.QueryBlocks(lessee, tabletId);

    ui32 observedGeneration = 0;
    ui32 responses = 0;
    auto queryObserver = env.Runtime.AddObserver<TEvBlobDepot::TEvQueryBlocksResult>(
        [&](TEvBlobDepot::TEvQueryBlocksResult::TPtr& ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.BlockedGenerationsSize(), 1);
            observedGeneration = ev->Get()->Record.GetBlockedGenerations(0);
            ++responses;
        });
    TBlockEvents<TEvTablet::TEvCommit> commits(env.Runtime, [](const auto& ev) {
        return ev->Get()->TabletID == TTestEnv::TabletId;
    });

    env.Block(blocker, tabletId);
    env.Runtime.WaitFor("block commit", [&] { return !commits.empty(); }, TDuration::Seconds(1));
    env.SendQueryBlocks(lessee, tabletId);
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));

    if (reboot) {
        const ui32 generationBeforeReboot = observedGeneration;
        commits.Stop();
        commits.clear();
        RebootTablet(env.Runtime, TTestEnv::TabletId, env.Runtime.AllocateEdgeActor());
        const auto reconnected = env.Connect(0);
        env.QueryBlocks(reconnected, tabletId);

        UNIT_ASSERT_C(generationBeforeReboot <= observedGeneration,
                      "Blocked generation decreased after BlobDepot reboot: "
                      << generationBeforeReboot << " -> " << observedGeneration);
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(responses, 0, "QueryBlocks replied before the block was committed");
        commits.Stop().Unblock();
        env.ExpectQueryBlocks(lessee, 1);
    }
}

void CheckExpiredCommit(bool pinGarbageCollection) {
    TTestEnv env(false);
    const auto otherAgent = env.Connect(1);
    const auto otherId = pinGarbageCollection ? std::make_optional(env.AllocateBlobSeqId(otherAgent)) : std::nullopt;
    const auto agent = env.Connect(0, 1, true);
    const auto expiredId = env.AllocateBlobSeqId(agent);

    bool hardBarrierIssued = false;
    bool softBarrierIssued = false;
    auto gcObserver = env.Runtime.AddObserver<TEvBlobStorage::TEvCollectGarbage>(
        [&](TEvBlobStorage::TEvCollectGarbage::TPtr& ev) {
            const auto& msg = *ev->Get();
            if (msg.TabletId == TTestEnv::TabletId && msg.Channel == expiredId.Channel && msg.Collect &&
                    std::make_pair(msg.CollectGeneration, msg.CollectStep) >=
                    std::make_pair(expiredId.Generation, expiredId.Step)) {
                (msg.Hard ? hardBarrierIssued : softBarrierIssued) = true;
            }
        });
    if (!pinGarbageCollection) {
        // Reclaiming an unused range alone only advances the hard barrier. Create trash in this step and keep
        // another blob live so the soft barrier has work to do while the hard barrier is still pinned.
        const auto trashId = env.AllocateBlobSeqId(otherAgent);
        const auto keptId = env.AllocateBlobSeqId(otherAgent);
        UNIT_ASSERT_VALUES_EQUAL(trashId.Step, expiredId.Step);
        UNIT_ASSERT_VALUES_EQUAL(keptId.Step, expiredId.Step);
        env.Commit(otherAgent, trashId, NKikimrProto::OK);
        env.Commit(otherAgent, keptId, NKikimrProto::OK);
        env.CollectGarbage(otherAgent, expiredId.Step, false, keptId);
    }
    env.Disconnect(agent);
    env.Runtime.SimulateSleep(TDuration::Minutes(2));
    if (!pinGarbageCollection) {
        env.Runtime.WaitFor("soft barrier covering the expired id", [&] { return softBarrierIssued; },
            TDuration::Seconds(1));
        UNIT_ASSERT(!hardBarrierIssued);
        // Drop the remaining kept blob, allowing the hard barrier to cover the same step as the soft barrier.
        env.CollectGarbage(otherAgent, expiredId.Step, true);
        env.Runtime.WaitFor("hard barrier covering the expired id", [&] { return hardBarrierIssued; },
            TDuration::Seconds(1));
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }

    NKikimrBlobDepot::TEvRegisterAgentResult registration;
    auto registrationObserver = env.Runtime.AddObserver<TEvBlobDepot::TEvRegisterAgentResult>(
        [&](TEvBlobDepot::TEvRegisterAgentResult::TPtr& ev) {
            registration = ev->Get()->Record;
        });
    const auto reconnected = env.Connect(0, 1, true);
    UNIT_ASSERT_VALUES_EQUAL(registration.GetGeneration(), expiredId.Generation);
    UNIT_ASSERT_VALUES_EQUAL(registration.InvalidatedStepsSize(), 1);
    const auto& invalidated = registration.GetInvalidatedSteps(0);
    UNIT_ASSERT_VALUES_EQUAL(invalidated.GetChannel(), expiredId.Channel);
    UNIT_ASSERT(invalidated.GetInvalidatedStep() >= expiredId.Step);
    UNIT_ASSERT_VALUES_EQUAL(hardBarrierIssued, !pinGarbageCollection);
    UNIT_ASSERT_VALUES_EQUAL(softBarrierIssued, !pinGarbageCollection);

    // Emulate a commit sent before the reconnecting agent has applied RegisterAgentResult.
    env.Commit(reconnected, expiredId, NKikimrProto::ERROR);
    const auto freshId = env.AllocateBlobSeqId(reconnected);
    UNIT_ASSERT(freshId.Step > invalidated.GetInvalidatedStep());
    env.Commit(reconnected, freshId, NKikimrProto::OK);
    if (otherId) {
        // Expiring one agent must not invalidate another agent's reservation in the same step.
        UNIT_ASSERT_VALUES_EQUAL(otherId->Step, expiredId.Step);
        env.Commit(otherAgent, *otherId, NKikimrProto::OK);
    }
}

void CheckPutCompletesDuringReconnect(bool holdRegistrationResult) {
    TTestEnv env(false);
    auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
    info->BlobDepotId = TTestEnv::TabletId;
    const auto agentId = env.Runtime.Register(CreateBlobDepotAgent(TTestEnv::VirtualGroupId, info, {}));
    env.Runtime.EnableScheduleForActor(agentId);
    const auto edge = env.Runtime.AllocateEdgeActor();

    TActorId pipeId;
    auto pipeObserver = env.Runtime.AddObserver<TEvTabletPipe::TEvClientConnected>(
        [&](TEvTabletPipe::TEvClientConnected::TPtr& ev) {
            if (ev->Recipient == agentId) {
                pipeId = ev->Get()->ClientId;
            }
        });
    TBlockEvents<TEvBlobStorage::TEvPutResult> puts(env.Runtime, [&](const auto& ev) {
        return ev->Recipient == agentId;
    });
    const TString data = "test-data";
    env.Runtime.SendAsync(new IEventHandle(agentId, edge, new TEvBlobStorage::TEvPut(
        TLogoBlobID(12345, 1, 1, 0, data.size(), 0), data, TInstant::Max())));
    env.Runtime.WaitFor("storage Put completion", [&] { return !puts.empty(); }, TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(puts.front()->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(pipeId);

    // Hold the pipe handshake so registration and any later cleanup retain their original delivery order.
    TBlockEvents<TEvTabletPipe::TEvConnect> connects(env.Runtime, [&](const auto& ev) {
        return !holdRegistrationResult && ev->Get()->Record.GetTabletId() == TTestEnv::TabletId;
    });
    TBlockEvents<TEvBlobDepot::TEvRegisterAgentResult> registrationResults(env.Runtime, [&](const auto& ev) {
        return holdRegistrationResult && ev->Recipient == agentId;
    });
    TBlockEvents<TEvBlobDepot::TEvCommitBlobSeq> commits(env.Runtime, [&](const auto& ev) {
        return ev->Sender == agentId;
    });
    env.Runtime.ClosePipe(pipeId, agentId, 0);
    env.Runtime.WaitFor("agent reconnect", [&] {
        return holdRegistrationResult ? !registrationResults.empty() : !connects.empty();
    }, TDuration::Seconds(1));
    puts.Stop().Unblock();
    const auto result = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(edge, TDuration::Seconds(1));
    UNIT_ASSERT_C(result, "Put did not fail while registration was pending");
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
    UNIT_ASSERT_STRING_CONTAINS(result->Get()->ErrorReason, "disconnected during write");

    connects.Stop().Unblock();
    registrationResults.Stop().Unblock();
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    UNIT_ASSERT_C(commits.empty(), "Put queued a commit before registration completed");
    commits.Stop();

    // A new Put can still succeed after registration completes.
    env.Runtime.SendAsync(new IEventHandle(agentId, edge, new TEvBlobStorage::TEvPut(
        TLogoBlobID(12345, 1, 2, 0, data.size(), 0), data, TInstant::Max())));
    const auto nextResult = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(edge, TDuration::Seconds(1));
    UNIT_ASSERT(nextResult);
    UNIT_ASSERT_VALUES_EQUAL(nextResult->Get()->Status, NKikimrProto::OK);
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotAgentDisconnect) {
    Y_UNIT_TEST(ExpiredCommitBeforeGarbageCollection) {
        CheckExpiredCommit(true);
    }

    Y_UNIT_TEST(ExpiredCommitAfterGarbageCollection) {
        CheckExpiredCommit(false);
    }

    Y_UNIT_TEST(PutCompletesBeforeReconnectRegistration) {
        CheckPutCompletesDuringReconnect(false);
    }

    Y_UNIT_TEST(PutCompletesBeforeReconnectRegistrationResult) {
        CheckPutCompletesDuringReconnect(true);
    }

    Y_UNIT_TEST(ExpiredStepsResetAfterTabletReboot) {
        TTestEnv env(false);
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
        info->BlobDepotId = TTestEnv::TabletId;
        const auto agentId = env.Runtime.Register(CreateBlobDepotAgent(TTestEnv::VirtualGroupId, info, {}));
        env.Runtime.EnableScheduleForActor(agentId);
        const auto edge = env.Runtime.AllocateEdgeActor();

        TActorId pipeId;
        auto pipeObserver = env.Runtime.AddObserver<TEvTabletPipe::TEvClientConnected>(
            [&](TEvTabletPipe::TEvClientConnected::TPtr& ev) {
                if (ev->Recipient == agentId) {
                    pipeId = ev->Get()->ClientId;
                }
            });
        NKikimrBlobDepot::TEvRegisterAgentResult registration;
        ui32 registrationsReceived = 0;
        auto registrationObserver = env.Runtime.AddObserver<TEvBlobDepot::TEvRegisterAgentResult>(
            [&](TEvBlobDepot::TEvRegisterAgentResult::TPtr& ev) {
                if (ev->Recipient == agentId) {
                    registration = ev->Get()->Record;
                    ++registrationsReceived;
                }
            });
        TBlobSeqId writtenId;
        auto putObserver = env.Runtime.AddObserver<TEvBlobStorage::TEvPut>(
            [&](TEvBlobStorage::TEvPut::TPtr& ev) {
                if (ev->Sender == agentId) {
                    writtenId = TBlobSeqId::FromLogoBlobId(ev->Get()->Id);
                }
            });
        auto put = [&](ui32 step) {
            writtenId = {};
            const TString data = "test-data";
            env.Runtime.SendAsync(new IEventHandle(agentId, edge, new TEvBlobStorage::TEvPut(
                TLogoBlobID(12345, 1, step, 0, data.size(), 0), data, TInstant::Max())));
            const auto result = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(
                edge, TDuration::Seconds(1));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ErrorReason);
            UNIT_ASSERT(writtenId);
            return writtenId;
        };

        const auto initialId = put(1);
        UNIT_ASSERT(pipeId);
        TBlockEvents<TEvBlobDepot::TEvRegisterAgent> registrations(env.Runtime, [&](const auto& ev) {
            return ev->Sender == agentId;
        });
        env.Runtime.ClosePipe(pipeId, agentId, 0);
        env.Runtime.WaitFor("agent reconnect", [&] { return !registrations.empty(); }, TDuration::Seconds(1));
        env.Runtime.SimulateSleep(TDuration::Minutes(2));
        const ui32 registrationsBeforeReconnect = registrationsReceived;
        registrations.Stop().Unblock();
        env.Runtime.WaitFor("registration with expired steps", [&] {
            return registrationsReceived > registrationsBeforeReconnect;
        }, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(registration.GetGeneration(), initialId.Generation);
        UNIT_ASSERT_VALUES_EQUAL(registration.InvalidatedStepsSize(), 1);
        const auto& invalidated = registration.GetInvalidatedSteps(0);
        UNIT_ASSERT_VALUES_EQUAL(invalidated.GetChannel(), initialId.Channel);
        UNIT_ASSERT(invalidated.GetInvalidatedStep() >= initialId.Step);
        const auto afterExpiryId = put(2);
        UNIT_ASSERT_VALUES_EQUAL(afterExpiryId.Generation, initialId.Generation);
        UNIT_ASSERT(afterExpiryId.Step > invalidated.GetInvalidatedStep());

        RebootTablet(env.Runtime, TTestEnv::TabletId, env.Runtime.AllocateEdgeActor());
        env.Runtime.WaitFor("registration in the new generation", [&] {
            return registration.GetGeneration() > initialId.Generation;
        }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(registration.InvalidatedStepsSize(), 0);
        const auto afterRebootId = put(3);
        UNIT_ASSERT_VALUES_EQUAL(afterRebootId.Generation, registration.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(afterRebootId.Step, 1);
    }

    Y_UNIT_TEST(QueryBlocksRemainMonotonicAfterReboot) {
        CheckQueryBlocksCommit(true);
    }

    Y_UNIT_TEST(QueryBlocksWaitForCommit) {
        CheckQueryBlocksCommit(false);
    }

    Y_UNIT_TEST(DisconnectReleasesS3WriteSlots) {
        TTestEnv env;
        const auto owner = env.Connect(0);
        const auto waiter = env.Connect(1);
        env.Prepare(owner, 1);
        env.ExpectPrepared(owner, 1);
        env.Prepare(waiter, 2);
        env.ExpectPending(2);
        env.Disconnect(owner);
        env.ExpectPrepared(waiter, 2);
    }

    Y_UNIT_TEST(DisconnectDropsQueuedWritesBeforeReleasingSlots) {
        TTestEnv env;
        const auto owner = env.Connect(0);
        const auto waiter = env.Connect(1);
        env.Prepare(owner, 1);
        env.ExpectPrepared(owner, 1);
        env.Prepare(owner, 2);
        env.Prepare(waiter, 3);
        env.ExpectPending(2);
        env.ExpectPending(3);
        env.Disconnect(owner);
        env.ExpectPrepared(waiter, 3);
        UNIT_ASSERT(!env.PrepareResults.contains(2));
    }

    Y_UNIT_TEST(DisconnectDropsQueuedWritesWithoutAllocatedSlots) {
        TTestEnv env;
        const auto owner = env.Connect(0);
        const auto departed = env.Connect(1);
        const auto waiter = env.Connect(2);
        env.Prepare(owner, 1);
        env.ExpectPrepared(owner, 1);
        env.Prepare(departed, 2);
        env.Prepare(waiter, 3);
        env.ExpectPending(2);
        env.ExpectPending(3);
        env.Disconnect(departed);
        env.ExpectPending(3);
        env.Disconnect(owner);
        env.ExpectPrepared(waiter, 3);
        UNIT_ASSERT(!env.PrepareResults.contains(2));
    }

    Y_UNIT_TEST(ReconnectSameInstanceReleasesS3WriteSlots) {
        CheckReconnectReleasesWrites(1);
    }

    Y_UNIT_TEST(ReconnectNewInstanceReleasesS3WriteSlots) {
        CheckReconnectReleasesWrites(2);
    }

    Y_UNIT_TEST(SupersededInitialRegistrationIsDropped) {
        CheckSupersededInitialRegistration(false);
    }

    Y_UNIT_TEST(SupersededInitialRegistrationAfterReplacementDisconnectIsDropped) {
        CheckSupersededInitialRegistration(true);
    }

    Y_UNIT_TEST(ReconnectDeliversPendingBlockWithoutInvalidatedSteps) {
        TTestEnv env;
        constexpr ui64 tabletId = 12345;
        const auto lessee = env.Connect(0);
        const auto blocker = env.Connect(1);
        env.QueryBlocks(lessee, tabletId);
        env.Disconnect(lessee);
        env.Block(blocker, tabletId);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));

        const auto replacement = env.Connect(0);
        const auto push = env.Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvPushNotify>(
            replacement.Edge, TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(push, "pending block was not delivered to the reconnected agent");
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.InvalidatedStepsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.BlockedTabletsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.GetBlockedTablets(0).GetTabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(push->Get()->Record.GetBlockedTablets(0).GetBlockedGeneration(), 1);
        auto ack = std::make_unique<TEvBlobDepot::TEvPushNotifyResult>();
        ack->Record.SetId(push->Cookie);
        env.Send(replacement, ack.release());
        env.ExpectBlocked(blocker);
    }

    Y_UNIT_TEST(BlockCanBeReissuedAfterDisconnectedAgentTimeout) {
        TTestEnv env;
        constexpr ui64 tabletId = 12345;
        const auto lessee = env.Connect(0);
        const auto blocker = env.Connect(1);
        env.QueryBlocks(lessee, tabletId);
        env.Disconnect(lessee);

        env.Block(blocker, tabletId);
        env.ExpectBlocked(blocker);
        env.Block(blocker, tabletId);
        env.ExpectBlocked(blocker, NKikimrProto::ALREADY);
    }
}

} // namespace NKikimr::NBlobDepot
