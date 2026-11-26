#include "../blob_depot.h"
#include "../agent/agent.h"
#include "../agent/agent_impl.h"
#include "../config.h"
#include "../events.h"
#include "../s3_request_timer.h"
#include "../s3_router.h"
#include "../s3_router_events.h"
#include "../types.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <google/protobuf/unknown_field_set.h>
#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NBlobDepot {
namespace {

struct TAgent {
    TActorId Edge;
    TActorId Pipe;
    ui32 NodeIndex;
};

struct TLocalMode {};
struct TSyncMode {
    bool VirtualGroup = false;
    bool NamespaceValidated = true;
};
struct TTestEnv {
    static constexpr ui64 TabletId = 72075186224000000;
    static constexpr ui32 VirtualGroupId = 0x80000000;
    TTestBasicRuntime Runtime{3};
    ui32 DataGroupId = 0;
    NKikimrBlobDepot::TBlobDepotConfig Config;
    THashSet<ui64> PrepareResults;
    THashMap<TActorId, ui64> RequestIds;
    THashMap<std::pair<TActorId, ui64>, ui64> PrepareCookies;
    TTestActorRuntime::TEventObserverHolder Observer;

    explicit TTestEnv(std::variant<TLocalMode, TSyncMode> mode = TSyncMode{}) {
        SetupTabletServices(Runtime, nullptr, true);
        Runtime.GetAppData().Icb->CreateConfigControls(true);
        TControlBoard::SetValue(1, Runtime.GetAppData().Icb->BlobDepotControls.S3MaxWritesInFlight);

        // Only locator allocation is under test. Park external S3 requests in an edge actor;
        // no HTTP server, S3 credentials or successful object uploads are needed.
        const auto router = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeBlobDepotS3RouterID(TabletId), router);
        Observer = Runtime.AddObserver([this, router](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NStorage::TEvNodeWardenAcquireBlobDepotS3Router::EventType: {
                    const auto& msg = *ev->Get<NStorage::TEvNodeWardenAcquireBlobDepotS3Router>();
                    Runtime.SendAsync(new IEventHandle(ev->Sender, ev->Recipient,
                        new NStorage::TEvNodeWardenBlobDepotS3RouterReady(msg.TabletId, msg.Generation, router), 0, ev->Cookie));
                    ev.Reset();
                    break;
                }
                case NStorage::TEvNodeWardenReleaseBlobDepotS3Router::EventType:
                    ev.Reset();
                    break;
                case TEvBlobDepot::TEvPrepareWriteS3Result::EventType:
                    if (const auto it = PrepareCookies.find(std::make_pair(ev->Recipient, ev->Cookie)); it != PrepareCookies.end()) {
                        PrepareResults.insert(it->second);
                    }
                    break;
            }
        });

        auto* info = CreateTestTabletInfo(TabletId, TTabletTypes::BlobDepot);
        info->Channels.resize(3); // two system channels and one data channel, matching the config below
        DataGroupId = info->GroupFor(2, 1);
        const auto bootstrapper = CreateTestBootstrapper(Runtime, info, CreateBlobDepot);
        Runtime.EnableScheduleForActor(bootstrapper);
        Config.SetName("disconnect-test");
        auto* system = Config.AddChannelProfiles();
        system->SetCount(2);
        system->SetChannelKind(NKikimrBlobDepot::TChannelKind::System);
        Config.AddChannelProfiles()->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
        if (const auto* sync = std::get_if<TSyncMode>(&mode)) {
            if (sync->VirtualGroup) {
                Config.SetVirtualGroupId(VirtualGroupId);
            }
            Config.SetS3NamespaceValidated(sync->NamespaceValidated);
            auto* s3 = Config.MutableS3BackendSettings();
            s3->MutableSyncMode();
            s3->MutableSettings()->SetBucket("test-bucket");
        } else {
            Config.SetVirtualGroupId(VirtualGroupId);
        }
        ApplyConfig(1);
    }

    void ApplyConfig(ui64 txId) {
        const auto edge = Runtime.AllocateEdgeActor();
        auto request = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
        request->Record.SetTxId(txId);
        *request->Record.MutableConfig() = Config;
        Runtime.SendToPipe(TabletId, edge, request.release(), 0, GetPipeConfigWithRetries());
        const auto reply = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvApplyConfigResult>(edge, TDuration::Seconds(5));
        UNIT_ASSERT_C(reply, "BlobDepot did not acknowledge config transaction " << txId);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetTabletId(), TabletId);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetTxId(), txId);
    }

    TActorId StartAgent() {
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
        info->BlobDepotId = TabletId;
        const TActorId id = Runtime.Register(CreateBlobDepotAgent(VirtualGroupId, info, {}));
        Runtime.EnableScheduleForActor(id);
        return id;
    }

    TAgent Connect(ui32 nodeIndex, ui64 instanceId = 1, bool supportsIdRangeExpiry = false) {
        const auto edge = Runtime.AllocateEdgeActor(nodeIndex);
        const auto pipe = Runtime.ConnectToPipe(TabletId, edge, nodeIndex, GetPipeConfigWithRetries());
        TAgent agent{edge, pipe, nodeIndex};
        auto request = std::make_unique<TEvBlobDepot::TEvRegisterAgent>();
        request->Record.SetAgentInstanceId(instanceId);
        request->Record.SetSupportsIdRangeExpiry(supportsIdRangeExpiry);
        Send(agent, request.release());
        UNIT_ASSERT_C(Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvRegisterAgentResult>(edge, TDuration::Seconds(1)),
            "BlobDepot did not register the test agent");
        return agent;
    }

    ui64 Send(const TAgent& agent, IEventBase* event) {
        // BlobDepot requires consecutive request cookies starting at one on every pipe.
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

    TS3Locator ExpectPrepared(const TAgent& agent, ui64 cookie) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvPrepareWriteS3Result>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT_C(response, "S3 write queue did not resume after agent disconnect/reconnect");
        UNIT_ASSERT_VALUES_EQUAL(PrepareCookies.at(std::make_pair(agent.Edge, response->Cookie)), cookie);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.ItemsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(record.GetItems(0).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT(record.GetItems(0).HasS3Locator());
        return TS3Locator::FromProto(record.GetItems(0).GetS3Locator());
    }

    void SendQueryBlocks(const TAgent& agent, ui64 tabletId) {
        auto request = std::make_unique<TEvBlobDepot::TEvQueryBlocks>();
        request->Record.AddTabletIds(tabletId);
        Send(agent, request.release());
    }

    TActorId ExpectQueryBlocks(const TAgent& agent, ui32 expectedGeneration = 0) {
        const auto response = Runtime.GrabEdgeEventRethrow<TEvBlobDepot::TEvQueryBlocksResult>(
            agent.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.BlockedGenerationsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetBlockedGenerations(0), expectedGeneration);
        return response->Sender;
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
    // The old connection owns both an allocated locator and a queued request.
    env.Prepare(oldAgent, 2);
    env.Prepare(otherAgent, 3);
    env.ExpectPending(2);
    env.ExpectPending(3);

    // Register the replacement before the old pipe reports disconnection.
    const auto replacement = env.Connect(0, newInstanceId);
    env.ExpectPrepared(otherAgent, 3);
    UNIT_ASSERT(!env.PrepareResults.contains(2));

    // A late disconnect of the superseded pipe must not disconnect the replacement.
    env.Disconnect(oldAgent);
    env.Prepare(replacement, 4);
    env.ExpectPending(4);
    env.Disconnect(otherAgent);
    env.ExpectPrepared(replacement, 4);
}

void CheckSupersededInitialRegistration(bool disconnectReplacement) {
    TTestEnv env(TLocalMode{});
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
    TTestEnv env(TLocalMode{});
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
    TTestEnv env(TLocalMode{});
    const auto agentId = env.StartAgent();
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

struct TS3WriteEnv : TTestEnv {
    using TStorage = NWrappers::TEvExternalStorage;
    const TActorId Edge = Runtime.AllocateEdgeActor();
    const TActorId AgentId;
    TBlockEvents<TStorage::TEvPutObjectRequest> Writes{Runtime};

    TS3WriteEnv()
        : TTestEnv(TSyncMode{.VirtualGroup = true})
        , AgentId(StartAgent())
    {}

    void Put(TLogoBlobID id, TInstant deadline) {
        Runtime.SendAsync(new IEventHandle(AgentId, Edge, new TEvBlobStorage::TEvPut(id, "abc", deadline)));
    }

    void FinishPut() {
        Runtime.WaitFor("S3 PUT", [&] { return !Writes.empty(); }, TDuration::Seconds(1));
        auto put = std::move(Writes.front());
        Writes.pop_front();
        ReplyPut(put);
    }

    void ReplyPut(const TStorage::TEvPutObjectRequest::TPtr& put) {
        Runtime.SendAsync(new IEventHandle(put->Sender, Edge, new TStorage::TEvPutObjectResponse(
            TString(put->Get()->GetRequest().GetKey()), Aws::S3::Model::PutObjectResult{})));
    }

    void ExpectPut(TLogoBlobID id, NKikimrProto::EReplyStatus status) {
        const auto result = Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(Edge, TDuration::Seconds(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Id, id);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, status, result->Get()->ErrorReason);
    }
};

void CheckS3PutTimeout(bool dispatched) {
    TS3WriteEnv env;
    const TLogoBlobID expired{12345, 1, 1, 0, 3, 0};
    const TLogoBlobID next{12345, 1, 2, 0, 3, 0};
    TBlockEvents<TEvBlobDepot::TEvPrepareWriteS3Result> prepares(env.Runtime);
    TBlockEvents<TEvBlobDepot::TEvDiscardSpoiledBlobSeq> discards(env.Runtime);
    TBlockEvents<TEvBlobDepot::TEvCommitBlobSeq> expiredCommits(env.Runtime, [&](const auto& ev) {
        return ev->Get()->Record.GetItems(0).GetKey() == expired.AsBinaryString();
    });

    env.Put(expired, env.Runtime.GetCurrentTime() + TDuration::MilliSeconds(300));
    env.Runtime.WaitFor("locator allocation", [&] { return !prepares.empty(); }, TDuration::Seconds(1));
    const auto& item = prepares.front()->Get()->Record.GetItems(0);
    UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT(item.HasS3Locator());
    const auto locator = TS3Locator::FromProto(item.GetS3Locator());
    const TActorId pipe = prepares.front()->Sender;
    TS3WriteEnv::TStorage::TEvPutObjectRequest::TPtr latePut;
    if (dispatched) {
        prepares.Stop().Unblock();
        env.Runtime.WaitFor("physical S3 PUT", [&] { return !env.Writes.empty(); }, TDuration::Seconds(1));
        latePut = std::move(env.Writes.front());
        env.Writes.pop_front();
    }
    env.ExpectPut(expired, NKikimrProto::TIMEOUT);

    TBlockEvents<TEvBlobDepot::TEvPrepareWriteS3> nextPrepare(env.Runtime);
    env.Put(next, TInstant::Max());
    env.Runtime.WaitFor("next locator request", [&] { return !nextPrepare.empty(); }, TDuration::Seconds(1));
    UNIT_ASSERT(expiredCommits.empty());
    if (!dispatched) {
        // This request reached the pipe before the old prepare reply could trigger its discard.
        nextPrepare.Stop().Unblock();
        UNIT_ASSERT(discards.empty());
        prepares.Stop().Unblock();
    }

    env.Runtime.WaitFor("expired locator discard", [&] { return !discards.empty(); }, TDuration::Seconds(1));
    const auto& discard = discards.front();
    UNIT_ASSERT_VALUES_EQUAL(discard->Recipient, pipe);
    const auto& msg = discard->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(msg.S3LocatorsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg.GetS3PutMayArriveLate(), dispatched);
    UNIT_ASSERT_VALUES_EQUAL(TS3Locator::FromProto(msg.GetS3Locators(0)).ToString(), locator.ToString());
    UNIT_ASSERT(env.Writes.empty());
    discards.Unblock(1);
    if (dispatched) {
        // The timed-out PUT sent its discard before the next prepare. Preserve pipe message order.
        nextPrepare.Stop().Unblock();
    }
    env.FinishPut();
    env.ExpectPut(next, NKikimrProto::OK);
    if (latePut) {
        env.ReplyPut(latePut);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    }
    UNIT_ASSERT(expiredCommits.empty());
    UNIT_ASSERT(discards.empty());
}

struct TS3ReadEnv : TS3WriteEnv {
    const TLogoBlobID Id{12345, 1, 1, 0, 3, 0};
    TBlockEvents<TStorage::TEvGetObjectRequest> Reads{Runtime};

    TS3ReadEnv() {
        TControlBoard::SetValue(1, Runtime.GetAppData().Icb->BlobDepotControls.S3MaxGetsInFlight);
        Put(Id, TInstant::Max());
        FinishPut();
        ExpectPut(Id, NKikimrProto::OK);
    }

    void Get(TActorId agent, ui32 offset, TInstant deadline) {
        Runtime.SendAsync(new IEventHandle(agent, Edge, new TEvBlobStorage::TEvGet(Id, offset, 1, deadline,
            NKikimrBlobStorage::FastRead)));
    }

    void Reply(ui32 expectedOffset, bool slowDown = false) {
        Runtime.WaitFor("S3 range GET", [&] { return !Reads.empty(); }, TDuration::Seconds(1));
        auto request = std::move(Reads.front());
        Reads.pop_front();
        const auto& get = request->Get()->GetRequest();
        UNIT_ASSERT_VALUES_EQUAL(TString(get.GetRange()), TStringBuilder() << "bytes=" << expectedOffset << '-' << expectedOffset);
        const std::pair<ui64, ui64> range{expectedOffset, expectedOffset};
        if (slowDown) {
            Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
                Aws::S3::S3Errors::SLOW_DOWN, "SlowDown", "slow down", true));
            Runtime.SendAsync(new IEventHandle(request->Sender, Edge,
                new TStorage::TEvGetObjectResponse(TString(get.GetKey()), range, error)));
        } else {
            Aws::S3::Model::GetObjectResult result;
            Runtime.SendAsync(new IEventHandle(request->Sender, Edge, new TStorage::TEvGetObjectResponse(
                TString(get.GetKey()), range, std::move(result), TString(1, "abc"[expectedOffset]))));
        }
    }

    void ExpectRead(char value) {
        const auto result = Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(Edge, TDuration::Seconds(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), TString(1, value));
    }
};

void CheckExpiredS3Reads(bool slowDown) {
    TS3ReadEnv env;
    const auto pending = GetServiceCounters(env.Runtime.GetAppData().Counters, "blob_depot_agent")
        ->GetSubgroup("group", ToString(TTestEnv::VirtualGroupId))->GetSubgroup("subsystem", "s3")
        ->GetCounter("GetsPendingQueueSize", false);
    const auto deadline = env.Runtime.GetCurrentTime() + TDuration::MilliSeconds(300);
    env.Get(env.AgentId, 0, deadline);
    env.Runtime.WaitFor("in-flight S3 GET", [&] { return !env.Reads.empty(); }, TDuration::Seconds(1));
    env.Get(env.AgentId, 1, deadline);
    const auto checkEdge = env.Runtime.AllocateEdgeActor();
    env.Runtime.SendAsync(new IEventHandle(env.AgentId, checkEdge,
        new TEvBlobStorage::TEvCheckIntegrity(env.Id, deadline, NKikimrBlobStorage::FastRead)));
    env.Get(env.AgentId, 2, TInstant::Max());
    env.Runtime.WaitFor("three queued reads", [&] { return pending->Val() == 3; }, TDuration::MilliSeconds(200));

    for (ui32 i = 0; i < 2; ++i) {
        const auto result = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(env.Edge, TDuration::Seconds(2));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::TIMEOUT);
    }
    const auto integrity = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvCheckIntegrityResult>(checkEdge, TDuration::Seconds(1));
    UNIT_ASSERT(integrity);
    UNIT_ASSERT_VALUES_EQUAL(integrity->Get()->Status, NKikimrProto::TIMEOUT);
    env.Runtime.WaitFor("live S3 GET dispatched", [&] { return env.Reads.size() == 2; }, TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(pending->Val(), 0);

    env.Get(env.AgentId, 1, TInstant::Max());
    env.Runtime.WaitFor("one GET waiting for the live read", [&] { return pending->Val() == 1; }, TDuration::Seconds(1));
    env.Reply(0, slowDown);
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    UNIT_ASSERT_VALUES_EQUAL(env.Reads.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(pending->Val(), 1);
    env.Reply(2);
    env.ExpectRead('c');
    env.Reply(1);
    env.ExpectRead('b');
    env.Runtime.SimulateSleep(TDuration::Seconds(1));
    UNIT_ASSERT(env.Reads.empty());
    UNIT_ASSERT_VALUES_EQUAL(pending->Val(), 0);
}

void CheckGetCompletesAfterDisconnect(bool nodata) {
    TTestEnv env(TLocalMode{});
    const auto agentId = env.StartAgent();
    const auto edge = env.Runtime.AllocateEdgeActor();

    TActorId pipeId;
    auto pipeObserver = env.Runtime.AddObserver<TEvTabletPipe::TEvClientConnected>(
        [&](TEvTabletPipe::TEvClientConnected::TPtr& ev) {
            if (ev->Recipient == agentId) {
                pipeId = ev->Get()->ClientId;
            }
        });
    const TString data = "test-data";
    const TLogoBlobID id(12345, 1, 1, 0, data.size(), 0);
    env.Runtime.SendAsync(new IEventHandle(agentId, edge, new TEvBlobStorage::TEvPut(id, data, TInstant::Max())));
    const auto putResult = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(edge, TDuration::Seconds(1));
    UNIT_ASSERT(putResult);
    UNIT_ASSERT_VALUES_EQUAL_C(putResult->Get()->Status, NKikimrProto::OK, putResult->Get()->ErrorReason);
    UNIT_ASSERT(pipeId);

    // Hold the physical read issued by the agent to the storage proxy.
    TBlockEvents<TEvBlobStorage::TEvGetResult> reads(env.Runtime, [&](const auto& ev) {
        return ev->Recipient == agentId;
    });
    env.Runtime.SendAsync(new IEventHandle(agentId, edge, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
        NKikimrBlobStorage::EGetHandleClass::FastRead)));
    env.Runtime.WaitFor("physical read", [&] { return !reads.empty(); }, TDuration::Seconds(1));

    if (nodata) {
        // NODATA makes the agent resolve the key again; disconnect while this resolve is in flight.
        for (auto& ev : reads) {
            auto& msg = *ev->Get();
            for (ui32 i = 0; i < msg.ResponseSz; ++i) {
                msg.Responses[i].Status = NKikimrProto::NODATA;
                msg.Responses[i].Buffer = {};
            }
        }
        TBlockEvents<TEvBlobDepot::TEvResolve> resolves(env.Runtime, [&](const auto& ev) {
            return ev->Sender == agentId;
        });
        reads.Stop().Unblock();
        env.Runtime.WaitFor("resolve after NODATA", [&] { return !resolves.empty(); }, TDuration::Seconds(1));
        env.Runtime.ClosePipe(pipeId, agentId, 0);
    } else {
        // The physical read is not a tablet request: it survives the disconnect and completes normally.
        env.Runtime.ClosePipe(pipeId, agentId, 0);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        reads.Stop().Unblock();
    }

    const auto result = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(edge, TDuration::Seconds(1));
    UNIT_ASSERT_C(result, "Get did not complete after BlobDepot tablet disconnect");
    const auto& msg = *result->Get();
    UNIT_ASSERT_VALUES_EQUAL(msg.ResponseSz, 1);
    if (nodata) {
        UNIT_ASSERT_VALUES_EQUAL(msg.Responses[0].Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(msg.ErrorReason, "disconnected");
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(msg.Responses[0].Status, NKikimrProto::OK, msg.ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(msg.Responses[0].Buffer.ConvertToString(), data);
    }
}

struct TRegisteringAgentEnv : TTestEnv {
    const TActorId Agent = StartAgent();
    const TActorId Edge = Runtime.AllocateEdgeActor();
    TBlockEvents<TEvBlobDepot::TEvRegisterAgentResult> Registrations{Runtime, [this](const auto& ev) {
        return ev->Recipient == Agent;
    }};
    const NMonitoring::TDynamicCounters::TCounterPtr Queued =
        GetServiceCounters(Runtime.GetAppData().Counters, "blob_depot_agent")
            ->GetSubgroup("group", ToString(VirtualGroupId))->GetSubgroup("subsystem", "pendingEventQueue")
            ->GetCounter("Items", false);

    TRegisteringAgentEnv()
        : TTestEnv(TLocalMode{})
    {
        Runtime.WaitFor("held registration", [&] { return !Registrations.empty(); }, TDuration::Seconds(1));
    }

    void Queue(IEventBase* request) {
        Runtime.Send(new IEventHandle(Agent, Edge, request));
    }

    template<class TResult>
    void ExpectStatus(NKikimrProto::EReplyStatus status) {
        const auto result = Runtime.GrabEdgeEventRethrow<TResult>(Edge, TDuration::Seconds(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, status);
    }
};

void CheckQueuedMetadataDeadlines(bool reconnectAtDeadline) {
    TRegisteringAgentEnv env;
    constexpr ui64 tabletId = 12345;
    const auto deadline = env.Runtime.GetCurrentTime() + TDuration::MilliSeconds(100);
    bool mutationIssued = false;
    auto observer = env.Runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->Sender == env.Agent && (ev->GetTypeRewrite() == TEvBlobDepot::EvBlock ||
                ev->GetTypeRewrite() == TEvBlobDepot::EvCollectGarbage)) {
            mutationIssued = true;
        }
    });
    env.Queue(new TEvBlobStorage::TEvBlock(tabletId, 1, deadline));
    env.Queue(new TEvBlobStorage::TEvGetBlock(tabletId, deadline));
    env.Queue(new TEvBlobStorage::TEvCollectGarbage(tabletId, 1, 0, 0, true, 1, 4, nullptr, nullptr, deadline,
        false, false, true));
    env.Queue(new TEvBlobStorage::TEvStatus(deadline));
    UNIT_ASSERT_VALUES_EQUAL(env.Queued->Val(), 4);

    if (reconnectAtDeadline) {
        env.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
        // Deliver registration directly while the watchdog is still in the scheduler.
        env.Runtime.Send(env.Registrations.Stop().front().Release());
        env.Registrations.pop_front();
    }
    env.ExpectStatus<TEvBlobStorage::TEvBlockResult>(NKikimrProto::TIMEOUT);
    env.ExpectStatus<TEvBlobStorage::TEvGetBlockResult>(NKikimrProto::TIMEOUT);
    env.ExpectStatus<TEvBlobStorage::TEvCollectGarbageResult>(NKikimrProto::TIMEOUT);
    env.ExpectStatus<TEvBlobStorage::TEvStatusResult>(NKikimrProto::TIMEOUT);
    env.Registrations.Stop().Unblock();
    env.Queue(new TEvBlobStorage::TEvStatus(TInstant::Max()));
    env.ExpectStatus<TEvBlobStorage::TEvStatusResult>(NKikimrProto::OK);
    UNIT_ASSERT(!mutationIssued);
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotAgentDisconnect) {
    Y_UNIT_TEST(TimedOutS3PutDiscardsLatePreparation) {
        CheckS3PutTimeout(false);
    }

    Y_UNIT_TEST(TimedOutS3PutDiscardsAfterPhysicalReply) {
        CheckS3PutTimeout(true);
    }

    Y_UNIT_TEST(S3ReadReplyAfterAgentEjection) {
        TS3ReadEnv env;
        env.Get(env.AgentId, 0, TInstant::Max());
        env.Runtime.WaitFor("old agent's S3 GET", [&] { return !env.Reads.empty(); }, TDuration::Seconds(1));
        env.Runtime.SendAsync(new IEventHandle(env.AgentId, env.Edge, new TEvents::TEvPoison));
        env.Runtime.WaitFor("old agent destroyed", [&] { return !env.Runtime.FindActor(env.AgentId); }, TDuration::Seconds(1));

        const auto oldRead = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(env.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(oldRead);
        UNIT_ASSERT_VALUES_EQUAL(oldRead->Get()->Status, NKikimrProto::ERROR);

        const TActorId replacement = env.StartAgent();
        env.Get(replacement, 1, TInstant::Max());
        env.Runtime.WaitFor("replacement agent's S3 GET", [&] { return env.Reads.size() == 2; }, TDuration::Seconds(1));
        env.Reply(0);
        env.Reply(1);
        env.ExpectRead('b');
    }

    Y_UNIT_TEST(ExpiredS3ReadsLeaveQueueAndDoNotRetrySlowDown) {
        CheckExpiredS3Reads(true);
    }

    Y_UNIT_TEST(LateS3GetSuccessPreservesNextReadSlot) {
        CheckExpiredS3Reads(false);
    }

    Y_UNIT_TEST(FailedRangeCancelsOutstandingS3Get) {
        TS3WriteEnv env;
        const TLogoBlobID first(12345, 1, 1, 0, 3, 0);
        const TLogoBlobID last(12345, 1, 2, 0, 3, 0);
        for (const auto& id : {first, last}) {
            env.Put(id, TInstant::Max());
            env.FinishPut();
            env.ExpectPut(id, NKikimrProto::OK);
        }
        TBlockEvents<TS3WriteEnv::TStorage::TEvGetObjectRequest> reads(env.Runtime);
        env.Runtime.SendAsync(new IEventHandle(env.AgentId, env.Edge,
            new TEvBlobStorage::TEvRange(first.TabletID(), first, last, false, TInstant::Max())));
        env.Runtime.WaitFor("both S3 reads of the range", [&] { return reads.size() == 2; }, TDuration::Seconds(1));

        const auto failed = std::move(reads.front());
        reads.pop_front();
        const auto canContinue = reads.front()->Get()->GetRequest().GetContinueRequestHandler();
        UNIT_ASSERT(canContinue);
        UNIT_ASSERT(canContinue(nullptr));
        env.Runtime.SendAsync(new IEventHandle(failed->Sender, env.Edge, new TEvents::TEvUndelivered(
            TS3WriteEnv::TStorage::TEvGetObjectRequest::EventType, TEvents::TEvUndelivered::ReasonActorUnknown)));
        const auto range = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvRangeResult>(env.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(range);
        UNIT_ASSERT_VALUES_EQUAL(range->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(!canContinue(nullptr));
    }

    Y_UNIT_TEST(GetPhysicalReadCompletesAfterDisconnect) {
        CheckGetCompletesAfterDisconnect(false);
    }

    Y_UNIT_TEST(GetNodataResolveFailsOnDisconnect) {
        CheckGetCompletesAfterDisconnect(true);
    }

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

    Y_UNIT_TEST(MetadataRequestsExpireDuringRegistration) {
        CheckQueuedMetadataDeadlines(false);
    }

    Y_UNIT_TEST(RegistrationDoesNotDispatchExpiredMetadataRequests) {
        CheckQueuedMetadataDeadlines(true);
    }

    Y_UNIT_TEST(PendingDeadlineUsesClockSampleAtEnqueue) {
        const auto received = TMonotonic::Seconds(10);
        const auto wallAtReceipt = TInstant::Seconds(1000);
        const auto elapsed = TDuration::MilliSeconds(40);
        const auto queueLimit = received + TDuration::Seconds(60);
        const auto calculate = [&](TDuration callerTimeout) {
            return TPendingEventExpiration::Calculate(queueLimit, wallAtReceipt + callerTimeout,
                wallAtReceipt + elapsed, received + elapsed);
        };
        const auto shortRequest = calculate(TDuration::MilliSeconds(100));
        UNIT_ASSERT_EQUAL(shortRequest.Time, received + TDuration::MilliSeconds(100));
        UNIT_ASSERT(shortRequest.AtRequestDeadline);
        const auto expired = calculate(TDuration::MilliSeconds(20));
        UNIT_ASSERT_EQUAL(expired.Time, received + TDuration::MilliSeconds(20));
        UNIT_ASSERT(expired.AtRequestDeadline);
        const auto longRequest = calculate(TDuration::Seconds(60) + TDuration::MilliSeconds(20));
        UNIT_ASSERT_EQUAL(longRequest.Time, queueLimit);
        UNIT_ASSERT(!longRequest.AtRequestDeadline);
    }

    Y_UNIT_TEST(PendingDeadlineWatchdogYieldsBetweenBatches) {
        TRegisteringAgentEnv env;
        constexpr i64 count = 600;
        const auto deadline = env.Runtime.GetCurrentTime() + TDuration::MilliSeconds(100);
        for (i64 i = 0; i < count; ++i) {
            env.Queue(new TEvBlobStorage::TEvStatus(deadline));
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Queued->Val(), count);
        bool sawPartialBatch = false;
        auto observer = env.Runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == env.Agent && 0 < env.Queued->Val() && env.Queued->Val() < count) {
                sawPartialBatch = true;
            }
        });
        env.Runtime.WaitFor("queued requests to expire", [&] { return env.Queued->Val() == 0; }, TDuration::Seconds(1));
        UNIT_ASSERT(sawPartialBatch);
    }

    Y_UNIT_TEST(RequestDeadlinesWhileRegistrationIsPending) {
        TTestEnv env(TLocalMode{});
        const auto agent = env.StartAgent();
        TBlockEvents<TEvBlobDepot::TEvRegisterAgentResult> registrations(env.Runtime, [&](const auto& ev) {
            return ev->Recipient == agent;
        });
        env.Runtime.WaitFor("agent registration response", [&] { return !registrations.empty(); }, TDuration::Seconds(1));

        const auto edge = env.Runtime.AllocateEdgeActor();
        const TString data = "test-data";
        const TLogoBlobID first(12345, 1, 1, 0, data.size(), 0);
        const TLogoBlobID second(12345, 1, 2, 0, data.size(), 0);
        const auto start = env.Runtime.GetCurrentTime();
        const auto getDeadline = start + TDuration::MilliSeconds(1200);
        const auto putDeadline = start + TDuration::MilliSeconds(1600);
        env.Runtime.SendAsync(new IEventHandle(agent, edge, new TEvBlobStorage::TEvPut(first, data, TInstant::Max())));
        const auto longWrites = env.Runtime.AllocateEdgeActor();
        const TString large(7'500'000, 'a');
        for (ui32 step = 3; step < 7; ++step) {
            const TLogoBlobID id(12345, 1, step, 0, large.size(), 0);
            env.Runtime.SendAsync(new IEventHandle(agent, longWrites,
                new TEvBlobStorage::TEvPut(id, large, start + TDuration::Minutes(10))));
        }
        const auto probe = env.Runtime.AllocateEdgeActor();
        const TString probeData(2'100'000, 'b');
        const TLogoBlobID probeId(12345, 1, 7, 0, probeData.size(), 0);
        auto sendProbe = [&] {
            env.Runtime.SendAsync(new IEventHandle(agent, probe, new TEvBlobStorage::TEvPut(probeId, probeData, TInstant::Max())));
        };
        sendProbe();
        const auto overflow = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(probe, TDuration::Seconds(1));
        UNIT_ASSERT(overflow);
        UNIT_ASSERT_VALUES_EQUAL(overflow->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(overflow->Get()->ErrorReason, "pending event queue overflow");
        env.Runtime.SendAsync(new IEventHandle(agent, edge, new TEvBlobStorage::TEvPut(second, data, putDeadline)));
        env.Runtime.SendAsync(new IEventHandle(agent, edge, new TEvBlobStorage::TEvGet(first, 0, 0, getDeadline,
            NKikimrBlobStorage::EGetHandleClass::FastRead)));

        const auto get = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(edge, TDuration::MilliSeconds(1500));
        UNIT_ASSERT(get);
        UNIT_ASSERT_VALUES_EQUAL(get->Get()->Status, NKikimrProto::TIMEOUT);
        UNIT_ASSERT(env.Runtime.GetCurrentTime() >= getDeadline);
        const auto put = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(edge, TDuration::MilliSeconds(600));
        UNIT_ASSERT(put);
        UNIT_ASSERT_VALUES_EQUAL(put->Get()->Id, second);
        UNIT_ASSERT_VALUES_EQUAL_C(put->Get()->Status, NKikimrProto::TIMEOUT, put->Get()->ErrorReason);
        UNIT_ASSERT(env.Runtime.GetCurrentTime() >= putDeadline);

        const auto queueLimit = start + TDuration::Seconds(60);
        const auto unbounded = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(edge,
            queueLimit + TDuration::Seconds(1) - env.Runtime.GetCurrentTime());
        UNIT_ASSERT(unbounded);
        UNIT_ASSERT_VALUES_EQUAL(unbounded->Get()->Id, first);
        UNIT_ASSERT_VALUES_EQUAL(unbounded->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(unbounded->Get()->ErrorReason, "pending event queue timeout");
        for (ui32 step = 3; step < 7; ++step) {
            const auto result = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(longWrites, TDuration::Seconds(1));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Id.Step(), step);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_STRING_CONTAINS(result->Get()->ErrorReason, "pending event queue timeout");
        }
        UNIT_ASSERT(env.Runtime.GetCurrentTime() >= queueLimit && env.Runtime.GetCurrentTime() < queueLimit + TDuration::Seconds(1));
        sendProbe();
        registrations.Stop().Unblock();
        const auto admitted = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(probe, TDuration::Seconds(1));
        UNIT_ASSERT(admitted);
        UNIT_ASSERT_VALUES_EQUAL_C(admitted->Get()->Status, NKikimrProto::OK, admitted->Get()->ErrorReason);
    }

    Y_UNIT_TEST(ExpiredStepsResetAfterTabletReboot) {
        TTestEnv env(TLocalMode{});
        const auto agentId = env.StartAgent();
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

    Y_UNIT_TEST(S3TrashWaitsForValidatedNamespaceAcrossRestart) {
        using TStorage = NWrappers::TEvExternalStorage;
        TTestEnv env(TSyncMode{.NamespaceValidated = false});
        TControlBoard::SetValue(3, env.Runtime.GetAppData().Icb->BlobDepotControls.S3MaxDeletesInFlight);
        TBlockEvents<TStorage::TEvListObjectsRequest> lists(env.Runtime);
        TBlockEvents<TStorage::TEvDeleteObjectRequest> deletes(env.Runtime);
        TBlockEvents<TStorage::TEvDeleteObjectsRequest> batches(env.Runtime);
        const auto writer = env.Connect(1);
        const auto& settings = env.Config.GetS3BackendSettings().GetSettings();
        const TString prefix = MakeS3BasePath(settings, env.Config.GetName());
        THashSet<TString> expected;
        for (ui64 cookie = 1; cookie <= 3; ++cookie) {
            env.Prepare(writer, cookie);
            const auto locator = env.ExpectPrepared(writer, cookie);
            UNIT_ASSERT(expected.insert(locator.MakeObjectName(prefix)).second);
            auto discard = std::make_unique<TEvBlobDepot::TEvDiscardSpoiledBlobSeq>();
            locator.ToProto(discard->Record.AddS3Locators());
            env.Send(writer, discard.release());
        }
        env.QueryBlocks(writer, 12345);
        UNIT_ASSERT(lists.empty() && deletes.empty() && batches.empty());

        const auto edge = env.Runtime.AllocateEdgeActor();
        RebootTablet(env.Runtime, TTestEnv::TabletId, edge);
        const auto deadline = env.Runtime.GetCurrentTime() + TDuration::Seconds(5);
        for (;;) {
            env.Runtime.SendToPipe(TTestEnv::TabletId, edge, new TEvTablet::TEvGetCounters, 0, GetPipeConfigWithRetries());
            const auto reply = env.Runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(edge, TDuration::Seconds(1));
            UNIT_ASSERT(reply);
            const auto& counters = reply->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters();
            if (std::ranges::any_of(counters, [&](const auto& entry) {
                return entry.GetName() == "BlobDepot/TotalS3TrashObjects" && entry.GetValue() == expected.size();
            })) {
                break;
            }
            UNIT_ASSERT_C(env.Runtime.GetCurrentTime() < deadline, "the tablet did not reload its staged S3 trash");
            env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        env.QueryBlocks(env.Connect(1, 2), 12345);
        UNIT_ASSERT(lists.empty() && deletes.empty() && batches.empty());

        env.Config.SetS3NamespaceValidated(true);
        env.ApplyConfig(2);
        auto sent = [&] {
            THashSet<TString> keys;
            for (const auto& ev : deletes) {
                const auto& request = ev->Get()->GetRequest();
                UNIT_ASSERT_VALUES_EQUAL(TString(request.GetBucket()), settings.GetBucket());
                keys.insert(TString(request.GetKey()));
            }
            for (const auto& ev : batches) {
                const auto& request = ev->Get()->GetRequest();
                UNIT_ASSERT_VALUES_EQUAL(TString(request.GetBucket()), settings.GetBucket());
                for (const auto& object : request.GetDelete().GetObjects()) {
                    keys.insert(TString(object.GetKey()));
                }
            }
            return keys;
        };
        env.Runtime.WaitFor("deletion of staged S3 keys in the validated namespace",
            [&] { return sent().size() >= expected.size(); }, TDuration::Seconds(5));
        UNIT_ASSERT(sent() == expected);
    }

    Y_UNIT_TEST(IdenticalConfigAcknowledgesEachTransactionOnTheSamePipe) {
        TTestEnv env;
        const auto agent = env.Connect(1);
        env.SendQueryBlocks(agent, 12345);
        const auto server = env.ExpectQueryBlocks(agent);
        for (ui64 txId : {41, 42}) {
            env.ApplyConfig(txId);
            env.SendQueryBlocks(agent, 12345);
            UNIT_ASSERT_VALUES_EQUAL(env.ExpectQueryBlocks(agent), server);
        }
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

        // No IDs were allocated, so only BlockToDeliver can trigger the push on reconnect.
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

        // The block finishes via storage while the disconnected agent's lease is still valid.
        // Reissuing the same generation/issuer must not collide with a stale BlockToDeliver.
        env.Block(blocker, tabletId);
        env.ExpectBlocked(blocker);
        env.Block(blocker, tabletId);
        // The mock storage reports that the first request already installed this block.
        env.ExpectBlocked(blocker, NKikimrProto::ALREADY);
    }

#ifndef KIKIMR_DISABLE_S3_OPS
    Y_UNIT_TEST(DelayedDeleteUsesStorageRouteAndKeyScope) {
        using namespace NWrappers::NExternalStorage;
        TTestEnv env;
        const TString bucketName = "delete-route-scope";
        const TString key = "archive/vg/1/a/b/1";
        auto* storage = Singleton<TFakeExternalStorage>();
        auto& bucket = storage->MutableBucket(bucketName);
        NKikimrBlobDepot::TS3BackendSettings settings;
        settings.MutableSyncMode();
        settings.MutableSettings()->SetBucket(bucketName);
        settings.MutableSettings()->SetObjectKeyPattern("archive");
        settings.MutableSettings()->SetEndpoint("fake.fake");
        settings.MutableSettings()->SetSecretKey(storage->GetSecretKey());
        const auto router = env.Runtime.Register(CreateBlobDepotS3Router(settings, TTestEnv::TabletId));
        env.Runtime.EnableScheduleForActor(router);
        const auto edge = env.Runtime.AllocateEdgeActor();
        const auto sendDelete = [&](const NKikimrBlobDepot::TS3BackendSettings& source) {
            auto route = std::make_shared<const TString>(TS3DeleteContext::MakeRouteKey(source));
            auto context = std::make_shared<TS3DeleteContext>(env.Runtime.GetCurrentMonotonicTime() + TDuration::Seconds(10),
                std::move(route));
            auto request = Aws::S3::Model::DeleteObjectRequest().WithBucket(bucketName).WithKey(key);
            env.Runtime.SendAsync(new IEventHandle(router, edge, new TEvDeleteObjectRequest(request, std::move(context))));
            const auto reply = env.Runtime.GrabEdgeEventRethrow<TEvDeleteObjectResponse>(edge, TDuration::Seconds(1));
            UNIT_ASSERT(reply);
            return reply->Get()->IsSuccess();
        };

        auto source = settings;
        source.MutableSettings()->SetObjectKeyPattern("/archive//");
        source.MutableSettings()->SetAccessKey("rotated access key");
        source.MutableSettings()->SetSecretKey("rotated secret");
        source.MutableSettings()->SetRequestTimeoutMs(3000);
        source.SetMetricsPushIntervalMs(5000);
        bucket.PutObject(key, "collected");
        UNIT_ASSERT(sendDelete(source));
        UNIT_ASSERT(!bucket.GetObject(key));

        bucket.PutObject(key, "live");
        source.MutableSettings()->SetObjectKeyPattern("another-prefix");
        UNIT_ASSERT(!sendDelete(source));
        source = settings;
        source.GetReflection()->MutableUnknownFields(&source)->AddVarint(1234, 1);
        UNIT_ASSERT(!sendDelete(source));
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetObject(key).value(), "live");
        bucket.Remove(key);
    }

    Y_UNIT_TEST(DelayedTabletDeleteCannotReachReplacementS3Backend) {
        using namespace NWrappers::NExternalStorage;
        TTestEnv env;
        const auto owner = env.Connect(0);
        env.Prepare(owner, 1);
        env.ExpectPrepared(owner, 1);

        // Losing the owner of the staged locator registers a tablet deleter.
        TBlockEvents<TEvents::TEvBootstrap> bootstraps(env.Runtime, [&](const auto& ev) {
            return env.Runtime.FindActorName(ev->GetRecipientRewrite()).Contains("TDeleterActor");
        });
        env.Disconnect(owner);
        UNIT_ASSERT_VALUES_EQUAL(bootstraps.size(), 1);
        const auto deleter = bootstraps.front()->GetRecipientRewrite();
        const auto latestOriginalDeadline = env.Runtime.GetCurrentMonotonicTime() + TS3RequestTimer::Timeout;
        env.Runtime.AdvanceCurrentTime(TDuration::Seconds(30));

        // The new endpoint has the same bucket and a live object at the old key.
        const TString bucketName = "test-bucket";
        auto* storage = Singleton<TFakeExternalStorage>();
        auto& bucket = storage->MutableBucket(bucketName);
        NKikimrBlobDepot::TS3BackendSettings replacement;
        replacement.MutableSyncMode();
        replacement.MutableSettings()->SetEndpoint("fake.fake");
        replacement.MutableSettings()->SetBucket(bucketName);
        replacement.MutableSettings()->SetSecretKey(storage->GetSecretKey());
        const auto router = env.Runtime.Register(CreateBlobDepotS3Router(replacement, TTestEnv::TabletId));
        env.Runtime.EnableScheduleForActor(router);
        const auto service = MakeBlobDepotS3RouterID(TTestEnv::TabletId);
        const auto originalRouter = env.Runtime.GetLocalServiceId(service);
        UNIT_ASSERT(originalRouter && originalRouter != router);
        env.Runtime.RegisterService(service, router);

        TString key;
        auto requests = env.Runtime.AddObserver<TEvDeleteObjectRequest>([&](TEvDeleteObjectRequest::TPtr& ev) {
            if (ev->Sender == deleter && key.empty()) {
                const auto& request = *ev->Get();
                key = request.GetRequest().GetKey().c_str();
                bucket.PutObject(key, "replacement object");
                const auto context = std::dynamic_pointer_cast<TS3RequestDeadline>(request.GetRequestContext());
                UNIT_ASSERT(context);
                UNIT_ASSERT(context->Deadline <= latestOriginalDeadline);
                if (ev->Recipient == originalRouter) {
                    // A deleter bound to a concrete router still addresses the original backend.
                    // Present that same request to the replacement to exercise its route check.
                    env.Runtime.SendAsync(ev->Forward(router));
                    ev.Reset();
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, service);
                }
            }
        });
        TBlockEvents<TEvDeleteObjectResponse> responses(env.Runtime, [&](const auto& ev) {
            return ev->Recipient == deleter;
        });
        bootstraps.Stop().Unblock();
        env.Runtime.WaitFor("stale delete rejected", [&] { return !responses.empty(); }, TDuration::Seconds(1));
        const auto& response = *responses.front()->Get();
        UNIT_ASSERT(!response.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetError().GetMessage().c_str(), "different BlobDepot configuration");
        UNIT_ASSERT(!key.empty());
        UNIT_ASSERT_VALUES_EQUAL(bucket.GetObject(key).value(), "replacement object");
        bucket.Remove(key);
    }
#endif
}

} // namespace NKikimr::NBlobDepot
