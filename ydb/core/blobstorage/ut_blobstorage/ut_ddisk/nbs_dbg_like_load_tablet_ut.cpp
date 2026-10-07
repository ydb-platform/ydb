#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/load_test/events.h>
#include <ydb/core/load_test/service_actor.h>
#include <ydb/core/load_test/nbs_dbg_like_load.h>
#include <ydb/core/load_test/nbs_dbg_like_load_tablet.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/protos/load_test.pb.h>
#include <ydb/core/protos/tablet.pb.h>
#include <library/cpp/monlib/service/mon_service_http_request.h>

#include <algorithm>
#include <optional>

namespace {
struct TResultsHttpRequest : NMonitoring::IHttpRequest {
    TCgiParameters Params;
    THttpHeaders Headers;

    const char* GetURI() const override { return "/?mode=results"; }
    const char* GetPath() const override { return "/"; }
    const TCgiParameters& GetParams() const override { return Params; }
    const TCgiParameters& GetPostParams() const override { return Params; }
    TStringBuf GetPostContent() const override { return {}; }
    HTTP_METHOD GetMethod() const override { return HTTP_METHOD_GET; }
    const THttpHeaders& GetHeaders() const override { return Headers; }
    TString GetRemoteAddr() const override { return {}; }
};
}

Y_UNIT_TEST_SUITE(NbsDbgLikeLoadTablet) {

    // End-to-end Create -> Run -> Delete fixture against a real BSController +
    // DDisk pool + Hive-managed NbsLoadTablet running in TTestActorSystem.
    struct TFixture {
        TEnvironmentSetup Env;
        TActorId Edge;

        explicit TFixture(ui32 numDDiskGroups = 4, bool enableChecksums = true)
            : Env({
                .NodeCount = 8,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
                .ConfigPreprocessor = [enableChecksums](ui32, TNodeWardenConfig& cfg) {
                    NYdb::NBS::NProto::TDDiskConfig ddisk;
                    ddisk.SetEnableChecksums(enableChecksums);
                    cfg.DDiskConfig = ddisk;

                    NYdb::NBS::NProto::TPBufferConfig pb;
                    pb.SetMaxChunks(10);
                    pb.SetMaxInMemoryCache(128_MB);
                    pb.SetEnableChecksums(enableChecksums);
                    cfg.PBufferConfig = pb;
                },
                .SetupHive = true})
        {
            //Env.Runtime->SetLogPriority(NKikimrServices::BS_LOAD_TEST, NLog::PRI_TRACE);
            //Env.Runtime->SetLogPriority(NKikimrServices::BS_DDISK, NLog::PRI_TRACE);

            Env.CreateBoxAndPool();
            Env.Sim(TDuration::Seconds(30));

            DefineDDiskPool(numDDiskGroups);

            Edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
        }

        void DefineDDiskPool(ui32 numDDiskGroups) {
            NKikimrBlobStorage::TConfigRequest request;
            auto* cmd = request.AddCommand()->MutableDefineDDiskPool();
            cmd->SetBoxId(1);
            cmd->SetName("ddisk_pool");
            auto* g = cmd->MutableGeometry();
            g->SetRealmLevelBegin(10);
            g->SetRealmLevelEnd(20);
            g->SetDomainLevelBegin(10);
            g->SetDomainLevelEnd(40);
            g->SetNumFailRealms(1);
            g->SetNumFailDomainsPerFailRealm(5);
            g->SetNumVDisksPerFailDomain(1);
            cmd->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::ROT);
            cmd->SetNumDDiskGroups(numDDiskGroups);
            auto res = Env.Invoke(request);
            UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
        }

        TInstant Deadline(TDuration d) {
            return Env.Runtime->GetClock() + d;
        }

        // Asks Hive to create one NbsLoadTablet with the given OwnerIdx, waits
        // for Hive to acknowledge creation, returns the assigned TabletId.
        ui64 CreateNbsLoadTabletViaHive(ui64 ownerIdx) {
            const ui64 hiveId = MakeDefaultHiveID();
            const TActorId clientId = Env.Runtime->Register(
                NTabletPipe::CreateClient(Edge, hiveId,
                    NTabletPipe::TClientRetryPolicy::WithRetries()), Edge.NodeId());

            {
                auto resp = Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(
                    Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(60)));
                UNIT_ASSERT(resp);
                UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Status, NKikimrProto::OK);
            }

            Env.Runtime->WrapInActorContext(Edge, [&] {
                auto ev = std::make_unique<TEvHive::TEvCreateTablet>();
                auto& rec = ev->Record;
                rec.SetOwner(0xB1610AD);
                rec.SetOwnerIdx(ownerIdx);
                rec.SetTabletType(NKikimrTabletBase::TTabletTypes::NbsLoadTablet);
                rec.SetChannelsProfile(0);
                for (ui32 j = 0; j < 3; ++j) {
                    auto* ch = rec.AddBindedChannels();
                    ch->SetStoragePoolName(Env.StoragePoolName);
                }
                NTabletPipe::SendData(Edge, clientId, ev.release());
            });

            ui64 tabletId = 0;
            {
                auto resp = Env.WaitForEdgeActorEvent<TEvHive::TEvCreateTabletReply>(
                    Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(60)));
                UNIT_ASSERT(resp);
                UNIT_ASSERT_VALUES_EQUAL_C(resp->Get()->Record.GetStatus(), NKikimrProto::OK,
                    "Hive create failed: " << NKikimrProto::EReplyStatus_Name(resp->Get()->Record.GetStatus()));
                tabletId = resp->Get()->Record.GetTabletID();
            }
            {
                auto resp = Env.WaitForEdgeActorEvent<TEvHive::TEvTabletCreationResult>(
                    Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(60)));
                UNIT_ASSERT(resp);
                UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Record.GetStatus(), NKikimrProto::OK);
            }

            Env.Runtime->WrapInActorContext(Edge, [&] {
                NTabletPipe::CloseClient(TActivationContext::AsActorContext(), clientId);
            });
            return tabletId;
        }

        TActorId OpenTabletPipe(ui64 tabletId) {
            TActorId pipe = Env.Runtime->Register(
                NTabletPipe::CreateClient(Edge, tabletId,
                    NTabletPipe::TClientRetryPolicy::WithRetries()), Edge.NodeId());
            auto resp = Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(
                Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(60)));
            UNIT_ASSERT(resp);
            UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Status, NKikimrProto::OK);
            return pipe;
        }

        void ClosePipe(TActorId pipe) {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                NTabletPipe::CloseClient(TActivationContext::AsActorContext(), pipe);
            });
            // Drain the resulting TEvClientDestroyed so it doesn't fire on
            // Edge while a later WaitForEdgeActorEvent is sweeping the queue
            // on a different sender (would panic in the testactorsys edge
            // actor since it's not in capture mode).
            Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientDestroyed>(
                Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(10)));
        }

        static void AddWritePayload(
            TEvLoad::TEvNbsWrite& ev,
            TRope payload,
            bool enableChecksums = true)
        {
            if (enableChecksums) {
                for (const ui64 checksum : NDDisk::CalculatePayloadChecksums(payload)) {
                    ev.Record.AddChecksums(checksum);
                }
            }
            const ui32 payloadId = ev.AddPayload(std::move(payload));
            ev.Record.SetPayloadId(payloadId);
        }

        ENbsLoadTabletStatus TabletCreate(
            TActorId pipe, ui32 numDirectBlockGroups, ui64 bscTabletId = 1, ui32 numVChunks = 1)
        {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                auto ev = std::make_unique<TEvLoad::TEvNbsLoadTabletAllocateGroups>();
                auto& cfg = *ev->Record.MutableAllocConfig();
                cfg.SetTabletId(bscTabletId);
                cfg.SetDDiskPoolName("ddisk_pool");
                cfg.SetPersistentBufferDDiskPoolName("ddisk_pool");
                cfg.SetNumDirectBlockGroups(numDirectBlockGroups);
                cfg.SetTargetNumVChunks(numVChunks);
                cfg.SetVChunkSizeBytes(128_MB);
                NTabletPipe::SendData(Edge, pipe, ev.release());
            });
            auto resp = Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsLoadTabletAllocateGroupsResult>(
                Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            return resp->Get()->Record.GetStatus();
        }

        struct TRunResultInfo {
            bool FinishedReceived = false;
            ui64 DurationMs = 0;
            TString ErrorReason;
        };

        // Registers and runs a TNbsDbgLikeLoadActor against the given tablet,
        // waits for TEvLoadTestFinished, and returns the result.
        //
        // stopOnWritesDoneCount: if non-zero, the actor stops after that many
        // writes succeed (DurationSeconds is set to a large safety timeout).
        // If zero, the actor stops after 1 simulated second (duration-based).
        TRunResultInfo RunViaLoadActor(
            ui64 tabletId,
            ui64 tag = 1,
            ui32 numDirectBlockGroupsToUse = 0,
            bool enableChecksums = true,
            bool automation = false)
        {
            TEvLoadTestRequest::TNbsDbgLikeLoad cmd;
            cmd.SetRequireReady(automation);
            cmd.SetStartupTimeoutSeconds(1);
            cmd.SetNbsDbgLikeTabletId(tabletId);
            cmd.SetTag(tag);
            auto& wc = *cmd.MutableWorkloadConfig();
            wc.SetTag(tag);
            wc.SetDelayBeforeMeasurementsSeconds(0);
            wc.SetMaxInFlight(1);
            wc.SetReadWriteSizeKiB(4);
            wc.SetStopOnWritesDoneCount(1000);
            wc.SetDurationSeconds(1);
            if (numDirectBlockGroupsToUse != 0) {
                wc.SetNumDirectBlockGroupsToUse(numDirectBlockGroupsToUse);
            }
            auto& tcfg = *wc.MutableTabletConfig();
            tcfg.SetMaxInflightLsns(64);
            tcfg.SetPBufferReplyTimeoutMicroseconds(500000); // 500 ms - slack for sim
            tcfg.SetEnableChecksums(enableChecksums);

            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            Env.Runtime->Register(
                NNbsDbgLike::CreateNbsDbgLikeLoadActor(cmd, Edge, counters, tag),
                Edge.NodeId());

            TRunResultInfo info;
            auto resp = Env.WaitForEdgeActorEvent<TEvLoad::TEvLoadTestFinished>(
                Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(120)));
            if (resp) {
                info.FinishedReceived = true;
                info.ErrorReason = resp->Get()->ErrorReason;
                if (resp->Get()->Report) {
                    info.DurationMs = resp->Get()->Report->Duration.MilliSeconds();
                }
            }
            return info;
        }

        struct TMultiRunResultInfo {
            bool FinishedReceived = false;
            TString ErrorReason;
            NJson::TJsonValue JsonResult;
        };

        // Registers the multi-tablet coordinator (TNbsDbgLikeMultiLoadActor via
        // CreateNbsDbgLikeLoadActor with Targets) against several tablets. Each
        // target uses NodeId=0 so the coordinator runs the per-tablet child
        // proxy locally (no load-service dependency in the test env). Waits for
        // the single combined TEvLoadTestFinished and returns it.
        TMultiRunResultInfo RunMultiViaLoadActor(
            const TVector<ui64>& tabletIds, ui64 tag = 100,
            ui64 stopOnWritesDoneCount = 50)
        {
            TEvLoadTestRequest::TNbsDbgLikeLoad cmd;
            cmd.SetTag(tag);
            auto& wc = *cmd.MutableWorkloadConfig();
            wc.SetTag(tag);
            wc.SetDelayBeforeMeasurementsSeconds(0);
            wc.SetMaxInFlight(1);
            wc.SetReadWriteSizeKiB(4);
            // Keep the per-tablet work tiny: each child runs several real load
            // workers against a real tablet, and the test actor system has heavy
            // per-event overhead. A small stop count makes every child finish
            // quickly so the coordinator can merge and report.
            wc.SetStopOnWritesDoneCount(stopOnWritesDoneCount);
            wc.SetDurationSeconds(1);
            auto& tcfg = *wc.MutableTabletConfig();
            tcfg.SetMaxInflightLsns(64);
            tcfg.SetPBufferReplyTimeoutMicroseconds(500000);

            for (ui64 tid : tabletIds) {
                auto* t = cmd.AddTargets();
                t->SetTabletId(tid);
                t->SetNodeId(0); // 0 => coordinator runs the child locally
            }

            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            Env.Runtime->Register(
                NNbsDbgLike::CreateNbsDbgLikeLoadActor(cmd, Edge, counters, tag),
                Edge.NodeId());

            TMultiRunResultInfo info;
            auto resp = Env.WaitForEdgeActorEvent<TEvLoad::TEvLoadTestFinished>(
                Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(180)));
            if (resp) {
                info.FinishedReceived = true;
                info.ErrorReason = resp->Get()->ErrorReason;
                info.JsonResult = resp->Get()->JsonResult;
            }
            return info;
        }

        // Returns a copy of the GetSummaryResult proto record.
        NKikimr::TEvNbsLoadTabletGetSummaryResult TabletGetSummary(TActorId pipe) {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                auto ev = std::make_unique<TEvLoad::TEvNbsLoadTabletGetSummary>();
                NTabletPipe::SendData(Edge, pipe, ev.release());
            });
            auto resp = Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsLoadTabletGetSummaryResult>(
                Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(30)));
            UNIT_ASSERT(resp);
            return resp->Get()->Record;
        }

        ENbsLoadTabletStatus TabletDelete(TActorId pipe) {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                auto ev = std::make_unique<TEvLoad::TEvNbsLoadTabletDelete>();
                NTabletPipe::SendData(Edge, pipe, ev.release());
            });
            auto resp = Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsLoadTabletDeleteResult>(
                Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            return resp->Get()->Record.GetStatus();
        }

        // Kill the tablet leader so Hive re-boots it. Pattern lifted from
        // RebootBlobDepotTablet in ../blob_depot.cpp.
        void RebootTablet(ui64 tabletId) {
            auto& runtime = *Env.Runtime;
            const TActorId sender = runtime.AllocateEdgeActor(1);
            auto* poison = new NActors::TEvents::TEvPoison();
            auto* nested = new IEventHandle(TActorId(), sender, poison);
            runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
                new TEvTabletResolver::TEvForward(tabletId, nested, {},
                    TEvTabletResolver::TEvForward::EActor::Tablet)),
                sender.NodeId());
            {
                auto fwd = Env.WaitForEdgeActorEvent<TEvTabletResolver::TEvForwardResult>(
                    sender, /*termOnCapture=*/false);
                UNIT_ASSERT(fwd);
                UNIT_ASSERT_VALUES_EQUAL_C(fwd->Get()->Status, NKikimrProto::OK,
                    fwd->Get()->ToString());
            }
            Env.Sim(TDuration::Seconds(5));
            runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
                new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())),
                sender.NodeId());
            Env.Sim(TDuration::Seconds(5));
            runtime.DestroyActor(sender);
            Env.Sim(TDuration::Seconds(5));
        }
    };

    void CheckPbWriteQuorumLoss(TStringBuf firstErrorReason) {
        using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);
        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_OK);
        f.Env.Sim(TDuration::Seconds(5));

        constexpr ui32 blockSize = 4096;
        constexpr ui64 requestCookie = 0x1234;
        constexpr ui64 configurationId = 1;
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            auto ev = std::make_unique<TEvLoad::TEvConfigureTablet>();
            auto& cfg = ev->Record;
            cfg.SetConfigurationId(configurationId);
            cfg.SetMaxInflightLsns(4);
            cfg.SetFlushBatchSize(1);
            cfg.SetEraseBatchSize(1);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(1);
            cfg.SetIoSizeBytes(blockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });
        auto configured = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
            f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(configured);
        UNIT_ASSERT_VALUES_EQUAL(configured->Get()->Record.GetConfigurationId(), configurationId);
        UNIT_ASSERT_C(configured->Get()->Record.GetSuccess(), configured->Get()->Record.GetError());

        TString firstPeer;
        ui32 injectedReplies = 0;
        std::unique_ptr<IEventHandle> lateReply;
        auto previousFilter = std::move(f.Env.Runtime->FilterFunction);
        f.Env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType) {
                auto& record = event->Get<NDDisk::TEvWritePersistentBuffersResult>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 3);
                for (const auto& sub : record.GetResult()) {
                    UNIT_ASSERT_C(sub.GetResult().GetStatus() == TStatus::OK, sub.DebugString());
                }

                auto original = std::make_unique<NDDisk::TEvWritePersistentBuffersResult>();
                original->Record = record;
                lateReply.reset(new IEventHandle(event->GetRecipientRewrite(), event->Sender,
                    original.release(), 0, event->Cookie));

                // Mutate the aggregate in place to preserve its LSN cookie and
                // the peer identities used by the tablet's quorum calculation.
                const auto& id = record.GetResult(0).GetPersistentBufferId();
                firstPeer = TStringBuilder() << id.GetNodeId() << ":" << id.GetPDiskId()
                    << ":" << id.GetDDiskSlotId();
                auto* first = record.MutableResult(0)->MutableResult();
                first->SetStatus(TStatus::ERROR);
                first->SetErrorReason(TString(firstErrorReason));
                auto* second = record.MutableResult(1)->MutableResult();
                second->SetStatus(TStatus::OVERFILL);
                second->SetErrorReason("later PB failure");
                ++injectedReplies;
            }
            return previousFilter ? previousFilter(node, event) : true;
        };

        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            auto ev = std::make_unique<TEvLoad::TEvNbsWrite>(/*address=*/0, blockSize);
            TFixture::AddWritePayload(*ev, TRope(TString(blockSize, 'x')));
            NTabletPipe::SendData(f.Edge, pipe, ev.release(), requestCookie);
        });
        auto reply = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsWriteResult>(
            f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(30)));
        f.Env.Runtime->FilterFunction = std::move(previousFilter);

        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(injectedReplies, 1);
        UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, requestCookie);
        UNIT_ASSERT_C(reply->Get()->Record.GetStatus() == NBSIO_QUORUM_LOST,
            reply->Get()->Record.DebugString());
        const auto& reason = reply->Get()->Record.GetReason();
        UNIT_ASSERT_STRING_CONTAINS(reason, "confirmed# 1");
        UNIT_ASSERT_STRING_CONTAINS(reason, "need# 3");
        UNIT_ASSERT_STRING_CONTAINS(reason, "PB");
        UNIT_ASSERT_STRING_CONTAINS(reason, firstPeer);
        UNIT_ASSERT_STRING_CONTAINS(reason, "ERROR");
        if (firstErrorReason) {
            UNIT_ASSERT_STRING_CONTAINS(reason, firstErrorReason);
        } else {
            UNIT_ASSERT_C(!reason.Contains("ERROR:"), reason);
        }
        UNIT_ASSERT_C(!reason.Contains("OVERFILL"), reason);
        UNIT_ASSERT_C(!reason.Contains("later PB failure"), reason);

        UNIT_ASSERT(lateReply);
        const ui32 replyNode = lateReply->Sender.NodeId();
        f.Env.Runtime->Send(lateReply.release(), replyNode);
        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    Y_UNIT_TEST(PbWriteQuorumLossIncludesFirstFailure) {
        CheckPbWriteQuorumLoss("first PB failure");
    }

    Y_UNIT_TEST(PbWriteQuorumLossWithoutErrorReason) {
        CheckPbWriteQuorumLoss("");
    }

    struct TIoFixture : TFixture {
        ui64 TabletId;
        TActorId Pipe;
        ui32 NumDbgs;
        ui32 IoSize = 4096;
        std::set<TActorId> Workers;
        std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> PreviousFilter;
        std::function<bool(IEventHandle&)> Hold;
        std::function<void(IEventHandle&)> Observe;
        std::vector<std::unique_ptr<IEventHandle>> Held;
        std::map<const IEventHandle*, ui32> HeldNodes;

        explicit TIoFixture(ui32 numDbgs = 1, ui32 numVChunks = 1)
            : TFixture(numDbgs > 1 ? 1 : 4)
            , TabletId(CreateNbsLoadTabletViaHive(1))
            , Pipe(OpenTabletPipe(TabletId))
            , NumDbgs(numDbgs)
        {
            UNIT_ASSERT_VALUES_EQUAL(TabletCreate(Pipe, numDbgs, 1, numVChunks), NBSLT_OK);
            Env.Sim(TDuration::Seconds(5));
            Configure();
            PreviousFilter = std::move(Env.Runtime->FilterFunction);
            Env.Runtime->FilterFunction = [this](ui32 node, std::unique_ptr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == NDDisk::TEvWritePersistentBuffers::EventType) {
                    Workers.insert(event->Sender);
                }
                if (Observe) {
                    Observe(*event);
                }
                if (Hold && Hold(*event)) {
                    HeldNodes.emplace(event.get(), node);
                    Held.push_back(std::move(event));
                    return false;
                }
                return PreviousFilter ? PreviousFilter(node, event) : true;
            };
        }

        ~TIoFixture() {
            Env.Runtime->FilterFunction = std::move(PreviousFilter);
        }

        void SendConfiguration(ui64 id = 1, ui32 gate = 1, ui32 ioSize = 4096,
            ui32 flushBatch = 16, ui32 eraseBatch = 16, ui32 cap = 2000, bool disableReplication = false)
        {
            IoSize = ioSize;
            Env.Runtime->WrapInActorContext(Edge, [&] {
                auto event = std::make_unique<TEvLoad::TEvConfigureTablet>();
                auto& cfg = event->Record;
                cfg.SetConfigurationId(id);
                cfg.SetMaxInflightLsns(cap);
                cfg.SetDisableReplication(disableReplication);
                cfg.SetFlushBatchSize(flushBatch);
                cfg.SetEraseBatchSize(eraseBatch);
                cfg.SetSyncRequestsBatchSize(gate);
                cfg.SetPBufferReplyTimeoutMicroseconds(500000);
                cfg.SetNumDirectBlockGroupsToUse(NumDbgs);
                cfg.SetIoSizeBytes(ioSize);
                NTabletPipe::SendData(Edge, Pipe, event.release(), id);
            });
        }

        void WaitConfigured(ui64 id) {
            auto reply = Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
                Edge, false, Deadline(TDuration::Seconds(30)));
            UNIT_ASSERT(reply);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetConfigurationId(), id);
            UNIT_ASSERT_C(reply->Get()->Record.GetSuccess(), reply->Get()->Record.GetError());
        }

        void Configure(ui64 id = 1, ui32 gate = 1, ui32 ioSize = 4096,
            ui32 flushBatch = 16, ui32 eraseBatch = 16, ui32 cap = 2000, bool disableReplication = false)
        {
            SendConfiguration(id, gate, ioSize, flushBatch, eraseBatch, cap, disableReplication);
            WaitConfigured(id);
        }

        void SendWrite(ui64 address, char value, ui64 cookie = 1) {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                auto event = std::make_unique<TEvLoad::TEvNbsWrite>(address, IoSize);
                AddWritePayload(*event, TRope(TString(IoSize, value)));
                NTabletPipe::SendData(Edge, Pipe, event.release(), cookie);
            });
        }

        void WaitWrite(ui64 cookie = 1, ENbsIoResultStatus status = NBSIO_OK) {
            auto reply = Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsWriteResult>(
                Edge, false, Deadline(TDuration::Seconds(30)));
            UNIT_ASSERT(reply);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, cookie);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reply->Get()->Record.GetStatus()), static_cast<int>(status));
        }

        void Write(ui64 address, char value, ui64 cookie = 1) {
            SendWrite(address, value, cookie);
            WaitWrite(cookie);
        }

        void SendRead(ui64 address, ui64 cookie = 1) {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                NTabletPipe::SendData(Edge, Pipe, new TEvLoad::TEvNbsRead(address, IoSize), cookie);
            });
        }

        void WaitRead(char value, ui64 cookie = 1) {
            auto reply = Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsReadResult>(
                Edge, false, Deadline(TDuration::Seconds(30)));
            UNIT_ASSERT(reply);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, cookie);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reply->Get()->Record.GetStatus()), static_cast<int>(NBSIO_OK));
            const auto& record = reply->Get()->Record;
            UNIT_ASSERT(record.HasPayloadId());
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->GetPayload(record.GetPayloadId()).ConvertToString(),
                TString(IoSize, value));
        }

        void SendDelete() {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                NTabletPipe::SendData(Edge, Pipe, new TEvLoad::TEvNbsLoadTabletDelete());
            });
        }

        void WaitDeleted() {
            auto reply = Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsLoadTabletDeleteResult>(
                Edge, false, Deadline(TDuration::Seconds(30)));
            UNIT_ASSERT(reply);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NBSLT_OK);
        }

        ui64 GateBlocked(const TString& name) {
            UNIT_ASSERT(!Workers.empty());
            auto counters = Env.Runtime->GetNode(Workers.begin()->NodeId())->AppData->Counters;
            return GetServiceCounters(counters, "load_actor")->GetSubgroup("load", "tablet")
                ->GetSubgroup("subsystem", "lsns")->GetCounter(name, true)->Val();
        }

        void Resume(std::unique_ptr<IEventHandle> event) {
            const auto node = HeldNodes.at(event.get());
            HeldNodes.erase(event.get());
            Env.Runtime->Schedule(Env.Runtime->GetClock(), event.release(), nullptr, node);
        }

        void ReleaseHeld() {
            auto held = std::move(Held);
            Held.clear();
            for (auto& event : held) {
                Resume(std::move(event));
            }
        }

        template <typename TPred>
        size_t ReleaseWhere(TPred predicate) {
            std::vector<std::unique_ptr<IEventHandle>> keep;
            std::vector<std::unique_ptr<IEventHandle>> chosen;
            keep.reserve(Held.size());
            for (auto& event : Held) {
                if (event && predicate(*event)) {
                    chosen.push_back(std::move(event));
                } else {
                    keep.push_back(std::move(event));
                }
            }
            Held.swap(keep);
            for (auto& event : chosen) {
                Resume(std::move(event));
            }
            return chosen.size();
        }
    };

    Y_UNIT_TEST(HeldSyncAllowsPBWritesAndReadsAndOrdersSuccessors) {
        TIoFixture f;
        std::vector<ui64> synced;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() == NDDisk::TEvSync::EventType) {
                for (const auto& source : event.Get<NDDisk::TEvSync>()->Record.GetSources()) {
                    for (const auto& segment : source.GetSegments()) {
                        synced.push_back(segment.GetPersistentBufferSegment().GetLsn());
                    }
                }
            }
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 3);
        f.Write(0, 'b', 2);
        f.SendRead(0);
        f.WaitRead('b');
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 3);
        f.Write(4096, 'c', 3);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 6);
        f.Hold = {};
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 9);
        UNIT_ASSERT_VALUES_EQUAL(synced.back(), 2);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(PBReadPinsEraseWhileNextSyncContinues) {
        TIoFixture f;
        ui32 syncs = 0;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        const TActorId worker = f.Held.front()->GetRecipientRewrite();
        f.Hold = [worker](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvReadPersistentBuffer::EventType
                && event.Sender == worker;
        };
        f.SendRead(0);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 4);
        auto pbRead = std::move(f.Held.back());
        f.Held.pop_back();
        f.ReleaseHeld();
        f.Write(0, 'b', 2);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 6);
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        // A wrong sender with the right cookie must not unpin the old version.
        f.Env.Runtime->Send(new IEventHandle(pbRead->Sender, f.Edge,
            new NDDisk::TEvReadPersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR),
            0, pbRead->Cookie), f.Edge.NodeId());
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        f.Hold = {};
        f.Resume(std::move(pbRead));
        f.WaitRead('a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 6);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(DDiskReadPinsNextSyncWithoutBlockingPBWrite) {
        TIoFixture f;
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        ui32 syncs = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvReadResult::EventType;
        };
        f.SendRead(0);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 1);
        f.Write(0, 'b', 2);
        f.SendRead(0, 2);
        f.WaitRead('b', 2);
        UNIT_ASSERT_VALUES_EQUAL(syncs, 0);
        const auto& held = f.Held.front();
        f.Env.Runtime->Send(new IEventHandle(held->GetRecipientRewrite(), held->Sender,
            new NDDisk::TEvReadPersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR),
            0, held->Cookie), held->Sender.NodeId());
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 0);
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitRead('a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 3);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(ReorderedPBRepliesPreserveVisibilityAndFlushOrder) {
        TIoFixture f;
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType
                && event.Cookie == 1;
        };
        f.SendWrite(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        f.Write(0, 'b', 2);
        f.SendRead(0);
        f.WaitRead('b');
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitWrite();
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(PendingOverwriteReadsPreviousAcknowledgedPBVersion) {
        TIoFixture f;
        f.Configure(2, 100);
        f.Write(0, 'a');
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType;
        };
        f.SendWrite(0, 'b', 2);
        f.Env.Sim(TDuration::MilliSeconds(50));
        f.SendRead(0);
        f.WaitRead('a');
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitWrite(2);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(SharedDDiskVChunksKeepDifferentDBGData) {
        TIoFixture f(2, 2);
        constexpr ui64 chunk = 128_MB;
        for (ui32 dbg = 0; dbg != 2; ++dbg) {
            for (ui32 v = 0; v != 2; ++v) {
                f.Write(dbg * (2 * chunk) + v * chunk, 'a' + dbg * 2 + v);
            }
        }
        f.Env.Sim(TDuration::MilliSeconds(50));
        ui32 pbReads = 0;
        f.Observe = [&](IEventHandle& event) {
            pbReads += event.GetTypeRewrite() == NDDisk::TEvReadPersistentBuffer::EventType;
        };
        for (ui32 dbg = 0; dbg != 2; ++dbg) {
            for (ui32 v = 0; v != 2; ++v) {
                f.SendRead(dbg * (2 * chunk) + v * chunk);
                f.WaitRead('a' + dbg * 2 + v);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(pbReads, 0);
    }

    Y_UNIT_TEST(MalformedAndWrongSenderSyncRepliesKeepReservations) {
        TIoFixture f;
        ui32 syncs = 0;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 3);
        f.Write(0, 'b', 2);
        const auto& held = f.Held.front();
        auto malformed = std::make_unique<NDDisk::TEvSyncResult>();
        malformed->Record.SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        f.Env.Runtime->Send(new IEventHandle(held->GetRecipientRewrite(), held->Sender,
            malformed.release(), 0, held->Cookie), held->Sender.NodeId());
        auto stray = std::make_unique<NDDisk::TEvSyncResult>();
        stray->Record = held->Get<NDDisk::TEvSyncResult>()->Record;
        f.Env.Runtime->Send(new IEventHandle(held->GetRecipientRewrite(), f.Edge,
            stray.release(), 0, held->Cookie), f.Edge.NodeId());
        // Hold only the genuine saved replies, allowing the injected replies
        // to reach the handler and exercise its validation.
        f.Hold = {};
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 3);
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 6);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(SyncFailureRetainsAllDestinationsUntilRetryCompletes) {
        TIoFixture f;
        std::vector<ui64> synced;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() == NDDisk::TEvSync::EventType) {
                synced.push_back(event.Get<NDDisk::TEvSync>()->Record.GetSources(0)
                    .GetSegments(0).GetPersistentBufferSegment().GetLsn());
            }
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        f.Write(0, 'b', 2);
        auto failed = std::move(f.Held.back());
        f.Held.pop_back();
        auto& record = failed->Get<NDDisk::TEvSyncResult>()->Record;
        record.SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR);
        record.MutableSegmentResults(0)->SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED);
        const ui64 failedCookie = failed->Cookie;
        f.Hold = [failedCookie](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType
                && event.Cookie != failedCookie;
        };
        f.Resume(std::move(failed));
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(synced.back(), 1);
        f.Hold = {};
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 7);
        UNIT_ASSERT_VALUES_EQUAL(synced.back(), 2);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(OlderEraseFailureBlocksNewerEraseButAllowsSync) {
        TIoFixture f;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvErasePersistentBufferResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 3);
        f.Write(0, 'b', 2);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 3);
        auto failed = std::move(f.Held.back());
        f.Held.pop_back();
        failed->Get<NDDisk::TEvErasePersistentBufferResult>()->Record.SetStatus(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED);
        const ui64 failedCookie = failed->Cookie;
        f.Hold = [failedCookie](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvErasePersistentBufferResult::EventType
                && event.Cookie != failedCookie;
        };
        f.Resume(std::move(failed));
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 4);
        f.Hold = {};
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 7);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(PartialDuplicateAndLatePBRepliesCleanFailedWriteOnce) {
        TIoFixture f;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType;
        };
        f.SendWrite(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 1);
        const auto& held = f.Held.front();
        const auto original = held->Get<NDDisk::TEvWritePersistentBuffersResult>()->Record;
        UNIT_ASSERT_VALUES_EQUAL(original.ResultSize(), 3);
        f.Hold = {};
        auto replyPart = [&](ui32 index, NKikimrBlobStorage::NDDisk::TReplyStatus::E status) {
            auto reply = std::make_unique<NDDisk::TEvWritePersistentBuffersResult>();
            *reply->Record.AddResult() = original.GetResult(index);
            reply->Record.MutableResult(0)->MutableResult()->SetStatus(status);
            f.Env.Runtime->Send(new IEventHandle(held->GetRecipientRewrite(), held->Sender,
                reply.release(), 0, held->Cookie), held->Sender.NodeId());
        };
        replyPart(0, NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        replyPart(0, NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        replyPart(1, NKikimrBlobStorage::NDDisk::TReplyStatus::OVERFILL);
        f.WaitWrite(1, NBSIO_QUORUM_LOST);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        replyPart(2, NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 2);
        // The held aggregate is now stale and must neither acknowledge the
        // client again nor recreate the erased LSN.
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 2);
    }

    Y_UNIT_TEST(ReconfigureFlushesBelowGatesBeforeChangingIoGeometry) {
        TIoFixture f;
        f.Configure(2, 100);
        ui32 syncs = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
        };
        f.Write(0, 'a');
        f.Write(4096, 'b', 2);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 0);
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.SendConfiguration(3, 100, 8192);
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 3);
        f.SendWrite(0, 'c', 3);
        f.WaitWrite(3, NBSIO_TABLET_NOT_READY);
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitConfigured(3);
        f.SendRead(0);
        auto reply = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsReadResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reply->Get()->Record.GetStatus()), static_cast<int>(NBSIO_OK));
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->GetPayload(reply->Get()->Record.GetPayloadId()).ConvertToString(),
            TString(4096, 'a') + TString(4096, 'b'));
    }

    Y_UNIT_TEST(ReconfigureWaitsForAcceptedPBReadAndErase) {
        TIoFixture f;
        f.Configure(2, 100);
        f.Write(0, 'a');
        // Only the client PB read is pinned; Sync's own source reads proceed.
        f.Hold = [&](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvReadPersistentBuffer::EventType
                && f.Workers.contains(event.Sender);
        };
        f.SendRead(0);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 1);
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.SendConfiguration(3);
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        f.SendWrite(4096, 'b', 2);
        f.WaitWrite(2, NBSIO_TABLET_NOT_READY);
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitRead('a');
        f.WaitConfigured(3);
        UNIT_ASSERT_VALUES_EQUAL(erases, 3);
        f.SendRead(0);
        f.WaitRead('a');
    }

    Y_UNIT_TEST(ConfigurationSupersessionRejectsStaleAndWrongSenderAcks) {
        TIoFixture f;
        f.Hold = [&](IEventHandle& event) {
            return event.GetTypeRewrite() == TEvLoad::TEvConfigureTabletResult::EventType
                && event.GetRecipientRewrite() != f.Edge;
        };
        f.SendConfiguration(2);
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 1);
        f.SendConfiguration(3);
        auto superseded = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(superseded);
        UNIT_ASSERT_VALUES_EQUAL(superseded->Get()->Record.GetConfigurationId(), 2);
        UNIT_ASSERT(!superseded->Get()->Record.GetSuccess());
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 2);
        auto oldAck = std::move(f.Held.front());
        f.Held.erase(f.Held.begin());
        f.Resume(std::move(oldAck));
        const auto& latest = f.Held.front();
        auto stray = std::make_unique<TEvLoad::TEvConfigureTabletResult>();
        stray->Record = latest->Get<TEvLoad::TEvConfigureTabletResult>()->Record;
        f.Env.Runtime->Send(new IEventHandle(latest->GetRecipientRewrite(), f.Edge,
            stray.release(), 0, latest->Cookie), f.Edge.NodeId());
        f.Hold = {};
        f.Env.Sim(TDuration::MilliSeconds(50));
        f.SendWrite(0, 'a');
        f.WaitWrite(1, NBSIO_TABLET_NOT_READY);
        f.ReleaseHeld();
        f.WaitConfigured(3);
        f.Write(0, 'a');
    }

    Y_UNIT_TEST(DeleteWaitsForFlushEraseAndDisconnectAcknowledgements) {
        TIoFixture f;
        f.Configure(2, 100);
        f.Write(0, 'a');
        ui32 disconnects = 0;
        ui32 deallocs = 0;
        TActorId tabletActor;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() == TEvLoad::TEvNbsLoadTabletDelete::EventType) {
                tabletActor = event.GetRecipientRewrite();
            }
            disconnects += event.GetTypeRewrite() == NDDisk::TEvDisconnect::EventType;
            if (event.GetTypeRewrite() == TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType) {
                const auto& record = event.Get<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>()->Record;
                if (record.QueriesSize() && !record.GetQueries(0).GetTargetNumVChunks()) {
                    ++deallocs;
                }
            }
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.SendDelete();
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(disconnects, 0);
        UNIT_ASSERT_VALUES_EQUAL(deallocs, 0);
        UNIT_ASSERT(tabletActor);
        auto stale = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>();
        stale->Record.SetStatus(NKikimrProto::OK);
        f.Env.Runtime->Send(new IEventHandle(tabletActor, f.Edge, stale.release(), 0, 0), f.Edge.NodeId());
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(deallocs, 0);
        f.SendConfiguration(3);
        auto rejected = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(rejected);
        UNIT_ASSERT(!rejected->Get()->Record.GetSuccess());
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvDisconnectResult::EventType;
        };
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(disconnects, 10);
        UNIT_ASSERT_VALUES_EQUAL(deallocs, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 10);
        const auto& held = f.Held.front();
        auto stray = std::make_unique<NDDisk::TEvDisconnectResult>();
        stray->Record.SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        f.Env.Runtime->Send(new IEventHandle(held->GetRecipientRewrite(), f.Edge,
            stray.release(), 0, held->Cookie), f.Edge.NodeId());
        f.Hold = {};
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(deallocs, 0);
        f.ReleaseHeld();
        f.WaitDeleted();
        UNIT_ASSERT_VALUES_EQUAL(deallocs, 1);
    }

    Y_UNIT_TEST(RepeatedPoisonDrainsAndWaitsForFinalDisconnect) {
        TIoFixture f;
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Workers.size(), 1);
        const auto worker = *f.Workers.begin();
        f.Env.Runtime->Send(new IEventHandle(worker, f.Edge, new TEvents::TEvPoison()), f.Edge.NodeId());
        f.Env.Runtime->Send(new IEventHandle(worker, f.Edge, new TEvents::TEvPoison()), f.Edge.NodeId());
        f.Env.Sim(TDuration::MilliSeconds(100));
        UNIT_ASSERT(f.Env.Runtime->GetActor(worker));
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvDisconnectResult::EventType;
        };
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 10);
        UNIT_ASSERT(f.Env.Runtime->GetActor(worker));
        f.Hold = {};
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(100));
        UNIT_ASSERT(!f.Env.Runtime->GetActor(worker));
    }

    Y_UNIT_TEST(AmbiguousPBFailureCannotAuthorizeDeleteUntilLateResults) {
        TIoFixture f;
        ui32 erases = 0;
        ui32 disconnects = 0;
        f.Observe = [&](IEventHandle& event) {
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
            disconnects += event.GetTypeRewrite() == NDDisk::TEvDisconnect::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType;
        };
        f.SendWrite(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 1);
        const auto& held = f.Held.front();
        auto ambiguous = std::make_unique<NDDisk::TEvWritePersistentBuffersResult>();
        ambiguous->Record = held->Get<NDDisk::TEvWritePersistentBuffersResult>()->Record;
        ambiguous->Record.MutableResult(0)->MutableResult()->SetStatus(
            NKikimrBlobStorage::NDDisk::TReplyStatus::SESSION_MISMATCH);
        f.Hold = {};
        f.Env.Runtime->Send(new IEventHandle(held->GetRecipientRewrite(), held->Sender,
            ambiguous.release(), 0, held->Cookie), held->Sender.NodeId());
        f.WaitWrite(1, NBSIO_QUORUM_LOST);
        f.SendDelete();
        f.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        UNIT_ASSERT_VALUES_EQUAL(disconnects, 0);
        f.ReleaseHeld();
        f.WaitDeleted();
        UNIT_ASSERT_VALUES_EQUAL(erases, 3);
        UNIT_ASSERT_VALUES_EQUAL(disconnects, 10);
    }

    Y_UNIT_TEST(SupersessionDuringDrainInstallsOnlyLatestConfiguration) {
        TIoFixture f;
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        f.SendConfiguration(2, 100);
        f.Env.Sim(TDuration::MilliSeconds(100));
        f.SendConfiguration(3, 1, 8192);
        auto superseded = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(superseded);
        UNIT_ASSERT_VALUES_EQUAL(superseded->Get()->Record.GetConfigurationId(), 2);
        UNIT_ASSERT(!superseded->Get()->Record.GetSuccess());
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitConfigured(3);
        f.Write(0, 'b');
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(LegacyConfigurationIdZeroStillDrainsAndReopensAdmission) {
        TIoFixture f;
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        f.SendConfiguration(0);
        f.Env.Sim(TDuration::MilliSeconds(100));
        f.SendWrite(0, 'b', 2);
        f.WaitWrite(2, NBSIO_TABLET_NOT_READY);
        f.Hold = {};
        f.ReleaseHeld();
        // Legacy callers receive no public configuration acknowledgement.
        f.Env.Sim(TDuration::MilliSeconds(200));
        f.Write(0, 'b', 3);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(RejectsAddressesOutsideConfiguredIoSlots) {
        TIoFixture f;
        f.Configure(2, 1, 8192);
        f.SendWrite(4096, 'a');
        f.WaitWrite(1, NBSIO_INVALID_ADDRESS);
        f.SendRead(4096);
        auto read = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsReadResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(read);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(read->Get()->Record.GetStatus()),
            static_cast<int>(NBSIO_INVALID_ADDRESS));
    }

    Y_UNIT_TEST(IdleCleanupUnblocksReorderedHeadAtLsnCap) {
        TIoFixture f;
        f.Configure(2, 2, 4096, 16, 16, /*cap=*/5);
        std::vector<ui64> synced;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() == NDDisk::TEvSync::EventType) {
                synced.push_back(event.Get<NDDisk::TEvSync>()->Record.GetSources(0)
                    .GetSegments(0).GetPersistentBufferSegment().GetLsn());
            }
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType;
        };
        for (ui64 lsn = 1; lsn <= 5; ++lsn) {
            f.SendWrite(0, 'a' + lsn, lsn);
        }
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 5);
        f.Hold = {};
        for (ui64 lsn : {2, 3, 4, 5, 1}) {
            UNIT_ASSERT_VALUES_EQUAL(f.ReleaseWhere([&](const IEventHandle& event) {
                return event.Cookie == lsn;
            }), 1);
            f.WaitWrite(lsn);
        }
        f.SendWrite(0, 'z', 6);
        f.WaitWrite(6, NBSIO_BACKPRESSURE);
        UNIT_ASSERT(synced.empty());
        f.Env.Sim(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 15);
        for (ui32 i = 0; i < synced.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(synced[i], i / 3 + 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(erases, 15);
        f.SendRead(0);
        f.WaitRead('f');
        f.Write(0, 'z', 7);
    }

    Y_UNIT_TEST(IdleCleanupErasesWithoutReplication) {
        TIoFixture f;
        f.Configure(2, 100, 4096, 16, 16, 1, /*disableReplication=*/true);
        ui32 syncs = 0;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Write(0, 'a');
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        f.Env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 0);
        UNIT_ASSERT_VALUES_EQUAL(erases, 1);
        f.Write(0, 'b', 2);
    }

    Y_UNIT_TEST(IdleCleanupSkipsVChunkWithIncompletePBWrite) {
        TIoFixture f(1, 2);
        f.Configure(2, 100);
        std::vector<ui64> synced;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() == NDDisk::TEvSync::EventType) {
                synced.push_back(event.Get<NDDisk::TEvSync>()->Record.GetSources(0)
                    .GetSegments(0).GetPersistentBufferSegment().GetLsn());
            }
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType
                && event.Cookie == 2;
        };
        f.Write(0, 'a');
        f.SendWrite(4096, 'b', 2);
        f.Write(128_MB, 'c', 3);
        f.Env.Sim(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 3);
        for (ui64 lsn : synced) {
            UNIT_ASSERT_VALUES_EQUAL(lsn, 3);
        }
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitWrite(2);
        f.Env.Sim(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 6); // three two-segment batches
        f.SendRead(0);
        f.WaitRead('a');
        f.SendRead(4096);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(MultiVChunkBatchesCompleteAndDelete) {
        for (ui32 flushBatch : {2, 16}) {
            TIoFixture f(1, 3);
            f.Configure(2, 6, 4096, flushBatch, 16, /*cap=*/6);
            std::map<ui64, ui32> syncSizes;
            f.Observe = [&](IEventHandle& event) {
                if (event.GetTypeRewrite() != NDDisk::TEvSync::EventType) {
                    return;
                }
                const auto& record = event.Get<NDDisk::TEvSync>()->Record;
                std::optional<ui64> vChunk;
                ui32 segments = 0;
                for (const auto& source : record.GetSources()) {
                    for (const auto& segment : source.GetSegments()) {
                        const ui64 current = segment.GetSelector().GetVChunkIndex();
                        if (vChunk) {
                            UNIT_ASSERT_VALUES_EQUAL(current, *vChunk);
                        } else {
                            vChunk = current;
                        }
                        ++segments;
                    }
                }
                UNIT_ASSERT(segments > 0 && segments <= flushBatch);
                syncSizes[event.Cookie] = segments;
            };
            f.Hold = [](IEventHandle& event) {
                return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
            };
            for (ui32 i = 0; i < 6; ++i) {
                // Interleaving must not turn the large-batch case into six
                // separate per-LSN requests to each destination.
                const ui64 address = (i % 3) * 128_MB + (i / 3) * 4096;
                f.Write(address, 'a' + i, i + 1);
            }
            f.Env.Sim(TDuration::MilliSeconds(50));
            UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), syncSizes.size());
            ui32 segments = 0;
            for (const auto& event : f.Held) {
                const auto& record = event->Get<NDDisk::TEvSyncResult>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(record.GetStatus()),
                    static_cast<int>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
                UNIT_ASSERT_VALUES_EQUAL(record.SegmentResultsSize(), syncSizes.at(event->Cookie));
                for (const auto& result : record.GetSegmentResults()) {
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result.GetStatus()),
                        static_cast<int>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
                }
                segments += record.SegmentResultsSize();
            }
            UNIT_ASSERT_VALUES_EQUAL(segments, 18);
            if (flushBatch == 16) {
                UNIT_ASSERT_VALUES_EQUAL(syncSizes.size(), 9);
                for (const auto& [cookie, size] : syncSizes) {
                    UNIT_ASSERT_VALUES_EQUAL(size, 2);
                }
            }
            auto counters = f.Env.Runtime->GetNode(f.Workers.begin()->NodeId())->AppData->Counters;
            auto allocated = GetServiceCounters(counters, "load_actor")->GetSubgroup("load", "tablet")
                ->GetSubgroup("subsystem", "lifecycle_worker")->GetCounter("DbgsAllocated", false);
            UNIT_ASSERT_VALUES_EQUAL(allocated->Val(), 1);
            f.SendWrite(0, 'z', 7);
            f.WaitWrite(7, NBSIO_BACKPRESSURE);
            f.Hold = {};
            f.ReleaseHeld();
            f.Env.Sim(TDuration::MilliSeconds(100));
            for (ui32 i = 0; i < 6; ++i) {
                f.SendRead((i % 3) * 128_MB + (i / 3) * 4096);
                f.WaitRead('a' + i);
            }
            // Reclaimed capacity accepts a final below-gate write; deletion
            // must complete normally through its remaining maintenance.
            f.Write(0, 'z', 8);
            f.SendDelete();
            f.WaitDeleted();
            UNIT_ASSERT_VALUES_EQUAL(allocated->Val(), 0);
        }
    }

    Y_UNIT_TEST(IdleCleanupAccountsVChunkSyncBatchesRetriesAndDuplicates) {
        TIoFixture f(1, 3);
        f.Configure(2, 3);
        std::vector<ui64> synced;
        std::vector<ui64> erased;
        std::map<ui64, ui64> syncLsnByCookie;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() == NDDisk::TEvSync::EventType) {
                const auto& record = event.Get<NDDisk::TEvSync>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.SourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetSources(0).SegmentsSize(), 1);
                const ui64 lsn = record.GetSources(0).GetSegments(0).GetPersistentBufferSegment().GetLsn();
                synced.push_back(lsn);
                syncLsnByCookie[event.Cookie] = lsn;
            } else if (event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType) {
                for (const auto& erase : event.Get<NDDisk::TEvBatchErasePersistentBuffer>()->Record.GetErases()) {
                    erased.push_back(erase.GetLsn());
                }
            }
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Write(128_MB, 'b', 2);
        f.Write(256_MB, 'c', 3);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 9);
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 9);
        for (const auto& event : f.Held) {
            const auto& record = event->Get<NDDisk::TEvSyncResult>()->Record;
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(record.GetStatus()),
                static_cast<int>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
            UNIT_ASSERT_VALUES_EQUAL(record.SegmentResultsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(record.GetSegmentResults(0).GetStatus()),
                static_cast<int>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
        }
        const auto failed = std::find_if(f.Held.begin(), f.Held.end(), [&](const auto& event) {
            return syncLsnByCookie.at(event->Cookie) == 1;
        });
        UNIT_ASSERT(failed != f.Held.end());
        const auto original = (*failed)->Get<NDDisk::TEvSyncResult>()->Record;
        const auto recipient = (*failed)->GetRecipientRewrite();
        const auto sender = (*failed)->Sender;
        const ui64 cookie = (*failed)->Cookie;
        f.Hold = {};
        auto inject = [&](const auto& record) {
            auto result = std::make_unique<NDDisk::TEvSyncResult>();
            result->Record = record;
            f.Env.Runtime->Send(new IEventHandle(recipient, sender, result.release(), 0, cookie), sender.NodeId());
        };
        auto invalid = original;
        invalid.SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN);
        inject(invalid);
        invalid.SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        invalid.ClearSegmentResults();
        inject(invalid);
        f.Write(4096, 'd', 4);
        f.Write(128_MB + 4096, 'e', 5);
        f.Env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 9);
        UNIT_ASSERT(erased.empty());

        // Consume the per-vChunk batches, with only vChunk 0 failing at one sink.
        // The fresh retry remains outstanding; duplicate old cookies cannot
        // release it or accidentally keep vChunks 1/2 busy.
        (*failed)->Get<NDDisk::TEvSyncResult>()->Record.MutableSegmentResults(0)->SetStatus(
            NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED);
        std::set<ui64> originals;
        for (const auto& event : f.Held) {
            originals.insert(event->Cookie);
        }
        f.Hold = [&](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType
                && !originals.contains(event.Cookie);
        };
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(synced.back(), 1);
        inject(original);
        f.Env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 13);
        for (ui32 i = 10; i < synced.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(synced[i], 5);
        }
        // Erase admission must precede pumping the new vChunk-1 flush.
        // Its held Sync does not undo erase admission from the same snapshot.
        UNIT_ASSERT_VALUES_EQUAL(erased.size(), 6);
        for (ui64 lsn : erased) {
            UNIT_ASSERT(lsn == 2 || lsn == 3);
        }
        f.Hold = {};
        f.ReleaseHeld();
        f.Env.Sim(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(erased.size(), 15);
        f.SendRead(0);
        f.WaitRead('a');
        f.SendRead(128_MB);
        f.WaitRead('b');
        f.SendRead(256_MB);
        f.WaitRead('c');
        f.SendRead(4096);
        f.WaitRead('d');
        f.SendRead(128_MB + 4096);
        f.WaitRead('e');
    }

    Y_UNIT_TEST(IdleCleanupPreservesDDiskAndPBReadPins) {
        TIoFixture f;
        f.Configure(2, 100);
        ui32 syncs = 0;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Hold = [&](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvReadResult::EventType
                || (event.GetTypeRewrite() == NDDisk::TEvReadPersistentBufferResult::EventType
                    && f.Workers.contains(event.GetRecipientRewrite()));
        };
        f.SendRead(0);
        f.Env.Sim(TDuration::MilliSeconds(50));
        f.Write(0, 'a');
        f.SendRead(0, 2);
        f.Env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(syncs, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.ReleaseWhere([](const IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvReadResult::EventType;
        }), 1);
        // Allow resumed DDisk reads through the filter.
        f.Hold = [&](IEventHandle& event) {
            return (event.GetTypeRewrite() == NDDisk::TEvReadPersistentBufferResult::EventType
                    && f.Workers.contains(event.GetRecipientRewrite()));
        };
        auto read = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsReadResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(read);
        f.Env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 3);
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        f.Hold = {};
        f.ReleaseHeld();
        f.WaitRead('a', 2);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(erases, 3);
    }

    Y_UNIT_TEST(ForcedCleanupDoesNotCountGateBlocks) {
        TIoFixture f;
        f.Configure(2, 100);
        f.Write(0, 'a');
        const ui64 flushBlocked = f.GateBlocked("SyncGateFlushBlocked");
        UNIT_ASSERT(flushBlocked > 0);
        f.Env.Sim(TDuration::MilliSeconds(1100));
        // A normal Sync completion can count the below-gate erase tail.
        UNIT_ASSERT_VALUES_EQUAL(f.GateBlocked("SyncGateFlushBlocked"), flushBlocked);
        const ui64 eraseBlocked = f.GateBlocked("SyncGateEraseBlocked");
        f.Env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(f.GateBlocked("SyncGateFlushBlocked"), flushBlocked);
        UNIT_ASSERT_VALUES_EQUAL(f.GateBlocked("SyncGateEraseBlocked"), eraseBlocked);
        f.Write(4096, 'b', 2);
        const ui64 beforeDrain = f.GateBlocked("SyncGateFlushBlocked");
        f.Configure(3, 100);
        UNIT_ASSERT_VALUES_EQUAL(f.GateBlocked("SyncGateFlushBlocked"), beforeDrain);
        UNIT_ASSERT_VALUES_EQUAL(f.GateBlocked("SyncGateEraseBlocked"), eraseBlocked);
    }

    Y_UNIT_TEST(StaleIdleCleanupCannotAdmitOrClearNewTimer) {
        TIoFixture f;
        f.SendConfiguration(0, 100);
        f.Env.Sim(TDuration::MilliSeconds(100));
        ui32 syncs = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(500));
        f.SendConfiguration(0, 100); // drain immediately; legacy ID is deliberately reused
        f.Env.Sim(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 3);
        f.Write(4096, 'b', 2);
        f.Env.Sim(TDuration::MilliSeconds(600)); // old timer fires here
        UNIT_ASSERT_VALUES_EQUAL(syncs, 3);
        f.Env.Sim(TDuration::MilliSeconds(500)); // new timer still fires
        UNIT_ASSERT_VALUES_EQUAL(syncs, 6);
        f.Env.Sim(TDuration::Seconds(2));
        f.SendRead(4096);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(AdmittedFlushAndEraseCohortsSurviveFallingBelowGate) {
        TIoFixture f;
        f.Configure(2, 2);
        ui32 syncs = 0;
        ui32 erases = 0;
        f.Observe = [&](IEventHandle& event) {
            syncs += event.GetTypeRewrite() == NDDisk::TEvSync::EventType;
            erases += event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType;
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType;
        };
        f.Write(0, 'a');
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 0);
        f.Write(0, 'b', 2);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(syncs, 3);
        UNIT_ASSERT_VALUES_EQUAL(erases, 0);
        f.Hold = {};
        f.ReleaseHeld();
        f.Env.Sim(TDuration::MilliSeconds(100));
        // The second version remains admitted when the first leaves Written,
        // and its erase remains admitted when the first leaves Flushed.
        UNIT_ASSERT_VALUES_EQUAL(syncs, 6);
        UNIT_ASSERT_VALUES_EQUAL(erases, 6);
        f.SendRead(0);
        f.WaitRead('b');
    }

    Y_UNIT_TEST(LaterCompletionsWaitForTheNextFlushCohort) {
        TIoFixture f;
        f.Configure(2, 3);
        std::vector<ui32> syncSegments;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() != NDDisk::TEvSync::EventType) {
                return;
            }
            ui32 segments = 0;
            const auto& record = event.Get<NDDisk::TEvSync>()->Record;
            for (ui32 source = 0; source < record.SourcesSize(); ++source) {
                segments += record.GetSources(source).SegmentsSize();
            }
            syncSegments.push_back(segments);
        };
        std::set<ui64> passLsns;
        f.Hold = [&](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType
                && !passLsns.contains(event.Cookie);
        };
        for (ui32 index = 0; index < 6; ++index) {
            f.SendWrite(index * 4096, 'a', index + 1);
        }
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 6);
        UNIT_ASSERT(syncSegments.empty());

        auto releaseWrite = [&](ui64 lsn) {
            passLsns.insert(lsn);
            const size_t released = f.ReleaseWhere([&](const IEventHandle& event) {
                return event.GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType
                    && event.Cookie == lsn;
            });
            UNIT_ASSERT_VALUES_EQUAL(released, 1);
            f.WaitWrite(lsn);
            f.Env.Sim(TDuration::MilliSeconds(50));
        };
        releaseWrite(4);
        releaseWrite(5);
        UNIT_ASSERT(syncSegments.empty());
        releaseWrite(6);
        UNIT_ASSERT_VALUES_EQUAL(syncSegments.size(), 3);
        for (ui32 segments : syncSegments) {
            UNIT_ASSERT_VALUES_EQUAL(segments, 3);
        }
        releaseWrite(1);
        releaseWrite(2);
        UNIT_ASSERT_VALUES_EQUAL(syncSegments.size(), 3);
        releaseWrite(3);
        UNIT_ASSERT_VALUES_EQUAL(syncSegments.size(), 6);
        for (ui32 index = 3; index < 6; ++index) {
            UNIT_ASSERT_VALUES_EQUAL(syncSegments[index], 3);
        }
    }

    Y_UNIT_TEST(EraseCohortsBatchOnlyAfterSyncCompletions) {
        TIoFixture f;
        f.Configure(2, 3, 4096, /*flushBatch=*/1, /*eraseBatch=*/16);
        std::map<ui64, ui64> syncLsnByCookie;
        std::vector<ui32> eraseSizes;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() == NDDisk::TEvSync::EventType) {
                const auto& record = event.Get<NDDisk::TEvSync>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.SourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetSources(0).SegmentsSize(), 1);
                syncLsnByCookie[event.Cookie] =
                    record.GetSources(0).GetSegments(0).GetPersistentBufferSegment().GetLsn();
            } else if (event.GetTypeRewrite() == NDDisk::TEvBatchErasePersistentBuffer::EventType) {
                eraseSizes.push_back(event.Get<NDDisk::TEvBatchErasePersistentBuffer>()->Record.ErasesSize());
            }
        };
        std::set<ui64> passSyncCookies;
        f.Hold = [&](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType
                && !passSyncCookies.contains(event.Cookie);
        };
        auto runCohort = [&](ui32 addressBase) {
            const ui32 erasesBefore = eraseSizes.size();
            syncLsnByCookie.clear();
            for (ui32 index = 0; index < 3; ++index) {
                f.Write((addressBase + index) * 4096, 'a', addressBase + index + 1);
            }
            f.Env.Sim(TDuration::MilliSeconds(50));
            UNIT_ASSERT_VALUES_EQUAL(syncLsnByCookie.size(), 9);
            std::map<ui64, ui32> resultsByLsn;
            for (const auto& [cookie, lsn] : syncLsnByCookie) {
                ++resultsByLsn[lsn];
            }
            UNIT_ASSERT_VALUES_EQUAL(resultsByLsn.size(), 3);
            ui32 releasedCohort = 0;
            for (const auto& [lsn, count] : resultsByLsn) {
                UNIT_ASSERT_VALUES_EQUAL(count, 3);
                for (const auto& [cookie, mappedLsn] : syncLsnByCookie) {
                    if (mappedLsn == lsn) {
                        passSyncCookies.insert(cookie);
                    }
                }
                const size_t released = f.ReleaseWhere([&](const IEventHandle& event) {
                    const auto found = syncLsnByCookie.find(event.Cookie);
                    return event.GetTypeRewrite() == NDDisk::TEvSyncResult::EventType
                        && found != syncLsnByCookie.end() && found->second == lsn;
                });
                UNIT_ASSERT_VALUES_EQUAL(released, 3);
                f.Env.Sim(TDuration::MilliSeconds(20));
                ++releasedCohort;
                if (releasedCohort < 3) {
                    UNIT_ASSERT_VALUES_EQUAL(eraseSizes.size(), erasesBefore);
                }
            }
            f.Env.Sim(TDuration::MilliSeconds(50));
            UNIT_ASSERT_VALUES_EQUAL(eraseSizes.size(), erasesBefore + 3);
            for (ui32 index = erasesBefore; index < eraseSizes.size(); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(eraseSizes[index], 3);
            }
        };
        runCohort(0);
        runCohort(3);
        UNIT_ASSERT_VALUES_EQUAL(eraseSizes.size(), 6);
    }

    Y_UNIT_TEST(DDiskReadPinDefersOnlyItsAdmittedSlot) {
        TIoFixture f;
        f.Configure(2, 2);
        std::vector<ui64> synced;
        f.Observe = [&](IEventHandle& event) {
            if (event.GetTypeRewrite() != NDDisk::TEvSync::EventType) {
                return;
            }
            const auto& record = event.Get<NDDisk::TEvSync>()->Record;
            for (ui32 source = 0; source < record.SourcesSize(); ++source) {
                for (ui32 segment = 0; segment < record.GetSources(source).SegmentsSize(); ++segment) {
                    synced.push_back(record.GetSources(source).GetSegments(segment)
                        .GetPersistentBufferSegment().GetLsn());
                }
            }
        };
        f.Hold = [](IEventHandle& event) {
            return event.GetTypeRewrite() == NDDisk::TEvReadResult::EventType;
        };
        f.SendRead(0);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(f.Held.size(), 1);
        f.Write(0, 'a');
        f.Write(4096, 'b', 2);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 3);
        for (ui64 lsn : synced) {
            UNIT_ASSERT_VALUES_EQUAL(lsn, 2);
        }
        f.Hold = {};
        f.ReleaseHeld();
        auto read = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsReadResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(read);
        f.Env.Sim(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(synced.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(synced.back(), 1);
    }

    // Create + Run + Delete with a single DBG. Verifies the full lifecycle:
    // - Hive boots the tablet
    // - tablet allocates 1 DBG (5 DDisks + 5 PBs) via BSC
    // - load actor drives a short workload and reports duration
    // - tablet de-allocates the DBG and clears its schema on Delete
    Y_UNIT_TEST(BasicSingleDbg) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_OK);

        // Allow DDisk/PB actor initialization and peer-connect handshake to
        // complete before issuing writes; matches the WriteRead1000Blocks pattern.
        f.Env.Sim(TDuration::Seconds(5));

        auto fin = f.RunViaLoadActor(tabletId, /*tag=*/1, /*numDbgsToUse=*/0);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    Y_UNIT_TEST(WriteChecksumsFollowRunConfiguration) {
        for (const bool enableChecksums : {false, true}) {
            TFixture f(/*numDDiskGroups=*/4, enableChecksums);
            const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
            TActorId pipe = f.OpenTabletPipe(tabletId);

            UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_OK);
            f.Env.Sim(TDuration::Seconds(5));

            ui64 nbsWrites = 0;
            ui64 pbWrites = 0;
            TString invalidReason;
            auto previousFilter = std::move(f.Env.Runtime->FilterFunction);
            f.Env.Runtime->FilterFunction =
                [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
                    if (event->GetTypeRewrite() == TEvLoad::TEvNbsWrite::EventType) {
                        const auto* msg = event->Get<TEvLoad::TEvNbsWrite>();
                        ++nbsWrites;
                        const ui32 expectedCount = enableChecksums
                            ? msg->Record.GetSizeBytes() / NDDisk::IntegrityUnitSize
                            : 0;
                        if (static_cast<ui32>(msg->Record.ChecksumsSize()) != expectedCount
                                && invalidReason.empty())
                        {
                            invalidReason = TStringBuilder()
                                << "TEvNbsWrite checksum count# " << msg->Record.ChecksumsSize()
                                << " expected# " << expectedCount;
                        }
                    } else if (event->GetTypeRewrite() == NDDisk::TEvWritePersistentBuffers::EventType) {
                        const auto* msg = event->Get<NDDisk::TEvWritePersistentBuffers>();
                        ++pbWrites;
                        const auto& record = msg->Record;
                        const NDDisk::TWriteInstruction instruction(record.GetInstruction());
                        if (!instruction.PayloadId
                                || *instruction.PayloadId >= msg->GetPayloadCount())
                        {
                            if (invalidReason.empty()) {
                                invalidReason = "TEvWritePersistentBuffers has no valid payload";
                            }
                        } else {
                            const auto expected = enableChecksums
                                ? NDDisk::CalculatePayloadChecksums(msg->GetPayload(*instruction.PayloadId))
                                : std::vector<ui64>{};
                            if (static_cast<size_t>(record.ChecksumsSize()) != expected.size()
                                    && invalidReason.empty())
                            {
                                invalidReason = TStringBuilder()
                                    << "TEvWritePersistentBuffers checksum count# " << record.ChecksumsSize()
                                    << " expected# " << expected.size();
                            }
                            for (ui32 i = 0; i < expected.size() && invalidReason.empty(); ++i) {
                                if (record.GetChecksums(i) != expected[i]) {
                                    invalidReason = TStringBuilder()
                                        << "TEvWritePersistentBuffers checksum mismatch at block# " << i;
                                }
                            }
                        }
                    }
                    return previousFilter ? previousFilter(nodeId, event) : true;
                };

            auto fin = f.RunViaLoadActor(
                tabletId, /*tag=*/1, /*numDbgsToUse=*/0, enableChecksums);
            f.Env.Runtime->FilterFunction = std::move(previousFilter);

            UNIT_ASSERT(fin.FinishedReceived);
            UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);
            UNIT_ASSERT_C(nbsWrites > 0, "no TEvNbsWrite observed");
            UNIT_ASSERT_C(pbWrites > 0, "no TEvWritePersistentBuffers observed");
            UNIT_ASSERT_C(invalidReason.empty(), invalidReason);

            UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
            f.ClosePipe(pipe);
        }
    }

    // Same lifecycle with 2 DBGs - exercises the cookie scheme that routes
    // wire replies to the right per-DBG state in the worker.
    Y_UNIT_TEST(MultiDbg) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/2), NBSLT_OK);
        f.Env.Sim(TDuration::Seconds(5));

        auto fin = f.RunViaLoadActor(tabletId, /*tag=*/1, /*numDbgsToUse=*/0);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // N=2 allocated, Run with M=1: load actor slices to first DBG only; must finish OK.
    Y_UNIT_TEST(SubsetOneOfTwoDbgs) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/2), NBSLT_OK);
        f.Env.Sim(TDuration::Seconds(5));

        auto fin = f.RunViaLoadActor(tabletId, /*tag=*/1, /*numDirectBlockGroupsToUse=*/1);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // GetSummary returns NumReadyDirectBlockGroups == NumDirectBlockGroups once
    // all peers have connected. Before any Create the ready count is 0. After
    // Create + peer-connect simulation both counts equal the allocated DBG count.
    Y_UNIT_TEST(GetSummaryReadyCounts) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        // Before Create: tablet is uninitialized — GetSummary returns NOT_INITIALIZED.
        {
            f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
                auto ev = std::make_unique<TEvLoad::TEvNbsLoadTabletGetSummary>();
                NTabletPipe::SendData(f.Edge, pipe, ev.release());
            });
            auto resp = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsLoadTabletGetSummaryResult>(
                f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(30)));
            UNIT_ASSERT(resp);
            UNIT_ASSERT_VALUES_EQUAL(resp->Get()->Record.GetStatus(), NBSLT_NOT_INITIALIZED);
        }

        constexpr ui32 kDbgs = 2;
        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/kDbgs), NBSLT_OK);

        // Allow peer-connect handshakes to complete for all kDbgs DBGs.
        f.Env.Sim(TDuration::Seconds(5));

        auto summary = f.TabletGetSummary(pipe);
        UNIT_ASSERT_VALUES_EQUAL_C(summary.GetStatus(), NBSLT_OK,
            "GetSummary failed: " << summary.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL_C(summary.GetNumDirectBlockGroups(), kDbgs,
            "expected NumDirectBlockGroups=" << kDbgs);
        UNIT_ASSERT_VALUES_EQUAL_C(summary.GetNumReadyDirectBlockGroups(), kDbgs,
            "expected NumReadyDirectBlockGroups=" << kDbgs
                << " (got " << summary.GetNumReadyDirectBlockGroups() << "); "
                << "some DBG peer-connects may not have completed");

        // RunViaLoadActor with numDirectBlockGroupsToUse=0 must use both ready
        // DBGs (EffectiveDbgCount == ReadyDbgCount == 2).
        auto fin = f.RunViaLoadActor(tabletId, /*tag=*/1, /*numDirectBlockGroupsToUse=*/0);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // Running against a tablet that has no DBGs allocated must fail gracefully:
    // the load actor gets NumDirectBlockGroups=0 from GetSummary and errors out.
    Y_UNIT_TEST(RunBeforeCreate) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);

        auto fin = f.RunViaLoadActor(tabletId);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(!fin.ErrorReason.empty(),
            "expected error when tablet has no DBGs allocated");
    }

    Y_UNIT_TEST(AutomationWaitsForEntirePrefixAndConfiguration) {
        for (bool blockConfiguration : {false, true}) {
            TFixture f;
            const ui64 tabletId = f.CreateNbsLoadTabletViaHive(1);
            const auto pipe = f.OpenTabletPipe(tabletId);
            UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, 2), NBSLT_OK);
            f.Env.Sim(TDuration::Seconds(5));
            ui32 writes = 0;
            auto previousFilter = std::move(f.Env.Runtime->FilterFunction);
            f.Env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvLoad::TEvNbsWrite::EventType) { ++writes; }
                if (blockConfiguration && event->GetTypeRewrite() == TEvLoad::TEvConfigureTabletResult::EventType) {
                    return false;
                }
                if (!blockConfiguration && event->GetTypeRewrite() == TEvLoad::TEvNbsLoadTabletGetSummaryResult::EventType) {
                    event->Get<TEvLoad::TEvNbsLoadTabletGetSummaryResult>()->Record.SetNumReadyDirectBlockGroups(1);
                }
                return previousFilter ? previousFilter(node, event) : true;
            };
            const auto result = f.RunViaLoadActor(tabletId, 1, 0, true, true);
            f.Env.Runtime->FilterFunction = std::move(previousFilter);
            UNIT_ASSERT(result.FinishedReceived);
            UNIT_ASSERT(!result.ErrorReason.empty());
            UNIT_ASSERT_VALUES_EQUAL(writes, 0);
            UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
            f.ClosePipe(pipe);
        }
    }

    Y_UNIT_TEST(AutomationControlAcrossNodes) {
        TFixture f;
        using TControl = NKikimrClient::TNbsDbgLikeLoadControl;
        using TResult = NKikimrClient::TNbsDbgLikeLoadResult;
        for (ui32 node = 1; node <= 8; ++node) {
            f.Env.Runtime->RegisterService(MakeLoadServiceID(node),
                f.Env.Runtime->Register(CreateLoadTestActor(MakeIntrusive<::NMonitoring::TDynamicCounters>()), node));
        }
        auto call = [&](const TControl& request) {
            auto event = std::make_unique<TEvLoad::TEvNbsDbgLikeLoadControl>();
            event->Record = request;
            f.Env.Runtime->Send(new IEventHandle(MakeLoadServiceID(2), f.Edge, event.release()), f.Edge.NodeId());
            auto response = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsDbgLikeLoadControlResponse>(
                f.Edge, false, f.Deadline(TDuration::Seconds(90)));
            UNIT_ASSERT(response);
            return response->Get()->Record;
        };
        TControl request;
        request.SetDatabase("/Root");
        request.SetOperation(TControl::CAPABILITIES);
        auto capabilities = call(request);
        UNIT_ASSERT_VALUES_EQUAL(capabilities.GetStatus(), 1);
        UNIT_ASSERT_VALUES_EQUAL(capabilities.GetCoordinatorNodeId(), 2);
        request.SetDatabase("/Other");
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        request.SetDatabase("/Root");
        request.SetCoordinatorNodeId(2);
        request.SetIncarnation(capabilities.GetIncarnation());
        request.SetOperation(TControl::CREATE);
        request.SetOwnerIndex(123);
        auto* allocation = request.MutableAllocation();
        allocation->SetDDiskPoolName("ddisk_pool");
        allocation->SetPersistentBufferDDiskPoolName("ddisk_pool");
        for (ui32 i = 0; i < 3; ++i) { allocation->AddTabletStoragePools(f.Env.StoragePoolName); }
        allocation->SetNumDirectBlockGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        auto list = request;
        list.SetOperation(TControl::LIST);
        UNIT_ASSERT_VALUES_EQUAL(call(list).TabletsSize(), 0); // validation precedes Hive creation
        allocation->ClearNumDirectBlockGroups();
        auto created = call(request);
        UNIT_ASSERT_VALUES_EQUAL_C(created.GetStatus(), 1, created.GetError());
        UNIT_ASSERT_VALUES_EQUAL(created.TabletsSize(), 1);
        const ui64 tabletId = created.GetTablets(0).GetTabletId();
        auto listed = call(list);
        UNIT_ASSERT_VALUES_EQUAL(listed.TabletsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listed.GetTablets(0).ChannelPoolsSize(), 3);
        for (const auto& pool : listed.GetTablets(0).GetChannelPools()) {
            UNIT_ASSERT_VALUES_EQUAL(pool, f.Env.StoragePoolName);
        }
        bool incompleteOnce = true;
        auto previousStorageFilter = std::move(f.Env.Runtime->FilterFunction);
        f.Env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            if (incompleteOnce && event->GetTypeRewrite() == TEvHive::TEvGetTabletStorageInfoResult::EventType) {
                auto& result = event->Get<TEvHive::TEvGetTabletStorageInfoResult>()->Record;
                if (result.GetTabletID() == tabletId) {
                    result.MutableInfo()->ClearChannels();
                    incompleteOnce = false;
                }
            }
            return previousStorageFilter ? previousStorageFilter(node, event) : true;
        };
        auto retry = call(request);
        UNIT_ASSERT_VALUES_EQUAL_C(retry.GetStatus(), 1, retry.GetError());
        UNIT_ASSERT(!incompleteOnce);
        f.Env.Runtime->FilterFunction = std::move(previousStorageFilter);
        auto previousTimeoutFilter = std::move(f.Env.Runtime->FilterFunction);
        f.Env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvHive::TEvGetTabletStorageInfoResult::EventType) {
                auto& result = event->Get<TEvHive::TEvGetTabletStorageInfoResult>()->Record;
                if (result.GetTabletID() == tabletId) { result.MutableInfo()->ClearChannels(); }
            }
            return previousTimeoutFilter ? previousTimeoutFilter(node, event) : true;
        };
        auto assignmentTimeout = call(request);
        UNIT_ASSERT_VALUES_EQUAL(assignmentTimeout.GetStatus(), 128);
        UNIT_ASSERT_VALUES_EQUAL(assignmentTimeout.TabletsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(assignmentTimeout.GetTablets(0).GetTabletId(), tabletId);
        UNIT_ASSERT_STRING_CONTAINS(assignmentTimeout.GetError(), "channel assignment deadline expired");
        f.Env.Runtime->FilterFunction = std::move(previousTimeoutFilter);
        allocation->SetTabletStoragePools(1, "different-pool");
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        allocation->SetTabletStoragePools(1, f.Env.StoragePoolName);
        allocation->SetNumDirectBlockGroups(2);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        const ui64 unrelatedTabletId = f.CreateNbsLoadTabletViaHive(124);
        ui32 unrelatedStorageRequests = 0;
        auto previousFilter = std::move(f.Env.Runtime->FilterFunction);
        f.Env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvHive::TEvGetTabletStorageInfo::EventType
                && event->Get<TEvHive::TEvGetTabletStorageInfo>()->Record.GetTabletID() == unrelatedTabletId) {
                ++unrelatedStorageRequests;
            }
            return previousFilter ? previousFilter(node, event) : true;
        };
        auto describe = request;
        describe.ClearAllocation();
        describe.SetOperation(TControl::DESCRIBE);
        UNIT_ASSERT_VALUES_EQUAL_C(call(describe).GetStatus(), 1, "describe healthy tablet");
        request.ClearAllocation();
        request.SetOperation(TControl::START);
        request.SetRequestId("original");
        auto* cmd = request.MutableLoad()->MutableNbsDbgLikeLoad();
        auto* target = cmd->AddTargets();
        target->SetTabletId(tabletId);
        target->SetNodeId(3); // force remote generation; exercises full-width child tags
        auto* workload = cmd->MutableWorkloadConfig();
        workload->SetDurationSeconds(1);
        workload->SetDelayBeforeMeasurementsSeconds(0);
        workload->SetMaxInFlight(0);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        workload->SetMaxInFlight(1);
        workload->SetStopOnWritesDoneCount(50);
        request.SetStartupTimeoutSeconds(3601);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128); // startup budget is bounded
        request.ClearStartupTimeoutSeconds();
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 1);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 1); // lost reply retry
        workload->SetMaxInFlight(2);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        workload->SetMaxInFlight(1);
        const auto originalStart = request;
        request.ClearLoad();
        request.SetOperation(TControl::GET);
        NKikimrClient::TNbsDbgLikeLoadControlResponse finished;
        for (ui32 attempt = 0; attempt < 100; ++attempt) {
            finished = call(request);
            UNIT_ASSERT_VALUES_EQUAL_C(finished.GetStatus(), 1, finished.GetError());
            if (finished.GetRun().HasFinishedAtMs()) { break; }
            f.Env.Sim(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(finished.GetRun().GetState()), static_cast<int>(TResult::SUCCEEDED), finished.GetRun().GetExecutionError());
        UNIT_ASSERT(finished.GetRun().GetTerminationConfirmed());
        UNIT_ASSERT_VALUES_EQUAL(finished.GetRun().GetEffectiveConfig().GetNbsDbgLikeLoad().GetTargets(0).GetNodeId(), 3);
        UNIT_ASSERT_VALUES_EQUAL(finished.GetRun().TabletsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(finished.TabletsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(call(originalStart).GetRun().GetStartedAtMs(), finished.GetRun().GetStartedAtMs());

        // A later trial resolves current placement, whereas retrying the old ID
        // keeps the original explicit generator node.
        auto next = originalStart;
        next.SetRequestId("cancel-running");
        next.MutableLoad()->MutableNbsDbgLikeLoad()->MutableTargets(0)->ClearNodeId();
        next.MutableLoad()->MutableNbsDbgLikeLoad()->MutableWorkloadConfig()->SetDurationSeconds(60);
        next.MutableLoad()->MutableNbsDbgLikeLoad()->MutableWorkloadConfig()->SetStopOnWritesDoneCount(0);
        UNIT_ASSERT_VALUES_EQUAL(call(next).GetStatus(), 1);
        ui32 writes = 0;
        const auto deadline = f.Deadline(TDuration::Seconds(90));
        f.Env.Runtime->Sim([&] { return !writes && f.Env.Runtime->GetClock() < deadline; },
            [&](IEventHandle& event) { writes += event.GetTypeRewrite() == TEvLoad::TEvNbsWrite::EventType; });
        UNIT_ASSERT(writes);
        TResultsHttpRequest httpRequest;
        httpRequest.Params.emplace("mode", "results");
        httpRequest.Params.emplace("uuid", "cancel-running");
        NMonitoring::TMonService2HttpRequest monRequest(nullptr, &httpRequest, nullptr, nullptr, "", nullptr);
        const TActorId htmlEdge = f.Env.Runtime->AllocateEdgeActor(2);
        f.Env.Runtime->Send(new IEventHandle(MakeLoadServiceID(2), htmlEdge,
            new NMon::TEvHttpInfo(monRequest)), 2);
        auto htmlResult = f.Env.WaitForEdgeActorEvent<NMon::TEvHttpInfoRes>(
            htmlEdge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(htmlResult);
        UNIT_ASSERT_STRING_CONTAINS(htmlResult->Get()->Answer,
            "No load actor result found for requested UUID");
        request.SetOperation(TControl::DELETE);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        request.SetRequestId(next.GetRequestId());
        request.SetOperation(TControl::STOP);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 1);
        request.SetOperation(TControl::GET);
        for (ui32 attempt = 0; attempt < 100; ++attempt) {
            finished = call(request);
            UNIT_ASSERT_VALUES_EQUAL_C(finished.GetStatus(), 1, finished.GetError());
            if (finished.GetRun().HasFinishedAtMs()) { break; }
            f.Env.Sim(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(finished.GetRun().GetState()), static_cast<int>(TResult::CANCELLED), finished.GetRun().GetExecutionError());
        UNIT_ASSERT(finished.GetRun().GetTerminationConfirmed());
        UNIT_ASSERT_VALUES_EQUAL(finished.GetRun().GetEffectiveConfig().GetNbsDbgLikeLoad().GetTargets(0).GetNodeId(), finished.GetTablets(0).GetNodeId());
        request.SetOperation(TControl::DELETE);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 1);
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 1);
        request.SetOperation(TControl::GET);
        request.SetIncarnation("old");
        UNIT_ASSERT_VALUES_EQUAL(call(request).GetStatus(), 128);
        f.Env.Runtime->FilterFunction = std::move(previousFilter);
        UNIT_ASSERT_VALUES_EQUAL(unrelatedStorageRequests, 0);
    }

    // Issuing a second Create after a successful Create must be rejected.
    Y_UNIT_TEST(DoubleCreate) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_OK);
        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_ALREADY_INITIALIZED);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // Reboots the tablet between Create and Run. Verifies that the persisted
    // DBG roster (held in NIceDb tables on top of the KV base) is restored
    // from the local DB at boot, so the load actor can drive a run against
    // the already-allocated DBGs without re-asking BSC. This is the main
    // payoff of running the tablet on top of TKeyValueFlat.
    Y_UNIT_TEST(CreateRestartRunDelete) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        {
            TActorId pipe = f.OpenTabletPipe(tabletId);
            UNIT_ASSERT_VALUES_EQUAL(
                f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_OK);
            // Drain DDisk/PB init events and let peer-connect handshake complete
            // before rebooting; otherwise the reboot races with the first-boot
            // connection establishment and the new tablet may find zero
            // PBConnected bits when writes arrive.
            f.Env.Sim(TDuration::Seconds(5));
            f.ClosePipe(pipe);
        }

        f.RebootTablet(tabletId);

        // If Dbgs failed to reload, GetSummary returns NumDirectBlockGroups=0
        // and the load actor errors out instead of running the workload.
        auto fin = f.RunViaLoadActor(tabletId, /*tag=*/1, /*numDbgsToUse=*/0);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);

        TActorId pipe = f.OpenTabletPipe(tabletId);
        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // End-to-end data-integrity test that drives the load tablet directly,
    // bypassing the load-actor / Run path. TEvNbsWrite/Read
    // travel over the tablet pipe and carry user payload via TRope. The
    // first 8 bytes of each block encode the block number; we write 1000
    // unique 4 KiB blocks and read them back, asserting the round-trip
    // payload matches.
    Y_UNIT_TEST(WriteRead1000Blocks) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_OK);

        // Allow the tablet's pre-connect handshake to all 6 PB+DD peers
        // of the freshly-allocated DBG to complete before we configure.
        f.Env.Sim(TDuration::Seconds(5));

        constexpr ui32 kNumBlocks = 1000;
        constexpr ui32 kBlockSize = 4096;

        // Send TEvConfigureTablet (pipe-routed) to install IoSizeBytes and
        // a generous MaxInflightLsns so all 1000 writes fit without
        // backpressure.
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            auto ev = std::make_unique<TEvLoad::TEvConfigureTablet>();
            auto& cfg = ev->Record;
            cfg.SetConfigurationId(1);
            cfg.SetMaxInflightLsns(2000);
            cfg.SetFlushBatchSize(16);
            cfg.SetEraseBatchSize(32);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(1);
            cfg.SetIoSizeBytes(kBlockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });
        auto configured = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(configured && configured->Get()->Record.GetSuccess());

        // Submit 1000 writes; payload[0..7] = block index.
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            for (ui64 i = 0; i < kNumBlocks; ++i) {
                auto ev = std::make_unique<TEvLoad::TEvNbsWrite>(
                    /*address=*/i * kBlockSize, /*sizeBytes=*/kBlockSize);
                TString data(kBlockSize, '\0');
                memcpy(data.Detach(), &i, sizeof(i));
                TFixture::AddWritePayload(*ev, TRope(std::move(data)));
                NTabletPipe::SendData(f.Edge, pipe, ev.release(), /*cookie=*/i);
            }
        });

        // Drain 1000 OK write acks; cookies must cover [0, kNumBlocks).
        TVector<bool> writeOk(kNumBlocks, false);
        for (ui32 got = 0; got < kNumBlocks; ++got) {
            auto resp = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsWriteResult>(
                f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            UNIT_ASSERT_C(resp->Get()->Record.GetStatus() == NBSIO_OK,
                "write i=" << resp->Cookie << " status=" << static_cast<int>(resp->Get()->Record.GetStatus()));
            const ui64 cookie = resp->Cookie;
            UNIT_ASSERT_C(cookie < kNumBlocks, "cookie=" << cookie);
            UNIT_ASSERT_C(!writeOk[cookie], "duplicate ack for cookie=" << cookie);
            writeOk[cookie] = true;
        }

        // Submit 1000 reads on the same addresses.
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            for (ui64 i = 0; i < kNumBlocks; ++i) {
                auto ev = std::make_unique<TEvLoad::TEvNbsRead>(
                    /*address=*/i * kBlockSize, /*sizeBytes=*/kBlockSize);
                NTabletPipe::SendData(f.Edge, pipe, ev.release(), /*cookie=*/i);
            }
        });

        // Drain 1000 read results; verify payload[0..7] == cookie.
        TVector<bool> readOk(kNumBlocks, false);
        for (ui32 got = 0; got < kNumBlocks; ++got) {
            auto resp = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsReadResult>(
                f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            UNIT_ASSERT_C(resp->Get()->Record.GetStatus() == NBSIO_OK,
                "read i=" << resp->Cookie << " status=" << static_cast<int>(resp->Get()->Record.GetStatus()));
            const ui64 cookie = resp->Cookie;
            UNIT_ASSERT_C(cookie < kNumBlocks, "cookie=" << cookie);
            UNIT_ASSERT_C(!readOk[cookie], "duplicate result for cookie=" << cookie);
            UNIT_ASSERT_C(resp->Get()->Record.HasPayloadId(),
                "missing PayloadId for cookie=" << cookie);
            const ui32 payloadId = resp->Get()->Record.GetPayloadId();
            UNIT_ASSERT_C(payloadId < resp->Get()->GetPayloadCount(),
                "bad PayloadId=" << payloadId << " for cookie=" << cookie
                    << " PayloadCount=" << resp->Get()->GetPayloadCount());
            const TString payload = resp->Get()->GetPayload(payloadId).ConvertToString();
            UNIT_ASSERT_VALUES_EQUAL_C(payload.size(), kBlockSize,
                "short payload for cookie=" << cookie);
            ui64 decoded = 0;
            memcpy(&decoded, payload.data(), sizeof(decoded));
            UNIT_ASSERT_VALUES_EQUAL_C(decoded, cookie,
                "payload mismatch for cookie=" << cookie);
            readOk[cookie] = true;
        }

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // Direct write/read across two DBGs. The proxy tablet must route each
    // request (by address / BytesPerDbg) to the correct per-DBG worker actor,
    // and each worker must reply straight back to the requestor. Addresses
    // span both DBGs so a routing regression (e.g. all traffic to DBG0) would
    // surface as wrong payloads or missing acks.
    Y_UNIT_TEST(WriteReadMultiDbg) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/2), NBSLT_OK);

        // Allow both DBGs' worker actors to finish their peer handshake.
        f.Env.Sim(TDuration::Seconds(5));

        constexpr ui32 kBlockSize = 4096;
        constexpr ui32 kBlocksPerDbg = 200;
        constexpr ui32 kNumDbgs = 2;
        constexpr ui32 kTotal = kBlocksPerDbg * kNumDbgs;
        // BytesPerDbg = TargetNumVChunks(1) * VChunkSizeBytes(128MB).
        const ui64 bytesPerDbg = 128_MB;

        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            auto ev = std::make_unique<TEvLoad::TEvConfigureTablet>();
            auto& cfg = ev->Record;
            cfg.SetConfigurationId(1);
            cfg.SetMaxInflightLsns(2000);
            cfg.SetFlushBatchSize(16);
            cfg.SetEraseBatchSize(32);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(kNumDbgs);
            cfg.SetIoSizeBytes(kBlockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });
        auto configured = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(configured && configured->Get()->Record.GetSuccess());

        auto addressOf = [&](ui32 dbg, ui32 i) -> ui64 {
            return dbg * bytesPerDbg + static_cast<ui64>(i) * kBlockSize;
        };
        auto cookieOf = [&](ui32 dbg, ui32 i) -> ui64 {
            return static_cast<ui64>(dbg) * kBlocksPerDbg + i;
        };

        // Submit writes interleaved across both DBGs; payload encodes cookie.
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            for (ui32 i = 0; i < kBlocksPerDbg; ++i) {
                for (ui32 dbg = 0; dbg < kNumDbgs; ++dbg) {
                    const ui64 cookie = cookieOf(dbg, i);
                    auto ev = std::make_unique<TEvLoad::TEvNbsWrite>(
                        addressOf(dbg, i), /*sizeBytes=*/kBlockSize);
                    TString data(kBlockSize, '\0');
                    memcpy(data.Detach(), &cookie, sizeof(cookie));
                    TFixture::AddWritePayload(*ev, TRope(std::move(data)));
                    NTabletPipe::SendData(f.Edge, pipe, ev.release(), cookie);
                }
            }
        });

        TVector<bool> writeOk(kTotal, false);
        for (ui32 got = 0; got < kTotal; ++got) {
            auto resp = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsWriteResult>(
                f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            UNIT_ASSERT_C(resp->Get()->Record.GetStatus() == NBSIO_OK,
                "write cookie=" << resp->Cookie << " status=" << static_cast<int>(resp->Get()->Record.GetStatus()));
            const ui64 cookie = resp->Cookie;
            UNIT_ASSERT_C(cookie < kTotal, "cookie=" << cookie);
            UNIT_ASSERT_C(!writeOk[cookie], "duplicate ack for cookie=" << cookie);
            writeOk[cookie] = true;
        }

        // Read everything back and verify payloads route to the right worker.
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            for (ui32 i = 0; i < kBlocksPerDbg; ++i) {
                for (ui32 dbg = 0; dbg < kNumDbgs; ++dbg) {
                    auto ev = std::make_unique<TEvLoad::TEvNbsRead>(
                        addressOf(dbg, i), /*sizeBytes=*/kBlockSize);
                    NTabletPipe::SendData(f.Edge, pipe, ev.release(), cookieOf(dbg, i));
                }
            }
        });

        TVector<bool> readOk(kTotal, false);
        for (ui32 got = 0; got < kTotal; ++got) {
            auto resp = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsReadResult>(
                f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            UNIT_ASSERT_C(resp->Get()->Record.GetStatus() == NBSIO_OK,
                "read cookie=" << resp->Cookie << " status=" << static_cast<int>(resp->Get()->Record.GetStatus()));
            const ui64 cookie = resp->Cookie;
            UNIT_ASSERT_C(cookie < kTotal, "cookie=" << cookie);
            UNIT_ASSERT_C(!readOk[cookie], "duplicate result for cookie=" << cookie);
            UNIT_ASSERT_C(resp->Get()->Record.HasPayloadId(),
                "missing PayloadId for cookie=" << cookie);
            const ui32 payloadId = resp->Get()->Record.GetPayloadId();
            UNIT_ASSERT_C(payloadId < resp->Get()->GetPayloadCount(),
                "bad PayloadId=" << payloadId << " PayloadCount="
                << resp->Get()->GetPayloadCount() << " for cookie=" << cookie);
            const TString payload = resp->Get()->GetPayload(payloadId).ConvertToString();
            UNIT_ASSERT_VALUES_EQUAL_C(payload.size(), kBlockSize,
                "short payload for cookie=" << cookie);
            ui64 decoded = 0;
            memcpy(&decoded, payload.data(), sizeof(decoded));
            UNIT_ASSERT_VALUES_EQUAL_C(decoded, cookie,
                "payload mismatch for cookie=" << cookie);
            readOk[cookie] = true;
        }

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // Regression for cross-DBG LSN collision. With a single DDisk group PB
    // slots are scarce, so the BSC packs both DBGs of one tablet onto the SAME
    // persistent-buffer slot instance (AllocatePersistentBuffer refcounts and
    // reuses slots; there is no cross-DBG exclusion). The original regression
    // predated the DBG index in PB record identity: independent sequences
    // starting at 1 collided on the shared slot and lost write quorum.
    // Current PB keys include the DBG index and load LSNs remain strided. Drives
    // a few writes per DBG at distinct offsets (so flushes hit different DD
    // blocks); every write must be accepted.
    Y_UNIT_TEST(MultiDbgSharedDDiskNoLsnCollision) {
        // One group => both DBGs share the same physical DDisks.
        TFixture f(/*numDDiskGroups=*/1);
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/2), NBSLT_OK);
        f.Env.Sim(TDuration::Seconds(5));

        constexpr ui32 kBlockSize = 4096;
        // A few writes per DBG: enough to overlap LSN ranges (DBG0/DBG1 both
        // start at Lsn=1 pre-fix) without overfilling the small test PB, whose
        // flush pipeline would otherwise churn under the forced full-share.
        constexpr ui32 kBlocksPerDbg = 4;
        constexpr ui32 kNumDbgs = 2;
        constexpr ui32 kTotal = kBlocksPerDbg * kNumDbgs;
        // BytesPerDbg = TargetNumVChunks(1) * VChunkSizeBytes(128MB).
        const ui64 bytesPerDbg = 128_MB;

        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            auto ev = std::make_unique<TEvLoad::TEvConfigureTablet>();
            auto& cfg = ev->Record;
            cfg.SetConfigurationId(1);
            cfg.SetMaxInflightLsns(2000); // keep all writes live so LSN ranges overlap
            cfg.SetFlushBatchSize(16);
            cfg.SetEraseBatchSize(32);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(kNumDbgs);
            cfg.SetIoSizeBytes(kBlockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });
        auto configured = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvConfigureTabletResult>(
            f.Edge, false, f.Deadline(TDuration::Seconds(30)));
        UNIT_ASSERT(configured && configured->Get()->Record.GetSuccess());

        // Distinct intra-DBG offsets per DBG: DBG0 uses blocks [0, kBlocksPerDbg)
        // and DBG1 uses [kBlocksPerDbg, 2*kBlocksPerDbg), all inside the DBG's
        // 128MB vchunk. The two DBGs therefore flush to different DD blocks (no
        // shared-DD overwrite churn), while their LSNs still collide pre-fix
        // because LSN assignment is independent of the write selector.
        auto addressOf = [&](ui32 dbg, ui32 i) -> ui64 {
            const ui64 intraOffsetBlock = static_cast<ui64>(dbg) * kBlocksPerDbg + i;
            return dbg * bytesPerDbg + intraOffsetBlock * kBlockSize;
        };
        auto cookieOf = [&](ui32 dbg, ui32 i) -> ui64 {
            return static_cast<ui64>(dbg) * kBlocksPerDbg + i;
        };

        // Interleave writes across both DBGs. At each i, DBG0 and DBG1 both emit
        // their next LSN (Lsn=i+1 pre-fix) with distinct payloads, so a shared
        // LSN triggers the duplicate-record rejection on the shared PB slot.
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            for (ui32 i = 0; i < kBlocksPerDbg; ++i) {
                for (ui32 dbg = 0; dbg < kNumDbgs; ++dbg) {
                    const ui64 cookie = cookieOf(dbg, i);
                    auto ev = std::make_unique<TEvLoad::TEvNbsWrite>(
                        addressOf(dbg, i), /*sizeBytes=*/kBlockSize);
                    TString data(kBlockSize, '\0');
                    memcpy(data.Detach(), &cookie, sizeof(cookie));
                    TFixture::AddWritePayload(*ev, TRope(std::move(data)));
                    NTabletPipe::SendData(f.Edge, pipe, ev.release(), cookie);
                }
            }
        });

        TVector<bool> writeOk(kTotal, false);
        for (ui32 got = 0; got < kTotal; ++got) {
            auto resp = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsWriteResult>(
                f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            const ui64 cookie = resp->Cookie;
            UNIT_ASSERT_C(cookie < kTotal, "cookie=" << cookie);
            UNIT_ASSERT_C(!writeOk[cookie], "duplicate ack for cookie=" << cookie);
            UNIT_ASSERT_C(resp->Get()->Record.GetStatus() == NBSIO_OK,
                "write cookie=" << cookie << " status="
                    << static_cast<int>(resp->Get()->Record.GetStatus())
                    << " reason=" << resp->Get()->Record.GetReason());
            writeOk[cookie] = true;
        }

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // Two back-to-back Run cycles with one Create. Verifies the tablet can
    // serve multiple consecutive runs from independent load actors.
    Y_UNIT_TEST(RunRunDelete) {
        TFixture f;
        const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1);
        TActorId pipe = f.OpenTabletPipe(tabletId);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletCreate(pipe, /*numDirectBlockGroups=*/1), NBSLT_OK);
        f.Env.Sim(TDuration::Seconds(5));

        auto fin1 = f.RunViaLoadActor(tabletId, /*tag=*/1, /*numDbgsToUse=*/0);
        UNIT_ASSERT(fin1.FinishedReceived);
        UNIT_ASSERT_C(fin1.ErrorReason.empty(), fin1.ErrorReason);

        auto fin2 = f.RunViaLoadActor(tabletId, /*tag=*/2, /*numDbgsToUse=*/0);
        UNIT_ASSERT(fin2.FinishedReceived);
        UNIT_ASSERT_C(fin2.ErrorReason.empty(), fin2.ErrorReason);

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    // Multi-tablet coordinator: create K tablets, run one combined load via the
    // coordinator (TNbsDbgLikeMultiLoadActor) which fans out a child proxy per
    // tablet (locally, NodeId=0), then verify the single combined finish event
    // carries both the aggregated result and a per-tablet breakdown of size K.
    Y_UNIT_TEST(MultiTablet) {
        constexpr ui32 kTablets = 2;
        // Give each tablet its own DDisk groups so the concurrent per-tablet
        // load runs do not contend on shared DDisk/PB resources.
        TFixture f(/*numDDiskGroups=*/4 * kTablets);

        TVector<ui64> tabletIds;
        TVector<TActorId> pipes;
        for (ui32 i = 0; i < kTablets; ++i) {
            const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1 + i);
            TActorId pipe = f.OpenTabletPipe(tabletId);
            // Each tablet gets a distinct storage identity (the tablet forces the
            // PB/DD owner to its own TabletID()), so their DBGs/persistent buffers
            // never collide on the same dedup namespace; the input bscTabletId is
            // ignored. See MultiTabletSharedBscTabletId for the shared-input case.
            UNIT_ASSERT_VALUES_EQUAL(
                f.TabletCreate(pipe, /*numDirectBlockGroups=*/1, /*bscTabletId=*/1 + i),
                NBSLT_OK);
            tabletIds.push_back(tabletId);
            pipes.push_back(pipe);
        }

        // Allow DDisk/PB init + peer-connect handshakes for every tablet's DBG.
        f.Env.Sim(TDuration::Seconds(10));

        auto fin = f.RunMultiViaLoadActor(tabletIds);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);

        // Combined per-tablet breakdown: one entry per target tablet.
        UNIT_ASSERT_C(fin.JsonResult.Has("tablets"),
            "combined result is missing the per-tablet 'tablets' array");
        const auto& tablets = fin.JsonResult["tablets"];
        UNIT_ASSERT_C(tablets.IsArray(), "'tablets' is not a JSON array");
        UNIT_ASSERT_VALUES_EQUAL(tablets.GetArray().size(), kTablets);

        // Every entry references one of our tablets, has no error, and exposes
        // the per-tablet metric fields the front-end renders.
        THashSet<ui64> seen;
        for (const auto& entry : tablets.GetArray()) {
            UNIT_ASSERT_C(entry.Has("tablet_id"), "per-tablet entry missing tablet_id");
            const ui64 tid = entry["tablet_id"].GetUInteger();
            UNIT_ASSERT_C(Find(tabletIds, tid) != tabletIds.end(),
                "unexpected tablet_id in breakdown: " << tid);
            UNIT_ASSERT_C(seen.insert(tid).second,
                "duplicate tablet_id in breakdown: " << tid);
            UNIT_ASSERT_C(!entry.Has("error"),
                "per-tablet run failed for tablet " << tid << ": "
                    << entry["error"].GetStringRobust());
            UNIT_ASSERT_C(entry.Has("write_rps"), "per-tablet entry missing write_rps");
        }

        for (ui32 i = 0; i < kTablets; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipes[i]), NBSLT_OK);
            f.ClosePipe(pipes[i]);
        }
    }

    // Regression: two tablets created with the SAME user-supplied bscTabletId
    // must NOT share a PB/DD dedup namespace. The tablet forces the storage
    // owner to its own (unique) TabletID(), so concurrent writes from both
    // tablets do not collide as "duplicate record with incorrect data". Before
    // the fix this run failed with NBSIO_QUORUM_LOST on every write.
    Y_UNIT_TEST(MultiTabletSharedBscTabletId) {
        constexpr ui32 kTablets = 2;
        TFixture f(/*numDDiskGroups=*/4 * kTablets);

        TVector<ui64> tabletIds;
        TVector<TActorId> pipes;
        for (ui32 i = 0; i < kTablets; ++i) {
            const ui64 tabletId = f.CreateNbsLoadTabletViaHive(/*ownerIdx=*/1 + i);
            TActorId pipe = f.OpenTabletPipe(tabletId);
            // Intentionally identical bscTabletId for every tablet: the fix must
            // make this harmless by overriding it with each tablet's TabletID().
            UNIT_ASSERT_VALUES_EQUAL(
                f.TabletCreate(pipe, /*numDirectBlockGroups=*/1, /*bscTabletId=*/9000),
                NBSLT_OK);
            tabletIds.push_back(tabletId);
            pipes.push_back(pipe);
        }

        f.Env.Sim(TDuration::Seconds(10));

        auto fin = f.RunMultiViaLoadActor(tabletIds);
        UNIT_ASSERT(fin.FinishedReceived);
        UNIT_ASSERT_C(fin.ErrorReason.empty(), fin.ErrorReason);

        UNIT_ASSERT_C(fin.JsonResult.Has("tablets"),
            "combined result is missing the per-tablet 'tablets' array");
        const auto& tablets = fin.JsonResult["tablets"];
        UNIT_ASSERT_C(tablets.IsArray(), "'tablets' is not a JSON array");
        UNIT_ASSERT_VALUES_EQUAL(tablets.GetArray().size(), kTablets);

        // No per-tablet run may report an error: distinct namespaces => no
        // duplicate-record cross-talk despite the shared input bscTabletId.
        for (const auto& entry : tablets.GetArray()) {
            UNIT_ASSERT_C(entry.Has("tablet_id"), "per-tablet entry missing tablet_id");
            const ui64 tid = entry["tablet_id"].GetUInteger();
            UNIT_ASSERT_C(!entry.Has("error"),
                "per-tablet run failed for tablet " << tid << ": "
                    << entry["error"].GetStringRobust());
        }

        for (ui32 i = 0; i < kTablets; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipes[i]), NBSLT_OK);
            f.ClosePipe(pipes[i]);
        }
    }

} // Y_UNIT_TEST_SUITE
