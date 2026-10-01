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
            TActorId pipe, ui32 numDirectBlockGroups, ui64 bscTabletId = 1)
        {
            Env.Runtime->WrapInActorContext(Edge, [&] {
                auto ev = std::make_unique<TEvLoad::TEvNbsLoadTabletAllocateGroups>();
                auto& cfg = *ev->Record.MutableAllocConfig();
                cfg.SetTabletId(bscTabletId);
                cfg.SetDDiskPoolName("ddisk_pool");
                cfg.SetPersistentBufferDDiskPoolName("ddisk_pool");
                cfg.SetNumDirectBlockGroups(numDirectBlockGroups);
                cfg.SetTargetNumVChunks(1);
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
        f.Env.Runtime->WrapInActorContext(f.Edge, [&] {
            auto ev = std::make_unique<TEvLoad::TEvConfigureTablet>();
            auto& cfg = ev->Record;
            cfg.SetMaxInflightLsns(4);
            cfg.SetFlushBatchSize(1);
            cfg.SetEraseBatchSize(1);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(1);
            cfg.SetIoSizeBytes(blockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });

        TString firstPeer;
        ui32 injectedReplies = 0;
        auto previousFilter = std::move(f.Env.Runtime->FilterFunction);
        f.Env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NDDisk::TEvWritePersistentBuffersResult::EventType) {
                auto& record = event->Get<NDDisk::TEvWritePersistentBuffersResult>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 3);
                for (const auto& sub : record.GetResult()) {
                    UNIT_ASSERT_C(sub.GetResult().GetStatus() == TStatus::OK, sub.DebugString());
                }

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

        UNIT_ASSERT_VALUES_EQUAL(f.TabletDelete(pipe), NBSLT_OK);
        f.ClosePipe(pipe);
    }

    Y_UNIT_TEST(PbWriteQuorumLossIncludesFirstFailure) {
        CheckPbWriteQuorumLoss("first PB failure");
    }

    Y_UNIT_TEST(PbWriteQuorumLossWithoutErrorReason) {
        CheckPbWriteQuorumLoss("");
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
            cfg.SetMaxInflightLsns(2000);
            cfg.SetFlushBatchSize(16);
            cfg.SetEraseBatchSize(32);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(1);
            cfg.SetIoSizeBytes(kBlockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });

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
            cfg.SetMaxInflightLsns(2000);
            cfg.SetFlushBatchSize(16);
            cfg.SetEraseBatchSize(32);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(kNumDbgs);
            cfg.SetIoSizeBytes(kBlockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });

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
            cfg.SetMaxInflightLsns(2000); // keep all writes live so LSN ranges overlap
            cfg.SetFlushBatchSize(16);
            cfg.SetEraseBatchSize(32);
            cfg.SetSyncRequestsBatchSize(1);
            cfg.SetPBufferReplyTimeoutMicroseconds(500000);
            cfg.SetNumDirectBlockGroupsToUse(kNumDbgs);
            cfg.SetIoSizeBytes(kBlockSize);
            NTabletPipe::SendData(f.Edge, pipe, ev.release());
        });

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
