#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/load_test/events.h>
#include <ydb/core/load_test/nbs_dbg_like_load.h>
#include <ydb/core/load_test/nbs_dbg_like_load_tablet.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/protos/load_test.pb.h>
#include <ydb/core/protos/tablet.pb.h>

Y_UNIT_TEST_SUITE(NbsDbgLikeLoadTablet) {

    // End-to-end Create -> Run -> Delete fixture against a real BSController +
    // DDisk pool + Hive-managed NbsLoadTablet running in TTestActorSystem.
    struct TFixture {
        TEnvironmentSetup Env;
        TActorId Edge;

        TFixture()
            : Env({
                .NodeCount = 8,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
                .ConfigPreprocessor = [](ui32, TNodeWardenConfig& cfg) {
                    NYdb::NBS::NProto::TPBufferConfig pb;
                    pb.SetMaxChunks(10);
                    pb.SetMaxInMemoryCache(128_MB);
                    cfg.PBufferConfig = pb;
                },
                .SetupHive = true})
        {
            //Env.Runtime->SetLogPriority(NKikimrServices::BS_LOAD_TEST, NLog::PRI_TRACE);
            //Env.Runtime->SetLogPriority(NKikimrServices::BS_DDISK, NLog::PRI_TRACE);

            Env.CreateBoxAndPool();
            Env.Sim(TDuration::Seconds(30));

            DefineDDiskPool(/*numDDiskGroups=*/4);

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
            ui32 numDirectBlockGroupsToUse = 0)
        {
            TEvLoadTestRequest::TNbsDbgLikeLoad cmd;
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

    // End-to-end data-integrity test that drives the merged tablet directly,
    // bypassing the load-actor / Run path. Spec §12.1: TEvNbsWrite/Read
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
                const ui32 payloadId = ev->AddPayload(TRope(std::move(data)));
                ev->Record.SetPayloadId(payloadId);
                NTabletPipe::SendData(f.Edge, pipe, ev.release(), /*cookie=*/i);
            }
        });

        // Drain 1000 OK write acks; cookies must cover [0, kNumBlocks).
        TVector<bool> writeOk(kNumBlocks, false);
        for (ui32 got = 0; got < kNumBlocks; ++got) {
            auto resp = f.Env.WaitForEdgeActorEvent<TEvLoad::TEvNbsWriteResult>(
                f.Edge, /*termOnCapture=*/false, f.Deadline(TDuration::Seconds(120)));
            UNIT_ASSERT(resp);
            UNIT_ASSERT_VALUES_EQUAL_C(resp->Get()->Record.GetStatus(), 0u,
                "write i=" << resp->Cookie << " status=" << resp->Get()->Record.GetStatus());
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
            UNIT_ASSERT_VALUES_EQUAL_C(resp->Get()->Record.GetStatus(), 0u,
                "read i=" << resp->Cookie << " status=" << resp->Get()->Record.GetStatus());
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

} // Y_UNIT_TEST_SUITE
