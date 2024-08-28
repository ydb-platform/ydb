#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/key.h>
#include <ydb/core/persqueue/partition.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/public/lib/base/msgbus.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NPQ {

void FillPQConfig(NKikimrPQ::TPQConfig& pqConfig, const TString& dbRoot, bool isFirstClass) {
    pqConfig.SetEnabled(true);
    // NOTE(shmel1k@): KIKIMR-14221
    pqConfig.SetTopicsAreFirstClassCitizen(isFirstClass);
    pqConfig.SetRequireCredentialsInNewProtocol(false);
    pqConfig.SetRoot(dbRoot);
    pqConfig.SetClusterTablePath(TStringBuilder() << dbRoot << "/Config/V2/Cluster");
    pqConfig.SetVersionTablePath(TStringBuilder() << dbRoot << "/Config/V2/Versions");
    pqConfig.MutableQuotingConfig()->SetEnableQuoting(false);
}

void PQTabletPrepare(const TTabletPreparationParameters& parameters,
                     const TVector<std::pair<TString, bool>>& users,
                     TTestActorRuntime& runtime,
                     ui64 tabletId,
                     TActorId edge) {
    TAutoPtr<IEventHandle> handle;
    static int version = 0;
    if (parameters.specVersion) {
        version = parameters.specVersion;
    } else {
        ++version;
    }
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            runtime.ResetScheduledCount();

            THolder<TEvPersQueue::TEvUpdateConfig> request(new TEvPersQueue::TEvUpdateConfig());
            for (ui32 i = 0; i < parameters.partitions; ++i) {
                request->Record.MutableTabletConfig()->AddPartitionIds(i);
            }
            request->Record.MutableTabletConfig()->SetCacheSize(10_MB);
            request->Record.SetTxId(12345);
            auto* tabletConfig = request->Record.MutableTabletConfig();
            if (runtime.GetAppData().PQConfig.GetTopicsAreFirstClassCitizen()) {
                tabletConfig->SetTopicName("topic");
                tabletConfig->SetTopicPath(runtime.GetAppData().PQConfig.GetDatabase() + "/topic");
                tabletConfig->SetYcCloudId(parameters.cloudId);
                tabletConfig->SetYcFolderId(parameters.folderId);
                tabletConfig->SetYdbDatabaseId(parameters.databaseId);
                tabletConfig->SetYdbDatabasePath(parameters.databasePath);
                tabletConfig->SetFederationAccount(parameters.account);
            } else {
                tabletConfig->SetTopicName("rt3.dc1--asdfgs--topic");
                tabletConfig->SetTopicPath("/Root/PQ/rt3.dc1--asdfgs--topic");
            }
            tabletConfig->SetTopic("topic");
            tabletConfig->SetVersion(version);
            tabletConfig->SetLocalDC(parameters.localDC);
            tabletConfig->AddReadRules("user");
            tabletConfig->AddReadFromTimestampsMs(parameters.readFromTimestampsMs);
            tabletConfig->SetMeteringMode(parameters.meteringMode);
            auto partitionConfig = tabletConfig->MutablePartitionConfig();
            if (parameters.writeSpeed > 0) {
                partitionConfig->SetWriteSpeedInBytesPerSecond(parameters.writeSpeed);
                partitionConfig->SetBurstSize(parameters.writeSpeed);
            }

            partitionConfig->SetMaxCountInPartition(parameters.maxCountInPartition);
            partitionConfig->SetMaxSizeInPartition(parameters.maxSizeInPartition);
            if (parameters.storageLimitBytes > 0) {
                partitionConfig->SetStorageLimitBytes(parameters.storageLimitBytes);
            } else {
                partitionConfig->SetLifetimeSeconds(parameters.deleteTime);
            }
            partitionConfig->SetSourceIdLifetimeSeconds(TDuration::Hours(1).Seconds());
            if (parameters.sidMaxCount > 0)
                partitionConfig->SetSourceIdMaxCounts(parameters.sidMaxCount);
            partitionConfig->SetMaxWriteInflightSize(90'000'000);
            partitionConfig->SetLowWatermark(parameters.lowWatermark);

            for (auto& u : users) {
                if (u.second)
                    partitionConfig->AddImportantClientId(u.first);
                if (u.first != "user")
                    tabletConfig->AddReadRules(u.first);
            }

            runtime.SendToPipe(tabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());
            TEvPersQueue::TEvUpdateConfigResponse* result =
                runtime.GrabEdgeEvent<TEvPersQueue::TEvUpdateConfigResponse>(handle);

            UNIT_ASSERT(result);
            auto& rec = result->Record;
            UNIT_ASSERT(rec.HasStatus() && rec.GetStatus() == NKikimrPQ::OK);
            UNIT_ASSERT(rec.HasTxId() && rec.GetTxId() == 12345);
            UNIT_ASSERT(rec.HasOrigin() && result->GetOrigin() == tabletId);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT(retriesLeft >= 1);
        }
    }
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {

            request.Reset(new TEvKeyValue::TEvRequest);
            auto read = request->Record.AddCmdRead();
            read->SetKey("_config");

            runtime.SendToPipe(tabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = runtime.GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT(retriesLeft >= 1);
        }
    }
}

void PQTabletPrepare(const TTabletPreparationParameters& parameters,
                     const TVector<std::pair<TString, bool>>& users,
                     TTestContext& context) {
    PQTabletPrepare(parameters, users, *context.Runtime, context.TabletId, context.Edge);
}


void CmdGetOffset(const ui32 partition, const TString& user, i64 expectedOffset, TTestContext& tc, i64 ctime,
                  ui64 writeTime) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    THolder<TEvPersQueue::TEvRequest> request;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvPersQueue::TEvRequest);
            auto req = request->Record.MutablePartitionRequest();
            req->SetPartition(partition);
            auto off = req->MutableCmdGetClientOffset();
            off->SetClientId(user);
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());

            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }

            UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
            UNIT_ASSERT(result->Record.GetPartitionResponse().HasCmdGetClientOffsetResult());
            auto resp = result->Record.GetPartitionResponse().GetCmdGetClientOffsetResult();
            if (ctime != -1) {
                UNIT_ASSERT_EQUAL(resp.HasCreateTimestampMS(), ctime > 0);
                if (ctime > 0) {
                    if (ctime == Max<i64>()) {
                        UNIT_ASSERT(resp.GetCreateTimestampMS() + 86'000'000 < TAppData::TimeProvider->Now().MilliSeconds());
                    } else {
                        UNIT_ASSERT_EQUAL((i64)resp.GetCreateTimestampMS(), ctime);
                    }
                }
            }
            UNIT_ASSERT_C((expectedOffset == -1 && !resp.HasOffset()) || (i64)resp.GetOffset() == expectedOffset,
                    "expectedOffset=" << expectedOffset << " resp.HasOffset()=" << resp.HasOffset() << " resp.GetOffset()=" << resp.GetOffset());
            if (writeTime > 0) {
                UNIT_ASSERT(resp.HasWriteTimestampEstimateMS());
                UNIT_ASSERT(resp.GetWriteTimestampEstimateMS() >= writeTime);
            }
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
}

void PQBalancerPrepare(const TString topic, const TVector<std::pair<ui32, std::pair<ui64, ui32>>>& map, const ui64 ssId,
                       TTestContext& context, const bool requireAuth, bool kill) {
    PQBalancerPrepare(topic, map, ssId, *context.Runtime, context.BalancerTabletId, context.Edge, requireAuth, kill);
}

void PQBalancerPrepare(const TString topic, const TVector<std::pair<ui32, std::pair<ui64, ui32>>>& map, const ui64 ssId,
                       TTestActorRuntime& runtime, ui64 balancerTabletId, TActorId edge, const bool requireAuth, bool kill) {
    TAutoPtr<IEventHandle> handle;
    static int version = 0;
    ++version;

    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            runtime.ResetScheduledCount();

            THolder<TEvPersQueue::TEvUpdateBalancerConfig> request(new TEvPersQueue::TEvUpdateBalancerConfig());
            for (const auto& p : map) {
                auto part = request->Record.AddPartitions();
                part->SetPartition(p.first);
                part->SetGroup(p.second.second);
                part->SetTabletId(p.second.first);
                part->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Active);

                auto tablet = request->Record.AddTablets();
                tablet->SetTabletId(p.second.first);
                tablet->SetOwner(1);
                tablet->SetIdx(p.second.first);

                auto* pp = request->Record.MutableTabletConfig()->AddPartitions();
                pp->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Active);
            }
            request->Record.SetTxId(12345);
            request->Record.SetPathId(1);
            request->Record.SetVersion(version);
            request->Record.SetTopicName(topic);
            request->Record.SetPath("/Root/" + topic);
            request->Record.SetSchemeShardId(ssId);
            request->Record.MutableTabletConfig()->AddReadRules("client");
            request->Record.MutableTabletConfig()->SetRequireAuthWrite(requireAuth);
            request->Record.MutableTabletConfig()->SetRequireAuthRead(requireAuth);

            runtime.SendToPipe(balancerTabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());
            TEvPersQueue::TEvUpdateConfigResponse* result = runtime.GrabEdgeEvent<TEvPersQueue::TEvUpdateConfigResponse>(handle);

            UNIT_ASSERT(result);
            auto& rec = result->Record;
            UNIT_ASSERT(rec.HasStatus() && rec.GetStatus() == NKikimrPQ::OK);
            UNIT_ASSERT(rec.HasTxId() && rec.GetTxId() == 12345);
            UNIT_ASSERT(rec.HasOrigin() && result->GetOrigin() == balancerTabletId);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT(retriesLeft >= 1);
        }
    }
    //TODO: check state
    if (kill) {
        ForwardToTablet(runtime, balancerTabletId, edge, new TEvents::TEvPoisonPill());
        TDispatchOptions rebootOptions;
        rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 2));
        runtime.DispatchEvents(rebootOptions);
    }
}

void PQGetPartInfo(ui64 startOffset, ui64 endOffset, TTestContext& tc) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvOffsetsResponse *result;
    THolder<TEvPersQueue::TEvOffsets> request;

    for (i32 retriesLeft = 3; retriesLeft > 0; --retriesLeft) {
        try {

            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvPersQueue::TEvOffsets);

            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvOffsetsResponse>(handle);
            UNIT_ASSERT(result);

            if (result->Record.PartResultSize() == 0 ||
                result->Record.GetPartResult(0).GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }

            UNIT_ASSERT(result->Record.PartResultSize());
            UNIT_ASSERT_VALUES_EQUAL((ui64)result->Record.GetPartResult(0).GetStartOffset(), startOffset);
            UNIT_ASSERT_VALUES_EQUAL((ui64)result->Record.GetPartResult(0).GetEndOffset(), endOffset);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT(retriesLeft > 0);
        }
    }
}

void PQTabletRestart(TTestContext& tc) {
    PQTabletRestart(*tc.Runtime, tc.TabletId, tc.Edge);
}

void PQTabletRestart(TTestActorRuntime& runtime, ui64 tabletId, TActorId edge) {
    ForwardToTablet(runtime, tabletId, edge, new TEvents::TEvPoisonPill());
    TDispatchOptions rebootOptions;
    rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 2));
    runtime.DispatchEvents(rebootOptions);
}

TActorId SetOwner(const ui32 partition, TTestContext& tc, const TString& owner, bool force) {
    return SetOwner(tc.Runtime.Get(), tc.TabletId, tc.Edge, partition, owner, force);
}

TActorId SetOwner(TTestActorRuntime* runtime, ui64 tabletId, const TActorId& sender, const ui32 partition, const TString& owner, bool force) {
    TActorId pipeClient = runtime->ConnectToPipe(tabletId, sender, 0, GetPipeConfigWithRetries());

    THolder<TEvPersQueue::TEvRequest> request;

    request.Reset(new TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();
    req->SetPartition(partition);
    req->MutableCmdGetOwnership()->SetOwner(owner);
    req->MutableCmdGetOwnership()->SetForce(force);
    ActorIdToProto(pipeClient, req->MutablePipeClient());

    runtime->SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries(), pipeClient);
    return pipeClient;
}

TActorId RegisterReadSession(const TString& session, TTestContext& tc, const TVector<ui32>& groups) {
    TActorId pipeClient = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());

    THolder<TEvPersQueue::TEvRegisterReadSession> request;

    request.Reset(new TEvPersQueue::TEvRegisterReadSession);
    auto& req = request->Record;
    req.SetSession(session);
    ActorIdToProto(pipeClient, req.MutablePipeClient());
    req.SetClientId("user");
    for (const auto& g : groups) {
        req.AddGroups(g);
    }

    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries(), pipeClient);
    return pipeClient;
}

void WaitReadSessionKill(TTestContext& tc) {
    TAutoPtr<IEventHandle> handle;

    tc.Runtime->ResetScheduledCount();

    TEvPersQueue::TEvError *result;
    result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvError>(handle);
    UNIT_ASSERT(result);
    Cerr << "ANS: " << result->Record << "\n";
//    UNIT_ASSERT_EQUAL(result->Record.GetSession(), session);
}

void WaitPartition(const TString &session, TTestContext& tc, ui32 partition, const TString& sessionToRelease, const TString& topic, const TActorId& pipe, bool ok) {
    TAutoPtr<IEventHandle> handle;

    tc.Runtime->ResetScheduledCount();

    for (ui32 i = 0; i < 3; ++i) {
        Cerr << "STEP " << i << " ok " << ok << "\n";

        try {
            tc.Runtime->ResetScheduledCount();
            if (i % 2 == 0) {
                TEvPersQueue::TEvLockPartition *result;
                result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(handle);
                UNIT_ASSERT(result);
                Cerr << "ANS: " << result->Record << "\n";
                UNIT_ASSERT(ok);
                UNIT_ASSERT_EQUAL(result->Record.GetSession(), session);
                break;
            } else {
                TEvPersQueue::TEvReleasePartition *result;
                result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReleasePartition>(handle);
                UNIT_ASSERT(result);

                Cerr << "ANS2: " << result->Record << "\n";

                UNIT_ASSERT_EQUAL(result->Record.GetSession(), sessionToRelease);
                UNIT_ASSERT(ok);

                auto request = MakeHolder<TEvPersQueue::TEvPartitionReleased>();

                auto& req = request->Record;
                req.SetSession(sessionToRelease);
                req.SetPartition(partition);
                req.SetTopic(topic);
                req.SetClientId("user");
                ActorIdToProto(pipe, req.MutablePipeClient());

                tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries(), pipe);
            }
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_C(i < 2 || !ok, "TSchedulingLimitReachedException i=" << i << " ok=" << ok);
        } catch (NActors::TEmptyEventQueueException) {
            UNIT_ASSERT_C(i < 2 || !ok, "TEmptyEventQueueException i=" << i << " ok=" << ok);
        }
    }
}

std::pair<TString, TActorId> CmdSetOwner(const ui32 partition, TTestContext& tc, const TString& owner, bool force) {
    return CmdSetOwner(tc.Runtime.Get(), tc.TabletId, tc.Edge, partition, owner, force);
}

std::pair<TString, TActorId> CmdSetOwner(TTestActorRuntime* runtime, ui64 tabletId, const TActorId& sender, const ui32 partition, const TString& owner, bool force) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    TString cookie;
    TActorId pipeClient;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            runtime->ResetScheduledCount();

            pipeClient = SetOwner(runtime, tabletId, sender, partition, owner, force);

            result = runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }

            if (result->Record.GetErrorReason().StartsWith("ownership session is killed by another session with id ")) {
                result = runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
                UNIT_ASSERT(result);
                UNIT_ASSERT(result->Record.HasStatus());
            }

            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }

            UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);

            UNIT_ASSERT(result->Record.HasPartitionResponse());
            UNIT_ASSERT(result->Record.GetPartitionResponse().HasCmdGetOwnershipResult());
            UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdGetOwnershipResult().HasOwnerCookie());
            cookie = result->Record.GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
            UNIT_ASSERT(!cookie.empty());
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            Cerr << "SCHEDULER LIMIT REACHED\n";
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
    return std::make_pair(cookie, pipeClient);
}

void WritePartData(const ui32 partition, const TString& sourceId, const i64 offset, const ui64 seqNo, const ui16 partNo, const ui16 totalParts,
                    const ui32 totalSize, const TString& data, TTestContext& tc, const TString& cookie, i32 msgSeqNo) {
    THolder<TEvPersQueue::TEvRequest> request;
    tc.Runtime->ResetScheduledCount();
    request.Reset(new TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();
    req->SetPartition(partition);
    req->SetOwnerCookie(cookie);
    req->SetMessageNo(msgSeqNo);
    if (offset != -1)
        req->SetCmdWriteOffset(offset);
    auto write = req->AddCmdWrite();
    write->SetSourceId(sourceId);
    write->SetSeqNo(seqNo);
    write->SetPartNo(partNo);
    write->SetTotalParts(totalParts);
    if (partNo == 0)
        write->SetTotalSize(totalSize);
    write->SetData(data);

    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
}

void WritePartDataWithBigMsg(const ui32 partition, const TString& sourceId, const ui64 seqNo, const ui16 partNo, const ui16 totalParts,
                    const ui32 totalSize, const TString& data, TTestContext& tc, const TString& cookie, i32 msgSeqNo, ui32 bigMsgSize) {
    THolder<TEvPersQueue::TEvRequest> request;
    tc.Runtime->ResetScheduledCount();
    request.Reset(new TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();
    req->SetPartition(partition);
    req->SetOwnerCookie(cookie);
    req->SetMessageNo(msgSeqNo);

    TString bigData(bigMsgSize, 'a');

    auto write = req->AddCmdWrite();
    write->SetSourceId(sourceId);
    write->SetSeqNo(seqNo);
    write->SetData(bigData);

    write = req->AddCmdWrite();
    write->SetSourceId(sourceId);
    write->SetSeqNo(seqNo + 1);
    write->SetPartNo(partNo);
    write->SetTotalParts(totalParts);
    if (partNo == 0)
        write->SetTotalSize(totalSize);
    write->SetData(data);


    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
}

void WriteData(const ui32 partition, const TString& sourceId, const TVector<std::pair<ui64, TString>> data, TTestContext& tc,
               const TString& cookie, i32 msgSeqNo, i64 offset, bool disableDeduplication) {
    WriteData(tc.Runtime.Get(), tc.TabletId, tc.Edge, partition, sourceId, data, cookie, msgSeqNo, offset, disableDeduplication);
}

void WriteData(TTestActorRuntime* runtime, ui64 tabletId, const TActorId& sender, const ui32 partition, const TString& sourceId,
               const TVector<std::pair<ui64, TString>> data, const TString& cookie, i32 msgSeqNo, i64 offset, bool disableDeduplication) {
    THolder<TEvPersQueue::TEvRequest> request;
    runtime->ResetScheduledCount();
    request.Reset(new TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();
    req->SetPartition(partition);
    req->SetOwnerCookie(cookie);
    req->SetMessageNo(msgSeqNo);
    if (offset >= 0)
        req->SetCmdWriteOffset(offset);
    for (auto& p : data) {
        auto write = req->AddCmdWrite();
        write->SetSourceId(sourceId);
        write->SetSeqNo(p.first);
        write->SetData(p.second);
        write->SetDisableDeduplication(disableDeduplication);
    }
    runtime->SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());
}

void CmdWrite(const ui32 partition, const TString& sourceId, const TVector<std::pair<ui64, TString>> data,
              TTestContext& tc, bool error, const THashSet<ui32>& alreadyWrittenSeqNo,
              bool isFirst, const TString& ownerCookie, i32 msn, i64 offset,
              bool treatWrongCookieAsError, bool treatBadOffsetAsError,
              bool disableDeduplication) {
    CmdWrite(tc.Runtime.Get(), tc.TabletId, tc.Edge, partition, sourceId, tc.MsgSeqNoMap[partition],
            data, error, alreadyWrittenSeqNo, isFirst, ownerCookie, msn, offset, treatWrongCookieAsError, treatBadOffsetAsError, disableDeduplication);

}

void CmdWrite(TTestActorRuntime* runtime, ui64 tabletId, const TActorId& sender, const ui32 partition,
              const TString& sourceId, ui32& msgSeqNo, const TVector<std::pair<ui64, TString>> data,
              bool error, const THashSet<ui32>& alreadyWrittenSeqNo,
              bool isFirst, const TString& ownerCookie, i32 msn, i64 offset,
              bool treatWrongCookieAsError, bool treatBadOffsetAsError,
              bool disableDeduplication) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;

    if (msn != -1) msgSeqNo = msn;
    TString cookie = ownerCookie;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            WriteData(runtime, tabletId, sender, partition, sourceId, data, cookie, msgSeqNo, offset, disableDeduplication);
            result = runtime->GrabEdgeEventIf<TEvPersQueue::TEvResponse>(handle,
                [](const TEvPersQueue::TEvResponse& ev){
                    if (ev.Record.HasPartitionResponse() &&
                        ev.Record.GetPartitionResponse().CmdWriteResultSize() > 0 ||
                        ev.Record.GetErrorCode() != NPersQueue::NErrorCode::OK)
                        return true;
                    return false;
            }); //there could be outgoing reads in TestReadSubscription test

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }

            if (!treatWrongCookieAsError &&
                result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRONG_COOKIE) {
                cookie = CmdSetOwner(runtime, tabletId, sender, partition).first;
                msgSeqNo = 0;
                retriesLeft = 3;
                continue;
            }

            if (!treatBadOffsetAsError &&
                result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRITE_ERROR_BAD_OFFSET) {
                return;
            }

            if (error) {
                UNIT_ASSERT(
                    result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_INACTIVE ||
                    result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_IS_FULL ||
                    result->Record.GetErrorCode() == NPersQueue::NErrorCode::BAD_REQUEST ||
                    result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRONG_COOKIE
                );
                break;
            } else {
                Cerr << result->Record.GetErrorReason();
                UNIT_ASSERT_VALUES_EQUAL((ui32)result->Record.GetErrorCode(), (ui32)NPersQueue::NErrorCode::OK);
            }
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetPartitionResponse().CmdWriteResultSize(), data.size());

            for (ui32 i = 0; i < data.size(); ++i) {
                UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdWriteResult(i).HasAlreadyWritten());
                UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdWriteResult(i).HasOffset());
                UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdWriteResult(i).HasMaxSeqNo() ==
                                result->Record.GetPartitionResponse().GetCmdWriteResult(i).GetAlreadyWritten());
                if (result->Record.GetPartitionResponse().GetCmdWriteResult(i).HasMaxSeqNo()) {
                    UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdWriteResult(i).GetMaxSeqNo() >= (i64)data[i].first);
                }
                if (isFirst || offset != -1) {
                    UNIT_ASSERT(result->Record.GetPartitionResponse().GetCmdWriteResult(i).GetAlreadyWritten()
                                      || result->Record.GetPartitionResponse().GetCmdWriteResult(i).GetOffset() == i + (offset == -1 ? 0 : offset));
                }
            }
            for (ui32 i = 0; i < data.size(); ++i) {
                auto res = result->Record.GetPartitionResponse().GetCmdWriteResult(i);
                UNIT_ASSERT(!alreadyWrittenSeqNo.contains(res.GetSeqNo()) || res.GetAlreadyWritten());
            }
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
            retriesLeft = 3;
        }
    }
    ++msgSeqNo;
}

void ReserveBytes(const ui32 partition, TTestContext& tc,
               const TString& cookie, i32 msgSeqNo, i64 size, const TActorId& pipeClient, bool lastRequest) {
    THolder<TEvPersQueue::TEvRequest> request;
    tc.Runtime->ResetScheduledCount();
    request.Reset(new TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();
    req->SetPartition(partition);
    req->SetOwnerCookie(cookie);
    req->SetMessageNo(msgSeqNo);
    ActorIdToProto(pipeClient, req->MutablePipeClient());
    req->MutableCmdReserveBytes()->SetSize(size);
    req->MutableCmdReserveBytes()->SetLastRequest(lastRequest);
    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());

    tc.Runtime->DispatchEvents();
}

void CmdReserveBytes(const ui32 partition, TTestContext& tc, const TString& ownerCookie, i32 msn, i64 size, TActorId pipeClient, bool noAnswer, bool lastRequest) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;

    ui32& msgSeqNo = tc.MsgSeqNoMap[partition];
    if (msn != -1) msgSeqNo = msn;
    TString cookie = ownerCookie;

    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            ReserveBytes(partition, tc, cookie, msgSeqNo, size, pipeClient, lastRequest);
            result = tc.Runtime->GrabEdgeEventIf<TEvPersQueue::TEvResponse>(handle, [](const TEvPersQueue::TEvResponse& ev){
                if (!ev.Record.HasPartitionResponse() || !ev.Record.GetPartitionResponse().HasCmdReadResult())
                    return true;
                return false;
            }); //there could be outgoing reads in TestReadSubscription test

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());

            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                retriesLeft = 3;
                continue;
            }

            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRONG_COOKIE) {
                auto p = CmdSetOwner(partition, tc);
                pipeClient = p.second;
                cookie = p.first;
                msgSeqNo = 0;
                retriesLeft = 3;
                continue;
            }
            UNIT_ASSERT(!noAnswer);

            UNIT_ASSERT_C(result->Record.GetErrorCode() == NPersQueue::NErrorCode::OK, result->Record);

            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            if (noAnswer)
                break;
            UNIT_ASSERT(retriesLeft == 2);
        }
    }
    ++msgSeqNo;
}


void CmdSetOffset(const ui32 partition, const TString& user, ui64 offset, bool error, TTestContext& tc, const TString& session) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    THolder<TEvPersQueue::TEvRequest> request;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvPersQueue::TEvRequest);
            auto req = request->Record.MutablePartitionRequest();
            req->SetPartition(partition);
            auto off = req->MutableCmdSetClientOffset();
            off->SetClientId(user);
            off->SetOffset(offset);
            if (!session.empty())
                off->SetSessionId(session);
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }
            if ((result->Record.GetErrorCode() == NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE ||
                 result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRONG_COOKIE) && error) {
                break;
            }
            UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
}


TActorId CmdCreateSession(const TPQCmdSettings& settings, TTestContext& tc) {

    TActorId pipeClient = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
    TActorId tabletPipe = tc.Runtime->ConnectToPipe(tc.TabletId, tc.Edge, 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    THolder<TEvPersQueue::TEvRequest> request;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvPersQueue::TEvRequest);
            auto req = request->Record.MutablePartitionRequest();

            ActorIdToProto(tabletPipe, req->MutablePipeClient());
            Cerr << "Set pipe for create session: " << tabletPipe.ToString() << Endl;

            req->SetPartition(settings.Partition);
            auto off = req->MutableCmdCreateSession();
            off->SetClientId(settings.User);
            off->SetSessionId(settings.Session);
            off->SetGeneration(settings.Generation);
            off->SetStep(settings.Step);
            off->SetPartitionSessionId(settings.PartitionSessionId);

            if (settings.KeepPipe) {
                tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries(), tabletPipe);
            } else {
                tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            }
            result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }

            if (settings.ToFail) {
                UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::WRONG_COOKIE);
                return pipeClient;
            }

            UNIT_ASSERT_EQUAL_C(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK, result->Record.DebugString());

            UNIT_ASSERT(result->Record.GetPartitionResponse().HasCmdGetClientOffsetResult());
            auto resp = result->Record.GetPartitionResponse().GetCmdGetClientOffsetResult();
            UNIT_ASSERT(resp.HasOffset() && (i64)resp.GetOffset() == settings.Offset);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
    return tabletPipe;
}

void CmdKillSession(const ui32 partition, const TString& user, const TString& session, TTestContext& tc, const TActorId& pipe) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    THolder<TEvPersQueue::TEvRequest> request;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvPersQueue::TEvRequest);
            auto req = request->Record.MutablePartitionRequest();
            req->SetPartition(partition);
            auto off = req->MutableCmdDeleteSession();
            off->SetClientId(user);
            off->SetSessionId(session);
            if (pipe) {
                ActorIdToProto(pipe, req->MutablePipeClient());
            }
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }
            UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
}


void CmdUpdateWriteTimestamp(const ui32 partition, ui64 timestamp, TTestContext& tc) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    THolder<TEvPersQueue::TEvRequest> request;
    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvPersQueue::TEvRequest);
            auto req = request->Record.MutablePartitionRequest();
            req->SetPartition(partition);
            auto off = req->MutableCmdUpdateWriteTimestamp();
            off->SetWriteTimeMS(timestamp);
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());

            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            }

            UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
}


TVector<TString> CmdSourceIdRead(TTestContext& tc) {
    TAutoPtr<IEventHandle> handle;
    TVector<TString> sourceIds;
    THolder<TEvKeyValue::TEvRequest> request;
    TEvKeyValue::TEvResponse *result;

    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            request.Reset(new TEvKeyValue::TEvRequest);
            sourceIds.clear();
            auto read = request->Record.AddCmdReadRange();
            auto range = read->MutableRange();
            NPQ::TKeyPrefix ikeyFrom(NPQ::TKeyPrefix::TypeInfo, TPartitionId(0), NPQ::TKeyPrefix::MarkProtoSourceId);
            range->SetFrom(ikeyFrom.Data(), ikeyFrom.Size());
            range->SetIncludeFrom(true);
            NPQ::TKeyPrefix ikeyTo(NPQ::TKeyPrefix::TypeInfo, TPartitionId(0), NPQ::TKeyPrefix::MarkUserDeprecated);
            range->SetTo(ikeyTo.Data(), ikeyTo.Size());
            range->SetIncludeTo(false);
            Cout << request.Get()->ToString() << Endl;
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
            UNIT_ASSERT(result);
            Cout << result->ToString() << Endl;
            UNIT_ASSERT(result->Record.HasStatus());
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
            for (ui64 idx = 0; idx < result->Record.ReadRangeResultSize(); ++idx) {
                const auto &readResult = result->Record.GetReadRangeResult(idx);
                UNIT_ASSERT(readResult.HasStatus());
                UNIT_ASSERT_EQUAL(readResult.GetStatus(), NKikimrProto::OK);
                for (size_t j = 0; j < readResult.PairSize(); ++j) {
                    const auto& pair = readResult.GetPair(j);
                    TString s = pair.GetKey().substr(NPQ::TKeyPrefix::MarkedSize());
                    sourceIds.push_back(s);
                }
            }
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
    return sourceIds;
}

bool CheckCmdReadResult(const TPQCmdReadSettings& settings, TEvPersQueue::TEvResponse* result) {
    Y_UNUSED(settings);

    UNIT_ASSERT(result);
    UNIT_ASSERT(result->Record.HasStatus());

    UNIT_ASSERT(result->Record.HasPartitionResponse());
    UNIT_ASSERT_EQUAL(result->Record.GetPartitionResponse().GetCookie(), 123);
    if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
        return false;
    }
    if (settings.Timeout) {
        UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
        UNIT_ASSERT(result->Record.GetPartitionResponse().HasCmdReadResult());
        auto res = result->Record.GetPartitionResponse().GetCmdReadResult();
        UNIT_ASSERT_EQUAL(res.ResultSize(), 0);
        return true;
    }
    if (settings.ToFail) {
        UNIT_ASSERT_C(result->Record.GetErrorCode() != NPersQueue::NErrorCode::OK, result->Record.DebugString());
        return true;
    }
    UNIT_ASSERT_EQUAL_C(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK, result->Record.DebugString());
    if (!settings.DirectReadId) {
        UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdReadResult(), result->Record.GetPartitionResponse().DebugString());
        auto res = result->Record.GetPartitionResponse().GetCmdReadResult();

        UNIT_ASSERT_EQUAL(res.ResultSize(), settings.ResCount);
        ui64 off = settings.Offset;

        for (ui32 i = 0; i < settings.ResCount; ++i) {
            auto r = res.GetResult(i);
            if (settings.Offsets.empty()) {
                if (settings.ReadTimestampMs == 0) {
                    UNIT_ASSERT_EQUAL((ui64)r.GetOffset(), off);
                }
                UNIT_ASSERT(r.GetSourceId().size() == 9 && r.GetSourceId().StartsWith("sourceid"));
                UNIT_ASSERT_EQUAL(ui32(r.GetData()[0]), off);
                UNIT_ASSERT_EQUAL(ui32((unsigned char)r.GetData().back()), r.GetSeqNo() % 256);
                ++off;
            } else {
                UNIT_ASSERT(settings.Offsets[i] == (i64)r.GetOffset());
            }
        }
    } else {
        UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdPrepareReadResult(), result->Record.GetPartitionResponse().DebugString());
        auto res = result->Record.GetPartitionResponse().GetCmdPrepareReadResult();
        UNIT_ASSERT(res.GetBytesSizeEstimate() > 0);
        UNIT_ASSERT(res.GetEndOffset() > 0);
        UNIT_ASSERT_VALUES_EQUAL(res.GetDirectReadId(), settings.DirectReadId);
    }
    return true;
}

void CmdRead(
        const ui32 partition, const ui64 offset, const ui32 count, const ui32 size, const ui32 resCount, bool timeouted,
        TTestContext& tc, TVector<i32> offsets, const ui32 maxTimeLagMs, const ui64 readTimestampMs, const TString user
) {
    return CmdRead(
            TPQCmdReadSettings("", partition, offset, count, size, resCount, timeouted,
                               offsets, maxTimeLagMs, readTimestampMs, user),
            tc
    );
}

void CmdRead(const TPQCmdReadSettings& settings, TTestContext& tc) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    THolder<TEvPersQueue::TEvRequest> request;

    for (ui32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvPersQueue::TEvRequest);
            auto req = request->Record.MutablePartitionRequest();
            req->SetPartition(settings.Partition);
            auto read = req->MutableCmdRead();
            read->SetOffset(settings.Offset);
            read->SetSessionId(settings.Session);
            read->SetClientId(settings.User);
            read->SetCount(settings.Count);
            read->SetBytes(settings.Size);
            if (settings.MaxTimeLagMs > 0) {
                read->SetMaxTimeLagMs(settings.MaxTimeLagMs);
            }
            if (settings.ReadTimestampMs > 0) {
                read->SetReadTimestampMs(settings.ReadTimestampMs);
            }
            if (settings.DirectReadId > 0) {
                read->SetDirectReadId(settings.DirectReadId);
            }
            if (settings.PartitionSessionId > 0) {
                read->SetPartitionSessionId(settings.PartitionSessionId);
            }
            if (settings.Pipe) {
                ActorIdToProto(settings.Pipe, req->MutablePipeClient());
            }

            req->SetCookie(123);

            Cerr << "Send read request: " << request->Record.DebugString() << " via pipe: " << tc.Edge.ToString() << Endl;

            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

            auto checkRes = CheckCmdReadResult(settings, result);
            if (!checkRes) {
                tc.Runtime->DispatchEvents();   // Dispatch events so that initialization can make progress
                retriesLeft = 3;
                continue;
            } else {
                break;
            }
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
        }
    }
}

template <class TProto>
void FillDirectReadKey(TProto* proto, const TCmdDirectReadSettings& settings) {
    proto->SetDirectReadId(settings.DirectReadId);
    auto* key = proto->MutableSessionKey();
    key->SetSessionId(settings.Session);
    key->SetPartitionSessionId(settings.PartitionSessionId);
}

template <class TEvent>
void CheckDirectReadEvent(TEvent* event, const TCmdDirectReadSettings& settings) {
    UNIT_ASSERT(event->ReadKey.ReadId == settings.DirectReadId);
    UNIT_ASSERT(event->ReadKey.SessionId == settings.Session);
    UNIT_ASSERT(event->ReadKey.PartitionSessionId > 0);
}

void CmdPublishOrForgetRead(const TCmdDirectReadSettings& settings, bool isPublish, TTestContext& tc) {
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse *result;
    THolder<TEvPersQueue::TEvRequest> request;
    tc.Runtime->ResetScheduledCount();
    request.Reset(new TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();

    ActorIdToProto(settings.Pipe, req->MutablePipeClient());

    req->SetPartition(settings.Partition);
    req->SetCookie(123);
    if (isPublish) {
        FillDirectReadKey(req->MutableCmdPublishRead(), settings);
    } else {
        FillDirectReadKey(req->MutableCmdForgetRead(), settings);
    }

    TAtomic hasEvent = 0;
    tc.Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& ev) {
                if (auto* msg = ev->CastAsLocal<TEvPQ::TEvStageDirectReadData>()) {
                    Cerr << "Got publish event\n";
                    UNIT_ASSERT(isPublish);
                    UNIT_ASSERT(msg->TabletGeneration);
                    //AtomicSet(hasEvent, 1);
                    UNIT_ASSERT(msg->Response != nullptr);
                } else if (auto* msg = ev->CastAsLocal<TEvPQ::TEvPublishDirectRead>()) {
                    Cerr << "Got publish event\n";
                    UNIT_ASSERT(isPublish);
                    CheckDirectReadEvent(msg, settings);
                    AtomicSet(hasEvent, 1);
                } else if (auto* msg = ev->CastAsLocal<TEvPQ::TEvForgetDirectRead>()) {
                    UNIT_ASSERT(!isPublish);
                    CheckDirectReadEvent(msg, settings);
                    AtomicSet(hasEvent, 1);
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            }
    );
    Cerr << "Send " << (isPublish? "publish " : "forget ") << "read request: " << req->DebugString() << Endl;

    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
    result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);

    UNIT_ASSERT(result);
    UNIT_ASSERT(result->Record.HasStatus());
    Cerr << "Got direct read response: " << result->Record.DebugString() << Endl;
    if (settings.Fail) {
        UNIT_ASSERT(result->Record.GetErrorCode() != NPersQueue::NErrorCode::OK);
        return;
    }
    UNIT_ASSERT_C(result->Record.GetErrorCode() == NPersQueue::NErrorCode::OK, result->Record.DebugString());

    UNIT_ASSERT(result->Record.HasPartitionResponse());
    UNIT_ASSERT_EQUAL(result->Record.GetPartitionResponse().GetCookie(), 123);
    if (isPublish) {
        UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdPublishReadResult(), result->Record.DebugString());
    } else {
        UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdForgetReadResult(), result->Record.DebugString());
    }
    //tc.Runtime->DispatchEvents();
    Cerr << "Expect failure: " << settings.Fail << ", event received: " << AtomicGet(hasEvent) << Endl;
    if (settings.Fail) {
        UNIT_ASSERT(!AtomicGet(hasEvent));
    } else {
        // UNIT_ASSERT(AtomicGet(hasEvent)); // ToDo: !! Fix this - event is send but not cathed for some reason;

    }
}

void CmdPublishRead(const TCmdDirectReadSettings& settings, TTestContext& tc) {
    return CmdPublishOrForgetRead(settings, true, tc);
}

void CmdForgetRead(const TCmdDirectReadSettings& settings, TTestContext& tc) {
    return CmdPublishOrForgetRead(settings, false, tc);
}

void FillUserInfo(NKikimrClient::TKeyValueRequest_TCmdWrite* write, const TString& client, ui32 partition, ui64 offset) {
    NPQ::TKeyPrefix ikey(NPQ::TKeyPrefix::TypeInfo, TPartitionId(partition), NPQ::TKeyPrefix::MarkUser);
    ikey.Append(client.c_str(), client.size());

    NKikimrPQ::TUserInfo userInfo;
    userInfo.SetOffset(offset);
    userInfo.SetGeneration(1);
    userInfo.SetStep(2);
    userInfo.SetSession("test-session");
    userInfo.SetOffsetRewindSum(10);
    userInfo.SetReadRuleGeneration(1);
    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD userInfo.SerializeToString(&out);

    TBuffer idata;
    idata.Append(out.c_str(), out.size());

    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(idata.Data(), idata.Size());
}

void FillDeprecatedUserInfo(NKikimrClient::TKeyValueRequest_TCmdWrite* write, const TString& client, ui32 partition, ui64 offset) {
    TString session = "test-session";
    ui32 gen = 1;
    ui32 step = 2;
    NPQ::TKeyPrefix ikeyDeprecated(NPQ::TKeyPrefix::TypeInfo, TPartitionId(partition), NPQ::TKeyPrefix::MarkUserDeprecated);
    ikeyDeprecated.Append(client.c_str(), client.size());

    TBuffer idataDeprecated = NPQ::NDeprecatedUserData::Serialize(offset, gen, step, session);
    write->SetKey(ikeyDeprecated.Data(), ikeyDeprecated.Size());
    write->SetValue(idataDeprecated.Data(), idataDeprecated.Size());
}

THolder<TEvPersQueue::TEvPeriodicTopicStats> GetReadBalancerPeriodicTopicStats(TTestActorRuntime& runtime, ui64 balancerId) {
    runtime.ResetScheduledCount();

    TActorId sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(balancerId, sender, new TEvPersQueue::TEvStatus(), 0, GetPipeConfigWithRetries());

    return runtime.GrabEdgeEvent<TEvPersQueue::TEvPeriodicTopicStats>(TDuration::Seconds(2));
}

} // namespace NKikimr::NPQ
