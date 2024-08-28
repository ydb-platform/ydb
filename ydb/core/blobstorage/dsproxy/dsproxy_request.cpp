#include "dsproxy_impl.h"
#include "dsproxy_monactor.h"
#include <ydb/core/base/feature_flags.h>


namespace NKikimr {

    void TBlobStorageGroupProxy::PushRequest(IActor *actor, TInstant deadline) {
        const TActorId actorId = Register(actor);
        if (deadline != TInstant::Max()) {
            ActiveRequests.try_emplace(actorId, DeadlineMap.emplace(deadline, actorId));
        } else {
            ActiveRequests.try_emplace(actorId);
        }
    }

    void TBlobStorageGroupProxy::CheckDeadlines() {
        const TInstant now = TActivationContext::Now();
        std::multimap<TInstant, TActorId>::iterator it;
        for (it = DeadlineMap.begin(); it != DeadlineMap.end() && it->first <= now; ++it) {
            TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvDeadline, 0, it->second, SelfId(), nullptr, 0));
            auto jt = ActiveRequests.find(it->second);
            Y_ABORT_UNLESS(jt != ActiveRequests.end());
            jt->second = {};
        }
        DeadlineMap.erase(DeadlineMap.begin(), it);
        TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(EvCheckDeadlines, 0, SelfId(), {}, nullptr, 0));
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvGet::TPtr &ev) {
        if (IsLimitedKeyless && !ev->Get()->PhantomCheck) {
            ErrorDescription = "Created as LIMITED without keys. It happens when tenant keys are missing on the node.";
            HandleError(ev);
            return;
        }

        if (StopGetBatchingEvent) {
            TActivationContext::Send(StopGetBatchingEvent.Release());
        }
        BatchedGetRequestCount++;

        EnsureMonitoring(true);
        LWTRACK(DSProxyGetHandle, ev->Get()->Orbit);
        EnableWilsonTracing(ev, Mon->GetSamplePPM);
        if (ev->Get()->IsIndexOnly) {
            Mon->EventIndexRestoreGet->Inc();
            PushRequest(CreateBlobStorageGroupIndexRestoreGetRequest(
                TBlobStorageGroupRestoreGetParameters{
                    .Common = {
                        .GroupInfo = Info,
                        .GroupQueues = Sessions->GroupQueues,
                        .Mon = Mon,
                        .Source = ev->Sender,
                        .Cookie = ev->Cookie,
                        .Now = TActivationContext::Now(),
                        .StoragePoolCounters = StoragePoolCounters,
                        .RestartCounter = ev->Get()->RestartCounter,
                        .TraceId = std::move(ev->TraceId),
                        .Event = ev->Get(),
                        .ExecutionRelay = ev->Get()->ExecutionRelay,
                    }
                }),
                ev->Get()->Deadline
            );
        } else {
            TLogoBlobID lastBlobId;
            const ui32 querySize = ev->Get()->QuerySize;
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> &queries = ev->Get()->Queries;
            ui32 differentBlobCount = 0;
            TQueryResultSizeTracker resultSize;
            resultSize.Init();
            for (ui32 queryIdx = 0; queryIdx < querySize; ++queryIdx) {
                const TEvBlobStorage::TEvGet::TQuery &query = queries[queryIdx];
                if (lastBlobId == query.Id && queryIdx != 0) {
                    continue;
                }
                lastBlobId = query.Id;
                resultSize.AddAllPartsOfLogoBlob(Info->Type, query.Id);
                differentBlobCount++;
            }
            bool isSmall = !resultSize.IsOverflow() && querySize <= 10000;

            TMaybe<TGroupStat::EKind> kind;
            switch (ev->Get()->GetHandleClass) {
                case NKikimrBlobStorage::FastRead:
                    // do not count async requests and discover requests
                    kind = TGroupStat::EKind::GET_FAST;
                    break;

                default:
                    break;
            }

            if (differentBlobCount == 1 || isSmall) {
                Mon->EventGet->Inc();
                PushRequest(CreateBlobStorageGroupGetRequest(
                    TBlobStorageGroupGetParameters{
                        .Common = {
                            .GroupInfo = Info,
                            .GroupQueues = Sessions->GroupQueues,
                            .Mon = Mon,
                            .Source = ev->Sender,
                            .Cookie = ev->Cookie,
                            .Now = TActivationContext::Now(),
                            .StoragePoolCounters = StoragePoolCounters,
                            .RestartCounter = ev->Get()->RestartCounter,
                            .TraceId = std::move(ev->TraceId),
                            .Event = ev->Get(),
                            .ExecutionRelay = ev->Get()->ExecutionRelay,
                            .LogAccEnabled = ev->Get()->IsVerboseNoDataEnabled || ev->Get()->CollectDebugInfo,
                            .LatencyQueueKind = kind,
                        },
                        .NodeLayout = TNodeLayoutInfoPtr(NodeLayoutInfo),
                        .AccelerationParams = GetAccelerationParams(),
                        .LongRequestThreshold = TDuration::MilliSeconds(LongRequestThresholdMs.Update(TActivationContext::Now())),
                    }),
                    ev->Get()->Deadline
                );
            } else {
                Mon->EventMultiGet->Inc();
                PushRequest(CreateBlobStorageGroupMultiGetRequest(
                    TBlobStorageGroupMultiGetParameters{
                        .Common = {
                            .GroupInfo = Info,
                            .GroupQueues = Sessions->GroupQueues,
                            .Mon = Mon,
                            .Source = ev->Sender,
                            .Cookie = ev->Cookie,
                            .Now = TActivationContext::Now(),
                            .StoragePoolCounters = StoragePoolCounters,
                            .RestartCounter = ev->Get()->RestartCounter,
                            .TraceId = std::move(ev->TraceId),
                            .Event = ev->Get(),
                            .ExecutionRelay = ev->Get()->ExecutionRelay,
                            .LatencyQueueKind = kind,
                        },
                    }),
                    ev->Get()->Deadline
                );
            }
        }
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvPut::TPtr &ev) {
        if (IsLimitedKeyless) {
            ErrorDescription = "Created as LIMITED without keys. It happens when tenant keys are missing on the node.";
            HandleError(ev);
            return;
        }
        LWTRACK(DSProxyPutHandle, ev->Get()->Orbit);
        EnsureMonitoring(true);
        const ui64 bytes = ev->Get()->Buffer.size();
        Mon->CountPutEvent(bytes);
        // Make sure actual data size matches one in the logoblobid.
        if (bytes != ev->Get()->Id.BlobSize()) {
            TStringStream str;
            str << "Actual data size# " << bytes
                << " does not match LogoBlobId# " << ev->Get()->Id
                << " Group# " << GroupId
                << " Marker# DSP53";
            std::unique_ptr<TEvBlobStorage::TEvPutResult> result(
                    new TEvBlobStorage::TEvPutResult(NKikimrProto::ERROR, ev->Get()->Id, 0, GroupId, 0.f));
            result->ErrorReason = str.Str();
            result->ExecutionRelay = std::move(ev->Get()->ExecutionRelay);
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PROXY,
                    "HandleNormal ev# " << ev->Get()->Print(false)
                    << " result# " << result->Print(false)
                    << " Marker# DSP54");
            Send(ev->Sender, result.release(), 0, ev->Cookie);
            return;
        }

        if (StopPutBatchingEvent) {
            TActivationContext::Send(StopPutBatchingEvent.Release());
        }
        BatchedPutRequestCount++;

        Send(MonActor, new TEvThroughputAddRequest(ev->Get()->HandleClass, bytes));
        EnableWilsonTracing(ev, Mon->PutSamplePPM);

        Y_DEBUG_ABORT_UNLESS(MinREALHugeBlobInBytes);
        const ui32 partSize = Info->Type.PartSize(ev->Get()->Id);

        if (EnablePutBatching && partSize < MinREALHugeBlobInBytes && partSize <= MaxBatchedPutSize) {
            NKikimrBlobStorage::EPutHandleClass handleClass = ev->Get()->HandleClass;
            TEvBlobStorage::TEvPut::ETactic tactic = ev->Get()->Tactic;
            Y_ABORT_UNLESS((ui64)handleClass <= PutHandleClassCount);
            Y_ABORT_UNLESS(tactic <= PutTacticCount);

            TBatchedQueue<TEvBlobStorage::TEvPut::TPtr> &batchedPuts = BatchedPuts[handleClass][tactic];
            if (batchedPuts.Queue.empty()) {
                PutBatchedBucketQueue.emplace_back(handleClass, tactic);
            }

            if (batchedPuts.Queue.size() == MaxBatchedPutRequests || batchedPuts.Bytes + partSize > MaxBatchedPutSize) {
                *Mon->PutsSentViaPutBatching += batchedPuts.Queue.size();
                ++*Mon->PutBatchesSent;
                ProcessBatchedPutRequests(batchedPuts, handleClass, tactic);
            }

            batchedPuts.Queue.push_back(ev.Release());
            batchedPuts.Bytes += partSize;
        } else {
            TMaybe<TGroupStat::EKind> kind = PutHandleClassToGroupStatKind(ev->Get()->HandleClass);

            TAppData *app = NKikimr::AppData(TActivationContext::AsActorContext());
            bool enableRequestMod3x3ForMinLatency = app->FeatureFlags.GetEnable3x3RequestsForMirror3DCMinLatencyPut();
            // TODO(alexvru): MinLatency support
            PushRequest(CreateBlobStorageGroupPutRequest(
                TBlobStorageGroupPutParameters{
                    .Common = {
                        .GroupInfo = Info,
                        .GroupQueues = Sessions->GroupQueues,
                        .Mon = Mon,
                        .Source = ev->Sender,
                        .Cookie = ev->Cookie,
                        .Now = TActivationContext::Now(),
                        .StoragePoolCounters = StoragePoolCounters,
                        .RestartCounter = ev->Get()->RestartCounter,
                        .TraceId = std::move(ev->TraceId),
                        .Event = ev->Get(),
                        .ExecutionRelay = ev->Get()->ExecutionRelay,
                        .LatencyQueueKind = kind
                    },
                    .TimeStatsEnabled = Mon->TimeStats.IsEnabled(),
                    .Stats = PerDiskStats,
                    .EnableRequestMod3x3ForMinLatency = enableRequestMod3x3ForMinLatency,
                    .AccelerationParams = GetAccelerationParams(),
                    .LongRequestThreshold = TDuration::MilliSeconds(LongRequestThresholdMs.Update(TActivationContext::Now())),
                }),
                ev->Get()->Deadline
            );
        }
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvBlock::TPtr &ev) {
        EnsureMonitoring(ev->Get()->IsMonitored);
        Mon->EventBlock->Inc();
        PushRequest(CreateBlobStorageGroupBlockRequest(
            TBlobStorageGroupBlockParameters{
                .Common = {
                    .GroupInfo = Info,
                    .GroupQueues = Sessions->GroupQueues,
                    .Mon = Mon,
                    .Source = ev->Sender,
                    .Cookie = ev->Cookie,
                    .Now = TActivationContext::Now(),
                    .StoragePoolCounters = StoragePoolCounters,
                    .RestartCounter = ev->Get()->RestartCounter,
                    .TraceId = std::move(ev->TraceId),
                    .Event = ev->Get(),
                    .ExecutionRelay = ev->Get()->ExecutionRelay
                }
            }),
            ev->Get()->Deadline
        );
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvPatch::TPtr &ev) {
        if (IsLimitedKeyless) {
            ErrorDescription = "Created as LIMITED without keys. It happens when tenant keys are missing on the node.";
            HandleError(ev);
            return;
        }
        EnsureMonitoring(true);
        Mon->EventPatch->Inc();
        TInstant now = TActivationContext::Now();
        PushRequest(CreateBlobStorageGroupPatchRequest(
            TBlobStorageGroupPatchParameters{
                .Common = {
                    .GroupInfo = Info,
                    .GroupQueues = Sessions->GroupQueues,
                    .Mon = Mon,
                    .Source = ev->Sender,
                    .Cookie = ev->Cookie,
                    .Now = now,
                    .StoragePoolCounters = StoragePoolCounters,
                    .RestartCounter = ev->Get()->RestartCounter,
                    .TraceId = std::move(ev->TraceId),
                    .Event = ev->Get(),
                    .ExecutionRelay = ev->Get()->ExecutionRelay
                },
                .UseVPatch = static_cast<bool>(EnableVPatch.Update(now))
            }),
            ev->Get()->Deadline
        );
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvDiscover::TPtr &ev) {
        if (IsLimitedKeyless) {
            // Consider adding && ev->Get()->ReadBody
            // to allow !ReadBody discovers (or even make a special discover flag for keyless case)
            ErrorDescription = "Created as LIMITED without keys. It happens when tenant keys are missing on the node.";
            HandleError(ev);
            return;
        }
        EnsureMonitoring(true);
        Mon->EventDiscover->Inc();
        EnableWilsonTracing(ev, Mon->DiscoverSamplePPM);
        TErasureType::EErasureSpecies erasure = Info->Type.GetErasure();
        auto&& callback = erasure == TBlobStorageGroupType::ErasureMirror3dc
            ? CreateBlobStorageGroupMirror3dcDiscoverRequest
            : erasure == TBlobStorageGroupType::ErasureMirror3of4
            ? CreateBlobStorageGroupMirror3of4DiscoverRequest
            : CreateBlobStorageGroupDiscoverRequest;
        PushRequest(callback(
            TBlobStorageGroupDiscoverParameters{
                .Common = {
                    .GroupInfo = Info,
                    .GroupQueues = Sessions->GroupQueues,
                    .Mon = Mon,
                    .Source = ev->Sender,
                    .Cookie = ev->Cookie,
                    .Now = TActivationContext::Now(),
                    .StoragePoolCounters = StoragePoolCounters,
                    .RestartCounter = ev->Get()->RestartCounter,
                    .TraceId = std::move(ev->TraceId),
                    .Event = ev->Get(),
                    .ExecutionRelay = ev->Get()->ExecutionRelay,
                    .LogAccEnabled = (erasure != TBlobStorageGroupType::ErasureMirror3dc) &&
                            (erasure != TBlobStorageGroupType::ErasureMirror3of4)
                }
            }),
            ev->Get()->Deadline
        );
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvRange::TPtr &ev) {
        if (IsLimitedKeyless) {
            ErrorDescription = "Created as LIMITED without keys. It happens when tenant keys are missing on the node.";
            HandleError(ev);
            return;
        }
        EnsureMonitoring(true);
        Mon->EventRange->Inc();
        PushRequest(CreateBlobStorageGroupRangeRequest(
            TBlobStorageGroupRangeParameters{
                .Common = {
                    .GroupInfo = Info,
                    .GroupQueues = Sessions->GroupQueues,
                    .Mon = Mon,
                    .Source = ev->Sender,
                    .Cookie = ev->Cookie,
                    .Now = TActivationContext::Now(),
                    .StoragePoolCounters = StoragePoolCounters,
                    .RestartCounter = ev->Get()->RestartCounter,
                    .TraceId = std::move(ev->TraceId),
                    .Event = ev->Get(),
                    .ExecutionRelay = ev->Get()->ExecutionRelay
                }
            }),
            ev->Get()->Deadline
        );
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvCollectGarbage::TPtr &ev) {
        EnsureMonitoring(ev->Get()->IsMonitored);

        if (!ev->Get()->IsMultiCollectAllowed || ev->Get()->PerGenerationCounterStepSize() == 1) {
            Mon->EventCollectGarbage->Inc();
            PushRequest(CreateBlobStorageGroupCollectGarbageRequest(
                TBlobStorageGroupCollectGarbageParameters{
                    .Common = {
                        .GroupInfo = Info,
                        .GroupQueues = Sessions->GroupQueues,
                        .Mon = Mon,
                        .Source = ev->Sender,
                        .Cookie = ev->Cookie,
                        .Now = TActivationContext::Now(),
                        .StoragePoolCounters = StoragePoolCounters,
                        .RestartCounter = ev->Get()->RestartCounter,
                        .TraceId = std::move(ev->TraceId),
                        .Event = ev->Get(),
                        .ExecutionRelay = ev->Get()->ExecutionRelay
                    }
                }),
                ev->Get()->Deadline
            );
        } else {
            Mon->EventMultiCollect->Inc();
            PushRequest(CreateBlobStorageGroupMultiCollectRequest(
                TBlobStorageGroupMultiCollectParameters{
                    .Common = {
                        .GroupInfo = Info,
                        .GroupQueues = Sessions->GroupQueues,
                        .Mon = Mon,
                        .Source = ev->Sender,
                        .Cookie = ev->Cookie,
                        .Now = TActivationContext::Now(),
                        .StoragePoolCounters = StoragePoolCounters,
                        .RestartCounter = ev->Get()->RestartCounter,
                        .TraceId = std::move(ev->TraceId),
                        .Event = ev->Get(),
                        .ExecutionRelay = ev->Get()->ExecutionRelay
                    }
                }),
                ev->Get()->Deadline
            );
        }
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvStatus::TPtr &ev) {
        if (IsLimitedKeyless) {
            ErrorDescription = "Created as LIMITED without keys. It happens when tenant keys are missing on the node.";
            HandleError(ev);
            return;
        }
        EnsureMonitoring(true);
        Mon->EventStatus->Inc();
        PushRequest(CreateBlobStorageGroupStatusRequest(
            TBlobStorageGroupStatusParameters{
                .Common = {
                    .GroupInfo = Info,
                    .GroupQueues = Sessions->GroupQueues,
                    .Mon = Mon,
                    .Source = ev->Sender,
                    .Cookie = ev->Cookie,
                    .Now = TActivationContext::Now(),
                    .StoragePoolCounters = StoragePoolCounters,
                    .RestartCounter = ev->Get()->RestartCounter,
                    .TraceId = std::move(ev->TraceId),
                    .Event = ev->Get(),
                    .ExecutionRelay = ev->Get()->ExecutionRelay
                }
            }),
            TInstant::Max()
        );
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvAssimilate::TPtr &ev) {
        EnsureMonitoring(true);
        Mon->EventAssimilate->Inc();
        PushRequest(CreateBlobStorageGroupAssimilateRequest(
            TBlobStorageGroupAssimilateParameters{
                .Common = {
                    .GroupInfo = Info,
                    .GroupQueues = Sessions->GroupQueues,
                    .Mon = Mon,
                    .Source = ev->Sender,
                    .Cookie = ev->Cookie,
                    .Now = TActivationContext::Now(),
                    .StoragePoolCounters = StoragePoolCounters,
                    .RestartCounter = ev->Get()->RestartCounter,
                    .TraceId = std::move(ev->TraceId),
                    .Event = ev->Get(),
                    .ExecutionRelay = ev->Get()->ExecutionRelay
                }
            }),
            TInstant::Max()
        );
    }

    void TBlobStorageGroupProxy::Handle(TEvDeathNote::TPtr ev) {
        const bool wasEmpty = ResponsivenessTracker.IsEmpty();
        for (const auto &item : ev->Get()->Responsiveness) {
            ResponsivenessTracker.Register(item.first, TActivationContext::Now(), item.second);
        }
        if (wasEmpty && !ResponsivenessTracker.IsEmpty()) {
            ScheduleUpdateResponsiveness();
        }
        const auto it = ActiveRequests.find(ev->Sender);
        Y_ABORT_UNLESS(it != ActiveRequests.end());
        if (it->second != std::multimap<TInstant, TActorId>::iterator()) {
            DeadlineMap.erase(it->second);
        }
        ActiveRequests.erase(it);
    }

    void TBlobStorageGroupProxy::Handle(TEvBlobStorage::TEvBunchOfEvents::TPtr ev) {
        ev->Get()->Process(this);
    }

    void TBlobStorageGroupProxy::ProcessBatchedPutRequests(TBatchedQueue<TEvBlobStorage::TEvPut::TPtr> &batchedPuts,
            NKikimrBlobStorage::EPutHandleClass handleClass, TEvBlobStorage::TEvPut::ETactic tactic) {
        TMaybe<TGroupStat::EKind> kind = PutHandleClassToGroupStatKind(handleClass);

        if (Info) {
            if (CurrentStateFunc() == &TThis::StateWork) {
                TAppData *app = NKikimr::AppData(TActivationContext::AsActorContext());
                bool enableRequestMod3x3ForMinLatency = app->FeatureFlags.GetEnable3x3RequestsForMirror3DCMinLatencyPut();
                // TODO(alexvru): MinLatency support
                if (batchedPuts.Queue.size() == 1) {
                    auto& ev = batchedPuts.Queue.front();
                    PushRequest(CreateBlobStorageGroupPutRequest(
                        TBlobStorageGroupPutParameters{
                            .Common = {
                                .GroupInfo = Info,
                                .GroupQueues = Sessions->GroupQueues,
                                .Mon = Mon,
                                .Source = ev->Sender,
                                .Cookie = ev->Cookie,
                                .Now = TActivationContext::Now(),
                                .StoragePoolCounters = StoragePoolCounters,
                                .RestartCounter = ev->Get()->RestartCounter,
                                .TraceId = std::move(ev->TraceId),
                                .Event = ev->Get(),
                                .ExecutionRelay = ev->Get()->ExecutionRelay,
                                .LatencyQueueKind = kind,
                            },
                            .TimeStatsEnabled = Mon->TimeStats.IsEnabled(),
                            .Stats = PerDiskStats,
                            .EnableRequestMod3x3ForMinLatency = enableRequestMod3x3ForMinLatency,
                            .AccelerationParams = GetAccelerationParams(),
                            .LongRequestThreshold = TDuration::MilliSeconds(LongRequestThresholdMs.Update(TActivationContext::Now())),
                        }),
                        ev->Get()->Deadline
                    );
                } else {
                    PushRequest(CreateBlobStorageGroupPutRequest(
                        TBlobStorageGroupMultiPutParameters{
                            .Common = {
                                .GroupInfo = Info,
                                .GroupQueues = Sessions->GroupQueues,
                                .Mon = Mon,
                                .Now = TActivationContext::Now(),
                                .StoragePoolCounters = StoragePoolCounters,
                                .RestartCounter = TBlobStorageGroupMultiPutParameters::CalculateRestartCounter(batchedPuts.Queue),
                                .LatencyQueueKind = kind,
                            },
                            .Events = batchedPuts.Queue,
                            .TimeStatsEnabled = Mon->TimeStats.IsEnabled(),
                            .Stats = PerDiskStats,
                            .HandleClass = handleClass,
                            .Tactic = tactic,
                            .EnableRequestMod3x3ForMinLatency = enableRequestMod3x3ForMinLatency,
                            .AccelerationParams = GetAccelerationParams(),
                            .LongRequestThreshold = TDuration::MilliSeconds(LongRequestThresholdMs.Update(TActivationContext::Now())),
                        }),
                        TInstant::Max()
                    );
                }
            } else {
                for (auto it = batchedPuts.Queue.begin(); it != batchedPuts.Queue.end(); ++it) {
                    TAutoPtr<IEventHandle> ev = it->Release();
                    Receive(ev);
                }
            }
        } else {
            for (auto it = batchedPuts.Queue.begin(); it != batchedPuts.Queue.end(); ++it) {
                HandleError(*it);
            }
        }

        batchedPuts.Queue.clear();
        batchedPuts.Bytes = 0;
    }

    void TBlobStorageGroupProxy::Handle(TEvStopBatchingPutRequests::TPtr& ev) {
        StopPutBatchingEvent = ev;
        for (auto &bucket : PutBatchedBucketQueue) {
            auto &batchedPuts = BatchedPuts[bucket.HandleClass][bucket.Tactic];
            Y_ABORT_UNLESS(!batchedPuts.Queue.empty());
            *Mon->PutsSentViaPutBatching += batchedPuts.Queue.size();
            ++*Mon->PutBatchesSent;
            ProcessBatchedPutRequests(batchedPuts, bucket.HandleClass, bucket.Tactic);
        }
        PutBatchedBucketQueue.clear();
        ++*Mon->EventStopPutBatching;
        LWPROBE(DSProxyBatchedPutRequest, BatchedPutRequestCount, GroupId.GetRawId());
        BatchedPutRequestCount = 0;
        EnablePutBatching.Update(TActivationContext::Now());
    }

    void TBlobStorageGroupProxy::Handle(TEvStopBatchingGetRequests::TPtr& ev) {
        StopGetBatchingEvent = ev;
        ++*Mon->EventStopGetBatching;
        LWPROBE(DSProxyBatchedGetRequest, BatchedGetRequestCount, GroupId.GetRawId());
        BatchedGetRequestCount = 0;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TBlobStorageGroupRequestActor::Registered(TActorSystem *as, const TActorId& parentId) {
        ProxyActorId = parentId;
        as->Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0, SelfId(), parentId, nullptr, 0));
        TActor::Registered(as, parentId);
    }

    STRICT_STFUNC(TBlobStorageGroupRequestActor::InitialStateFunc,
        cFunc(TEvents::TSystem::Bootstrap, BootstrapImpl);
    )

    void TBlobStorageGroupRequestActor::BootstrapImpl() {
        GetActiveCounter()->Inc();
        Bootstrap();
    }

    TActorId TBlobStorageGroupRequestActor::GetVDiskActorId(const TVDiskIdShort &shortId) const {
        return Info->GetActorId(shortId);
    }

    bool TBlobStorageGroupRequestActor::CheckForTermErrors(bool suppressCommonErrors, const NProtoBuf::Message& record,
            ui32 type, NKikimrProto::EReplyStatus status, TVDiskID vdiskId, const NKikimrBlobStorage::TGroupInfo *group,
            bool& setErrorAndPostpone, bool& setRaceToError) {
        if (status == NKikimrProto::NOTREADY) { // special case from BS_QUEUE -- when connection is not yet established
            setErrorAndPostpone = true;
            return true; // event has been processed early
        }

        auto done = [&](NKikimrProto::EReplyStatus status, const TString& message) {
            ErrorReason = message;
            DSP_LOG_LOG_S(PriorityForStatusResult(status), "DSP10", "Query failed " << message);
            ReplyAndDie(status);
            return true;
        };

        // sanity check for matching group id
        if (vdiskId.GroupID != Info->GroupID) {
            return done(NKikimrProto::ERROR, TStringBuilder() << "incorrect VDiskId# " << vdiskId << " GroupId# "
                << Info->GroupID);
        }

        // sanity check for correct VDisk generation ??? possible race
        Y_VERIFY_S(status == NKikimrProto::RACE || vdiskId.GroupGeneration <= Info->GroupGeneration ||
            type == TEvBlobStorage::EvVStatusResult || type == TEvBlobStorage::EvVAssimilateResult,
            "status# " << NKikimrProto::EReplyStatus_Name(status) << " vdiskId.GroupGeneration# " << vdiskId.GroupGeneration
            << " Info->GroupGeneration# " << Info->GroupGeneration << " Response# " << SingleLineProto(record));

        if (status != NKikimrProto::RACE && status != NKikimrProto::BLOCKED && status != NKikimrProto::DEADLINE) {
            return false; // these statuses are non-terminal
        } else if (status != NKikimrProto::RACE) {
            if (suppressCommonErrors) {
                return false; // these errors will be handled in host code
            }
            // this status is terminal and we have nothing to do about it
            return done(status, TStringBuilder() << "status# " << NKikimrProto::EReplyStatus_Name(status) << " from# "
                << vdiskId.ToString());
        }

        DSP_LOG_INFO_S("DSP99", "Handing RACE response from " << vdiskId << " GroupGeneration# " << Info->GroupGeneration
            << " Response# " << SingleLineProto(record));

        // process the RACE status
        const TActorId& nodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
        if (vdiskId.GroupGeneration < Info->GroupGeneration) { // vdisk is older than our group
            RacingDomains |= {&Info->GetTopology(), vdiskId};
            if (RacingDomains.GetNumSetItems() <= 1) {
                setRaceToError = true;
                return false;
            }
        } else if (Info->GroupGeneration < vdiskId.GroupGeneration) { // our config is older that vdisk's one
            if (group && (group->GetGroupID() != Info->GroupID.GetRawId() || group->GetGroupGeneration() != vdiskId.GroupGeneration)) {
                return done(NKikimrProto::ERROR, "incorrect RecentGroup for RACE response");
            }
            Send(nodeWardenId, new TEvBlobStorage::TEvUpdateGroupInfo(vdiskId.GroupID, vdiskId.GroupGeneration,
                group ? std::make_optional(*group) : std::nullopt));
        }

        // make NodeWarden restart the query just after proxy reconfiguration
        Y_DEBUG_ABORT_UNLESS(RestartCounter < 100);
        auto q = RestartQuery(RestartCounter + 1);
        if (q->Type() != TEvBlobStorage::EvBunchOfEvents) {
            SetExecutionRelay(*q, std::exchange(ExecutionRelay, {}));
        }
        ++*Mon->NodeMon->RestartHisto[Min<size_t>(Mon->NodeMon->RestartHisto.size() - 1, RestartCounter)];
        const TActorId& proxyId = MakeBlobStorageProxyID(Info->GroupID);
        TActivationContext::Send(new IEventHandle(nodeWardenId, Source, q.release(), 0, Cookie, &proxyId, Span.GetTraceId()));
        PassAway();
        return true;
    }

    bool TBlobStorageGroupRequestActor::ProcessEvent(TAutoPtr<IEventHandle>& ev, bool suppressCommonErrors) {
        auto processEventKind = [&](auto& record, ui32 type) {
            if (!record.HasStatus() || !record.HasVDiskID()) { // some mandatory fields are missing
                return false;
            }
            bool setErrorAndPostpone = false;
            bool setRaceToError = false;
            const bool result = CheckForTermErrors(suppressCommonErrors, record, type, record.GetStatus(),
                VDiskIDFromVDiskID(record.GetVDiskID()), record.HasRecentGroup() ? &record.GetRecentGroup() : nullptr,
                setErrorAndPostpone, setRaceToError);
            if (setErrorAndPostpone) {
                record.SetStatus(NKikimrProto::ERROR);
                PostponedQ.emplace_back(ev.Release());
                CheckPostponedQueue();
            }
            if (setRaceToError) {
                record.SetStatus(NKikimrProto::ERROR);
                auto adjustStatus = [](auto *v) {
                    for (int i = 0; i < v->size(); ++i) {
                        auto *p = v->Mutable(i);
                        if (p->GetStatus() == NKikimrProto::RACE) {
                            p->SetStatus(NKikimrProto::ERROR);
                        }
                    }
                };
                using TRecord = std::decay_t<decltype(record)>;
                if constexpr (std::is_same_v<TRecord, NKikimrBlobStorage::TEvVGetResult>) {
                    adjustStatus(record.MutableResult());
                } else if constexpr (std::is_same_v<TRecord, NKikimrBlobStorage::TEvVMultiPutResult>) {
                    adjustStatus(record.MutableItems());
                }
            }
            return result;
        };

        switch (const ui32 type = ev->GetTypeRewrite()) {
#define CHECK(T) case TEvBlobStorage::T::EventType: return processEventKind(ev->Get<TEvBlobStorage::T>()->Record, type);
            CHECK(TEvVPutResult);
            CHECK(TEvVMultiPutResult);
            CHECK(TEvVGetResult);
            CHECK(TEvVBlockResult);
            CHECK(TEvVGetBlockResult);
            CHECK(TEvVCollectGarbageResult);
            CHECK(TEvVGetBarrierResult);
            CHECK(TEvVStatusResult);
            CHECK(TEvVAssimilateResult);
#undef CHECK

            case TEvBlobStorage::EvProxySessionsState: {
                GroupQueues = ev->Get<TEvProxySessionsState>()->GroupQueues;
                return true;
            }

            case TEvAbortOperation::EventType: {
                if (IsEarlyRequestAbortEnabled) {
                    ErrorReason = "Request got EvAbortOperation, IsEarlyRequestAbortEnabled# true";
                    ReplyAndDie(NKikimrProto::ERROR);
                }
                return true;
            }

            case TEvents::TSystem::Poison: {
                ErrorReason = "Request got Poison";
                ReplyAndDie(NKikimrProto::ERROR);
                return true;
            }

            case TEvBlobStorage::EvDeadline: {
                ErrorReason = "Deadline timer hit";
                ReplyAndDie(NKikimrProto::DEADLINE);
                return true;
            }
        }

        return false;
    }

    TLogoBlobID TBlobStorageGroupRequestActor::GetBlobId(TEvBlobStorage::TEvVPut &ev) {
        Y_ABORT_UNLESS(ev.Record.HasBlobID());
        return LogoBlobIDFromLogoBlobID(ev.Record.GetBlobID());
    }

    TLogoBlobID TBlobStorageGroupRequestActor::GetBlobId(TEvBlobStorage::TEvVMultiPut &ev) {
        Y_ABORT_UNLESS(ev.Record.ItemsSize());
        return LogoBlobIDFromLogoBlobID(ev.Record.GetItems(0).GetBlobID());
    }

    TLogoBlobID TBlobStorageGroupRequestActor::GetBlobId(TEvBlobStorage::TEvVMovedPatch &ev) {
        Y_ABORT_UNLESS(ev.Record.HasPatchedBlobId());
        return LogoBlobIDFromLogoBlobID(ev.Record.GetPatchedBlobId());
    }

    TLogoBlobID TBlobStorageGroupRequestActor::GetBlobId(TEvBlobStorage::TEvVPatchStart &ev) {
        Y_ABORT_UNLESS(ev.Record.HasOriginalBlobId());
        return LogoBlobIDFromLogoBlobID(ev.Record.GetOriginalBlobId());
    }

    TLogoBlobID TBlobStorageGroupRequestActor::GetBlobId(TEvBlobStorage::TEvVPatchDiff &ev) {
        Y_ABORT_UNLESS(ev.Record.HasPatchedPartBlobId());
        return LogoBlobIDFromLogoBlobID(ev.Record.GetPatchedPartBlobId());
    }

    void TBlobStorageGroupRequestActor::SendToProxy(std::unique_ptr<IEventBase> event, ui64 cookie, NWilson::TTraceId traceId) {
        Send(ProxyActorId, event.release(), 0, cookie, std::move(traceId));
    }

    void TBlobStorageGroupRequestActor::SendResponseAndDie(std::unique_ptr<IEventBase>&& ev,
            TBlobStorageGroupProxyTimeStats *timeStats, TActorId source, ui64 cookie) {
        SendResponse(std::move(ev), timeStats, source, cookie);
        PassAway();
    }

    void TBlobStorageGroupRequestActor::SendResponseAndDie(std::unique_ptr<IEventBase>&& ev,
            TBlobStorageGroupProxyTimeStats *timeStats) {
        SendResponseAndDie(std::move(ev), timeStats, Source, Cookie);
    }

    void TBlobStorageGroupRequestActor::PassAway() {
        // ensure we didn't keep execution relay on occasion
        Y_VERIFY_DEBUG_S(!ExecutionRelay, LogCtx.RequestPrefix << " actor died without properly sending response");

        // ensure that we are dying for the first time
        Y_ABORT_UNLESS(!std::exchange(Dead, true));
        GetActiveCounter()->Dec();
        SendToProxy(std::make_unique<TEvDeathNote>(Responsiveness));
        TActor::PassAway();
    }

    void TBlobStorageGroupRequestActor::SendResponse(std::unique_ptr<IEventBase>&& ev,
            TBlobStorageGroupProxyTimeStats *timeStats, TActorId source, ui64 cookie, bool term) {
        const TInstant now = TActivationContext::Now();

        NKikimrProto::EReplyStatus status;
        TString errorReason;

        switch (ev->Type()) {
#define XX(T) \
            case TEvBlobStorage::Ev##T##Result: { \
                auto& msg = static_cast<TEvBlobStorage::TEv##T##Result&>(*ev); \
                status = msg.Status; \
                errorReason = msg.ErrorReason; \
                Mon->RespStat##T->Account(status); \
                break; \
            }

            XX(Put)
            XX(Get)
            XX(Block)
            XX(Discover)
            XX(Range)
            XX(CollectGarbage)
            XX(Status)
            XX(Patch)
            XX(Assimilate)
            default:
                Y_ABORT();
#undef XX
        }

        if (ExecutionRelay) {
            SetExecutionRelay(*ev, std::exchange(ExecutionRelay, {}));
            ExecutionRelayUsed = true;
        } else {
            Y_ABORT_UNLESS(!ExecutionRelayUsed);
        }

        // ensure that we are dying for the first time
        Y_ABORT_UNLESS(!Dead);
        if (RequestHandleClass && PoolCounters) {
            PoolCounters->GetItem(*RequestHandleClass, RequestBytes).Register(
                RequestBytes, GeneratedSubrequests, GeneratedSubrequestBytes, Timer.Passed());
        }

        if (timeStats) {
            SendToProxy(std::make_unique<TEvTimeStats>(std::move(*timeStats)));
        }

        if (LatencyQueueKind) {
            SendToProxy(std::make_unique<TEvLatencyReport>(*LatencyQueueKind, now - RequestStartTime));
        }

        // KIKIMR-6737
        if (ev->Type() == TEvBlobStorage::EvGetResult) {
            static_cast<TEvBlobStorage::TEvGetResult&>(*ev).Sent = now;
        }

        if (term) {
            if (status == NKikimrProto::OK) {
                ParentSpan.EndOk();
                Span.EndOk();
            } else {
                ParentSpan.EndError(errorReason);
                Span.EndError(std::move(errorReason));
            }
        }

        // send the reply to original request sender
        Send(source, ev.release(), 0, cookie);
    };

    void TBlobStorageGroupRequestActor::SendResponse(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats) {
        SendResponse(std::move(ev), timeStats, Source, Cookie);
    }

    double TBlobStorageGroupRequestActor::GetStartTime(const NKikimrBlobStorage::TTimestamps& timestamps) {
        return timestamps.GetSentByDSProxyUs() / 1e6;
    }

    double TBlobStorageGroupRequestActor::GetTotalTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps) {
        return double(timestamps.GetReceivedByDSProxyUs() - timestamps.GetSentByDSProxyUs())/1000.0;
    }

    double TBlobStorageGroupRequestActor::GetVDiskTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps) {
        return double(timestamps.GetSentByVDiskUs() - timestamps.GetReceivedByVDiskUs())/1000.0;
    }

    void TBlobStorageGroupRequestActor::CheckPostponedQueue() {
        if (PostponedQ.size() == RequestsInFlight) {
            for (auto& ev : std::exchange(PostponedQ, {})) {
                TActivationContext::Send(ev.release());
            }
        }
    }

    void TBlobStorageGroupRequestActor::CountPut(ui32 bufferBytes) {
        ++GeneratedSubrequests;
        GeneratedSubrequestBytes += bufferBytes;
    }

    void TBlobStorageGroupRequestActor::CountEvent(IEventBase *ev, ui32 type) const {
        Mon->CountEvent(GetRequestType(), ev, type);
    }

    void TBlobStorageGroupRequestActor::SendToQueue(std::unique_ptr<IEventBase> event, ui64 cookie, bool timeStatsEnabled) {
        const ui32 type = event->Type();

        CountEvent(event.get(), type);

        TVDiskID vdiskId;
        NKikimrBlobStorage::EVDiskQueueId queueId;

        auto preprocess = [&](auto& ev) {
            Y_DEBUG_ABORT_UNLESS(ev.Record.HasVDiskID());
            vdiskId = VDiskIDFromVDiskID(ev.Record.GetVDiskID());

            using T = std::decay_t<decltype(ev)>;

            if constexpr (!std::is_same_v<T, TEvBlobStorage::TEvVGetBlock> &&
                    !std::is_same_v<T, TEvBlobStorage::TEvVBlock> &&
                    !std::is_same_v<T, TEvBlobStorage::TEvVStatus> &&
                    !std::is_same_v<T, TEvBlobStorage::TEvVCollectGarbage> &&
                    !std::is_same_v<T, TEvBlobStorage::TEvVAssimilate> &&
                    !std::is_same_v<T, TEvBlobStorage::TEvVGetBarrier>) {
                const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
                ev.Record.MutableTimestamps()->SetSentByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
            }

            if constexpr (!std::is_same_v<T, TEvBlobStorage::TEvVStatus> &&
                    !std::is_same_v<T, TEvBlobStorage::TEvVAssimilate>) {
                ev.MessageRelevanceTracker = MessageRelevanceTracker;
                ui64 cost;
                if constexpr (std::is_same_v<T, TEvBlobStorage::TEvVMultiPut>) {
                    bool internalQueue;
                    cost = CostModel->GetCost(ev, &internalQueue);
                } else {
                    cost = CostModel->GetCost(ev);
                }
                *PoolCounters->DSProxyDiskCostCounter += cost;

                LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::BS_REQUEST_COST,
                    "DSProxy Request Type# " << TypeName<T>() << " Cost# " << cost);
            }

            if constexpr (std::is_same_v<T, TEvBlobStorage::TEvVPut> ||
                    std::is_same_v<T, TEvBlobStorage::TEvVMultiPut> ||
                    std::is_same_v<T, TEvBlobStorage::TEvVPatchDiff> ||
                    std::is_same_v<T, TEvBlobStorage::TEvVMovedPatch> ||
                    std::is_same_v<T, TEvBlobStorage::TEvVPatchStart>) {
                const TLogoBlobID id = GetBlobId(ev);
                LWTRACK(DSProxyPutVPutIsSent, ev.Orbit, Info->GetFailDomainOrderNumber(vdiskId), Info->GroupID.GetRawId(),
                    id.Channel(), id.PartId(), id.ToString(), id.BlobSize());
            }

            if (timeStatsEnabled) {
                if constexpr (!std::is_same_v<T, TEvBlobStorage::TEvVStatus> &&
                        !std::is_same_v<T, TEvBlobStorage::TEvVAssimilate>) {
                    const TInstant now = TAppData::TimeProvider->Now();
                    ev.Record.MutableMsgQoS()->MutableExecTimeStats()->SetSubmitTimestamp(now.GetValue());
                }
            }

            queueId = TGroupQueues::TVDisk::TQueues::VDiskQueueId(ev);
        };

        switch (type) {
#define XX(T) case T::EventType: preprocess(static_cast<T&>(*event)); break;
            DSPROXY_ENUM_DISK_EVENTS(XX)
#undef XX
        }

        GroupQueues->Send(*this, Info->GetTopology(), std::move(event), cookie, Span.GetTraceId(), vdiskId, queueId);
        ++RequestsInFlight;
    }

    void TBlobStorageGroupRequestActor::ProcessReplyFromQueue(IEventBase *ev) {
        Y_ABORT_UNLESS(RequestsInFlight);
        --RequestsInFlight;
        CountEvent(ev, ev->Type());
        CheckPostponedQueue();
    }


    void TBlobStorageGroupProxy::Handle(TEvGetQueuesInfo::TPtr ev) {
        ui32 groupSize = Info->GetTotalVDisksNum();
        std::unique_ptr<TEvQueuesInfo> res = std::make_unique<TEvQueuesInfo>(groupSize);
        if (Sessions && Sessions->GroupQueues) {
            for (ui32 orderNum = 0; orderNum < groupSize; ++orderNum) {
                TGroupQueues::TVDisk* vdisk = Sessions->GroupQueues->DisksByOrderNumber[orderNum];
                if (vdisk) {
                    const TGroupQueues::TVDisk::TQueues::TQueue& queue = vdisk->Queues.GetQueue(ev->Get()->QueueId);
                    res->AddInfoForQueue(orderNum, queue.ActorId, queue.FlowRecord);
                }
            }
        }
        TActivationContext::Send(ev->Sender, std::move(res));
    }

} // NKikimr
