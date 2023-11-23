#include "dsproxy_impl.h"
#include "dsproxy_monactor.h"
#include <ydb/core/base/feature_flags.h>


namespace NKikimr {

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
        TActorId reqID;
        if (ev->Get()->IsIndexOnly) {
            Mon->EventIndexRestoreGet->Inc();
            reqID = Register(
                CreateBlobStorageGroupIndexRestoreGetRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
                    ev->Get(), ev->Cookie, std::move(ev->TraceId), {}, TActivationContext::Now(), StoragePoolCounters));
            ActiveRequests.insert(reqID);
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
                reqID = Register(
                    CreateBlobStorageGroupGetRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
                        ev->Get(), ev->Cookie, std::move(ev->TraceId), TNodeLayoutInfoPtr(NodeLayoutInfo),
                        kind, TActivationContext::Now(), StoragePoolCounters, false));
                ActiveRequests.insert(reqID);
            } else {
                Mon->EventMultiGet->Inc();
                reqID = Register(
                    CreateBlobStorageGroupMultiGetRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
                        ev->Get(), ev->Cookie, std::move(ev->TraceId), kind, TActivationContext::Now(), StoragePoolCounters));
                ActiveRequests.insert(reqID);
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
            const TActorId reqID = Register(
                    CreateBlobStorageGroupPutRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
                        ev->Get(), ev->Cookie, std::move(ev->TraceId), Mon->TimeStats.IsEnabled(),
                        PerDiskStats, kind, TActivationContext::Now(), StoragePoolCounters,
                        enableRequestMod3x3ForMinLatency));
            ActiveRequests.insert(reqID);
        }
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvBlock::TPtr &ev) {
        EnsureMonitoring(ev->Get()->IsMonitored);
        Mon->EventBlock->Inc();
        const TActorId reqID = Register(CreateBlobStorageGroupBlockRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
                ev->Get(), ev->Cookie, std::move(ev->TraceId), TActivationContext::Now(), StoragePoolCounters));
        ActiveRequests.insert(reqID);
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
        const TActorId reqId = Register(
                CreateBlobStorageGroupPatchRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
                        ev->Get(), ev->Cookie, std::move(ev->TraceId), now,
                        StoragePoolCounters, EnableVPatch.Update(now)));
        ActiveRequests.insert(reqId);
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
        auto&& callback = Info->Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc
            ? CreateBlobStorageGroupMirror3dcDiscoverRequest
            : Info->Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3of4
            ? CreateBlobStorageGroupMirror3of4DiscoverRequest
            : CreateBlobStorageGroupDiscoverRequest;
        const TActorId reqID = Register(callback(Info, Sessions->GroupQueues, ev->Sender, Mon,
            ev->Get(), ev->Cookie, std::move(ev->TraceId), TActivationContext::Now(), StoragePoolCounters));
        ActiveRequests.insert(reqID);
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvRange::TPtr &ev) {
        if (IsLimitedKeyless) {
            ErrorDescription = "Created as LIMITED without keys. It happens when tenant keys are missing on the node.";
            HandleError(ev);
            return;
        }
        EnsureMonitoring(true);
        Mon->EventRange->Inc();
        const TActorId reqID = Register(
            CreateBlobStorageGroupRangeRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
                ev->Get(), ev->Cookie, std::move(ev->TraceId), TActivationContext::Now(), StoragePoolCounters));
        ActiveRequests.insert(reqID);
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvCollectGarbage::TPtr &ev) {
        EnsureMonitoring(ev->Get()->IsMonitored);

        if (!ev->Get()->IsMultiCollectAllowed || ev->Get()->PerGenerationCounterStepSize() == 1) {
            Mon->EventCollectGarbage->Inc();
            const TActorId reqID = Register(CreateBlobStorageGroupCollectGarbageRequest(Info, Sessions->GroupQueues,
                ev->Sender, Mon, ev->Get(), ev->Cookie, std::move(ev->TraceId), TActivationContext::Now(),
                StoragePoolCounters));
            ActiveRequests.insert(reqID);
        } else {
            Mon->EventMultiCollect->Inc();
            const TActorId reqID = Register(CreateBlobStorageGroupMultiCollectRequest(Info, Sessions->GroupQueues,
                ev->Sender, Mon, ev->Get(), ev->Cookie, std::move(ev->TraceId), TActivationContext::Now(),
                StoragePoolCounters));
            ActiveRequests.insert(reqID);
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
        const TActorId reqID = Register(CreateBlobStorageGroupStatusRequest(Info, Sessions->GroupQueues, ev->Sender, Mon,
            ev->Get(), ev->Cookie, std::move(ev->TraceId), TActivationContext::Now(), StoragePoolCounters));
        ActiveRequests.insert(reqID);
    }

    void TBlobStorageGroupProxy::HandleNormal(TEvBlobStorage::TEvAssimilate::TPtr &ev) {
        EnsureMonitoring(true);
        Mon->EventAssimilate->Inc();
        const TActorId reqID = Register(CreateBlobStorageGroupAssimilateRequest(Info, Sessions->GroupQueues, ev->Sender,
            Mon, ev->Get(), ev->Cookie, std::move(ev->TraceId), TActivationContext::Now(), StoragePoolCounters));
        ActiveRequests.insert(reqID);
    }

    void TBlobStorageGroupProxy::Handle(TEvDeathNote::TPtr ev) {
        const bool wasEmpty = ResponsivenessTracker.IsEmpty();
        for (const auto &item : ev->Get()->Responsiveness) {
            ResponsivenessTracker.Register(item.first, TActivationContext::Now(), item.second);
        }
        if (wasEmpty && !ResponsivenessTracker.IsEmpty()) {
            ScheduleUpdateResponsiveness();
        }
        ActiveRequests.erase(ev->Sender);
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
                    const TActorId reqID = Register(
                        CreateBlobStorageGroupPutRequest(Info, Sessions->GroupQueues, batchedPuts.Queue.front()->Sender,
                            Mon, batchedPuts.Queue.front()->Get(), batchedPuts.Queue.front()->Cookie,
                            std::move(batchedPuts.Queue.front()->TraceId), Mon->TimeStats.IsEnabled(), PerDiskStats, kind,
                            TActivationContext::Now(), StoragePoolCounters, enableRequestMod3x3ForMinLatency));
                    ActiveRequests.insert(reqID);
                } else {
                    const TActorId reqID = Register(
                        CreateBlobStorageGroupPutRequest(Info, Sessions->GroupQueues,
                            Mon, batchedPuts.Queue, Mon->TimeStats.IsEnabled(), PerDiskStats, kind, TActivationContext::Now(),
                            StoragePoolCounters, handleClass, tactic, enableRequestMod3x3ForMinLatency));
                    ActiveRequests.insert(reqID);
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
        LWPROBE(DSProxyBatchedPutRequest, BatchedPutRequestCount, GroupId);
        BatchedPutRequestCount = 0;
        EnablePutBatching.Update(TActivationContext::Now());
    }

    void TBlobStorageGroupProxy::Handle(TEvStopBatchingGetRequests::TPtr& ev) {
        StopGetBatchingEvent = ev;
        ++*Mon->EventStopGetBatching;
        LWPROBE(DSProxyBatchedGetRequest, BatchedGetRequestCount, GroupId);
        BatchedGetRequestCount = 0;
    }

} // NKikimr
