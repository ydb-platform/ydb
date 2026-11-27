#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "root_cause.h"
#include <ydb/core/blobstorage/dsproxy/dsproxy_request_reporting.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/library/actors/retro_tracing/collector/retro_collector.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <util/generic/set.h>
#include <util/system/datetime.h>
#include <ydb/library/actors/wilson/wilson_event_budget.h>
#include "dsproxy_get_impl.h"

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

struct TEvAccelerateGet : public TEventLocal<TEvAccelerateGet, TEvBlobStorage::EvAccelerateGet> {
    ui64 CauseIdx;
    TEvAccelerateGet(ui64 causeIdx)
        : CauseIdx(causeIdx)
    {}
};

struct TEvAcceleratePut : public TEventLocal<TEvAcceleratePut, TEvBlobStorage::EvAcceleratePut> {
    ui64 CauseIdx;
    TEvAcceleratePut(ui64 causeIdx)
        : CauseIdx(causeIdx)
    {}
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// GET request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupGetRequest : public TBlobStorageGroupRequestActor {
    TGetImpl GetImpl;
    TRootCause RootCauseTrack;
    NLWTrace::TOrbit Orbit;
    const TInstant Deadline;
    TMonotonic StartTimePut;
    ui32 RequestsSent = 0;
    ui32 ResponsesReceived = 0;
    ui32 GroupSize;
    i64 ReportedBytes;
    ui32 MaxSaneRequests = 0;
    bool IsPutStarted = false;

    struct TDiskCounters {
        ui32 Sent = 0;
        ui32 Received = 0;
    };

    TStackVec<TDiskCounters, TypicalDisksInGroup> DiskCounters;

    ui32 GetsAccelerated = 0;
    bool IsGetAccelerateScheduled = false;
    ui32 PutsAccelerated = 0;
    bool IsPutAccelerateScheduled = false;

    struct TTraceQuery {
        TLogoBlobID BlobId;
        ui64 Shift;
        ui64 Size;
        std::optional<ui32> BlobIndex;
    };
    struct TVGetTrace {
        NWilson::TSpan Span;
        TVector<TTraceQuery> Queries;
    };
    THashMap<ui64, TVGetTrace> VGetTraces;
    THashMap<TLogoBlobID, ui32> TraceBlobIndices;
    NWilson::TSpanEventBudget TraceEvents;
    ui32 VGetTraceCount = 0;
    ui64 VGetTraceDropped = 0;

    TAccelerationParams AccelerationParams;
    TDuration LongRequestThreshold;

    void Handle(TEvAccelerateGet::TPtr &ev) {
        IsGetAccelerateScheduled = false;
        RootCauseTrack.OnAccelerate(ev->Get()->CauseIdx);
        AccelerateGet("get_acceleration_timer");
    }

    void Handle(TEvAcceleratePut::TPtr &ev) {
        IsPutAccelerateScheduled = false;
        RootCauseTrack.OnAccelerate(ev->Get()->CauseIdx);
        AcceleratePut("put_acceleration_timer");
    }

    void AccelerateGet(const char* cause = "get_acceleration_threshold") {
        if (GetsAccelerated == 2) {
            return;
        }
        GetsAccelerated++;

        GetImpl.History.AddAcceleration(false);
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.AccelerateGet(LogCtx, GetUnresponsiveDisksMask(), vGets, vPuts);
        *Mon->NodeMon->AccelerateEvVPutCount += vPuts.size();
        *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
        SendVGetsAndVPuts(vGets, vPuts, cause);
        TryScheduleGetAcceleration();
    }

    void AcceleratePut(const char* cause = "put_acceleration_threshold") {
        if (PutsAccelerated == 2) {
            return;
        }
        PutsAccelerated++;

        GetImpl.History.AddAcceleration(true);
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.AcceleratePut(LogCtx, GetUnresponsiveDisksMask(), vGets, vPuts);
        *Mon->NodeMon->AccelerateEvVPutCount += vPuts.size();
        *Mon->NodeMon->AccelerateEvVGetCount += vGets.size();
        SendVGetsAndVPuts(vGets, vPuts, cause);
        TrySchedulePutAcceleration();
    }

    void SendVGetsAndVPuts(TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &vGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &vPuts, const char* cause) {
        ReportBytes(GetImpl.GrabBytesToReport());
        RequestsSent += vGets.size() + vPuts.size();
        for (const auto& vPut : vPuts) {
            CountPut(vPut->GetBufferBytes());
        }
        if (vPuts.size()) {
            if (!IsPutStarted) {
                IsPutStarted = true;
                StartTimePut = TActivationContext::Monotonic();
            }
        }
        for (size_t i = 0; i < vGets.size(); ++i) {
            if (RootCauseTrack.IsOn) {
                vGets[i]->Record.SetCookie(RootCauseTrack.RegisterCause());
            }
            Y_ABORT_UNLESS(vGets[i]->Record.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(vGets[i]->Record.GetVDiskID());
            const TVDiskIdShort shortId(vDiskId);
            ui32 orderNumber = Info->GetOrderNumber(shortId);
            if (DiskCounters.size() <= orderNumber) {
                DiskCounters.resize(orderNumber + 1);
            }
            DiskCounters[orderNumber].Sent++;
            TraceVGetSent(*vGets[i], orderNumber, cause);

            LWTRACK(
                DSProxyVGetSent, Orbit,
                vDiskId.ToStringWOGeneration(),
                orderNumber,
                vGets.size()
            );
        }
        for (size_t i = 0; i < vPuts.size(); ++i) {
            if (RootCauseTrack.IsOn) {
                vPuts[i]->Record.SetCookie(RootCauseTrack.RegisterCause());
            }
            Y_ABORT_UNLESS(vPuts[i]->Record.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(vPuts[i]->Record.GetVDiskID());
            const TVDiskIdShort shortId(vDiskId);
            ui32 orderNumber = Info->GetOrderNumber(shortId);
            if (DiskCounters.size() <= orderNumber) {
                DiskCounters.resize(orderNumber + 1);
            }
            DiskCounters[orderNumber].Sent++;

            LWTRACK(
                DSProxyVPutSent, Orbit,
                vPuts[i]->Type(),
                vDiskId.ToStringWOGeneration(),
                Info->GetFailDomainOrderNumber(vDiskId),
                1,
                vPuts[i]->GetBufferBytes(),
                true
            );
        }
        for (auto& ev : vGets) {
            const ui64 cookie = ev->Record.GetCookie();
            SendToQueue(std::move(ev), cookie);
        }
        for (auto& ev : vPuts) {
            const ui64 cookie = ev->Record.GetCookie();
            SendToQueue(std::move(ev), cookie);
        }
        GetImpl.History.AddAllWaiting();
    }

    std::optional<ui32> TraceBlobIndex(const TLogoBlobID& id) {
        const auto full = id.FullID();
        if (auto it = TraceBlobIndices.find(full); it != TraceBlobIndices.end()) {
            return it->second;
        }
        if (TraceBlobIndices.size() == 32) {
            return {};
        }
        const ui32 index = TraceBlobIndices.size();
        TraceBlobIndices.emplace(full, index);
        return index;
    }

    NWilson::TKeyValueList TraceDecisionAttributes(const TBlobState& state) {
        NWilson::TKeyValueList attrs;
        attrs["capture_version"] = i64(1);
        attrs["decision_id"] = static_cast<i64>(GetImpl.TraceDecisionSequence);
        attrs["strategy"] = TString(GetImpl.TraceStrategy);
        attrs["group_generation"] = static_cast<i64>(Info->GroupGeneration);
        attrs["erasure"] = static_cast<i64>(Info->Type.GetErasure());
        if (const auto index = TraceBlobIndex(state.Id)) {
            attrs["blob_index"] = static_cast<i64>(*index);
        } else {
            attrs["blob_index_truncated"] = true;
        }
        attrs["full_request_replay_supported"] = false;
        return attrs;
    }

    void TraceReadChoice(const TBlobState& state, const TReadChoiceTrace& choice) {
        if (auto* owner = Span.GetWilsonSpanPtr(); owner && *owner) {
            TraceEvents.Event(*owner, 14, "Mirror read candidate choice", [&] {
                auto attrs = TraceDecisionAttributes(state);
                attrs["replay_scope"] = TString("mirror3dc_candidate_choice");
                attrs["candidates_complete"] = choice.Complete;
                attrs["new_request"] = choice.NewRequest;
                attrs["selected"] = bool(choice.Selected);
                if (choice.Selected) {
                    attrs["selected_subgroup_index"] = static_cast<i64>(*choice.Selected);
                }
                NWilson::TArrayValue candidates;
                for (const auto& candidate : choice.Candidates) {
                    NWilson::TKeyValueList item;
                    item["score"] = static_cast<i64>(candidate.Score);
                    item["subgroup_index"] = static_cast<i64>(candidate.SubgroupIndex);
                    item["disk_order"] = static_cast<i64>(candidate.DiskOrder);
                    item["eligible"] = candidate.Eligible;
                    item["slow"] = candidate.Slow;
                    candidates.emplace_back(std::move(item));
                }
                attrs["candidates"] = std::move(candidates);
                return attrs;
            });
        }
    }

    void TraceSlowDisks(const TBlobState& state, const TDiskDelayPredictions& predictions,
            ui64 threshold, bool isPut) {
        if (auto* owner = Span.GetWilsonSpanPtr(); owner && *owner) {
            TraceEvents.Event(*owner, 14, "Slow disk decision inputs", [&] {
                auto attrs = TraceDecisionAttributes(state);
                attrs["is_put"] = isPut;
                attrs["prediction_age_known"] = false;
                attrs["slow_threshold_ns"] = static_cast<i64>(threshold);
                attrs["prediction_multiplier"] = double(AccelerationParams.PredictedDelayMultiplier);
                attrs["slow_disk_threshold"] = double(AccelerationParams.SlowDiskThreshold);
                attrs["max_slow_disks"] = static_cast<i64>(AccelerationParams.MaxNumOfSlowDisks);
                attrs["queue_id"] = static_cast<i64>(isPut
                    ? HandleClassToQueueId(GetImpl.Blackboard.PutHandleClass)
                    : HandleClassToQueueId(GetImpl.Blackboard.GetHandleClass));
                NWilson::TArrayValue disks;
                for (const auto& prediction : predictions) {
                    if (disks.size() == 32) {
                        break;
                    }
                    const auto& disk = state.Disks[prediction.DiskIdx];
                    NWilson::TKeyValueList item;
                    item["subgroup_index"] = static_cast<i64>(prediction.DiskIdx);
                    item["disk_order"] = static_cast<i64>(disk.OrderNumber);
                    item["scaled_prediction_ns"] = static_cast<i64>(prediction.PredictedNs);
                    item["slow"] = disk.IsSlow;
                    disks.emplace_back(std::move(item));
                }
                attrs["predictions_truncated"] = predictions.size() > 32;
                attrs["predictions"] = std::move(disks);
                return attrs;
            });
        }
    }

    void TraceVGetSent(const TEvBlobStorage::TEvVGet& event, ui32 disk, const char* cause) {
        auto* owner = Span.GetWilsonSpanPtr();
        if (!owner || !*owner || owner->GetTraceId().GetVerbosity() < 13) {
            return;
        }
        const ui64 cookie = event.Record.GetCookie();
        if (cookie == TRootCause::InvalidCauseIdx || VGetTraceCount >= 32) {
            ++VGetTraceDropped;
            return;
        }
        ++VGetTraceCount;
        auto& trace = VGetTraces[cookie];
        trace.Span = owner->CreateChild(13, "DSProxy VGet roundtrip", NWilson::EFlags::AUTO_END);
        trace.Span.Attribute("ydb.storage.cause_id", static_cast<i64>(cookie));
        const bool triggerKnown = RootCauseTrack.CurrentCauseIdx < RootCauseTrack.Items.size();
        trace.Span.Attribute("ydb.storage.trigger_cause_known", triggerKnown);
        if (triggerKnown) {
            trace.Span.Attribute("ydb.storage.trigger_cause_id", static_cast<i64>(RootCauseTrack.CurrentCauseIdx));
        }
        trace.Span.Attribute("ydb.storage.send_cause", TString(cause));
        trace.Span.Attribute("ydb.storage.disk_order", static_cast<i64>(disk));
        trace.Span.Attribute("ydb.storage.group_generation", static_cast<i64>(Info->GroupGeneration));
        trace.Span.Attribute("ydb.storage.requested_items", static_cast<i64>(event.Record.ExtremeQueriesSize()));
        for (const auto& query : event.Record.GetExtremeQueries()) {
            if (trace.Queries.size() == 32) {
                break;
            }
            const auto blob = LogoBlobIDFromLogoBlobID(query.GetId());
            const ui64 shift = query.GetShift();
            const ui64 partSize = Info->Type.PartSize(blob);
            const ui64 size = query.GetSize() ? query.GetSize() : (partSize >= shift ? partSize - shift : 0);
            trace.Queries.push_back({blob, shift, size, TraceBlobIndex(blob)});
        }
        trace.Span.Attribute("ydb.storage.items_truncated", event.Record.ExtremeQueriesSize() > 32);
        TraceEvents.Event(trace.Span, 13, "VGet dispatched");
    }

    void TraceVGetResult(const TEvBlobStorage::TEvVGetResult& event, size_t nextGets, size_t nextPuts, bool replied) {
        const auto& record = event.Record;
        if (!record.HasCookie()) {
            return;
        }
        const auto it = VGetTraces.find(record.GetCookie());
        if (it == VGetTraces.end()) {
            return;
        }
        auto& trace = it->second;
        trace.Span.Attribute("ydb.storage.response_status", static_cast<i64>(record.GetStatus()));
        trace.Span.Attribute("ydb.storage.response_items", static_cast<i64>(record.ResultSize()));
        trace.Span.Attribute("ydb.storage.next_gets", static_cast<i64>(nextGets));
        trace.Span.Attribute("ydb.storage.next_puts", static_cast<i64>(nextPuts));
        trace.Span.Attribute("ydb.storage.request_replied", replied);
        // Bound work as well as output. Raw blob IDs and intervals stay local.
        for (size_t index = 0; index < trace.Queries.size(); ++index) {
            const auto& requested = trace.Queries[index];
            const NKikimrBlobStorage::TQueryResult* result = nullptr;
            for (ui32 i = 0; i < record.ResultSize() && i < 32; ++i) {
                const auto& candidate = record.GetResult(i);
                if (LogoBlobIDFromLogoBlobID(candidate.GetBlobID()) == requested.BlobId
                        && candidate.GetShift() == requested.Shift) {
                    result = &candidate;
                    break;
                }
            }
            TraceEvents.Event(trace.Span, 13, "VGet item coverage", [&] {
                NWilson::TKeyValueList attributes;
                attributes["item_index"] = static_cast<i64>(index);
                attributes["requested_part_bytes"] = static_cast<i64>(requested.Size);
                attributes["response_item_observed"] = result != nullptr;
                attributes["result_scan_truncated"] = record.ResultSize() > 32;
                if (result) {
                    const ui64 bytes = event.GetBlobSize(*result);
                    const ui64 begin = Max(requested.Shift, static_cast<ui64>(result->GetShift()));
                    const ui64 end = Min(requested.Shift + requested.Size, static_cast<ui64>(result->GetShift()) + bytes);
                    const ui64 covered = result->GetStatus() == NKikimrProto::OK && end > begin ? end - begin : 0;
                    attributes["item_status"] = static_cast<i64>(result->GetStatus());
                    attributes["returned_part_bytes"] = static_cast<i64>(bytes);
                    attributes["missing_requested_part_bytes"] = static_cast<i64>(requested.Size - covered);
                }
                const auto blob = requested.BlobId.FullID();
                const auto& board = GetImpl.Blackboard;
                const TBlobState* state = nullptr;
                if (auto active = board.BlobStates.find(blob); active != board.BlobStates.end()) {
                    state = &active->second;
                } else if (auto done = board.DoneBlobStates.find(blob); done != board.DoneBlobStates.end()) {
                    state = &done->second;
                }
                if (state) {
                    if (requested.BlobIndex) {
                        attributes["blob_index"] = static_cast<i64>(*requested.BlobIndex);
                    } else {
                        attributes["blob_index_truncated"] = true;
                    }
                    attributes["strategy_whole_situation"] = static_cast<i64>(state->WholeSituation);
                }
                return attributes;
            });
        }
        trace.Span.End();
        VGetTraces.erase(it);
    }

    void FinishGetTrace() {
        if (auto* owner = Span.GetWilsonSpanPtr(); owner && *owner) {
            owner->Attribute("ydb.storage.vget_spans_dropped", static_cast<i64>(VGetTraceDropped));
            owner->Attribute("ydb.trace.event_budget.scope", TString("dsproxy_get_coverage"));
            TraceEvents.Annotate(*owner);
        }
        for (auto& [cookie, trace] : VGetTraces) {
            trace.Span.Attribute("ydb.storage.wait_outcome", TString("request_ended_before_reply"));
            trace.Span.End();
        }
        VGetTraces.clear();
    }

    ui32 CountDisksWithActiveRequests() {
        ui32 activeCount = 0;
        for (size_t i = 0; i < DiskCounters.size(); ++i) {
            if (DiskCounters[i].Sent != DiskCounters[i].Received) {
                ++activeCount;
            }
        }
        return activeCount;
    }

    ui32 GetUnresponsiveDisksMask() {
        i32 unresponsiveDisksMask = 0;
        for (size_t i = 0; i < DiskCounters.size(); ++i) {
            if (DiskCounters[i].Sent != DiskCounters[i].Received) {
                unresponsiveDisksMask |= 1 << i;
            }
        }
        return unresponsiveDisksMask;
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());

        ui64 totalSize = 0;
        ui64 tabletId = 0;
        ui32 channel = 0;
        for (ui32 i = 0; i < record.ResultSize(); ++i) {
            const NKikimrBlobStorage::TQueryResult &queryResult = record.GetResult(i);
            if (record.GetStatus() == NKikimrProto::OK) {
                totalSize += ev->Get()->GetBlobSize(queryResult);
            }
            const TLogoBlobID blob = LogoBlobIDFromLogoBlobID(queryResult.GetBlobID());
            tabletId = blob.TabletID();
            channel = blob.Channel();
        }
        ++GeneratedSubrequests;
        GeneratedSubrequestBytes += totalSize;

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vdisk);
        ui32 orderNumber = GetImpl.GetOrderNumber(shortId);

        LWTRACK(DSProxyVDiskRequestDuration, Orbit, TEvBlobStorage::EvVGet, totalSize, tabletId, vdisk.GroupID.GetRawId(), channel,
                Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EGetHandleClass_Name(GetImpl.GetHandleClass()),
                NKikimrProto::EReplyStatus_Name(record.GetStatus()));
        if (RootCauseTrack.IsOn && record.HasCookie()) {
            RootCauseTrack.OnReply(record.GetCookie(),
                    GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                    GetVDiskTimeMs(record.GetTimestamps()));
        }

        if (DiskCounters.size() <= orderNumber) {
            DiskCounters.resize(orderNumber + 1);
        }
        DiskCounters[orderNumber].Received++;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        ResponsesReceived++;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.OnVGetResult(LogCtx, *ev->Get(), vGets, vPuts, getResult);
        TraceVGetResult(*ev->Get(), vGets.size(), vPuts.size(), bool(getResult));
        SendVGetsAndVPuts(vGets, vPuts, "vget_result");

        if (getResult) {
            SendReplyAndDie(getResult);
            return;
        }
        Y_ABORT_UNLESS(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu32 " ResponsesReceived# %" PRIu32
                " GetImpl.DumpFullState# %s", RequestsSent, ResponsesReceived, GetImpl.DumpFullState().c_str());

        TryScheduleGetAcceleration();
        if (IsPutStarted) {
            TrySchedulePutAcceleration();
        }
        SanityCheck(); // May Die
    }

    void SanityCheck() {
        if (RequestsSent <= MaxSaneRequests) {
            return;
        }
        TStringStream err;
        err << "Group# " << Info->GroupID
            << " sent over MaxSaneRequests# " << MaxSaneRequests
            << " requests, internal state# " << GetImpl.DumpFullState();
        ErrorReason = err.Str();
        DSP_LOG_CRIT_S("BPG70", ErrorReason);
        ReplyAndDie(NKikimrProto::ERROR);
    }

    TLogoBlobID GetFirstBlobId(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        return LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetBlobID());
    }

    ui64 SumBlobSize(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        return GetFirstBlobId(ev).BlobSize();
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());
        HandleVPutResult(ev);
    }

    void HandleVPutResult(TEvBlobStorage::TEvVPutResult::TPtr &ev) {
        Y_ABORT_UNLESS(ev->Get()->Record.HasStatus());

        const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
        ev->Get()->Record.MutableTimestamps()->SetReceivedByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        const auto &record = ev->Get()->Record;
        const TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        TVDiskIdShort shortId(vDiskId);
        ui32 orderNumber = GetImpl.GetOrderNumber(shortId);
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        NActors::NLog::EPriority priority = PriorityForStatusInbound(status);
        DSP_LOG_LOG_S(priority, "BPG30", "Handle VPuEventResult"
            << " status# " << NKikimrProto::EReplyStatus_Name(status).data()
            << " node# " << GetVDiskActorId(shortId).NodeId());

        const TLogoBlobID blob = GetFirstBlobId(ev);
        ui64 sumBlobSize = SumBlobSize(ev);
        LWTRACK(DSProxyVDiskRequestDuration, Orbit, TEvBlobStorage::EvVPut, sumBlobSize, blob.TabletID(),
                Info->GroupID.GetRawId(), blob.Channel(), Info->GetFailDomainOrderNumber(shortId),
                GetStartTime(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()),
                GetVDiskTimeMs(record.GetTimestamps()),
                GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                NKikimrBlobStorage::EPutHandleClass_Name(GetImpl.GetPutHandleClass()),
                NKikimrProto::EReplyStatus_Name(status));

        if (RootCauseTrack.IsOn && record.HasCookie()) {
            RootCauseTrack.OnReply(record.GetCookie(),
                    GetTotalTimeMs(record.GetTimestamps()) - GetVDiskTimeMs(record.GetTimestamps()),
                    GetVDiskTimeMs(record.GetTimestamps()));
        }

        if (DiskCounters.size() <= orderNumber) {
            DiskCounters.resize(orderNumber + 1);
        }
        DiskCounters[orderNumber].Received++;

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        ResponsesReceived++;

        GetImpl.OnVPutResult(LogCtx, *ev->Get(), vGets, vPuts, getResult);
        SendVGetsAndVPuts(vGets, vPuts, "vput_result");
        if (getResult) {
            SendReplyAndDie(getResult);
            return;
        }

        Y_ABORT_UNLESS(RequestsSent > ResponsesReceived, "RequestsSent# %" PRIu64 " ResponsesReceived# %" PRIu64,
                ui64(RequestsSent), ui64(ResponsesReceived));

        TrySchedulePutAcceleration();
        SanityCheck(); // May Die
    }

    void TryScheduleGetAcceleration() {
        if (!IsGetAccelerateScheduled && GetsAccelerated < AccelerationParams.MaxNumOfSlowDisks) {
            // Count VDisks with requests in flight, if there are <= the maximum number of slow VDisks, Accelerate
            if (CountDisksWithActiveRequests() <= AccelerationParams.MaxNumOfSlowDisks) {
                ui64 timeToAccelerateUs = GetImpl.GetTimeToAccelerateGetNs(LogCtx) / 1000;
                TDuration timeToAccelerate = TDuration::MicroSeconds(timeToAccelerateUs);
                TMonotonic now = TActivationContext::Monotonic();
                TMonotonic nextAcceleration = RequestStartTime + timeToAccelerate;
                LWTRACK(DSProxyScheduleAccelerate, Orbit, nextAcceleration > now ? (nextAcceleration - now).MicroSeconds() / 1000.0 : 0.0, "Get");
                if (nextAcceleration > now) {
                    ui64 causeIdx = RootCauseTrack.RegisterAccelerate("Get");
                    Schedule(nextAcceleration - now, new TEvAccelerateGet(causeIdx));
                    IsGetAccelerateScheduled = true;
                } else {
                    AccelerateGet();
                }
            }
        }
    }

    void TrySchedulePutAcceleration() {
        if (!IsPutAccelerateScheduled && PutsAccelerated < AccelerationParams.MaxNumOfSlowDisks) {
            // Count VDisks with requests in flight, if there are <= the maximum number of slow VDisks, Accelerate
            if (CountDisksWithActiveRequests() <= AccelerationParams.MaxNumOfSlowDisks) {
                ui64 timeToAccelerateUs = GetImpl.GetTimeToAcceleratePutNs(LogCtx) / 1000;
                TDuration timeToAccelerate = TDuration::MicroSeconds(timeToAccelerateUs);
                TMonotonic now = TActivationContext::Monotonic();
                TMonotonic nextAcceleration = RequestStartTime + timeToAccelerate;
                LWTRACK(DSProxyScheduleAccelerate, Orbit, nextAcceleration > now ? (nextAcceleration - now).MicroSeconds() / 1000.0 : 0.0, "Put");
                if (nextAcceleration > now) {
                    ui64 causeIdx = RootCauseTrack.RegisterAccelerate("Put");
                    Schedule(nextAcceleration - now, new TEvAcceleratePut(causeIdx));
                    IsPutAccelerateScheduled = true;
                } else {
                    AcceleratePut();
                }
            }
        }
    }

    void SendReplyAndDie(TAutoPtr<TEvBlobStorage::TEvGetResult> &evResult) {
        if (auto* owner = Span.GetWilsonSpanPtr(); owner && *owner) {
            for (ui32 index = 0; index < Min(evResult->ResponseSz, 32u); ++index) {
                const auto& response = evResult->Responses[index];
                TraceEvents.Event(*owner, 13, "Get item result", [&] {
                    NWilson::TKeyValueList attributes;
                    attributes["query_index"] = static_cast<i64>(index);
                    attributes["status"] = static_cast<i64>(response.Status);
                    attributes["requested_bytes"] = static_cast<i64>(response.RequestedSize);
                    attributes["returned_bytes"] = static_cast<i64>(response.Buffer.size());
                    return attributes;
                });
            }
            owner->Attribute("ydb.storage.get_items_truncated", evResult->ResponseSz > 32);
        }
        FinishGetTrace();
        const TMonotonic now = TActivationContext::Monotonic();
        const TDuration duration = now - RequestStartTime;
        NKikimrBlobStorage::EGetHandleClass handleClass = GetImpl.GetHandleClass();
        Mon->CountGetResponseTime(Info->GetDeviceType(), handleClass, evResult->PayloadSizeBytes(), duration);
        *Mon->ActiveGetCapacity -= ReportedBytes;
        ReportedBytes = 0;
        bool success = evResult->Status == NKikimrProto::OK;
        ui64 requestSize = 0;
        ui64 tabletId = 0;
        ui32 channel = 0;
        for (ui32 i = 0; i < evResult->ResponseSz; ++i) {
            tabletId = evResult->Responses[i].Id.TabletID();
            channel = evResult->Responses[i].Id.Channel();
            requestSize += evResult->Responses[i].RequestedSize;
        }
        RootCauseTrack.RenderTrack(Orbit);
        LWTRACK(DSProxyGetReply, Orbit);
        evResult->Orbit = std::move(Orbit);
        LWPROBE(DSProxyRequestDuration, TEvBlobStorage::EvGet, requestSize, duration.SecondsFloat() * 1000.0, tabletId,
                evResult->GroupId, channel, NKikimrBlobStorage::EGetHandleClass_Name(handleClass),
                success);
        DSP_LOG_LOG_S(success ? NLog::PRI_INFO : NLog::PRI_NOTICE, "BPG68", "Result# " << evResult->Print(false) <<" GroupId# " << Info->GroupID);

        if ((TActivationContext::Monotonic() - RequestStartTime >= LongRequestThreshold)) {
            if (PopAllowToken(handleClass)) {
                YDB_LOG_WARN_COMP(BS_PROXY_GET, "Long TEvGet request detected",
                    {"marker", "BPG71"},
                    {"longRequestThreshold", LongRequestThreshold},
                    {"groupId", Info->GroupID},
                    {"subrequestsCount", evResult->ResponseSz},
                    {"requestTotalSize", requestSize},
                    {"handleClass", NKikimrBlobStorage::EGetHandleClass_Name(handleClass)},
                    {"restartCounter", RestartCounter},
                    {"history", GetImpl.PrintHistory()});
            }

            if (EnableStorageRetroTraceCollectionSlowRequests) {
                if (TNamedSpan* retroSpan = Span.GetRetroSpanPtr()) {
                    retroSpan->DemandTraceOnEnd();
                }
            }
        }

        auto resultStatusPriority = PriorityForStatusResult(evResult->Status);
        if (IS_LOG_PRIORITY_ENABLED(resultStatusPriority, LogCtx.LogComponent) && PopAllowToken(handleClass)) {
            YDB_LOG_COMP(resultStatusPriority, BS_PROXY_GET, "Query history",
                {"marker", "BPG72"},
                {"groupId", Info->GroupID},
                {"handleClass", NKikimrBlobStorage::EGetHandleClass_Name(handleClass)},
                {"history", GetImpl.PrintHistory()});
        }

        return SendResponseAndDie(std::unique_ptr<TEvBlobStorage::TEvGetResult>(evResult.Release()));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartIndexRestoreGet;
        return GetImpl.RestartQuery(counter);
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveGet;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Get;
    }


    TBlobStorageGroupGetRequest(TBlobStorageGroupGetParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , GetImpl(Info, GroupQueues, params.Common.Event, std::move(params.NodeLayout),
                params.AccelerationParams, params.Common.EnableChecksumCalcAndValidationOnDsProxy, LogCtx.RequestPrefix)
        , Orbit(std::move(params.Common.Event->Orbit))
        , Deadline(params.Common.Event->Deadline)
        , StartTimePut(RequestStartTime)
        , GroupSize(Info->Type.BlobSubgroupSize())
        , ReportedBytes(0)
        , AccelerationParams(params.AccelerationParams)
        , LongRequestThreshold(params.LongRequestThreshold)
    {
        ReportBytes(sizeof(*this));
        MaxSaneRequests = params.Common.Event->QuerySize * Info->Type.TotalPartCount() *
                (1 + Info->Type.Handoff()) * 3;

        RequestBytes = GetImpl.CountRequestBytes();
        RequestHandleClass = HandleClassToHandleClass(params.Common.Event->GetHandleClass);
        if (Orbit.HasShuttles()) {
            RootCauseTrack.IsOn = true;
        }
        if (auto* wilson = Span.GetWilsonSpanPtr(); wilson && *wilson && wilson->GetTraceId().GetVerbosity() >= 13) {
            // Reuse the existing bounded cause-cookie mechanism for Wilson too.
            RootCauseTrack.IsOn = true;
            if (wilson->GetTraceId().GetVerbosity() >= 14) {
                GetImpl.Blackboard.TraceReadChoice = [this](const TBlobState& state, const TReadChoiceTrace& choice) {
                    TraceReadChoice(state, choice);
                };
                GetImpl.Blackboard.TraceSlowDisks = [this](const TBlobState& state,
                        const TDiskDelayPredictions& predictions, ui64 threshold, bool isPut) {
                    TraceSlowDisks(state, predictions, threshold, isPut);
                };
            }
        }
    }

    void ReportBytes(i64 bytes) {
        ReportedBytes += bytes;
        *Mon->ActiveGetCapacity += bytes;
    }

    void Bootstrap() override {
        DSP_LOG_INFO_S("BPG01", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " Query# " << GetImpl.DumpQuery()
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter);

        LWTRACK(DSProxyGetBootstrap, Orbit);

        LWTRACK(
            DSProxyGetRequest, Orbit,
            Info->GroupID.GetRawId(),
            DeviceTypeStr(Info->GetDeviceType(), true),
            NKikimrBlobStorage::EGetHandleClass_Name(GetImpl.GetHandleClass())
        );

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        GetImpl.GenerateInitialRequests(LogCtx, vGets);
        SendVGetsAndVPuts(vGets, vPuts, "initial");
        TryScheduleGetAcceleration();

        Y_ABORT_UNLESS(RequestsSent > ResponsesReceived);
        Become(&TBlobStorageGroupGetRequest::StateWait);
        SanityCheck(); // May Die
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        GetImpl.PrepareReply(status, ErrorReason, LogCtx, getResult);
        SendReplyAndDie(getResult);
        return;
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
            hFunc(TEvBlobStorage::TEvVPutResult, Handle);
            hFunc(TEvAccelerateGet, Handle);
            hFunc(TEvAcceleratePut, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupGetRequest(TBlobStorageGroupGetParameters params) {
    return new TBlobStorageGroupGetRequest(params);
}

}//NKikimr
