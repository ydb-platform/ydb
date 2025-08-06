#pragma once

#include "defs.h"


#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_gate.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_requestimpl.h"

#include <util/system/type_name.h>

#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

class TReqCreator {
private:
    // PDisk info
    const ui32 PDiskId;
    TActorSystem *ActorSystem;
    TPDiskMon *Mon;
    TDriveModel *Model;
    TAtomic *EstimatedLogChunkIdx;
    bool SeparateHugePriorities = false;

public:
    // Self variables
    TAtomic LastReqId;

private:
    void Classify(TRequestBase* request) {
        request->EstimateCost(*Model);
        request->TotalCost = request->Cost;
        switch (request->GetType()) {
            case ERequestType::RequestLogRead:
                request->GateId = GateLog;
                request->IsSensitive = true;
                return;
            case ERequestType::RequestLogReadContinue:
                request->GateId = GateLog;
                request->IsSensitive = true;
                return;
            case ERequestType::RequestLogSectorRestore:
                request->GateId = GateLog;
                request->IsSensitive = true;
                return;
            case ERequestType::RequestLogReadResultProcess:
                request->GateId = GateLog;
                request->IsSensitive = true;
                return;
            case ERequestType::RequestLogWrite:
                request->GateId = GateLog;
                request->IsSensitive = true;
                return;
            case ERequestType::RequestChunkForget:
                request->GateId = GateLog;
                request->IsSensitive = true;
                return;
            case ERequestType::RequestChunkRead:
                request->IsFast = (request->PriorityClass == NPriRead::HullOnlineOther);
                request->IsSensitive = (request->PriorityClass == NPriRead::HullOnlineRt);
                switch (request->PriorityClass) {
                    case NPriRead::HullComp:
                        request->GateId = GateComp;
                        break;
                    case NPriRead::HullOnlineRt:
                        request->GateId = GateFastRead;
                        break;
                    case NPriRead::HullOnlineOther:
                        request->GateId = GateOtherRead;
                        break;
                    case NPriRead::HullLoad:
                        request->GateId = GateLoad;
                        break;
                    case NPriRead::SyncLog:
                        request->GateId = GateSyncLog;
                        break;
                    case NPriRead::HullLow:
                        request->GateId = GateLow;
                        break;
                    default:
                        request->GateId = GateOtherRead;
                        break;
                }
                return;
            case ERequestType::RequestChunkWrite:
                request->IsFast = (request->PriorityClass == NPriWrite::HullHugeAsyncBlob ||
                        request->PriorityClass == NPriWrite::HullHugeUserData);
                switch (request->PriorityClass) {
                    case NPriWrite::HullFresh:
                        request->GateId = GateFresh;
                        break;
                    case NPriWrite::HullComp:
                        request->GateId = GateComp;
                        break;
                    case NPriWrite::HullHugeAsyncBlob:
                        request->GateId = SeparateHugePriorities ? GateHugeAsync : GateHugeUser;
                        break;
                    case NPriWrite::HullHugeUserData:
                        request->GateId = GateHugeUser;
                        break;
                    case NPriWrite::SyncLog:
                        request->GateId = GateSyncLog;
                        break;
                    default:
                        request->GateId = GateHugeUser;
                        break;
                }
                request->IsSensitive = false;
                return;
            case ERequestType::RequestChunkTrim:
                request->GateId = GateTrim;
                request->IsSensitive = false;
                return;
            default: // FastOperationsQueue
                request->GateId = GateFastOperation;
                request->IsSensitive = false;
                return;
        }
    }

    template <class TRequest>
    TRequest* NewRequest(TRequest* request, double* burstMs = nullptr) {
        // Note that call to Classify() is thread-safe (thanks to the fact that queues are not created dynamically)
        Classify(request);
        CountRequest(*request);
        LWTRACK(PDiskNewRequest, request->Orbit, PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
                double(request->Cost) / 1000000.0, request->IsSensitive, request->IsFast,
                request->Owner, request->PriorityClass, (ui32)request->GetType());
        double tmpBurstMs = 0;
        if (request->GateId != GateFastOperation && request->GateId != GateTrim) {
            if (request->IsSensitive) {
                tmpBurstMs = Mon->SensitiveBurst.Increment(request->Cost);
            } else {
                tmpBurstMs = Mon->BestEffortBurst.Increment(request->Cost);
            }
            LWTRACK(PDiskBurst, request->Orbit, PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
                    request->IsSensitive, double(request->Cost) / 1000000.0, tmpBurstMs);
        }
        if (burstMs) {
            *burstMs = tmpBurstMs;
        }
        return request;
    }

#define CASE_COUNT_REQUEST(name) \
    case ERequestType::Request##name: Mon->name.CountRequest(); break;


    template<typename T>
    void CountRequest(const T& req) {
        switch (req.GetType()) {
        CASE_COUNT_REQUEST(YardInit);
        CASE_COUNT_REQUEST(CheckSpace);
        CASE_COUNT_REQUEST(Harakiri);
        CASE_COUNT_REQUEST(YardSlay);
        CASE_COUNT_REQUEST(ChunkReserve);
        CASE_COUNT_REQUEST(YardControl);
        CASE_COUNT_REQUEST(LogRead);
        default: break;
        }
    }
#undef CASE_COUNT_REQUEST

    template<typename TEv>
    static TString ToString(const TEv &ev) {
        return ev.ToString();
    }

    template<typename TEv>
    static TString ToString(const TAutoPtr<NActors::TEventHandle<TEv>> &ev) {
        Y_ABORT_UNLESS(ev && ev->Get());
        return ev->Get()->ToString();
    }

public:
    TReqCreator(ui32 pDiskId, TPDiskMon *mon, TDriveModel *model, TAtomic *estimatedChunkIdx, bool separateHugePriorities)
        : PDiskId(pDiskId)
        , ActorSystem(nullptr)
        , Mon(mon)
        , Model(model)
        , EstimatedLogChunkIdx(estimatedChunkIdx)
        , SeparateHugePriorities(separateHugePriorities)
        , LastReqId(ui64(PDiskId) * 10000000ull)
    {}

    void SetActorSystem(TActorSystem *actorSystem) {
        ActorSystem = actorSystem;
    }

    template<typename TReq, typename TEvPtr>
    [[nodiscard]] TReq* CreateFromEvPtr(TEvPtr &ev, double *burstMs = nullptr) {
        auto& sender = ev->Sender;
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId << " ev# "
                << ToString(ev) << " Sender# " << sender.LocalId() << " ReqId# " << AtomicGet(LastReqId));
        auto req = MakeHolder<TReq>(ev, PDiskId, AtomicIncrement(LastReqId));
        NewRequest(req.Get(), burstMs);
        return req.Release();
    }

    template<typename TReq, typename TEv>
    [[nodiscard]] TReq* CreateFromEv(TEv &&ev, const TActorId &sender, double *burstMs = nullptr) {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId << " ev# "
                << ToString(ev) << " Sender# " << sender.LocalId() << " ReqId# " << AtomicGet(LastReqId));
        auto req = MakeHolder<TReq>(std::forward<TEv>(ev), sender, AtomicIncrement(LastReqId));
        NewRequest(req.Get(), burstMs);
        return req.Release();
    }

    template<typename TReq, typename... TArgs>
    [[nodiscard]] TReq* CreateFromArgs(TArgs&&... args) {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId << " create req# "
                << TypeName<TReq>() << " ReqId# " << AtomicGet(LastReqId));
        auto req = MakeHolder<TReq>(std::forward<TArgs>(args)..., AtomicIncrement(LastReqId));
        NewRequest(req.Get(), nullptr);
        return req.Release();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TODO: Make all functions in style
    [[nodiscard]] TChunkTrim* CreateChunkTrim(ui32 chunkIdx, ui32 offset, ui64 size, const NWilson::TSpan& parent) {
        NWilson::TSpan span = parent.CreateChild(TWilson::PDiskTopLevel, "PDisk.ChunkTrim");
        span.Attribute("chunk_idx", chunkIdx)
            .Attribute("offset", offset)
            .Attribute("size", static_cast<i64>(size))
            .Attribute("pdisk_id", PDiskId);
        Mon->Trim.CountRequest(size);
        return CreateFromArgs<TChunkTrim>(chunkIdx, offset, size, std::move(span));
    }

    [[nodiscard]] TLogWrite* CreateLogWrite(NPDisk::TEvLog &ev, const TActorId &sender, double& burstMs, NWilson::TTraceId traceId) {
        NWilson::TSpan span(TWilson::PDiskTopLevel, std::move(traceId), "PDisk.LogWrite", NWilson::EFlags::AUTO_END, ActorSystem);
        span.Attribute("pdisk_id", PDiskId);

        TReqId reqId(TReqId::LogWrite, AtomicIncrement(LastReqId));
        LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s Sender# %" PRIu64 " ReqId# %" PRIu64,
            (ui32)PDiskId, ev.ToString().c_str(), (ui64)sender.LocalId(), (ui64)reqId.Id);
        Mon->QueueRequests->Inc();
        *Mon->QueueBytes += ev.Data.size();
        Mon->WriteLog.CountRequest(ev.Data.size());
        if (ev.Data.size() > (1 << 20)) {
            Mon->WriteHugeLog.CountRequest();
        }
        return NewRequest(new TLogWrite(ev, sender, AtomicGet(*EstimatedLogChunkIdx), reqId, std::move(span)), &burstMs);
    }

    [[nodiscard]] TChunkRead* CreateChunkRead(const NPDisk::TEvChunkRead &ev, const TActorId &sender, double& burstMs,
            NWilson::TTraceId traceId) {
        NWilson::TSpan span(TWilson::PDiskTopLevel, std::move(traceId), "PDisk.ChunkRead", NWilson::EFlags::AUTO_END, ActorSystem);
        span.Attribute("pdisk_id", PDiskId);

        TReqId reqId(TReqId::ChunkRead, AtomicIncrement(LastReqId));
        LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s Sender# %" PRIu64 " ReqId# %" PRIu64,
            (ui32)PDiskId, ev.ToString().c_str(), (ui64)sender.LocalId(), (ui64)reqId.Id);
        Mon->QueueRequests->Inc();
        *Mon->QueueBytes += ev.Size;
        Mon->GetReadCounter(ev.PriorityClass)->CountRequest(ev.Size);
        auto read = new TChunkRead(ev, sender, reqId, std::move(span));
        read->SelfPointer = read;
        return NewRequest(read, &burstMs);
    }

    [[nodiscard]] TChunkWrite* CreateChunkWrite(const NPDisk::TEvChunkWrite &ev, const TActorId &sender, double& burstMs,
            NWilson::TTraceId traceId) {
        NWilson::TSpan span(TWilson::PDiskTopLevel, std::move(traceId), "PDisk.ChunkWrite", NWilson::EFlags::AUTO_END, ActorSystem);
        span.Attribute("pdisk_id", PDiskId);

        TReqId reqId(TReqId::ChunkWrite, AtomicIncrement(LastReqId));
        LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s Sender# %" PRIu64 " ReqId# %" PRIu64,
            (ui32)PDiskId, ev.ToString().c_str(), (ui64)sender.LocalId(), (ui64)reqId.Id);
        Mon->QueueRequests->Inc();
        ui32 size = ev.PartsPtr ? ev.PartsPtr->ByteSize() : 0;
        ev.Validate();
        *Mon->QueueBytes += size;
        Mon->GetWriteCounter(ev.PriorityClass)->CountRequest(size);
        return NewRequest(new TChunkWrite(ev, sender, reqId, std::move(span)), &burstMs);
    }
};

} // namespace NKikimr::NPDisk {
