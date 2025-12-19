#pragma once
#include "defs.h"
#include "keyvalue_intermediate.h"
#include "keyvalue_request_stat.h"
#include "keyvalue_helpers.h"
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/keyvalue/protos/events.pb.h>


namespace NKikimr {

namespace NKeyValue {
    struct TIntermediate;
};

namespace TEvKeyValue {
    enum EEv {
        EvRequest = EventSpaceBegin(TKikimrEvents::ES_KEYVALUE),
        EvIntermediate,
        EvNotify,
        EvStoreCollect,
        EvCollect,
        EvPeriodicRefresh,
        EvReportWriteLatency,
        EvUpdateWeights,
        EvCompleteGC,
        EvVacuumRequest,

        EvRead = EvRequest + 16,
        EvReadRange,
        EvExecuteTransaction,
        EvGetStorageChannelStatus,
        EvAcquireLock,

        EvResponse = EvRequest + 512,
        EvForceTabletVacuum,
        EvVacuumResponse,

        EvReadResponse = EvResponse + 16,
        EvReadRangeResponse,
        EvExecuteTransactionResponse,
        EvGetStorageChannelStatusResponse,
        EvAcquireLockResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_KEYVALUE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_KEYVALUE)");

    struct TEvReadResponse;

    struct TEvRead : public TEventPB<TEvRead,
            NKikimrKeyValue::ReadRequest, EvRead> {

        using TResponse = TEvReadResponse;
        TEvRead() { }
    };

    struct TEvReadResponse : public TEventPB<TEvReadResponse,
            NKikimrKeyValue::ReadResult, EvReadResponse> {
        TEvReadResponse() { }

        void SetBuffer(TRope&& buffer) {
            ui32 id = AddPayload(std::move(buffer));
            Record.set_payload_id(id);
        }

        bool IsPayload() const {
            return Record.has_payload_id();
        }

        TRope GetBuffer() const {
            return GetPayload(Record.payload_id());
        }
    };

    struct TEvReadRangeResponse;

    struct TEvReadRange : public TEventPB<TEvReadRange,
            NKikimrKeyValue::ReadRangeRequest, EvReadRange> {

        using TResponse = TEvReadRangeResponse;
        TEvReadRange() { }
    };

    struct TEvReadRangeResponse : public TEventPB<TEvReadRangeResponse,
            NKikimrKeyValue::ReadRangeResult, EvReadRangeResponse> {
        TEvReadRangeResponse() { }

        void SetBuffer(TRope&& buffer, ui32 itemIdx) {
            ui32 id = AddPayload(std::move(buffer));
            Record.mutable_pair(itemIdx)->set_payload_id(id);
        }

        bool IsPayload(ui32 itemIdx) const {
            return Record.pair(itemIdx).has_payload_id();
        }

        TRope GetBuffer(ui32 itemIdx) const {
            return GetPayload(Record.pair(itemIdx).payload_id());
        }
    };

    struct TEvExecuteTransactionResponse;

    struct TEvExecuteTransaction : public TEventPB<TEvExecuteTransaction,
            NKikimrKeyValue::ExecuteTransactionRequest, EvExecuteTransaction> {

        using TResponse = TEvExecuteTransactionResponse;
        TEvExecuteTransaction() { }
    };

    struct TEvExecuteTransactionResponse : public TEventPB<TEvExecuteTransactionResponse,
            NKikimrKeyValue::ExecuteTransactionResult, EvExecuteTransactionResponse> {
        TEvExecuteTransactionResponse() { }
    };

    struct TEvGetStorageChannelStatusResponse;

    struct TEvGetStorageChannelStatus : public TEventPB<TEvGetStorageChannelStatus,
            NKikimrKeyValue::GetStorageChannelStatusRequest, EvGetStorageChannelStatus> {

        using TResponse = TEvGetStorageChannelStatusResponse;
        TEvGetStorageChannelStatus() { }
    };

    struct TEvGetStorageChannelStatusResponse : public TEventPB<TEvGetStorageChannelStatusResponse,
            NKikimrKeyValue::GetStorageChannelStatusResult, EvGetStorageChannelStatusResponse> {
        TEvGetStorageChannelStatusResponse() { }
    };

    struct TEvAcquireLockResponse;

    struct TEvAcquireLock : public TEventPB<TEvAcquireLock,
            NKikimrKeyValue::AcquireLockRequest, EvAcquireLock> {

        using TResponse = TEvAcquireLockResponse;
        TEvAcquireLock() { }
    };

    struct TEvAcquireLockResponse : public TEventPB<TEvAcquireLockResponse,
            NKikimrKeyValue::AcquireLockResult, EvAcquireLockResponse> {
        TEvAcquireLockResponse() { }
    };

    struct TEvRequest : public TEventPB<TEvRequest,
            NKikimrClient::TKeyValueRequest, EvRequest> {
        TEvRequest() { }
    };

    struct TEvResponse : public TEventPB<TEvResponse,
            NKikimrClient::TResponse, EvResponse> {
        TEvResponse() { }
    };

    struct TEvIntermediate : public TEventLocal<TEvIntermediate, EvIntermediate> {
        THolder<NKeyValue::TIntermediate> Intermediate;

        TEvIntermediate() { }

        TEvIntermediate(THolder<NKeyValue::TIntermediate>&& intermediate)
            : Intermediate(std::move(intermediate))
        {}
    };

    struct TEvNotify : public TEventLocal<TEvNotify, EvNotify> {
        ui64 RequestUid;
        ui64 Generation;
        ui64 Step;
        NKeyValue::TRequestStat Stat;
        NMsgBusProxy::EResponseStatus Status;
        std::deque<std::pair<TLogoBlobID, bool>> RefCountsIncr;

        TEvNotify() { }

        TEvNotify(ui64 requestUid, ui64 generation, ui64 step, const NKeyValue::TRequestStat &stat,
                NMsgBusProxy::EResponseStatus status, std::deque<std::pair<TLogoBlobID, bool>>&& refCountsIncr)
            : RequestUid(requestUid)
            , Generation(generation)
            , Step(step)
            , Stat(stat)
            , Status(status)
            , RefCountsIncr(std::move(refCountsIncr))
        {}

        TEvNotify(ui64 requestUid, ui64 generation, ui64 step, const NKeyValue::TRequestStat &stat,
                NKikimrKeyValue::Statuses::ReplyStatus status, std::deque<std::pair<TLogoBlobID, bool>>&& refCountsIncr)
            : RequestUid(requestUid)
            , Generation(generation)
            , Step(step)
            , Stat(stat)
            , Status(ConvertStatus(status))
            , RefCountsIncr(std::move(refCountsIncr))
        {}

        static NMsgBusProxy::EResponseStatus ConvertStatus(NKikimrKeyValue::Statuses::ReplyStatus status) {
            switch (status) {
            case NKikimrKeyValue::Statuses::RSTATUS_OK:
                return NMsgBusProxy::MSTATUS_OK;
            case NKikimrKeyValue::Statuses::RSTATUS_ERROR:
                return NMsgBusProxy::MSTATUS_ERROR;
            case NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT:
                return NMsgBusProxy::MSTATUS_TIMEOUT;
            case NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR:
                return NMsgBusProxy::MSTATUS_INTERNALERROR;
            case NKikimrKeyValue::Statuses::RSTATUS_BLOCKED:
                return NMsgBusProxy::MSTATUS_ERROR;
            default:
                return NMsgBusProxy::MSTATUS_INTERNALERROR;
            }
        }
    };

    struct TEvCollect : public TEventLocal<TEvCollect, EvCollect> {
        TEvCollect() { }
    };

    struct TEvPeriodicRefresh : public TEventLocal<TEvPeriodicRefresh, EvPeriodicRefresh> {
        TEvPeriodicRefresh() { }
    };

    struct TEvCompleteGC : public TEventLocal<TEvCompleteGC, EvCompleteGC> {
        const bool Repeat;

        TEvCompleteGC(bool repeat)
            : Repeat(repeat)
        {}
    };

    struct TEvVacuumResponse;

    struct TEvVacuumRequest : public TEventPB<TEvVacuumRequest,
            NKikimrKeyValue::VacuumRequest, EvVacuumRequest> {
        using TResponse = TEvVacuumResponse;

        TEvVacuumRequest() = default;

        TEvVacuumRequest(ui64 generation, bool reset=false) {
            Record.set_generation(generation);
            Record.set_reset_actual_generation(reset);
        }
    };

    struct TEvVacuumResponse : public TEventPB<TEvVacuumResponse,
            NKikimrKeyValue::VacuumResponse, EvVacuumResponse> {
        using TRequest = TEvVacuumRequest;

        TEvVacuumResponse() = default;

        TEvVacuumResponse(ui64 generation, NKikimrKeyValue::VacuumResponse::Status status, const TString& errorReason, ui64 actualGeneration, ui64 tabletId) {
            Record.set_generation(generation);
            Record.set_status(status);
            Record.set_error_reason(errorReason);
            Record.set_actual_generation(actualGeneration);
            Record.set_tablet_id(tabletId);
        }

        static std::unique_ptr<TEvVacuumResponse> MakeSuccess(ui64 generation, ui64 tabletId) {
            return std::make_unique<TEvVacuumResponse>(generation, NKikimrKeyValue::VacuumResponse::STATUS_SUCCESS, "", generation, tabletId);
        }

        static std::unique_ptr<TEvVacuumResponse> MakeAborted(ui64 generation, const TString& errorReason, ui64 actualGeneration, ui64 tabletId) {
            return std::make_unique<TEvVacuumResponse>(generation, NKikimrKeyValue::VacuumResponse::STATUS_ABORTED, errorReason, actualGeneration, tabletId);
        }

        static std::unique_ptr<TEvVacuumResponse> MakeAlreadyCompleted(ui64 generation, ui64 actualGeneration, ui64 tabletId) {
            return std::make_unique<TEvVacuumResponse>(generation, NKikimrKeyValue::VacuumResponse::STATUS_ALREADY_COMPLETED, "", actualGeneration, tabletId);
        }

        static std::unique_ptr<TEvVacuumResponse> MakeError(ui64 generation, const TString& errorReason, ui64 actualGeneration, ui64 tabletId) {
            return std::make_unique<TEvVacuumResponse>(generation, NKikimrKeyValue::VacuumResponse::STATUS_ERROR, errorReason, actualGeneration, tabletId);
        }
    };

    struct TEvForceTabletVacuum : public TEventLocal<TEvForceTabletVacuum, EvForceTabletVacuum> {
        ui64 Generation;

        TEvForceTabletVacuum(ui64 generation)
            : Generation(generation)
        {}
    };

}

} // NKikimr
