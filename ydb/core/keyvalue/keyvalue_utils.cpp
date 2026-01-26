#include <ydb/core/keyvalue/keyvalue_utils.h>

namespace NKikimr {
namespace NKeyValue {

NKikimrKeyValue::Statuses::ReplyStatus ConvertStatus(NMsgBusProxy::EResponseStatus status) {
    switch (status) {
    case NMsgBusProxy::MSTATUS_ERROR:
        return NKikimrKeyValue::Statuses::RSTATUS_ERROR;
    case NMsgBusProxy::MSTATUS_TIMEOUT:
        return NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT;
    case NMsgBusProxy::MSTATUS_REJECTED:
        return NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION;
    case NMsgBusProxy::MSTATUS_INTERNALERROR:
        return NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR;
    default:
        return NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR;
    };
}


template <typename TEvent>
struct THasCookie : std::true_type {};

template <>
struct THasCookie<TEvKeyValue::TEvGetStorageChannelStatusResponse> : std::false_type {};



template<typename TEvent>
std::unique_ptr<IEventBase> MakeErrorResponseImpl(const TIntermediate *intermediateResults, NKikimrKeyValue::Statuses::ReplyStatus status, TString errorDescription) {
    std::unique_ptr<TEvent> response(new TEvent);
    if constexpr (THasCookie<TEvent>::value) {
        if (intermediateResults->HasCookie) {
            response->Record.set_cookie(intermediateResults->Cookie);
        }
    }
    response->Record.set_msg(errorDescription);
    response->Record.set_status(status);
    return response;
}


std::unique_ptr<IEventBase> MakeErrorResponse(const TIntermediate *intermediateResults, NMsgBusProxy::EResponseStatus status, TString errorDescription) {
    if (intermediateResults->EvType == TEvKeyValue::TEvRequest::EventType) {
        std::unique_ptr<TEvKeyValue::TEvResponse> response(new TEvKeyValue::TEvResponse);
        if (intermediateResults->HasCookie) {
            response->Record.SetCookie(intermediateResults->Cookie);
        }
        response->Record.SetErrorReason(errorDescription);
        response->Record.SetStatus(status);
        return response;
    }
    if (intermediateResults->EvType == TEvKeyValue::TEvRead::EventType) {
        return MakeErrorResponseImpl<TEvKeyValue::TEvReadResponse>(intermediateResults, ConvertStatus(status), errorDescription);
    }
    if (intermediateResults->EvType == TEvKeyValue::TEvReadRange::EventType) {
        return MakeErrorResponseImpl<TEvKeyValue::TEvReadRangeResponse>(intermediateResults, ConvertStatus(status), errorDescription);
    }
    if (intermediateResults->EvType == TEvKeyValue::TEvExecuteTransaction::EventType) {
        return MakeErrorResponseImpl<TEvKeyValue::TEvExecuteTransactionResponse>(intermediateResults, ConvertStatus(status), errorDescription);
    }
    if (intermediateResults->EvType == TEvKeyValue::TEvGetStorageChannelStatus::EventType) {
        return MakeErrorResponseImpl<TEvKeyValue::TEvGetStorageChannelStatusResponse>(intermediateResults, ConvertStatus(status), errorDescription);
    }

    return nullptr;
}


} // NKeyValue
} // NKikimr