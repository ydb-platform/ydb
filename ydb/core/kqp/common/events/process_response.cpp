#include "process_response.h"

namespace NKikimr::NKqp::NPrivateEvents {

THolder<NKikimr::NKqp::NPrivateEvents::TEvProcessResponse> TEvProcessResponse::Error(Ydb::StatusIds::StatusCode ydbStatus, const TString& error) {
    auto ev = MakeHolder<TEvProcessResponse>();
    ev->Record.SetYdbStatus(ydbStatus);
    ev->Record.SetError(error);
    return ev;
}

THolder<NKikimr::NKqp::NPrivateEvents::TEvProcessResponse> TEvProcessResponse::Success() {
    auto ev = MakeHolder<TEvProcessResponse>();
    ev->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
    return ev;
}

} // namespace NKikimr::NKqp::NPrivateEvents
