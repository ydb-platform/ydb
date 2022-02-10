#include "msgbus.h"
#include "msgbus_status.h"

namespace NKikimr {
namespace NMsgBusProxy {

void ExplainProposeTransactionStatus(ui32 status, TString& name, TString& description) {
    name = ToString(status);
    description = "Unknown status";

    auto field = NKikimrClient::TResponse::descriptor()->FindFieldByNumber(
        NKikimrClient::TResponse::kProxyErrorCodeFieldNumber);

    auto extension = field->options().GetExtension(NKikimrClient::EnumValueHint);

    for (auto item : extension.GetHints()) {
        if (status == item.GetValue()) {
            name = item.GetName();
            description = item.GetMan();
            break;
        }
    }
}

void ExplainExecutionEngineStatus(ui32 status, TString& name, TString& description) {
    name = ToString(status);
    description = "Unknown status";

    auto field = NKikimrClient::TResponse::descriptor()->FindFieldByNumber(
        NKikimrClient::TResponse::kExecutionEngineStatusFieldNumber);

    auto extension = field->options().GetExtension(NKikimrClient::EnumValueHint);

    for (auto item : extension.GetHints()) {
        if (status == item.GetValue()) {
            name = item.GetName();
            description = item.GetMan();
            break;
        }
    }
}

void ExplainResponseStatus(ui32 status, TString& name, TString& description) {
    name = ToString(status);
    description = "Unknown status";

    auto field = NKikimrClient::TResponse::descriptor()->FindFieldByNumber(
        NKikimrClient::TResponse::kStatusFieldNumber);

    auto extension =
        field->options().GetExtension(NKikimrClient::EnumValueHint);

    for (auto item : extension.GetHints()) {
        if (status == item.GetValue()) {
            name = item.GetName();
            description = item.GetMan();
            break;
        }
    }
}

} // namespace NKikimr
} // namespace NMsgBusProxy
