#include "cli.h"
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {
namespace NDriverClient {

void DumpProxyErrorCodes(IOutputStream &o, const NKikimrClient::TResponse &response) {
    o << "status: " << response.GetStatus() << Endl;
    o << "status transcript: " << static_cast<NMsgBusProxy::EResponseStatus>(response.GetStatus()) << Endl;
    if (response.HasProxyErrorCode()) {
        o << "proxy error code: " << response.GetProxyErrorCode() << Endl;
        o << "proxy error code transcript: " << static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(response.GetProxyErrorCode()) << Endl;
    }
}

void HideOptions(NLastGetopt::TOpts& opts, const TString& prefix) {
    for (auto opt : opts.Opts_) {
        if (opt.Get()->GetName().StartsWith(prefix)) {
            opt.Get()->Hidden_ = true;
        }
    }
}

void HideOptions(NLastGetopt::TOpts& opts) {
    for (auto opt : opts.Opts_) {
        opt.Get()->Hidden_ = true;
    }
}

}
}
