#include "cli.h"
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {
namespace NDriverClient {

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
