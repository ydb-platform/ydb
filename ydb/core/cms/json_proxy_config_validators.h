#pragma once

#include "json_proxy.h"

#include <util/string/split.h>

namespace NKikimr::NCms {

class TJsonProxyConfigValidators : public TJsonProxyConsole<NConsole::NEvConsole::TEvListConfigValidatorsRequest,
                                                             NConsole::NEvConsole::TEvListConfigValidatorsResponse>
{
public:
    TJsonProxyConfigValidators(NMon::TEvHttpInfo::TPtr &event)
        : TJsonProxyConsole<NConsole::NEvConsole::TEvListConfigValidatorsRequest,
                            NConsole::NEvConsole::TEvListConfigValidatorsResponse>(event)
    {
    }

    TAutoPtr<TRequest> PrepareRequest(const TActorContext &) override {
        TAutoPtr<TRequest> request = new TRequest;
        return request;
    }
};

} // namespace NKikimr::NCms
