#include "http_req.h"

#include "datastreams.h"
#include "sqs.h"
#include "ymq.h"

#include <util/generic/algorithm.h>

namespace NKikimr::NHttpProxy {

    namespace {
        const std::array<const IHttpController*, 3> Controllers = {
            GetSqsHttpController(),
            GetYmqHttpController(),
            GetDataStreamsHttpController()
        };

        THttpControllerRegistry Instance;
    }

    const THttpControllerRegistry& GetHttpControllerRegistry() {
        return Instance;
    }

    const IHttpController* THttpControllerRegistry::GetEnabledController(const TStringBuf apiVersion, const NKikimrConfig::TServerlessProxyConfig& config) const {
        for (const auto& controller : Controllers) {
            if (controller->IsEnabled(config) && controller->IsPossible(apiVersion)) {
                return controller;
            }
        }
        return nullptr;
    }

    const IHttpController* THttpControllerRegistry::GetController(const TStringBuf apiVersion) const {
        for (const auto& controller : Controllers) {
            if (controller->IsPossible(apiVersion)) {
                return controller;
            }
        }
        return nullptr;
    }

} // namespace NKikimr::NHttpProxy