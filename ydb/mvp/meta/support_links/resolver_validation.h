#pragma once

#include "grafana_dashboard_resolver.h"
#include "grafana_dashboard_search_resolver.h"

#include <util/generic/hash.h>
#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

class TLinkSourceConfigValidators {
public:
    using TValidateFn = void (*)(const TResolverValidationContext&);

    struct TValidationRegistration {
        TString SourceName;
        TValidateFn Validate = nullptr;
    };

    void Register(TValidationRegistration registration) {
        Validators[std::move(registration.SourceName)] = registration.Validate;
    }

    void Validate(const TSupportLinkEntryConfig& linkConfig, const TGrafanaSupportConfig& grafanaConfig, TStringBuf where) const {
        if (linkConfig.Source.empty()) {
            ythrow yexception() << where << ": source is required";
        }

        auto it = Validators.find(linkConfig.Source);
        if (it == Validators.end()) {
            ythrow yexception() << where << ": unsupported source=" << linkConfig.Source;
        }

        it->second(TResolverValidationContext{
            .LinkConfig = linkConfig,
            .GrafanaConfig = grafanaConfig,
            .Where = where,
        });
    }

    static const TLinkSourceConfigValidators& Default() {
        static const TLinkSourceConfigValidators validators = [] {
            TLinkSourceConfigValidators v;
            v.Register(TValidationRegistration{
                .SourceName = SOURCE_GRAFANA_DASHBOARD,
                .Validate = &ValidateGrafanaDashboardResolverConfig,
            });
            v.Register(TValidationRegistration{
                .SourceName = SOURCE_GRAFANA_DASHBOARD_SEARCH,
                .Validate = &ValidateGrafanaDashboardSearchResolverConfig,
            });
            return v;
        }();
        return validators;
    }

private:
    THashMap<TString, TValidateFn> Validators;
};

} // namespace NMVP::NSupportLinks
