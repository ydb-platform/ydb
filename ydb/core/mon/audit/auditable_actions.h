#pragma once

#include "url_matcher.h"

#include <util/generic/vector.h>

namespace NMonitoring::NAudit {

// Audit logging is based on HTTP methods like POST or PUT
// but some handlers use GET for changes
// AUDITABLE_ACTIONS lists URLs that always require audit logging
// despite the HTTP methods associated with getting information
extern const TVector<TUrlPattern> AUDITABLE_ACTIONS;

inline TUrlMatcher CreateAuditableActionsMatcher() {
    TUrlMatcher policy;
    for (const auto& pattern : AUDITABLE_ACTIONS) {
        policy.AddPattern(pattern);
    }
    return policy;
}

}
