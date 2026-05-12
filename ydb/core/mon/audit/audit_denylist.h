#pragma once

#include "url_matcher.h"

#include <util/generic/vector.h>

namespace NMonitoring::NAudit {

// Audit logging is enabled for all requests that either
// 1) use modifying HTTP methods (POST, PUT, DELETE), or
// 2) target endpoints not listed in AUDIT_DENYLIST.
// The denylist excludes frequently queried read-only endpoints with no security value.
extern const TVector<TUrlPattern> AUDIT_DENYLIST;

}
