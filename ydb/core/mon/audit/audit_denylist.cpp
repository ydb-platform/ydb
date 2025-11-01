#include "url_matcher.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMonitoring::NAudit {

// Audit logging is enabled for all requests that either
// 1) use modifying HTTP methods (POST, PUT, DELETE), or
// 2) target endpoints not listed in AUDIT_DENYLIST.
// The denylist excludes frequently queried read-only endpoints with no security value.
const TVector<TUrlPattern> AUDIT_DENYLIST = {
    { .Path = "/" },
    { .Path = "/internal"},
    { .Path = "/ver"},
    { .Path = "/counters", .Recursive = true },

    // viewer endpoints
    { .Path = "/viewer", .Recursive = true },
    { .Path = "/vdisk", .Recursive = true },
    { .Path = "/pdisk", .Recursive = true },
    { .Path = "/monitoring", .Recursive = true },
    { .Path = "/healthcheck", .Recursive = true },
    { .Path = "/operation", .Recursive = true },
    { .Path = "/query", .Recursive = true },
    { .Path = "/scheme", .Recursive = true },
    { .Path = "/storage", .Recursive = true },

    // static resources such as JS, CSS, images
    { .Path = "/static", .Recursive = true },
    { .Path = "/jquery.tablesorter.js" },
    { .Path = "/jquery.tablesorter.css" },
    { .Path = "/lwtrace/mon/static", .Recursive = true },
};

}
