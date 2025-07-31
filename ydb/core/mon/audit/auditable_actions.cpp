#include "url_matcher.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMonitoring::NAudit {

// Audit logging is based on HTTP methods like POST or PUT
// but some handlers use GET for changes
// AUDITABLE_ACTIONS lists URLs that always require audit logging
// despite the HTTP methods associated with getting information
const TVector<TUrlPattern> AUDITABLE_ACTIONS = {
    {.Path = "/actors/blobstorageproxies/*", .ParamName = "PutSamplingRate"},
    {.Path = "/actors/blobstorageproxies/*", .ParamName = "GetSamplingRate"},
    {.Path = "/actors/blobstorageproxies/*", .ParamName = "DiscoverSamplingRate"},
    {.Path = "/actors/blobstorageproxies/*", .ParamName = "submit_timestats"},
    {.Path = "/actors/failure_injection", .ParamName = "queue"},
    {.Path = "/actors/failure_injection", .ParamName = "probe"},
    {.Path = "/actors/failure_injection", .ParamName = "enable"},
    {.Path = "/actors/failure_injection", .ParamName = "terminate"},
    {.Path = "/actors/kqp_proxy", .ParamName = "force_shutdown"},
    {.Path = "/actors/logger", .ParamName = "c"},
    {.Path = "/actors/logger", .ParamName = "p"},
    {.Path = "/actors/logger", .ParamName = "sp"},
    {.Path = "/actors/logger", .ParamName = "sr"},
    {.Path = "/actors/logger", .ParamName = "allowdrop"},
    {.Path = "/actors/pql2", .ParamName = "submit"},
    {.Path = "/actors/profiler", .ParamName = "action", .ParamValue = "start"},
    {.Path = "/actors/profiler", .ParamName = "action", .ParamValue = "stop-display"},
    {.Path = "/actors/profiler", .ParamName = "action", .ParamValue = "stop-save"},
    {.Path = "/actors/profiler", .ParamName = "action", .ParamValue = "stop-log"},
    {.Path = "/actors/vdisks/*", .ParamName = "type", .ParamValue = "restart"},
    {.Path = "/actors/sqsgc/rescan"},
    {.Path = "/actors/sqsgc/clean"},
    {.Path = "/actors/sqsgc/clear_history"},
    {.Path = "/cms/api/console/removevolatileyamlconfig"},
    {.Path = "/cms/api/console/configureyamlconfig"},
    {.Path = "/cms/api/console/configurevolatileyamlconfig"},
    {.Path = "/cms/api/console/configure"},
    {.Path = "/cms/api/json/toggleconfigvalidator"},
    {.Path = "/memory/heap", .ParamName = "action", .ParamValue = "log"},
    {.Path = "/nodetabmon", .ParamName = "action", .ParamValue = "kill_tablet"},
    {.Path = "/trace"},
    {.Path = "/fq_diag/quotas", .ParamName = "submit"},
    {.Path = "/login"},
    {.Path = "/logout"},
    {.Path = "/tablets", .ParamName = "KillTabletID"},
    {.Path = "/tablets", .ParamName = "RestartTabletID"},
    {.Path = "/tablets/app"},
};

}
