#pragma once

#include "url_tree.h"

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NActors {

// Audit logging is based on HTTP methods like POST or PUT
// but some handlers use GET for changes
// FORCE_AUDIT lists URLs that always require audit logging
// despite the HTTP methods associated with getting information
static const TVector<TUrlPattern> FORCE_AUDIT = {
    {"/actors/failure_injection", "queue"},
    {"/actors/failure_injection", "probe"},
    {"/actors/failure_injection", "enable"},
    {"/actors/failure_injection", "terminate"},
    {"/actors/kqp_proxy", "force_shutdown"},
    {"/actors/logger", "p"},
    {"/actors/logger", "sp"},
    {"/actors/logger", "sr"},
    {"/actors/pdisks", "chunkLockByCount"},
    {"/actors/pdisks", "chunkLockByColor"},
    {"/actors/pdisks", "chunkUnlock"},
    {"/actors/pdisks", "restartPDisk"},
    {"/actors/pdisks", "stopPDisk"},
    {"/actors/pql2", "submit"},
    {"/actors/profiler", "action", "start"},
    {"/actors/profiler", "action", "stop-display"},
    {"/actors/profiler", "action", "stop-save"},
    {"/actors/profiler", "action", "stop-log"},
    {"/actors/vdisks", "type", "restart"},
    {"/actors/sqsgc/rescan"},
    {"/actors/sqsgc/clean"},
    {"/actors/sqsgc/clear_history"},
    {"/cms/api/console/removevolatileyamlconfig"},
    {"/cms/api/console/configureyamlconfig"},
    {"/cms/api/console/configurevolatileyamlconfig"},
    {"/cms/api/console/configure"},
    {"/cms/api/json/toggleconfigvalidator"},
    {"/memory/heap", "action", "log"},
    {"/nodetabmon", "action", "kill_tablet"},
    {"/trace"},
    {"/fq_diag/quotas", "submit"},
    {"/login"},
    {"/logout"},
    {"/tablets", "KillTabletID"},
    {"/tablets", "RestartTabletID"},
    {"/tablets/app"},
};

inline TUrlTree CreateAuditUrlPattern() {
    TUrlTree policy;
    for (const auto& pattern : FORCE_AUDIT) {
        policy.AddPattern(pattern);
    }
    return policy;
}

}
