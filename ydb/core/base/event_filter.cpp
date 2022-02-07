#include "event_filter.h"
#include "events.h"

namespace NKikimr {

    const TKikimrScopeId TKikimrScopeId::DynamicTenantScopeId(Max<ui64>() /*SchemeshardId*/, 0);

} // NKikimr
