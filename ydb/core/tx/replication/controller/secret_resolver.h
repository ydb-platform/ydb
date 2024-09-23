#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication::NController {

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid, const TPathId& pathId, const TString& secretName);

}
