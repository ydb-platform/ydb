#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication::NTestHelpers {

IActor* CreateReplicationMockService(const TActorId& edge);

}
