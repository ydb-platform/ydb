#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr {
namespace NReplication {
namespace NController {

IActor* CreateStreamCreator(const TActorId& parent, ui64 rid, ui64 tid, const TActorId& proxy);

} // NController
} // NReplication
} // NKikimr
