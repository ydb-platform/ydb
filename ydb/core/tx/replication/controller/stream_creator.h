#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication::NController {

IActor* CreateStreamCreator(const TActorId& parent, ui64 rid, ui64 tid, const TActorId& proxy);

}
