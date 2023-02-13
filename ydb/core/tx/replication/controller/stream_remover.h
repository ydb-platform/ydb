#pragma once

#include "replication.h"

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication::NController {

IActor* CreateStreamRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
    TReplication::ETargetKind kind, const TString& srcPath, const TString& streamName);

}
