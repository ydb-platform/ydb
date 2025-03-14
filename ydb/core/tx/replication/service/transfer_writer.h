#pragma once

#include <ydb/core/base/defs.h>

#include <util/generic/string.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NReplication::NService {

IActor* CreateTransferWriter(const TString& transformLambda, const TPathId& tablePathId,
    const TActorId& compileServiceId, TDuration flushInterval, ui64 batchSizeBytes);

}
