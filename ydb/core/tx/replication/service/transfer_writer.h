#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/replication.pb.h>

#include <util/generic/string.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimrReplication {
    class TBatchingSettings;
}

namespace NKikimr::NReplication::NService {

IActor* CreateTransferWriter(const TString& transformLambda, const TPathId& tablePathId,
    const TActorId& compileServiceId, const NKikimrReplication::TBatchingSettings& batchingSettings);

}
