#pragma once

#include "spilling_counters.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/actors/core/actor.h>

#include <util/system/types.h>
#include <util/generic/strbuf.h>
#include <util/folder/path.h>
#include <util/generic/guid.h>

namespace NYql::NDq {

struct TFileSpillingServiceConfig {
    TString Root;
    TString SpillingSessionId = CreateGuidAsString();
    ui64 MaxTotalSize = 0;
    ui64 MaxFileSize = 0;
    ui64 MaxFilePartSize = 0;

    ui32 IoThreadPoolWorkersCount = 2;
    ui32 IoThreadPoolQueueSize = 1000;
    bool CleanupOnShutdown = false;
};

inline NActors::TActorId MakeDqLocalFileSpillingServiceID(ui32 nodeId) {
    const char name[12] = "dq_lfspill";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

TFsPath GetTmpSpillingRootForCurrentUser();

NActors::IActor* CreateDqLocalFileSpillingActor(TTxId txId, const TString& details, const NActors::TActorId& client, bool removeBlobsAfterRead);

NActors::IActor* CreateDqLocalFileSpillingService(const TFileSpillingServiceConfig& config, TIntrusivePtr<TSpillingCounters> counters);

} // namespace NYql::NDq
