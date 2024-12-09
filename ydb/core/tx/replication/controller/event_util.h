#pragma once

#include "replication.h"

#include <ydb/core/tx/replication/service/service.h>

namespace NKikimr::NReplication::NController {

THolder<TEvService::TEvRunWorker> MakeTEvRunWorker(const TReplication::TPtr replication, const TReplication::ITarget& target, ui64 partitionId);
THolder<TEvService::TEvRunWorker> MakeTEvRunWorker(ui64 replicationId, ui64 targetId, ui64 partitionId, const NKikimrReplication::TConnectionParams& connectionParams,
            const TString& srcStreamPath, const TPathId& dstPathId);

}
