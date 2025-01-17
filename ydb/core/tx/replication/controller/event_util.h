#pragma once

#include "replication.h"

#include <ydb/core/tx/replication/service/service.h>

namespace NKikimr::NReplication::NController {

THolder<TEvService::TEvRunWorker> MakeRunWorkerEv(
    const TReplication::TPtr replication,
    const TReplication::ITarget& target,
    ui64 workerId);

THolder<TEvService::TEvRunWorker> MakeRunWorkerEv(
    ui64 replicationId,
    ui64 targetId,
    ui64 workerId,
    const NKikimrReplication::TConnectionParams& connectionParams,
    const NKikimrReplication::TConsistencySettings& consistencySettings,
    const TString& srcStreamPath,
    const TPathId& dstPathId);

}
