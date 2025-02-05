#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/replication.pb.h>

#include <util/generic/string.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NReplication::NService {

IActor* CreateTransferWriter(const NKikimrReplication::TReplicationConfig& config,
    const TPathId& tablePathId, const TString& tableName,
    const TActorId& compileServiceId);

}
