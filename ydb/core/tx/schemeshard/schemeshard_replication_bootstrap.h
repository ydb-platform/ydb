#pragma once

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

inline bool IsReplicationBootstrapConfig(const NKikimrSchemeOp::TTableReplicationConfig& config) {
    return config.GetMode() == NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY
        && config.GetBootstrapInfo().GetStatus() == NKikimrSchemeOp::TBootstrapInfo::IMPORTING;
}

inline bool IsReplicationBootstrapTable(const TPath& path, TSchemeShard* ss) {
    if (!path.IsResolved() || path.IsDeleted() || !path->IsTable()) {
        return false;
    }
    const auto table = ss->Tables.find(path.Base()->PathId);
    return table != ss->Tables.end() && IsReplicationBootstrapConfig(table->second->ReplicationConfig());
}

inline bool IsWaitingImportTable(const TPath& path, TSchemeShard* ss) {
    if (!path.IsResolved() || path.IsDeleted() || !path->IsTable()) {
        return false;
    }
    return ss->Tables.at(path.Base()->PathId)->ReplicationConfig().GetBootstrapInfo().GetStatus()
        == NKikimrSchemeOp::TBootstrapInfo::WAITING_IMPORT;
}

} // namespace NKikimr::NSchemeShard
