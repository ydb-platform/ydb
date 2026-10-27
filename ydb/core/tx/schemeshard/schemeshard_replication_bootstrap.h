#pragma once

#include "schemeshard_impl.h"
#include "schemeshard__operation_part.h"

namespace NKikimr::NSchemeShard {

inline TVector<TPath> GetReplicationBootstrapTables(const TPathId& replicationId, TSchemeShard* ss) {
    TVector<TPath> result;
    for (const auto& [pathId, table] : ss->Tables) {
        const auto& config = table->ReplicationConfig();
        if (config.HasBootstrapReplication()
            && TPathId::FromProto(config.GetBootstrapReplication()) == replicationId) {
            auto path = TPath::Init(pathId, ss);
            if (!path.IsDeleted()) {
                result.push_back(std::move(path));
            }
        }
    }
    return result;
}

inline bool CheckReplicationBootstrapTables(const TPathId& replicationId, TProposeResponse& result, TSchemeShard* ss) {
    for (const auto& path : GetReplicationBootstrapTables(replicationId, ss)) {
        const auto checks = path.Check();
        checks.NotUnderOperation();
        if (!checks) {
            result.SetError(checks.GetStatus(), checks.GetError());
            return false;
        }
    }
    return true;
}

template <typename TRequest>
void FillReplicationBootstrapTables(const TPathId& replicationId, TRequest& request, TSchemeShard* ss) {
    for (const auto& path : GetReplicationBootstrapTables(replicationId, ss)) {
        auto& table = *request.AddBootstrapTables();
        path.Base()->PathId.ToProto(table.MutablePathId());
        table.SetPath(path.PathString());
    }
}

} // namespace NKikimr::NSchemeShard
