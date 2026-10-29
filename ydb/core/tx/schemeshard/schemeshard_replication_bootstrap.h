#pragma once

#include "schemeshard_impl.h"
#include "schemeshard__operation_part.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NSchemeShard {

inline bool GetImportReplicationConfig(const TString& destination, TSchemeShard* ss,
        NKikimrSchemeOp::TTableReplicationConfig& tableConfig, TString& error) {
    const auto canonicalDestination = CanonizePath(destination);
    TPathId owner;
    for (const auto& [pathId, replication] : ss->Replications) {
        const auto& description = replication->AlterData
            ? replication->AlterData->Description : replication->Description;
        const auto& config = description.GetConfig();
        if (TPath::Init(pathId, ss).IsDeleted() || description.GetState().HasDone() || !config.HasSpecific()) {
            continue;
        }
        for (const auto& target : config.GetSpecific().GetTargets()) {
            if (CanonizePath(target.GetDstPath()) != canonicalDestination) {
                continue;
            }
            if (owner) {
                error = TStringBuilder() << "Multiple replications target import destination: " << destination;
                return false;
            }
            owner = pathId;
        }
    }
    if (!owner) {
        return true;
    }

    const auto ownerPath = TPath::Init(owner, ss);
    const auto checks = ownerPath.Check();
    checks.NotUnderOperation();
    if (!checks) {
        error = checks.GetError();
        return false;
    }
    const auto& replication = ss->Replications.at(owner)->Description;
    if (replication.GetConfig().GetInitialScan() || !replication.GetState().HasPaused()) {
        error = TStringBuilder() << "Replication must be paused with initial scan disabled before importing destination: " << destination;
        return false;
    }
    tableConfig.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
    tableConfig.SetConsistencyLevel(replication.GetConfig().GetConsistencySettings().HasGlobal()
        ? NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_GLOBAL
        : NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_ROW);
    owner.ToProto(tableConfig.MutableBootstrapReplication());
    return true;
}

inline bool IsReplicationBootstrapTable(const TPath& path, TSchemeShard* ss) {
    if (!path.IsResolved() || !path->IsTable()) {
        return false;
    }
    const auto table = ss->Tables.find(path.Base()->PathId);
    return table != ss->Tables.end() && table->second->ReplicationConfig().HasBootstrapReplication();
}

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
