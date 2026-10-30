#pragma once

#include "schemeshard_impl.h"
#include "schemeshard__operation_part.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NSchemeShard {

inline bool ValidateImportReplicationConfig(const NKikimrSchemeOp::TTableReplicationConfig& config,
        TSchemeShard* ss, TString& error) {
    if (!config.HasBootstrapReplication()) {
        return true;
    }
    const auto owner = TPathId::FromProto(config.GetBootstrapReplication());
    const auto ownerPath = TPath::Init(owner, ss);
    const auto checks = ownerPath.Check();
    checks.IsResolved().NotDeleted().NotUnderOperation();
    if (!checks) {
        error = checks.GetError();
        return false;
    }
    const auto it = ss->Replications.find(owner);
    if (it == ss->Replications.end()
        || config.GetMode() != NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY) {
        error = "Import destination is not a read-only replication bootstrap table";
        return false;
    }
    const auto& replication = it->second->Description;
    if (replication.GetConfig().GetInitialScan() || !replication.GetState().HasPaused()) {
        error = "Replication must be paused with initial scan disabled before importing destination";
        return false;
    }
    return true;
}

inline bool GetImportReplicationConfig(const TString& destination, TSchemeShard* ss,
        NKikimrSchemeOp::TTableReplicationConfig& config, TString& error) {
    config.Clear();
    const auto path = TPath::Resolve(destination, ss);
    if (path.IsResolved() && !path.IsDeleted()) {
        const auto table = ss->Tables.find(path.Base()->PathId);
        if (table != ss->Tables.end()) {
            config = table->second->ReplicationConfig();
        }
    } else {
        // A failed import may have removed its placeholder before creating the
        // replacement. Its checkpoint retains ownership for a new import attempt.
        ui64 previousImport = 0;
        for (const auto& [id, import] : ss->Imports) {
            if (import->State != TImportInfo::EState::Cancelled || id <= previousImport) {
                continue;
            }
            for (const auto& item : import->Items) {
                if (item.ReplicationConfig.HasBootstrapReplication()
                    && CanonizePath(item.DstPathName) == CanonizePath(destination)) {
                    config = item.ReplicationConfig;
                    previousImport = id;
                }
            }
        }
    }
    return ValidateImportReplicationConfig(config, ss, error);
}

inline bool IsReplicationBootstrapTable(const TPath& path, TSchemeShard* ss) {
    if (!path.IsResolved() || path.IsDeleted() || !path->IsTable()) {
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

inline bool CheckReplicationBootstrapTables(const TPathId& replicationId, TProposeResponse& result, TSchemeShard* ss, bool requireImported = false) {
    for (const auto& [id, import] : ss->Imports) {
        if (import->IsFinished()) {
            if (requireImported) {
                for (const auto& item : import->Items) {
                    if (!item.ReplicationConfig.HasBootstrapReplication()
                        || TPathId::FromProto(item.ReplicationConfig.GetBootstrapReplication()) != replicationId) {
                        continue;
                    }
                    const auto path = TPath::Resolve(item.DstPathName, ss);
                    if (!path.IsResolved() || path.IsDeleted()) {
                        result.SetError(NKikimrScheme::StatusPreconditionFailed,
                            TStringBuilder() << "Retry S3 import before starting replication: " << item.DstPathName);
                        return false;
                    }
                }
            }
            continue;
        }
        for (const auto& item : import->Items) {
            auto config = item.ReplicationConfig;
            if (!config.HasBootstrapReplication()) {
                const auto path = TPath::Resolve(item.DstPathName, ss);
                if (IsReplicationBootstrapTable(path, ss)) {
                    config = ss->Tables.at(path.Base()->PathId)->ReplicationConfig();
                }
            }
            if (config.HasBootstrapReplication()
                && TPathId::FromProto(config.GetBootstrapReplication()) == replicationId) {
                result.SetError(NKikimrScheme::StatusMultipleModifications, "Import is using a replication destination");
                return false;
            }
        }
    }
    for (const auto& path : GetReplicationBootstrapTables(replicationId, ss)) {
        if (requireImported) {
            const auto& config = ss->Tables.at(path.Base()->PathId)->ReplicationConfig();
            if (config.GetAwaitingImport() || !config.HasBootstrapImportId()) {
                result.SetError(NKikimrScheme::StatusPreconditionFailed,
                    TStringBuilder() << "Complete or retry S3 import before starting replication: " << path.PathString());
                return false;
            }
        }
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
