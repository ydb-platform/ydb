#pragma once

#include "schema.h"
#include "target_table.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NReplication::NController {

// SchemeShard knows about tables restored before target discovery or attachment.
// Persist them as targets before starting DROP/DONE so cleanup survives reboot.
inline void RegisterBootstrapTables(TReplication& replication,
        const google::protobuf::RepeatedPtrField<NKikimrReplication::TBootstrapTable>& tables,
        NIceDb::TNiceDb& db) {
    using Schema = TControllerSchema;
    if (tables.empty()) {
        return;
    }
    for (const auto& table : tables) {
        const auto pathId = TPathId::FromProto(table.GetPathId());
        TReplication::ITarget* target = nullptr;
        for (auto* candidate : replication.GetTargets()) {
            if (CanonizePath(candidate->GetDstPath()) == CanonizePath(table.GetPath())) {
                target = candidate;
                break;
            }
        }
        if (!target) {
            auto config = std::make_shared<TTargetTable::TTableConfig>(TString(), table.GetPath());
            const auto id = replication.AddTarget(TReplication::ETargetKind::Table, config);
            target = replication.FindTarget(id);
            target->SetStreamState(TReplication::EStreamState::Removed);
            db.Table<Schema::Targets>().Key(replication.GetId(), id).Update(
                NIceDb::TUpdate<Schema::Targets::Kind>(target->GetKind()),
                NIceDb::TUpdate<Schema::Targets::SrcPath>(target->GetSrcPath()),
                NIceDb::TUpdate<Schema::Targets::DstPath>(target->GetDstPath()));
            db.Table<Schema::SrcStreams>().Key(replication.GetId(), id).Update(
                NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState()));
        }
        if (target->GetDstPathId() || target->GetPendingDstPathId()) {
            continue;
        }
        target->SetDstPathId(pathId);
        target->SetPendingDstPathId(pathId);
        db.Table<Schema::Targets>().Key(replication.GetId(), target->GetId()).Update(
            NIceDb::TUpdate<Schema::Targets::DstPathOwnerId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::Targets::DstPathLocalId>(pathId.LocalPathId),
            NIceDb::TUpdate<Schema::Targets::PendingDstPathOwnerId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::Targets::PendingDstPathLocalId>(pathId.LocalPathId));
    }
    db.Table<Schema::Replications>().Key(replication.GetId()).Update(
        NIceDb::TUpdate<Schema::Replications::NextTargetId>(replication.GetNextTargetId()));
}

} // namespace NKikimr::NReplication::NController
