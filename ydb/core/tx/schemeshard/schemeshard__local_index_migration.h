#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSchemeShard {

struct TLocalIndexMigrationItem {
    TString WorkingDir;
    NKikimrSchemeOp::TIndexCreationConfig IndexConfig;

    TString DebugString() const;
};

THolder<NActors::IActor> CreateLocalIndexMigrator(TTabletId selfTabletId, NActors::TActorId selfActorId,
                                                  TVector<std::pair<TTxId, TLocalIndexMigrationItem>>&& items);

} // namespace NKikimr::NSchemeShard
