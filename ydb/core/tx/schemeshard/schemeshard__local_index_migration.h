#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/core/util/backoff.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

struct TLocalIndexMigrationItem {
    TString WorkingDir;
    NKikimrSchemeOp::TIndexCreationConfig IndexConfig;
    // Column tables migrate via ESchemeOpAlterColumnTable; row tables migrate via
    // ESchemeOpAlterTable. For row tables, the engine artifact already exists,
    // so migration only registers the scheme object.
    bool IsColumnTable = true;
    TBackoff Backoff{10};

    TString DebugString() const;
};

THolder<NActors::IActor> CreateLocalIndexMigrator(TTabletId selfTabletId, NActors::TActorId selfActorId,
                                                  TSchemeShard* schemeshard,
                                                  TVector<TLocalIndexMigrationItem>&& items);

} // namespace NKikimr::NSchemeShard
