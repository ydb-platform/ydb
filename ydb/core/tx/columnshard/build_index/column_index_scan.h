#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_types_defs.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

namespace NKikimr::NColumnShard {

struct TColumnIndexScanColumn {
    TString Name;
    ui32 Id = 0;
    NScheme::TTypeInfo Type;
};

NActors::IActor* CreateColumnTableIndexScan(
    const NActors::TActorId& columnShardId,
    NKikimrTxDataShard::TEvBuildIndexCreateRequest request,
    TVector<TColumnIndexScanColumn> columns,
    ui32 keyColumnCount);

}
