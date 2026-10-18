#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/services/sql/parse/parser.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>
#include <utility>
#include <vector>

namespace NKikimr::NSql {

inline constexpr ui32 EvQueryResult = EventSpaceBegin(TKikimrEvents::ES_SQL) + 3;

struct TQueryColumn {
    TString Name;
    ui32 TypeOid = 25;
    i16 TypeSize = -1;
};

struct TQueryCell {
    bool Null = false;
    TString Text;
};

// A write held until COMMIT. One shard and no locks is sent as MODE_IMMEDIATE.
struct TDeferredWrite {
    ui64 OwnerId = 0;
    ui64 LocalPathId = 0;
    ui64 SchemaVersion = 0;
    ui64 TabletId = 0;
    ui32 Operation = 0;
    std::vector<ui32> ColumnIds;
    TString Payload;
};

struct TEvQueryResult : public NActors::TEventLocal<TEvQueryResult, EvQueryResult> {
    bool Ok = false;
    TString SqlState;
    TString Message;
    TString CommandTag;
    TVector<TQueryColumn> Columns;
    TVector<TVector<TQueryCell>> Rows;
    std::optional<TDeferredWrite> Deferred;
};

NActors::IActor* CreateSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, ui32 internalErrorRetries, std::optional<std::pair<ui64, ui64>> snapshot = std::nullopt);
NActors::IActor* CreateInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries, bool defer = false);
NActors::IActor* CreateCommitActor(const NActors::TActorId& replyTo, TVector<TDeferredWrite> writes, ui32 internalErrorRetries);
NActors::IActor* CreateInsertSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TInsertStmt statement, ui32 internalErrorRetries);

} // namespace NKikimr::NSql
