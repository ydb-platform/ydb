#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/services/sql/parse/parser.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

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

struct TEvQueryResult : public NActors::TEventLocal<TEvQueryResult, EvQueryResult> {
    bool Ok = false;
    TString SqlState;
    TString Message;
    TVector<TQueryColumn> Columns;
    TVector<TVector<TQueryCell>> Rows;
};

NActors::IActor* CreateSelectActor(const NActors::TActorId& replyTo, TString database, TString text, TSelectStmt statement, ui32 internalErrorRetries);

} // namespace NKikimr::NSql
