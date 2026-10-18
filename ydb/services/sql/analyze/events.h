#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/services/sql/parse/parser.h>

namespace NKikimr::NSql {

inline constexpr ui32 EvAnalyzeResult = EventSpaceBegin(TKikimrEvents::ES_SQL) + 1;

struct TEvAnalyzeResult : public NActors::TEventLocal<TEvAnalyzeResult, EvAnalyzeResult> {
    bool Ok = false;
    TString SqlState;
    TString Message;
    TString Path;
    TVector<ui32> ColumnIds;
};

NActors::IActor* CreateSelectAnalyzer(const NActors::TActorId& replyTo, TString database, TSelectStmt statement);

} // namespace NKikimr::NSql
