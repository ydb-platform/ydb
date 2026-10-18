#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/services/sql/parse/parser.h>

namespace NKikimr::NSql {

inline constexpr ui32 EvDdlResult = EventSpaceBegin(TKikimrEvents::ES_SQL) + 2;

struct TEvDdlResult : public NActors::TEventLocal<TEvDdlResult, EvDdlResult> {
    bool Ok = false;
    TString SqlState;
    TString Message;
    TString CommandTag;
};

NActors::IActor* CreateTableActor(const NActors::TActorId& replyTo, TString database, TCreateTableStmt statement);
NActors::IActor* CreateAlterTableActor(const NActors::TActorId& replyTo, TString database, TAlterTableStmt statement);
NActors::IActor* CreateSequenceActor(const NActors::TActorId& replyTo, TString database, TCreateSequenceStmt statement);
NActors::IActor* CreateAlterSequenceActor(const NActors::TActorId& replyTo, TString database, TAlterSequenceStmt statement);

} // namespace NKikimr::NSql
