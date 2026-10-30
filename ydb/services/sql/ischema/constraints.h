#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/services/sql/parse/parser.h>

namespace NKikimr::NSql {

NActors::IActor* CreateConstraintsActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement);

} // namespace NKikimr::NSql
