#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/services/sql/parse/parser.h>

namespace NKikimr::NSql {

NActors::IActor* CreateColumnSelectActor(const NActors::TActorId& replyTo, TString database, TSelectStmt statement, ui32 internalErrorRetries, ui64 lockTxId);
NActors::IActor* CreateColumnInsertActor(const NActors::TActorId& replyTo, TString database, TInsertStmt statement, ui32 internalErrorRetries, bool defer);
NActors::IActor* CreateColumnUpdateActor(const NActors::TActorId& replyTo, TString database, TUpdateStmt statement, ui32 internalErrorRetries, bool defer);

}
