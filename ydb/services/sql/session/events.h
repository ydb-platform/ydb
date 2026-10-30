#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/services/sql/datashard/events.h>

namespace NKikimr::NSql {

inline constexpr ui32 EvExecute = EventSpaceBegin(TKikimrEvents::ES_SQL) + 4;
inline constexpr ui32 EvExecuteResult = EventSpaceBegin(TKikimrEvents::ES_SQL) + 5;

struct TBoundParam {
    bool Null = false;
    bool Number = false;
    TString Text;
};

struct TEvExecute : public NActors::TEventLocal<TEvExecute, EvExecute> {
    TString Sql;
    TVector<TBoundParam> Params;
};

// Result of one statement. The wire session turns this into protocol frames.
// A test reads these fields and does not parse the socket.
struct TEvExecuteResult : public NActors::TEventLocal<TEvExecuteResult, EvExecuteResult> {
    bool Ok = false;
    TString SqlState;
    TString Message;
    TString CommandTag;
    TVector<TQueryColumn> Columns;
    TVector<TVector<TQueryCell>> Rows;
    char TxStatus = 'I';
    TString GucName;
    TString GucValue;
    TString AccessPath;
};

NActors::IActor* CreateSqlSession(TString database, ui32 internalErrorRetries, ui32 recursionLimit);

} // namespace NKikimr::NSql
