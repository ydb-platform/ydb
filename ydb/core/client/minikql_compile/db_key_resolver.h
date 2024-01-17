#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/core/scheme/tablet_scheme_defs.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>


#include <library/cpp/threading/future/future.h>


namespace NYql {

using NKikimr::TTagDetails;

class IDbSchemeResolver {
public:
    struct TTable {
        TString TableName;
        TSet<TString> ColumnNames;
        ui64 RefreshAgainst = 0;
    };

    struct TTableResult {
        enum EStatus {
            Ok = 0,
            Error = 1,
            LookupError = 2
        };

        TTableResult(EStatus status, const TString& reason = TString())
            : Status(status)
            , Reason(reason)
        {}

        struct TColumn {
            ui32 Column;
            i32 KeyPosition;
            NKikimr::NScheme::TTypeInfo Type;
            ui32 AllowInplaceMode;
            NKikimr::EColumnTypeConstraint TypeConstraint;
        };

        EStatus Status;
        TString Reason;
        TTable Table;
        TAutoPtr<NKikimr::TTableId> TableId;
        ui32 KeyColumnCount = 0;
        TMap<TString, TColumn> Columns;
        ui64 CacheGeneration = 0;
    };

    using TTableResults = TVector<TTableResult>;

    virtual ~IDbSchemeResolver() {}

    // Future-based API.
    virtual NThreading::TFuture<TTableResults> ResolveTables(const TVector<TTable>& tables) = 0;

    // MessagePassing-based API.
    struct TEvents {
        enum {
            EvResolveTablesResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            End
        };
        static_assert(End < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect End < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvResolveTablesResult : public NActors::TEventLocal<TEvResolveTablesResult, EvResolveTablesResult> {
            TEvResolveTablesResult(TTableResults&& result);

            TTableResults Result;
        };
    };

    virtual void ResolveTables(const TVector<TTable>& tables, NActors::TActorId responseTo) = 0; // TEvResolveTablesResult.
};

} // namespace NYql
