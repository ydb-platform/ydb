#pragma once

#include <ydb/core/kqp/common/simple/kqp_compute_column.h>
#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/aclib/aclib.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NSysView {

// Full system-view scans are supplied by the application. Embedded servers
// can omit them without linking SQL-specific system views and formatters.
class ISystemViewScanFactory {
public:
    virtual ~ISystemViewScanFactory() = default;

    virtual THolder<NActors::IActor> Create(const NActors::TActorId& ownerId, ui32 scanId,
        const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
        const TTableId& tableId, const TString& tablePath,
        TVector<TSerializedTableRange> ranges, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse) const = 0;

    virtual THolder<NActors::IActor> Create(const NActors::TActorId& ownerId, ui32 scanId,
        const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
        const TTableId& tableId, const TString& tablePath,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse) const = 0;

};

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
    const TTableId& tableId, const TString& tablePath,
    TVector<TSerializedTableRange> ranges, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse);

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
    const TTableId& tableId, const TString& tablePath,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse);

} // NSysView
} // NKikimr
