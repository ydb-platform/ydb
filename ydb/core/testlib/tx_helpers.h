#pragma once

#include <ydb/core/protos/scheme_log.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>

namespace NKikimrMiniKQL {
    class TResult;
}

namespace NKikimr {

NKikimrProto::EReplyStatus LocalQuery(TTestActorRuntime& runtime, ui64 tabletId,
    const TString& program, NKikimrMiniKQL::TResult& result);

NKikimrProto::EReplyStatus LocalSchemeTx(TTestActorRuntime& runtime, ui64 tabletId,
    const NTabletFlatScheme::TSchemeChanges& schemeChanges, bool dryRun,
    NTabletFlatScheme::TSchemeChanges& scheme, TString& err);

NKikimrProto::EReplyStatus LocalSchemeTx(TTestActorRuntime& runtime, ui64 tabletId,
    const TString& schemeChangesStr, bool dryRun,
    NTabletFlatScheme::TSchemeChanges& scheme, TString& err);

ui64 GetExecutorCacheSize(TTestActorRuntime& runtime, ui64 tabletId);

} // namespace NKikimr
