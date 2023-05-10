#pragma once

#include <ydb/core/protos/scheme_log.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>

namespace NKikimr {

NKikimrProto::EReplyStatus LocalSchemeTx(TTestActorRuntime& runtime, ui64 tabletId, const TString& schemeChangesStr, bool dryRun, NTabletFlatScheme::TSchemeChanges& scheme, TString& err);

ui64 GetExecutorCacheSize(TTestActorRuntime& runtime, ui64 tabletId);

} // namespace NKikimr
