#pragma once

#include <ydb/core/testlib/test_client.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSchemeShardUT_Private {

ui32 CountSharedShardsRows(TTestActorRuntime& runtime, ui64 localShardIdx = 0);
ui64 GetShardOwnerLocalPathId(TTestActorRuntime& runtime, ui64 localShardIdx);
TVector<ui64> GetColumnShardTabletIds(TTestActorRuntime& runtime, const TString& path);
ui64 ResolveLocalShardIdxByTabletId(TTestActorRuntime& runtime, ui64 tabletId);
ui32 CountShardsTableRows(TTestActorRuntime& runtime);
ui64 GetLocalPathId(TTestActorRuntime& runtime, const TString& path);

} // namespace NSchemeShardUT_Private
