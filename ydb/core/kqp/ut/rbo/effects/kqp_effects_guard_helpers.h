#pragma once

#include "kqp_effects_helpers.h"

namespace NKikimr::NKqp::NEffectsGuardTests {

inline NKikimrConfig::TAppConfig GuardConfig(bool fallback = true, bool streamIndex = true) {
    auto config = NewRboAppConfig();
    config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(fallback);
    config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(streamIndex);
    return config;
}

inline void CreateTables(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE A (k Uint64 NOT NULL, v Uint64, PRIMARY KEY (k));
        CREATE TABLE B (k Uint64 NOT NULL, v Uint64, PRIMARY KEY (k));
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace NKikimr::NKqp::NEffectsGuardTests
