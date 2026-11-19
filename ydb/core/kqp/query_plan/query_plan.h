#pragma once

#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKqpProto {
class TKqpStatsQuery;
}

namespace NYql::NDqProto {
class TDqExecutionStats;
}

namespace NKikimr::NKqp {

TString SerializeTxPlans(const TVector<const TString>& txPlans, const TString commonPlanInfo = "",
    const TString& queryStats = "", const NJson::TJsonValue& optimizerStats = {});

/*
 * Fill stages in given txPlan with ExecutionStats fields. Each plan stage stores StageGuid which is
 * used to find corresponding TKqpStatsExecution object.
 */
TString AddExecStatsToTxPlan(const TString& txPlan, const NYql::NDqProto::TDqExecutionStats& stats, bool newRboEnabled);

TString SerializeAnalyzePlan(const NKqpProto::TKqpStatsQuery& queryStats, bool newRboEnabled, const TString& poolId = "");

TString SerializeScriptPlan(const TVector<const TString>& queryPlans);

TString SerializeRBOExplainPlan(NJson::TJsonValue txPlan);
TString SerializeRBOAnalyzePlan(const TVector<const TString>& txPlans, const NKqpProto::TKqpStatsQuery& queryStats, const TString& poolId = "");

} // namespace NKikimr::NKqp
