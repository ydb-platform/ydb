#pragma once

// Pure coverage-floor contract: no cluster, environment, filesystem, or process access.
#include <library/cpp/json/json_value.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <set>

namespace NKikimr::NKqp::NVerificationCoverage {

inline constexpr ui64 RowBound = 2;
inline constexpr ui64 TaskBound = 2;
inline constexpr const char* CoveragePolicyFormat = "ydb-rbo-benchmark-coverage-policy";
inline constexpr ui64 CoveragePolicyVersion = 5;
inline constexpr const char* CoveragePolicyEvaluationFormat =
    "ydb-rbo-benchmark-coverage-policy-evaluation";
inline constexpr ui64 CoveragePolicyEvaluationVersion = 4;

enum class ECoverageMode {
    FormulaDashboard,
    SolverExperiment,
    ProofFloor,
};

struct TSuite {
    TString Name;
    TString Slug;
    TString Schema;
    TString QueryPrefix;
    ui32 QueryCount;
};

inline const TSuite Tpch{
    "TPCH_YQL", "tpch", "schema/tpch.sql", "yql-tpch/q", 22};
inline const TSuite Tpcds{
    "TPCDS_YQL", "tpcds", "schema/tpcds.sql", "yql-tpcds/q", 99};

struct TSuiteCoveragePolicy {
    ui32 QueryCount = 0;
    std::set<ui32> RequiredPrepareSuccessQueries;
    std::set<ui32> RequiredSnapshotPairQueries;
    std::set<ui32> RequiredVerifierEntryQueries;
    std::set<ui32> RequiredFormulaQueries;
    std::set<ui32> RequiredVerifiedQueries;
};

struct TCoveragePolicy {
    TMap<TString, TSuiteCoveragePolicy> Suites;
};

struct TPolicyEvaluation {
    bool Valid = true;
    ECoverageMode Mode = ECoverageMode::FormulaDashboard;
    bool FullSelection = false;
    bool PrepareSuccessFloorEnforced = false;
    bool SnapshotPairFloorEnforced = false;
    bool VerifierEntryFloorEnforced = false;
    bool FormulaFloorEnforced = false;
    bool ProofFloorEnforced = false;
    std::set<ui32> SelectedQueries;
    std::set<ui32> RequiredPrepareSuccessQueries;
    std::set<ui32> PrepareSuccessFloorQueries;
    std::set<ui32> PrepareSuccessQueries;
    std::set<ui32> RequiredSnapshotPairQueries;
    std::set<ui32> SnapshotPairFloorQueries;
    std::set<ui32> SnapshotPairQueries;
    std::set<ui32> RequiredVerifierEntryQueries;
    std::set<ui32> VerifierEntryQueries;
    std::set<ui32> RequiredFormulaQueries;
    std::set<ui32> FormulaEmittedQueries;
    std::set<ui32> RequiredVerifiedQueries;
    std::set<ui32> VerifiedBoundedQueries;
    TVector<TString> Violations;
};


TCoveragePolicy DecodeCoveragePolicy(TStringBuf text);
std::set<ui32> SnapshotPairFloorQueries(const TSuiteCoveragePolicy& suitePolicy);
TPolicyEvaluation EvaluateCoveragePolicy(
    const TCoveragePolicy& policy,
    const TSuite& suite,
    const std::set<ui32>& selected,
    const TMap<ui32, TString>& statuses,
    const std::set<ui32>& snapshotPairQueries,
    const std::set<ui32>& verifierEntryQueries,
    const std::set<ui32>& prepareSuccessQueries,
    ECoverageMode mode);
NJson::TJsonValue JsonIds(const TVector<ui32>& ids);
NJson::TJsonValue JsonIds(const std::set<ui32>& ids);
NJson::TJsonValue PolicyEvaluationJson(const TPolicyEvaluation& evaluation);

} // namespace NKikimr::NKqp::NVerificationCoverage
