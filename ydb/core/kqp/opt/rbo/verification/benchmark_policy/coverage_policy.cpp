#include "coverage_policy.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <initializer_list>
#include <utility>

namespace NKikimr::NKqp::NVerificationCoverage {
namespace {

void RequirePolicyKeys(
    const NJson::TJsonValue& value,
    std::initializer_list<TStringBuf> required,
    TStringBuf context)
{
    if (!value.IsMap()) {
        ythrow yexception() << context << " must be an object";
    }
    std::set<TString> expected;
    for (const TStringBuf key : required) {
        expected.emplace(key);
    }
    const auto& fields = value.GetMapSafe();
    for (const auto& [key, field] : fields) {
        Y_UNUSED(field);
        if (!expected.contains(key)) {
            ythrow yexception()
                << context << " has unexpected field " << key;
        }
    }
    for (const auto& key : expected) {
        if (!fields.contains(key)) {
            ythrow yexception()
                << context << " is missing field " << key;
        }
    }
}

ui64 PolicyUint(
    const NJson::TJsonValue& value,
    TStringBuf context)
{
    if (!value.IsUInteger()) {
        ythrow yexception() << context << " must be an unsigned integer";
    }
    return value.GetUIntegerSafe();
}

std::set<ui32> PolicyQueryIds(
    const NJson::TJsonValue& value,
    const TSuite& suite,
    TStringBuf field,
    TStringBuf context)
{
    if (!value.IsArray()) {
        ythrow yexception()
            << context << " " << field << " must be an array";
    }

    std::set<ui32> result;
    ui32 previous = 0;
    for (const auto& encodedId : value.GetArraySafe()) {
        const ui64 id = PolicyUint(
            encodedId,
            TStringBuilder() << context << " " << field << " query id");
        if (id < 1 || id > suite.QueryCount) {
            ythrow yexception()
                << context << " " << field << " query id " << id
                << " is outside the corpus";
        }
        if (id <= previous) {
            ythrow yexception()
                << context << " " << field
                << " query ids must be strictly increasing";
        }
        previous = static_cast<ui32>(id);
        result.insert(previous);
    }
    return result;
}

bool IsFullSelection(
    const TSuite& suite,
    const std::set<ui32>& selected)
{
    if (selected.size() != suite.QueryCount) {
        return false;
    }
    for (ui32 queryId = 1; queryId <= suite.QueryCount; ++queryId) {
        if (!selected.contains(queryId)) {
            return false;
        }
    }
    return true;
}

TStringBuf CoverageModeName(ECoverageMode mode) {
    switch (mode) {
        case ECoverageMode::FormulaDashboard:
            return "formula_dashboard";
        case ECoverageMode::SolverExperiment:
            return "solver_experiment";
        case ECoverageMode::ProofFloor:
            return "proof_floor";
    }
    ythrow yexception() << "unknown coverage mode";
}

} // namespace

TCoveragePolicy DecodeCoveragePolicy(TStringBuf text) {
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(text, &root, false)) {
        ythrow yexception() << "coverage policy is not valid JSON";
    }
    RequirePolicyKeys(
        root,
        {"format", "version", "row_bound", "task_bound", "suites"},
        "coverage policy");
    if (!root["format"].IsString() ||
        root["format"].GetStringSafe() != CoveragePolicyFormat)
    {
        ythrow yexception()
            << "coverage policy has unsupported format";
    }
    if (PolicyUint(root["version"], "coverage policy version") !=
        CoveragePolicyVersion)
    {
        ythrow yexception()
            << "coverage policy has unsupported version";
    }
    if (PolicyUint(root["row_bound"], "coverage policy row_bound") != RowBound ||
        PolicyUint(root["task_bound"], "coverage policy task_bound") != TaskBound)
    {
        ythrow yexception()
            << "coverage policy bounds do not match the dashboard";
    }

    const auto& suites = root["suites"];
    RequirePolicyKeys(suites, {Tpch.Name, Tpcds.Name}, "coverage policy suites");

    TCoveragePolicy policy;
    for (const TSuite* suite : {&Tpch, &Tpcds}) {
        const auto& encoded = suites[suite->Name];
        const TString context = TStringBuilder()
            << "coverage policy suite " << suite->Name;
        RequirePolicyKeys(
            encoded,
            {
                "query_count",
                "required_prepare_success_queries",
                "required_snapshot_pair_queries",
                "required_verifier_entry_queries",
                "required_formula_queries",
                "required_verified_queries",
            },
            context);
        if (PolicyUint(encoded["query_count"], context + " query_count") !=
            suite->QueryCount)
        {
            ythrow yexception()
                << context << " query_count does not match the corpus";
        }
        TSuiteCoveragePolicy suitePolicy;
        suitePolicy.QueryCount = suite->QueryCount;
        suitePolicy.RequiredPrepareSuccessQueries = PolicyQueryIds(
            encoded["required_prepare_success_queries"],
            *suite,
            "required_prepare_success_queries",
            context);
        suitePolicy.RequiredSnapshotPairQueries = PolicyQueryIds(
            encoded["required_snapshot_pair_queries"],
            *suite,
            "required_snapshot_pair_queries",
            context);
        suitePolicy.RequiredVerifierEntryQueries = PolicyQueryIds(
            encoded["required_verifier_entry_queries"],
            *suite,
            "required_verifier_entry_queries",
            context);
        suitePolicy.RequiredFormulaQueries = PolicyQueryIds(
            encoded["required_formula_queries"],
            *suite,
            "required_formula_queries",
            context);
        suitePolicy.RequiredVerifiedQueries = PolicyQueryIds(
            encoded["required_verified_queries"],
            *suite,
            "required_verified_queries",
            context);
        for (const ui32 queryId : suitePolicy.RequiredSnapshotPairQueries) {
            if (suitePolicy.RequiredVerifierEntryQueries.contains(queryId)) {
                ythrow yexception()
                    << context << " required snapshot-pair query q" << queryId
                    << " is also a required verifier-entry query";
            }
            if (suitePolicy.RequiredFormulaQueries.contains(queryId)) {
                ythrow yexception()
                    << context << " required snapshot-pair query q" << queryId
                    << " is also a required formula query";
            }
        }
        if (suitePolicy.RequiredVerifiedQueries.empty()) {
            ythrow yexception()
                << context << " required_verified_queries must not be empty";
        }
        for (const ui32 queryId : suitePolicy.RequiredVerifiedQueries) {
            if (!suitePolicy.RequiredFormulaQueries.contains(queryId)) {
                ythrow yexception()
                    << context << " required verified query q" << queryId
                    << " is not a required formula query";
            }
        }
        policy.Suites.emplace(suite->Name, std::move(suitePolicy));
    }
    return policy;
}

std::set<ui32> SnapshotPairFloorQueries(
    const TSuiteCoveragePolicy& suitePolicy)
{
    auto result = suitePolicy.RequiredSnapshotPairQueries;
    result.insert(
        suitePolicy.RequiredVerifierEntryQueries.begin(),
        suitePolicy.RequiredVerifierEntryQueries.end());
    result.insert(
        suitePolicy.RequiredFormulaQueries.begin(),
        suitePolicy.RequiredFormulaQueries.end());
    return result;
}

TPolicyEvaluation EvaluateCoveragePolicy(
    const TCoveragePolicy& policy,
    const TSuite& suite,
    const std::set<ui32>& selected,
    const TMap<ui32, TString>& statuses,
    const std::set<ui32>& snapshotPairQueries,
    const std::set<ui32>& verifierEntryQueries,
    const std::set<ui32>& prepareSuccessQueries,
    ECoverageMode mode)
{
    const auto suitePolicy = policy.Suites.find(suite.Name);
    if (suitePolicy == policy.Suites.end() ||
        suitePolicy->second.QueryCount != suite.QueryCount)
    {
        ythrow yexception()
            << "coverage policy does not match suite " << suite.Name;
    }

    TPolicyEvaluation result;
    result.Mode = mode;
    result.SelectedQueries = selected;
    result.FullSelection = IsFullSelection(suite, selected);
    result.RequiredPrepareSuccessQueries =
        suitePolicy->second.RequiredPrepareSuccessQueries;
    result.PrepareSuccessQueries = prepareSuccessQueries;
    result.RequiredSnapshotPairQueries =
        suitePolicy->second.RequiredSnapshotPairQueries;
    result.SnapshotPairFloorQueries =
        SnapshotPairFloorQueries(suitePolicy->second);
    result.SnapshotPairQueries = snapshotPairQueries;
    result.RequiredVerifierEntryQueries =
        suitePolicy->second.RequiredVerifierEntryQueries;
    result.VerifierEntryQueries = verifierEntryQueries;
    result.RequiredFormulaQueries =
        suitePolicy->second.RequiredFormulaQueries;
    result.RequiredVerifiedQueries =
        suitePolicy->second.RequiredVerifiedQueries;
    for (const auto& [queryId, status] : statuses) {
        if (status == "FORMULA_EMITTED") {
            result.FormulaEmittedQueries.insert(queryId);
        } else if (status == "VERIFIED_BOUNDED") {
            result.VerifiedBoundedQueries.insert(queryId);
        }
    }

    if (mode == ECoverageMode::SolverExperiment) {
        return result;
    }

    if (mode == ECoverageMode::FormulaDashboard) {
        result.PrepareSuccessFloorEnforced = result.FullSelection;
        result.SnapshotPairFloorEnforced = result.FullSelection;
        result.VerifierEntryFloorEnforced = result.FullSelection;
        result.FormulaFloorEnforced = result.FullSelection;
        if (!result.FormulaFloorEnforced) {
            return result;
        }
        result.PrepareSuccessFloorQueries =
            result.RequiredPrepareSuccessQueries;
        for (const ui32 queryId : result.PrepareSuccessFloorQueries) {
            if (!result.PrepareSuccessQueries.contains(queryId)) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " regressed before successful query preparation");
            }
        }
        for (const ui32 queryId : result.SnapshotPairFloorQueries) {
            if (result.SnapshotPairQueries.contains(queryId)) {
                continue;
            }
            const auto status = statuses.find(queryId);
            if (status == statuses.end()) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " has no coverage outcome; expected exact Initial/Final"
                       " snapshot pair");
            } else {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " regressed before exact Initial/Final snapshot pair with status "
                    << status->second);
            }
        }
        for (const ui32 queryId : result.RequiredVerifierEntryQueries) {
            const auto status = statuses.find(queryId);
            if (status == statuses.end()) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " has no coverage outcome; expected verifier entry");
            } else if (!result.VerifierEntryQueries.contains(queryId)) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " regressed before verifier entry with status "
                    << status->second);
            }
        }
        for (const ui32 queryId : result.RequiredFormulaQueries) {
            const auto status = statuses.find(queryId);
            if (status == statuses.end()) {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " has no coverage outcome; expected FORMULA_EMITTED");
            } else if (
                status->second != "FORMULA_EMITTED" &&
                status->second != "VERIFIED_BOUNDED")
            {
                result.Violations.push_back(TStringBuilder()
                    << suite.Name << " q" << queryId
                    << " regressed from FORMULA_EMITTED to " << status->second);
            }
        }
        return result;
    }

    result.PrepareSuccessFloorEnforced = true;
    result.ProofFloorEnforced = true;
    for (const ui32 queryId : result.RequiredVerifiedQueries) {
        if (result.RequiredPrepareSuccessQueries.contains(queryId)) {
            result.PrepareSuccessFloorQueries.insert(queryId);
        }
    }
    if (selected != result.RequiredVerifiedQueries) {
        result.Violations.push_back(TStringBuilder()
            << suite.Name
            << " proof floor did not select exactly its required verified queries");
    }
    for (const ui32 queryId : result.PrepareSuccessFloorQueries) {
        if (!result.PrepareSuccessQueries.contains(queryId)) {
            result.Violations.push_back(TStringBuilder()
                << suite.Name << " q" << queryId
                << " proof obligation did not complete query preparation");
        }
    }
    for (const ui32 queryId : result.RequiredVerifiedQueries) {
        const auto status = statuses.find(queryId);
        if (status == statuses.end()) {
            result.Violations.push_back(TStringBuilder()
                << suite.Name << " q" << queryId
                << " has no proof outcome; expected VERIFIED_BOUNDED");
        } else if (status->second != "VERIFIED_BOUNDED") {
            result.Violations.push_back(TStringBuilder()
                << suite.Name << " q" << queryId
                << " regressed from VERIFIED_BOUNDED to " << status->second);
        }
    }
    return result;
}

NJson::TJsonValue JsonIds(const TVector<ui32>& ids) {
    NJson::TJsonValue result(NJson::JSON_ARRAY);
    for (const ui32 id : ids) {
        result.AppendValue(id);
    }
    return result;
}

NJson::TJsonValue JsonIds(const std::set<ui32>& ids) {
    return JsonIds(TVector<ui32>(ids.begin(), ids.end()));
}

NJson::TJsonValue PolicyEvaluationJson(
    const TPolicyEvaluation& evaluation)
{
    NJson::TJsonValue result(NJson::JSON_MAP);
    result["format"] = CoveragePolicyEvaluationFormat;
    result["version"] = CoveragePolicyEvaluationVersion;
    result["valid"] = evaluation.Valid;
    result["mode"] = CoverageModeName(evaluation.Mode);
    result["full_selection"] = evaluation.FullSelection;
    result["prepare_success_floor_enforced"] =
        evaluation.PrepareSuccessFloorEnforced;
    result["snapshot_pair_floor_enforced"] =
        evaluation.SnapshotPairFloorEnforced;
    result["verifier_entry_floor_enforced"] =
        evaluation.VerifierEntryFloorEnforced;
    result["formula_floor_enforced"] = evaluation.FormulaFloorEnforced;
    result["proof_floor_enforced"] = evaluation.ProofFloorEnforced;
    result["selected_queries"] = JsonIds(evaluation.SelectedQueries);
    result["required_prepare_success_queries"] =
        JsonIds(evaluation.RequiredPrepareSuccessQueries);
    result["prepare_success_floor_queries"] =
        JsonIds(evaluation.PrepareSuccessFloorQueries);
    result["prepare_success_queries"] =
        JsonIds(evaluation.PrepareSuccessQueries);
    result["required_snapshot_pair_queries"] =
        JsonIds(evaluation.RequiredSnapshotPairQueries);
    result["snapshot_pair_floor_queries"] =
        JsonIds(evaluation.SnapshotPairFloorQueries);
    result["snapshot_pair_queries"] =
        JsonIds(evaluation.SnapshotPairQueries);
    result["required_verifier_entry_queries"] =
        JsonIds(evaluation.RequiredVerifierEntryQueries);
    result["verifier_entry_queries"] =
        JsonIds(evaluation.VerifierEntryQueries);
    result["required_formula_queries"] =
        JsonIds(evaluation.RequiredFormulaQueries);
    result["formula_emitted_queries"] =
        JsonIds(evaluation.FormulaEmittedQueries);
    result["required_verified_queries"] =
        JsonIds(evaluation.RequiredVerifiedQueries);
    result["verified_bounded_queries"] =
        JsonIds(evaluation.VerifiedBoundedQueries);
    NJson::TJsonValue violations(NJson::JSON_ARRAY);
    for (const auto& violation : evaluation.Violations) {
        violations.AppendValue(violation);
    }
    result["violations"] = std::move(violations);
    return result;
}

} // namespace NKikimr::NKqp::NVerificationCoverage
