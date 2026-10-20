#include "common.h"

#include <ydb/library/aclib/aclib.h>

#include <util/system/env.h>

#include <atomic>
#include <csignal>

using namespace NKikimrRun;

namespace NFqRun {

namespace {
std::atomic<bool> Interrupted = false;
static_assert(std::atomic<bool>::is_always_lock_free);

void InterruptHandler(int) {
    Interrupted.store(true, std::memory_order_relaxed);
}
}   // namespace

void InstallInterruptHandlers() {
    std::signal(SIGINT, InterruptHandler);
    std::signal(SIGTERM, InterruptHandler);
}

bool IsInterrupted() {
    return Interrupted.load(std::memory_order_relaxed);
}

TDuration CancellationTimeout() {
    // The FQ pinger delivers control actions on its default 15-second heartbeat.
    // Allow that round trip before falling back to local runtime shutdown.
    return TDuration::Seconds(30);
}

void TFqSetupSettings::ConfigureCompute(std::optional<EComputeBackend> backend, bool hasAnalytics) {
    if (!backend) {
        EnableYdbCompute |= hasAnalytics;
        return;
    }
    Y_ENSURE(*backend == EComputeBackend::Ydb || (!EnableYdbCompute && !SingleComputeDatabase && SharedComputeDatabases.empty()),
        "--compute=in-place cannot be combined with --single-compute-db or --shared-compute-db");
    EnableYdbCompute = *backend == EComputeBackend::Ydb;
    auto& compute = *AppConfig.MutableFederatedQueryConfig()->MutableCompute();
    // Explicit CLI selection wins over all scope-specific mappings in --cfg.
    compute.ClearComputeMapping();
    compute.SetDefaultCompute(EnableYdbCompute ? NFq::NConfig::YDB : NFq::NConfig::IN_PLACE);
}

TExternalDatabase TExternalDatabase::Parse(const TString& optionValue, const TString& tokenVar) {
    TStringBuf database, endpoint;
    TStringBuf(optionValue).Split('@', database, endpoint);
    if (database.empty() || endpoint.empty()) {
        ythrow yexception() << "Incorrect external database mapping, expected form database@endpoint";
    }

    TExternalDatabase result = { .Endpoint = TString(endpoint), .Database = TString(database), .Token = GetEnv(tokenVar) };

    if (!result.Token) {
        result.Token = GetEnv(YQL_TOKEN_VARIABLE);
        if (!result.Token) {
            result.Token = BUILTIN_ACL_ROOT;
        }
    }

    return result;
}

void SetupAcl(FederatedQuery::Acl* acl) {
    if (acl->visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        acl->set_visibility(FederatedQuery::Acl::SCOPE);
    }
}

NYql::TIssue GroupIssues(NYql::TIssue rootIssue, const NYql::TIssues& childrenIssues) {
    for (const auto& issue : childrenIssues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }
    return rootIssue;
}

bool IsFinalStatus(FederatedQuery::QueryMeta::ComputeStatus status) {
    using EStatus = FederatedQuery::QueryMeta;
    return IsIn({ EStatus::FAILED, EStatus::COMPLETED, EStatus::ABORTED_BY_USER, EStatus::ABORTED_BY_SYSTEM }, status);
}

}   // namespace NFqRun
