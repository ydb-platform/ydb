#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <memory>
#include <algorithm>
#include <mutex>
#include <optional>
#include <tuple>
#include <vector>

namespace NKikimr::NKqp {

enum class ECompileDependency {
    SchemeCache,
    StatisticsService,
};

enum class ECompileDependencyStatus {
    Unknown,
    Ok,
    Error,
};

struct TCompileDependencyDiagnostic {
    ECompileDependency Dependency;
    TString Target;
    TInstant Start;
    TInstant End;
    ECompileDependencyStatus Status = ECompileDependencyStatus::Unknown;
};

struct TCompileActorDiagnostic {
    TInstant Start;
    TInstant End;
};

struct TCompileDiagnostics {
    std::vector<TCompileDependencyDiagnostic> Dependencies;
    size_t Dropped = 0;
};

constexpr size_t MaxCompileAttempts = 32;
constexpr size_t MaxCompileDependencyDiagnosticsPerQuery = 128;

struct TCompileAttemptDiagnostic {
    TInstant Start;
    TInstant End;
    bool FromCache = false;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    std::shared_ptr<const TCompileDiagnostics> Dependencies;
    std::optional<TCompileActorDiagnostic> Actor;
    bool Partial = false;
};

inline auto CompileAttemptRank(const TCompileAttemptDiagnostic& attempt) {
    const bool pending = attempt.End == TInstant::Zero();
    const bool failed = attempt.Status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
        && attempt.Status != Ydb::StatusIds::SUCCESS;
    const ui64 durationUs = attempt.Start != TInstant::Zero() && attempt.End >= attempt.Start
        ? (attempt.End - attempt.Start).MicroSeconds() : 0;
    return std::tuple(pending, failed, !attempt.FromCache, durationUs);
}

inline void KeepCompileAttempt(std::vector<TCompileAttemptDiagnostic>& attempts,
        TCompileAttemptDiagnostic&& candidate, size_t& dropped) {
    if (attempts.size() < MaxCompileAttempts) {
        attempts.push_back(std::move(candidate));
        return;
    }
    ++dropped;
    auto least = std::min_element(attempts.begin(), attempts.end(), [](const auto& lhs, const auto& rhs) {
        return CompileAttemptRank(lhs) < CompileAttemptRank(rhs);
    });
    if (least != attempts.end() && CompileAttemptRank(*least) < CompileAttemptRank(candidate)) {
        *least = std::move(candidate);
    }
}

class ICompileDependencyDiagnostics {
public:
    struct THandle {
        ui64 Id = 0;
    };

    virtual ~ICompileDependencyDiagnostics() = default;

    virtual THandle Begin(ECompileDependency dependency, TString target) = 0;
    virtual void Finish(THandle handle, ECompileDependencyStatus status) = 0;
};

class TCompileDiagnosticsCollector final : public ICompileDependencyDiagnostics {
    struct TSlot {
        ui64 Id;
        TCompileDependencyDiagnostic Diagnostic;
    };

public:
    THandle Begin(ECompileDependency dependency, TString target) override {
        const TInstant start = TInstant::Now();
        std::lock_guard guard(Mutex);
        return {.Id = AddLocked(dependency, std::move(target), start)};
    }

    void Finish(THandle handle, ECompileDependencyStatus status) override {
        if (handle.Id == 0) {
            return;
        }
        const TInstant end = TInstant::Now();
        std::lock_guard guard(Mutex);
        auto it = std::find_if(Dependencies.begin(), Dependencies.end(), [&](const auto& slot) {
            return slot.Id == handle.Id;
        });
        if (it == Dependencies.end()) {
            return;
        }
        auto& dependency = it->Diagnostic;
        dependency.End = end;
        dependency.Status = status;
    }

private:
    ui64 AddLocked(ECompileDependency dependency, TString target, TInstant start,
            TInstant end = TInstant::Zero(),
            ECompileDependencyStatus status = ECompileDependencyStatus::Unknown) {
        const ui64 id = NextId++;
        if (Dependencies.size() >= MaxDependencies) {
            ++Dropped;
            auto least = Dependencies.end();
            for (auto it = Dependencies.begin(); it != Dependencies.end(); ++it) {
                if (it->Diagnostic.End == TInstant::Zero()) {
                    continue;
                }
                if (least == Dependencies.end()
                        || DependencyRank(it->Diagnostic) < DependencyRank(least->Diagnostic)) {
                    least = it;
                }
            }
            if (least == Dependencies.end()) {
                return 0;
            }
            *least = {id, {dependency, std::move(target), start, end, status}};
            return id;
        }
        Dependencies.push_back({id, {dependency, std::move(target), start, end, status}});
        return id;
    }

public:

    std::shared_ptr<const TCompileDiagnostics> Snapshot(TInstant end) const {
        std::lock_guard guard(Mutex);
        std::vector<TCompileDependencyDiagnostic> dependencies;
        dependencies.reserve(Dependencies.size());
        for (const auto& slot : Dependencies) {
            auto dependency = slot.Diagnostic;
            if (dependency.End == TInstant::Zero()) {
                dependency.End = end;
            }
            dependencies.push_back(std::move(dependency));
        }
        return std::make_shared<const TCompileDiagnostics>(TCompileDiagnostics{
            .Dependencies = std::move(dependencies),
            .Dropped = Dropped,
        });
    }

private:
    static std::tuple<bool, ui64> DependencyRank(const TCompileDependencyDiagnostic& dependency) {
        const bool failed = dependency.Status == ECompileDependencyStatus::Error;
        const ui64 durationUs = dependency.Start != TInstant::Zero() && dependency.End >= dependency.Start
            ? (dependency.End - dependency.Start).MicroSeconds() : 0;
        return std::tuple(failed, durationUs);
    }

    static constexpr size_t MaxDependencies = 64;
    mutable std::mutex Mutex;
    std::vector<TSlot> Dependencies;
    ui64 NextId = 1;
    size_t Dropped = 0;
};

} // namespace NKikimr::NKqp
