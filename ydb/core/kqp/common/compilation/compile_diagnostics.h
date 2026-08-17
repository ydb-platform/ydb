#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <memory>
#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>
#include <optional>
#include <tuple>
#include <vector>

namespace NKikimr::NKqp {

enum class ECompileDependency {
    SchemeCache,
    StatisticsService,
    Count,
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
        ECompileDependency Dependency = ECompileDependency::SchemeCache;
        TString Target;
        bool StartedBeforeEnable = false;
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

    struct TEnabledState {
        std::vector<TSlot> Dependencies;
        size_t Dropped = 0;
    };

public:
    explicit TCompileDiagnosticsCollector(bool enabled = false)
        : Enabled(enabled)
        , EnabledAt(enabled ? TInstant::Now() : TInstant::Zero())
    {
        if (enabled) {
            State = std::make_unique<TEnabledState>();
        }
    }

    void Enable() {
        if (IsEnabled()) {
            return;
        }
        std::lock_guard guard(Mutex);
        if (Enabled.load(std::memory_order_relaxed)) {
            return;
        }
        EnabledAt = TInstant::Now();
        State = std::make_unique<TEnabledState>();
        Enabled.store(true, std::memory_order_release);
    }

    bool IsEnabled() const {
        return Enabled.load(std::memory_order_acquire);
    }

    THandle Begin(ECompileDependency dependency, TString target) override {
        if (!IsEnabled()) {
            const size_t dependencyIndex = ToIndex(dependency);
            ActiveBeforeEnable[dependencyIndex].fetch_add(1, std::memory_order_relaxed);
            if (!IsEnabled()) {
                return {
                    .Dependency = dependency,
                    .Target = std::move(target),
                    .StartedBeforeEnable = true,
                };
            }
            ActiveBeforeEnable[dependencyIndex].fetch_sub(1, std::memory_order_relaxed);
        }

        const TInstant start = TInstant::Now();
        std::lock_guard guard(Mutex);
        return {
            .Id = AddLocked(dependency, target, start),
            .Dependency = dependency,
        };
    }

    void Finish(THandle handle, ECompileDependencyStatus status) override {
        if (handle.StartedBeforeEnable) {
            if (!IsEnabled()) {
                ActiveBeforeEnable[ToIndex(handle.Dependency)].fetch_sub(1, std::memory_order_relaxed);
                return;
            }
            const TInstant end = TInstant::Now();
            std::lock_guard guard(Mutex);
            ActiveBeforeEnable[ToIndex(handle.Dependency)].fetch_sub(1, std::memory_order_relaxed);
            AddLocked(handle.Dependency, handle.Target, EnabledAt, end, status);
            return;
        }

        if (handle.Id == 0 || !IsEnabled()) {
            return;
        }
        const TInstant end = TInstant::Now();
        std::lock_guard guard(Mutex);
        auto it = std::find_if(State->Dependencies.begin(), State->Dependencies.end(), [&](const auto& slot) {
            return slot.Id == handle.Id;
        });
        if (it == State->Dependencies.end()) {
            return;
        }
        auto& dependency = it->Diagnostic;
        dependency.End = end;
        dependency.Status = status;
    }

private:
    ui64 AddLocked(ECompileDependency dependency, const TString& target, TInstant start,
            TInstant end = TInstant::Zero(),
            ECompileDependencyStatus status = ECompileDependencyStatus::Unknown) {
        const ui64 id = NextId++;
        if (State->Dependencies.size() >= MaxDependencies) {
            ++State->Dropped;
            auto least = State->Dependencies.end();
            for (auto it = State->Dependencies.begin(); it != State->Dependencies.end(); ++it) {
                if (it->Diagnostic.End == TInstant::Zero()) {
                    continue;
                }
                if (least == State->Dependencies.end()
                        || DependencyRank(it->Diagnostic) < DependencyRank(least->Diagnostic)) {
                    least = it;
                }
            }
            if (least == State->Dependencies.end()) {
                return 0;
            }
            *least = {id, {dependency, target, start, end, status}};
            return id;
        }
        State->Dependencies.push_back({id, {dependency, target, start, end, status}});
        return id;
    }

public:

    std::shared_ptr<const TCompileDiagnostics> Snapshot(TInstant end) const {
        std::lock_guard guard(Mutex);
        std::vector<TCompileDependencyDiagnostic> dependencies;
        dependencies.reserve(State->Dependencies.size() + ActiveBeforeEnable.size());
        for (const auto& slot : State->Dependencies) {
            dependencies.push_back(slot.Diagnostic);
        }

        size_t dropped = State->Dropped;
        for (size_t i = 0; i < ActiveBeforeEnable.size(); ++i) {
            const size_t active = ActiveBeforeEnable[i].load(std::memory_order_relaxed);
            if (!active) {
                continue;
            }
            // Exact targets live in request handles; retaining them globally before sampling
            // would put allocations and synchronization back on every compilation.
            dependencies.push_back({
                .Dependency = FromIndex(i),
                .Start = EnabledAt,
                .Status = ECompileDependencyStatus::Unknown,
            });
            dropped += active - 1;
        }
        if (dependencies.size() > MaxDependencies) {
            const size_t removed = dependencies.size() - MaxDependencies;
            std::nth_element(dependencies.begin(), dependencies.begin() + MaxDependencies,
                dependencies.end(), [](const auto& lhs, const auto& rhs) {
                    return DependencyRank(lhs) > DependencyRank(rhs);
                });
            dependencies.resize(MaxDependencies);
            dropped += removed;
        }
        for (auto& dependency : dependencies) {
            if (dependency.End == TInstant::Zero()) {
                dependency.End = end;
            }
        }
        return std::make_shared<const TCompileDiagnostics>(TCompileDiagnostics{
            .Dependencies = std::move(dependencies),
            .Dropped = dropped,
        });
    }

private:
    std::atomic<bool> Enabled = false;
    TInstant EnabledAt;

    static std::tuple<bool, bool, ui64> DependencyRank(const TCompileDependencyDiagnostic& dependency) {
        const bool pending = dependency.End == TInstant::Zero();
        const bool failed = dependency.Status == ECompileDependencyStatus::Error;
        const ui64 durationUs = dependency.Start != TInstant::Zero() && dependency.End >= dependency.Start
            ? (dependency.End - dependency.Start).MicroSeconds() : 0;
        return std::tuple(pending, failed, durationUs);
    }

    static constexpr size_t ToIndex(ECompileDependency dependency) {
        return static_cast<size_t>(dependency);
    }

    static constexpr ECompileDependency FromIndex(size_t index) {
        return static_cast<ECompileDependency>(index);
    }

    static constexpr size_t MaxDependencies = 64;
    mutable std::mutex Mutex;
    std::unique_ptr<TEnabledState> State;
    std::array<std::atomic<size_t>, static_cast<size_t>(ECompileDependency::Count)> ActiveBeforeEnable = {};
    ui64 NextId = 1;
};

} // namespace NKikimr::NKqp
