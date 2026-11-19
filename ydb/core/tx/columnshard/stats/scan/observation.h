#pragma once

#include "collector.h"

#include <google/protobuf/descriptor.h>
#include <library/cpp/time_provider/monotonic.h>
#include <util/generic/array_ref.h>
#include <util/generic/strbuf.h>
#include <util/system/yassert.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace NKikimr::NOlap::NStats::NScan {

class TLocalStats;
class TOperationScope;
class TCallHandle;

enum class EChildWorkAccounting {
    Include,
    Exclude,
};

class TOperationNode {
    TLocalStats* const Owner;
    TOperationNode* const Parent;
    const std::shared_ptr<const TStatsUpdate::TNodeRegistration> Registration;
    std::vector<std::unique_ptr<TOperationNode>> Children;
    NColumnShardStats::TOperation Measurements;
    ui64 Revision = 0;
    bool Observed = false;
    bool Dirty = false;

    friend class TLocalStats;
    friend class TOperationScope;
    TOperationNode(TLocalStats& owner, TOperationNode* parent, std::shared_ptr<const TStatsUpdate::TNodeRegistration> registration);
    void MarkObserved();
    void MarkDirty();

public:
    TOperationNode(const TOperationNode&) = delete;
    TOperationNode& operator=(const TOperationNode&) = delete;

    const NColumnShardStats::TOperation& GetDefinition() const noexcept {
        return *Registration->Definition;
    }

    TOperationNode* AddChild(const NColumnShardStats::TOperation& definition, std::optional<ui64> order = {});
    TOperationNode* FindChild(TStringBuf key) const noexcept;

    template <class TPredicate>
    TOperationNode* FindChildIf(TPredicate&& predicate) const noexcept(noexcept(predicate(GetDefinition()))) {
        for (const auto& child : Children) {
            if (predicate(child->GetDefinition())) {
                return child.get();
            }
        }
        return nullptr;
    }

    template <class TPredicate>
    TOperationNode* FindNodeIf(TPredicate&& predicate) const noexcept(noexcept(predicate(GetDefinition()))) {
        if (predicate(GetDefinition())) {
            return const_cast<TOperationNode*>(this);
        }
        for (const auto& child : Children) {
            if (auto* node = child->FindNodeIf(predicate)) {
                return node;
            }
        }
        return nullptr;
    }

    template <class TPredicate>
    TOperationNode* FindLeafIf(TPredicate&& predicate) const noexcept(noexcept(predicate(GetDefinition()))) {
        if (Children.empty()) {
            return predicate(GetDefinition()) ? const_cast<TOperationNode*>(this) : nullptr;
        }
        for (const auto& child : Children) {
            if (auto* node = child->FindLeafIf(predicate)) {
                return node;
            }
        }
        return nullptr;
    }

    TOperationScope Measure();
    TOperationScope Measure(const TCallHandle& parent, EChildWorkAccounting childWork = EChildWorkAccounting::Include);
    void ObserveLoss() noexcept;
};

class TMeasurementsView {
    NColumnShardStats::TOperation& Observation;

    template <class TObject>
    TObject& Message() {
        if constexpr (std::is_same_v<TObject, NColumnShardStats::TStatisticsDescription>) {
            return *Observation.MutableStatistics();
        } else {
            const auto* field = Observation.GetDescriptor()->FindFieldByNumber(Observation.details_case());
            Y_ABORT_UNLESS(field && field->message_type() == TObject::descriptor(), "metric for another operation");
            return static_cast<TObject&>(*Observation.GetReflection()->MutableMessage(&Observation, field));
        }
    }

    template <class TObject, class TResult>
    static TResult& Follow(TObject& object, TResult* (TObject::*accessor)()) {
        static_assert(
            std::is_same_v<TResult, NColumnShardStats::TNumericObservation> || std::is_same_v<TResult, NColumnShardStats::TBooleanObservation>);
        return *(object.*accessor)();
    }

    template <class TObject, class TResult, class... TAccessors>
    static decltype(auto) Follow(TObject& object, TResult* (TObject::*accessor)(), TAccessors... remaining) {
        return Follow(*(object.*accessor)(), remaining...);
    }

    friend class TOperationScope;
    friend class TCallHandle;

    explicit TMeasurementsView(NColumnShardStats::TOperation& observation)
        : Observation(observation)
    {
    }

public:
    template <class TObject, class TResult, class... TAccessors>
    decltype(auto) Metric(TResult* (TObject::*accessor)(), TAccessors... remaining) {
        return Follow(Message<TObject>(), accessor, remaining...);
    }
};

class TOperationScope {
    struct TCallRecord {
        TLocalStats* Owner;
        TOperationNode* Node;
        const ui64 Id;
        const std::shared_ptr<TCallRecord> Parent;
        const EChildWorkAccounting ChildWork;
        NColumnShardStats::TOperation Observation;
        std::map<TOperationNode*, google::protobuf::RepeatedPtrField<NColumnShardStats::TDataSource>> ReadAttributions;
        NMonotonic::TMonotonic Since;
        TDuration ActiveTime{};
        TDuration WaitTime{};
        bool Waiting = false;

        TCallRecord(TLocalStats& owner, TOperationNode& node, ui64 id, std::shared_ptr<TCallRecord> parent, EChildWorkAccounting childWork);
    };

    std::shared_ptr<TCallRecord> Call;

    friend class TLocalStats;
    friend class TCallHandle;
    explicit TOperationScope(std::shared_ptr<TCallRecord> call) noexcept;
    static void AccountTime(TCallRecord& call) noexcept;
    static void Complete(const std::shared_ptr<TCallRecord>& call, bool observed, bool measuredTime = true);
    static void Suspend(TCallRecord& call);
    static void Resume(TCallRecord& call);
    static void Publish(TCallRecord& call);
    void Complete(bool observed, bool measuredTime = true);

public:
    TOperationScope() noexcept = default;
    TOperationScope(const TOperationScope&) = delete;
    TOperationScope& operator=(const TOperationScope&) = delete;
    TOperationScope(TOperationScope&& other) noexcept = default;
    TOperationScope& operator=(TOperationScope&& other);
    ~TOperationScope();

    TOperationNode* GetNode() const noexcept {
        return Call ? Call->Node : nullptr;
    }

    explicit operator bool() const noexcept {
        return GetNode() != nullptr;
    }

    TCallHandle GetHandle() const noexcept;

    template <class TFill>
    void Observe(TFill&& fill) {
        if (GetNode()) {
            TMeasurementsView measurements(Call->Observation);
            std::forward<TFill>(fill)(measurements);
            Publish(*Call);
        }
    }

    void ObserveLoss() noexcept;
    void Suspend();
    void Resume();
    void Finish();
    void FinishWithoutTime();

    void RecordRead(const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes, TConstArrayRef<TOperationNode*> consumers = {},
        NColumnShardStats::EMissingReason reason = NColumnShardStats::UNAVAILABLE);
    void RecordReadSelf(const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes,
        NColumnShardStats::EMissingReason reason = NColumnShardStats::UNAVAILABLE);
    void AttributeRead(const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes,
        NColumnShardStats::EMissingReason reason = NColumnShardStats::UNAVAILABLE);
};

class TCallHandle {
    std::shared_ptr<TOperationScope::TCallRecord> Call;

    friend class TOperationScope;
    friend class TLocalStats;

    explicit TCallHandle(std::shared_ptr<TOperationScope::TCallRecord> call) noexcept
        : Call(std::move(call))
    {
    }

public:
    TCallHandle() noexcept = default;

    explicit operator bool() const noexcept {
        return bool(Call);
    }

    bool operator==(const TCallHandle& other) const noexcept {
        return Call == other.Call;
    }

    TOperationNode* GetNode() const noexcept {
        return Call ? Call->Node : nullptr;
    }

    TCallHandle GetParent() const noexcept;

    template <class TFill>
    void Observe(TFill&& fill) const {
        if (GetNode()) {
            TMeasurementsView measurements(Call->Observation);
            std::forward<TFill>(fill)(measurements);
            TOperationScope::Publish(*Call);
        }
    }

    void Suspend() const;
    void Resume() const;
    void ResumeForChildWork() const;
    void ObserveLoss() const noexcept;
    bool AttributeRead(const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes,
        NColumnShardStats::EMissingReason reason = NColumnShardStats::UNAVAILABLE) const;
};

TCallHandle SelectCurrentCall(TCallHandle active, TCallHandle program) noexcept;

class TCallStatisticsResume {
    TCallHandle Current;

public:
    TCallStatisticsResume(const TCallStatisticsResume&) = delete;
    TCallStatisticsResume& operator=(const TCallStatisticsResume&) = delete;

    explicit TCallStatisticsResume(TCallHandle current);
    ~TCallStatisticsResume();

    void Suspend();
};

class TLocalStats {
    TStatsUpdate State;
    std::function<void(TStatsUpdate&&)> Send;
    ui64 NextNodeId = 0;
    const std::unique_ptr<TOperationNode> Root;
    std::vector<TOperationNode*> DirtyNodes;
    std::map<ui64, std::shared_ptr<TOperationScope::TCallRecord>> OpenCalls;
    ui64 NextCallId = 0;
    enum class ELifecycle {
        Active,
        Closing,
        Closed,
    };
    ELifecycle Lifecycle = ELifecycle::Active;

    friend class TOperationNode;
    friend class TOperationScope;
    friend class TCallHandle;
    void RecordRead(TOperationScope::TCallRecord& call, const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes,
        TConstArrayRef<TOperationNode*> consumers, NColumnShardStats::EMissingReason reason, bool canonical, bool includeParents);
    void Emit(bool closed);
    void ClearDirty() noexcept;
    TStatsUpdate Snapshot(bool full);
    std::unique_ptr<TLocalStats> ForkWithDefinition(
        std::shared_ptr<const NColumnShardStats::TOperation> rootDefinition, std::function<void(TStatsUpdate&&)> send);

    TLocalStats(
        TStatsUpdate state, std::shared_ptr<const NColumnShardStats::TOperation> rootDefinition, std::function<void(TStatsUpdate&&)> send);

public:
    static std::unique_ptr<TLocalStats> Create(const NColumnShardStats::TScanIdentity& identity,
        const NColumnShardStats::TOperation& rootDefinition, std::function<void(TStatsUpdate&&)> send = {});

    TLocalStats(const TLocalStats&) = delete;
    TLocalStats& operator=(const TLocalStats&) = delete;
    ~TLocalStats();

    TOperationNode& GetRoot() noexcept {
        return *Root;
    }

    const TOperationNode& GetRoot() const noexcept {
        return *Root;
    }

    const NColumnShardStats::TScanIdentity& GetIdentity() const noexcept {
        return *State.Producer->Identity;
    }

    TOperationNode* AddChild(TOperationNode& parent, const NColumnShardStats::TOperation& definition, std::optional<ui64> order = {});
    TOperationNode* CloneBranch(const TOperationNode& source);
    std::unique_ptr<TLocalStats> Fork(std::function<void(TStatsUpdate&&)> send = {});
    std::unique_ptr<TLocalStats> Fork(const NColumnShardStats::TOperation& rootDefinition, std::function<void(TStatsUpdate&&)> send = {});
    void SetSender(std::function<void(TStatsUpdate&&)> send);
    void PublishTo(const std::function<void(TStatsUpdate&&)>& sink);
    TOperationScope Measure(
        TOperationNode* node, const TCallHandle& parent = {}, EChildWorkAccounting childWork = EChildWorkAccounting::Include);
    void ObserveLoss() noexcept;
    void Publish();
    void Close();
};

}   // namespace NKikimr::NOlap::NStats::NScan
