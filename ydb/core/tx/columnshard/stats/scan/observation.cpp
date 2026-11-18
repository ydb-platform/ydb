#include "metrics.h"
#include "observation.h"

#include <set>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

std::shared_ptr<const NColumnShardStats::TOperation> MakeDefinition(const NColumnShardStats::TOperation& definition) {
    return std::make_shared<const NColumnShardStats::TOperation>(Definition(definition));
}

NColumnShardStats::TOperation& Unfinished(NColumnShardStats::TOperation& operation) {
    if (!operation.HasUnfinished()) {
        *operation.MutableUnfinished() = MeasurementPrototype(operation);
    }
    return *operation.MutableUnfinished();
}

}   // namespace

TOperationNode::TOperationNode(TLocalStats& owner, TOperationNode* parent, std::shared_ptr<const TStatsUpdate::TNodeRegistration> registration)
    : Owner(&owner)
    , Parent(parent)
    , Registration(std::move(registration))
    , Measurements(MeasurementPrototype(*Registration->Definition))
{
}

void TOperationNode::MarkDirty() {
    if (!Dirty) {
        Owner->DirtyNodes.push_back(this);
        Dirty = true;
    }
}

void TOperationNode::MarkObserved() {
    for (auto* node = this; node; node = node->Parent) {
        if (!node->Observed) {
            node->Observed = true;
            node->MarkDirty();
        }
    }
    MarkDirty();
}

TOperationNode* TOperationNode::AddChild(const NColumnShardStats::TOperation& definition, std::optional<ui64> order) {
    return Owner->AddChild(*this, definition, order);
}

TOperationNode* TOperationNode::FindChild(TStringBuf key) const noexcept {
    for (const auto& child : Children) {
        if (child->GetDefinition().GetKey() == key) {
            return child.get();
        }
    }
    return nullptr;
}

TOperationScope TOperationNode::Measure() {
    return Owner->Measure(this);
}

TOperationScope TOperationNode::Measure(const TCallHandle& parent, EChildWorkAccounting childWork) {
    return Owner->Measure(this, parent, childWork);
}

void TOperationNode::ObserveLoss() noexcept {
    Owner->ObserveLoss();
}

TLocalStats::TLocalStats(
    TStatsUpdate state, std::shared_ptr<const NColumnShardStats::TOperation> rootDefinition, std::function<void(TStatsUpdate&&)> send)
    : State(std::move(state))
    , Send(std::move(send))
    , Root(new TOperationNode(*this, nullptr,
          std::make_shared<const TStatsUpdate::TNodeRegistration>(
              TStatsUpdate::TNodeRegistration{ ++NextNodeId, {}, 0, std::move(rootDefinition) })))
{
}

std::unique_ptr<TLocalStats> TLocalStats::Create(const NColumnShardStats::TScanIdentity& identity,
    const NColumnShardStats::TOperation& rootDefinition, std::function<void(TStatsUpdate&&)> send) {
    TStatsUpdate state;
    state.Producer = std::make_shared<const TStatsUpdate::TProducer>(
        TStatsUpdate::TProducer{ std::make_shared<const NColumnShardStats::TScanIdentity>(identity), {} });
    return std::unique_ptr<TLocalStats>(new TLocalStats(std::move(state), MakeDefinition(rootDefinition), std::move(send)));
}

TLocalStats::~TLocalStats() {
    if (Lifecycle == ELifecycle::Closed) {
        return;
    }
    Lifecycle = ELifecycle::Closing;
    while (!OpenCalls.empty()) {
        const auto call = OpenCalls.begin()->second;
        TOperationScope::Complete(call, false);
    }
    Close();
}

std::unique_ptr<TLocalStats> TLocalStats::ForkWithDefinition(
    std::shared_ptr<const NColumnShardStats::TOperation> rootDefinition, std::function<void(TStatsUpdate&&)> send) {
    if (Lifecycle != ELifecycle::Active) {
        return {};
    }
    TStatsUpdate state;
    state.Producer = std::make_shared<const TStatsUpdate::TProducer>(TStatsUpdate::TProducer{ State.Producer->Identity, State.Producer });
    return std::unique_ptr<TLocalStats>(new TLocalStats(std::move(state), std::move(rootDefinition), send ? std::move(send) : Send));
}

std::unique_ptr<TLocalStats> TLocalStats::Fork(std::function<void(TStatsUpdate&&)> send) {
    return ForkWithDefinition(Root->Registration->Definition, std::move(send));
}

std::unique_ptr<TLocalStats> TLocalStats::Fork(const NColumnShardStats::TOperation& rootDefinition, std::function<void(TStatsUpdate&&)> send) {
    return ForkWithDefinition(MakeDefinition(rootDefinition), std::move(send));
}

TOperationNode* TLocalStats::AddChild(TOperationNode& parent, const NColumnShardStats::TOperation& definition, std::optional<ui64> order) {
    if (Lifecycle != ELifecycle::Active) {
        return nullptr;
    }
    Y_ABORT_UNLESS(parent.Owner == this, "foreign operation parent");
    if (auto* child = parent.FindChild(definition.GetKey())) {
        return child;
    }
    const auto id = ++NextNodeId;
    auto registration = std::make_shared<const TStatsUpdate::TNodeRegistration>(
        TStatsUpdate::TNodeRegistration{ id, parent.Registration, order.value_or(id), MakeDefinition(definition) });
    auto node = std::unique_ptr<TOperationNode>(new TOperationNode(*this, &parent, std::move(registration)));
    auto* result = node.get();
    parent.Children.push_back(std::move(node));
    return result;
}

TOperationNode* TLocalStats::CloneBranch(const TOperationNode& source) {
    if (source.Owner == this) {
        return const_cast<TOperationNode*>(&source);
    }
    const auto clone = [&](const auto& self, const TOperationNode& node) -> TOperationNode* {
        if (!node.Parent) {
            return Root.get();
        }
        auto* parent = self(self, *node.Parent);
        if (auto* child = parent->FindChild(node.GetDefinition().GetKey())) {
            return child;
        }
        const auto id = ++NextNodeId;
        auto registration = std::make_shared<const TStatsUpdate::TNodeRegistration>(
            TStatsUpdate::TNodeRegistration{ id, parent->Registration, node.Registration->Order, node.Registration->Definition });
        auto child = std::unique_ptr<TOperationNode>(new TOperationNode(*this, parent, std::move(registration)));
        auto* result = child.get();
        parent->Children.push_back(std::move(child));
        return result;
    };
    return clone(clone, source);
}

TOperationScope TLocalStats::Measure(TOperationNode* node, const TCallHandle& parent, EChildWorkAccounting childWork) {
    if (!node || Lifecycle != ELifecycle::Active) {
        return {};
    }
    Y_ABORT_UNLESS(node->Owner == this, "foreign operation node");
    auto call = std::make_shared<TOperationScope::TCallRecord>(*this, *node, ++NextCallId, parent.Call, childWork);
    OpenCalls.emplace(call->Id, call);
    node->MarkObserved();
    Publish();
    return TOperationScope(std::move(call));
}

void TLocalStats::ObserveLoss() noexcept {
    State.ObservationLost = true;
}

void TLocalStats::Emit(bool closed) {
    if (Send) {
        ++State.Revision;
        Send(Snapshot(false));
        ClearDirty();
    }
    if (closed) {
        Lifecycle = ELifecycle::Closed;
    }
}

void TLocalStats::Publish() {
    if (Lifecycle != ELifecycle::Closed) {
        Emit(false);
    }
}

void TLocalStats::Close() {
    if (Lifecycle == ELifecycle::Closed) {
        return;
    }
    Lifecycle = ELifecycle::Closing;
    if (OpenCalls.empty()) {
        Emit(true);
    }
}

TOperationScope::TCallRecord::TCallRecord(
    TLocalStats& owner, TOperationNode& node, ui64 id, std::shared_ptr<TCallRecord> parent, EChildWorkAccounting childWork)
    : Owner(&owner)
    , Node(&node)
    , Id(id)
    , Parent(std::move(parent))
    , ChildWork(childWork)
    , Observation(MeasurementPrototype(node.GetDefinition()))
    , Since(NMonotonic::TMonotonic::Now())
{
}

TOperationScope::TOperationScope(std::shared_ptr<TCallRecord> call) noexcept
    : Call(std::move(call))
{
}

TOperationScope& TOperationScope::operator=(TOperationScope&& other) {
    if (this != &other) {
        Complete(Call && !Call->Waiting);
        Call = std::move(other.Call);
    }
    return *this;
}

TOperationScope::~TOperationScope() {
    Complete(Call && !Call->Waiting);
}

TCallHandle TOperationScope::GetHandle() const noexcept {
    return TCallHandle(Call);
}

void TOperationScope::ObserveLoss() noexcept {
    if (GetNode()) {
        Call->Owner->ObserveLoss();
    }
}

void TOperationScope::AccountTime(TCallRecord& call) noexcept {
    const auto now = NMonotonic::TMonotonic::Now();
    (call.Waiting ? call.WaitTime : call.ActiveTime) += now - call.Since;
    call.Since = now;
}

void TOperationScope::Suspend(TCallRecord& call) {
    if (call.Node && !call.Waiting) {
        AccountTime(call);
        call.Waiting = true;
        Publish(call);
    }
}

void TOperationScope::Resume(TCallRecord& call) {
    if (call.Node && call.Waiting) {
        AccountTime(call);
        call.Waiting = false;
        Publish(call);
    }
}

void TOperationScope::Suspend() {
    if (Call) {
        Suspend(*Call);
    }
}

void TOperationScope::Resume() {
    if (Call) {
        Resume(*Call);
    }
}

void TOperationScope::Complete(const std::shared_ptr<TCallRecord>& call, bool observed, bool measuredTime) {
    if (!call || !call->Node) {
        return;
    }
    auto* node = std::exchange(call->Node, nullptr);
    auto* owner = std::exchange(call->Owner, nullptr);
    if (observed && measuredTime) {
        AccountTime(*call);
    } else if (!observed) {
        owner->ObserveLoss();
    }
    auto* statistics = call->Observation.MutableStatistics();
    auto* active = statistics->MutableActiveTimeUs();
    auto* wait = statistics->MutableWaitTimeUs();
    active->SetApplicability(NColumnShardStats::APPLICABLE);
    wait->SetApplicability(NColumnShardStats::APPLICABLE);
    if (observed && measuredTime) {
        active->SetValue(call->ActiveTime.MicroSeconds());
        wait->SetValue(call->WaitTime.MicroSeconds());
    } else {
        active->MutableCoverage()->SetMissingReason(observed ? NColumnShardStats::UNAVAILABLE : NColumnShardStats::OBSERVATION_LOST);
        wait->MutableCoverage()->SetMissingReason(observed ? NColumnShardStats::UNAVAILABLE : NColumnShardStats::OBSERVATION_LOST);
    }
    NormalizeCall(call->Observation);
    if (observed) {
        AddMeasurements(node->Measurements, call->Observation);
        node->Measurements.SetCallsCount(node->Measurements.GetCallsCount() + 1);
        for (const auto& [target, sources] : call->ReadAttributions) {
            for (const auto& source : sources) {
                AddRead(*target->Measurements.MutableStatistics()->MutableDataSources(), source);
            }
            target->MarkObserved();
        }
    }
    call->Observation.Clear();
    call->ReadAttributions.clear();
    node->MarkDirty();
    owner->OpenCalls.erase(call->Id);
    if (owner->Lifecycle == TLocalStats::ELifecycle::Closing && owner->OpenCalls.empty()) {
        owner->Emit(true);
    } else {
        owner->Publish();
    }
}

void TOperationScope::Complete(bool observed, bool measuredTime) {
    const auto call = std::exchange(Call, {});
    Complete(call, observed, measuredTime);
}

void TOperationScope::Finish() {
    Complete(true);
}

void TOperationScope::FinishWithoutTime() {
    Complete(true, false);
}

void TOperationScope::Publish(TCallRecord& call) {
    if (call.Node) {
        call.Node->MarkDirty();
        call.Owner->Publish();
    }
}

void TLocalStats::SetSender(std::function<void(TStatsUpdate&&)> send) {
    Send = std::move(send);
    Publish();
}

void TLocalStats::PublishTo(const std::function<void(TStatsUpdate&&)>& sink) {
    ++State.Revision;
    sink(Snapshot(true));
    ClearDirty();
}

TStatsUpdate TLocalStats::Snapshot(bool full) {
    for (const auto& [_, call] : OpenCalls) {
        if (call->Node) {
            call->Node->MarkDirty();
        }
        for (const auto& [node, _] : call->ReadAttributions) {
            node->MarkDirty();
        }
    }
    std::vector<TOperationNode*> nodes;
    if (full) {
        const auto collect = [&](const auto& self, TOperationNode& node) -> void {
            if (!node.Observed) {
                return;
            }
            nodes.push_back(&node);
            for (const auto& child : node.Children) {
                self(self, *child);
            }
        };
        collect(collect, *Root);
    } else {
        nodes = DirtyNodes;
    }
    TStatsUpdate update = State;
    update.Nodes.reserve(nodes.size());
    std::map<const TOperationNode*, size_t> positions;
    for (auto* node : nodes) {
        if (!node->Observed) {
            continue;
        }
        if (node->Dirty || !node->Revision) {
            ++node->Revision;
        }
        positions.emplace(node, update.Nodes.size());
        update.Nodes.push_back({ node->Registration, node->Revision, node->Measurements });
    }
    for (const auto& [_, call] : OpenCalls) {
        if (!call->Node) {
            continue;
        }
        auto observation = call->Observation;
        const auto elapsed = NMonotonic::TMonotonic::Now() - call->Since;
        auto* statistics = observation.MutableStatistics();
        statistics->MutableActiveTimeUs()->SetValue((call->ActiveTime + (call->Waiting ? TDuration::Zero() : elapsed)).MicroSeconds());
        statistics->MutableWaitTimeUs()->SetValue((call->WaitTime + (call->Waiting ? elapsed : TDuration::Zero())).MicroSeconds());
        NormalizeCall(observation);
        auto& pending = Unfinished(update.Nodes.at(positions.at(call->Node)).Measurements);
        pending.SetCallsCount(pending.GetCallsCount() + 1);
        AddMeasurements(pending, observation);
        for (const auto& [node, sources] : call->ReadAttributions) {
            for (const auto& source : sources) {
                AddRead(*Unfinished(update.Nodes.at(positions.at(node)).Measurements).MutableStatistics()->MutableDataSources(), source);
            }
        }
    }
    return update;
}

void TLocalStats::ClearDirty() noexcept {
    for (auto* node : DirtyNodes) {
        node->Dirty = false;
    }
    DirtyNodes.clear();
}

void TLocalStats::RecordRead(TOperationScope::TCallRecord& call, const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes,
    TConstArrayRef<TOperationNode*> consumers, NColumnShardStats::EMissingReason reason, bool canonical, bool includeParents) {
    NColumnShardStats::TDataSource read = source;
    read.ClearVolumeCoverage();
    read.SetReads(1);
    if (bytes) {
        read.MutableReadBytes()->SetValue(*bytes);
    } else {
        read.MutableReadBytes()->ClearValue();
    }
    auto* coverage = read.MutableVolumeCoverage();
    coverage->SetMeasured(bytes ? 1 : 0);
    coverage->SetApplicable(1);
    coverage->SetMissingReason(bytes                                       ? NColumnShardStats::MISSING_NONE
                               : reason == NColumnShardStats::MISSING_NONE ? NColumnShardStats::NOT_REPORTED
                                                                           : reason);
    if (canonical) {
        AddRead(State.DataSources, read);
    }
    if (!includeParents) {
        call.Node->MarkObserved();
        AddRead(call.ReadAttributions[call.Node], read);
        Publish();
        return;
    }
    std::set<TOperationNode*> targets;
    const auto include = [&](TOperationNode* node) {
        Y_ABORT_UNLESS(!node || node->Owner == this, "foreign read consumer");
        for (; node; node = node->Parent) {
            targets.insert(node);
        }
    };
    include(call.Node);
    for (auto* consumer : consumers) {
        include(consumer);
    }
    for (auto* target : targets) {
        target->MarkObserved();
        AddRead(call.ReadAttributions[target], read);
    }
    Publish();
}

void TOperationScope::RecordRead(const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes,
    TConstArrayRef<TOperationNode*> consumers, NColumnShardStats::EMissingReason reason) {
    if (GetNode()) {
        Call->Owner->RecordRead(*Call, source, bytes, consumers, reason, true, true);
    }
}

void TOperationScope::RecordReadSelf(
    const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes, NColumnShardStats::EMissingReason reason) {
    if (GetNode()) {
        Call->Owner->RecordRead(*Call, source, bytes, {}, reason, true, false);
    }
}

void TOperationScope::AttributeRead(
    const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes, NColumnShardStats::EMissingReason reason) {
    GetHandle().AttributeRead(source, bytes, reason);
}

TCallHandle TCallHandle::GetParent() const noexcept {
    return TCallHandle(Call ? Call->Parent : nullptr);
}

void TCallHandle::Suspend() const {
    if (Call) {
        TOperationScope::Suspend(*Call);
    }
}

void TCallHandle::Resume() const {
    if (Call) {
        TOperationScope::Resume(*Call);
    }
}

void TCallHandle::ResumeForChildWork() const {
    if (Call && Call->ChildWork == EChildWorkAccounting::Include) {
        TOperationScope::Resume(*Call);
    }
}

void TCallHandle::ObserveLoss() const noexcept {
    if (GetNode()) {
        Call->Owner->ObserveLoss();
    }
}

TCallHandle SelectCurrentCall(TCallHandle active, TCallHandle program) noexcept {
    while (active && !active.GetNode()) {
        active = active.GetParent();
    }
    while (program && !program.GetNode()) {
        program = program.GetParent();
    }
    if (!active) {
        return program;
    }
    for (auto call = program; call; call = call.GetParent()) {
        if (call == active) {
            return program;
        }
    }
    return active;
}

TCallStatisticsResume::TCallStatisticsResume(TCallHandle current)
    : Current(std::move(current))
{
    for (auto call = Current; call; call = call.GetParent()) {
        call.ResumeForChildWork();
    }
}

TCallStatisticsResume::~TCallStatisticsResume() {
    Suspend();
}

void TCallStatisticsResume::Suspend() {
    for (auto call = Current; call; call = call.GetParent()) {
        call.Suspend();
    }
    Current = {};
}

bool TCallHandle::AttributeRead(
    const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes, NColumnShardStats::EMissingReason reason) const {
    if (!GetNode()) {
        return false;
    }
    Call->Owner->RecordRead(*Call, source, bytes, {}, reason, false, false);
    return true;
}

}   // namespace NKikimr::NOlap::NStats::NScan
