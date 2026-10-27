#include "compute.h"
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <map>
#include <set>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <atomic>
#include <util/system/datetime.h>

namespace NKikimr::NMiniKQL::NAsync {

struct TExecutionServices::TImpl {
    struct TWakeupGate {
        const TWakeup Enqueue;
        std::atomic<bool> Pending = false;
        std::atomic<bool> Closed = false;
        void Notify(ui64 generation) {
            if (!Closed.load() && !Pending.exchange(true)) {
                Enqueue(generation);
            }
        }
    };
    std::shared_ptr<TWakeupGate> Gate;
    TScheduler Context;
    TExecutionSettings Settings;
    std::shared_ptr<TMemoryReservation> AllocatorMemory;
    ui64 CpuStart = 0;
    ui64 CpuSample = 0;
    TComputeStats Stats;
    bool CpuExhausted = false;
    bool Running = false;
    THashSet<TString> Providers;
    THashSet<ui64> Live;
    THashMap<ui64, TResult> Ready;
    const size_t StepBudget;
    size_t Steps = 0;
    ui64 Turn = 0;
    bool Closed = false;
    THashMap<TString, std::shared_ptr<const IReplayCodec>> Codecs;
    std::map<ui32, std::function<bool()>> Participants;
    std::set<ui32> ExpectedParticipants;
    ui64 CheckpointId = 0;
    ui64 LastCheckpointId = 0;
    ui64 SnapshotRevision = 0;
    ui64 Produced = 0;
    ui64 Acknowledged = 0;
    bool InputCut = false;
    bool Saving = false;
    bool Saved = false;
    bool Durable = false;
    bool Restoring = false;
    bool Parsed = false;
    std::shared_ptr<TMemoryReservation> SnapshotMemory;
    bool Drained() const {
        for (const auto& [id, drained] : Participants) {
            if (!drained()) {
                return false;
            }
        }
        return Live.empty();
    }

    TImpl(TWakeup enqueue, size_t budget, TLimits limits, TExecutionSettings settings)
        : Gate(std::make_shared<TWakeupGate>(std::move(enqueue)))
        , Context([gate = Gate](ui64 generation) { gate->Notify(generation); }, limits, settings.Scheduler)
        , Settings(std::move(settings))
        , StepBudget(budget)
    {
        Y_ENSURE(budget && Gate->Enqueue, "Async execution requires a budget and an enqueue callback");
        if (!Settings.CpuClock) {
            Settings.CpuClock = [] { return ThreadCPUTime(); };
        }
        AllocatorMemory = Context.GetQuota()->TryReserve(0);
    }

    void Drain() {
        for (auto& result : Context.TakeReady()) {
            if (Live.contains(result.Id.Sequence)) {
                Ready.emplace(result.Id.Sequence, std::move(result));
            }
        }
    }
};

TExecutionServices::TExecutionServices(TWakeup enqueue, size_t stepBudget, TLimits limits, TExecutionSettings settings)
    : Impl_(std::make_unique<TImpl>(std::move(enqueue), stepBudget, limits, std::move(settings)))
{
}
TExecutionServices::~TExecutionServices() {
    Shutdown();
}
void TExecutionServices::RegisterProvider(TString name, std::shared_ptr<IProvider> provider) {
    Impl_->Context.RegisterProvider(name, std::move(provider));
    Impl_->Providers.insert(std::move(name));
}
bool TExecutionServices::HasProvider(TStringBuf name) const {
    return Impl_->Providers.contains(name);
}
void TExecutionServices::BeginTurn() {
    EnsureCanExecute();
    Y_ENSURE(!Impl_->Closed, "Async execution is closed");
    // Clear BEFORE processing. A completion racing Drain, or a budget rerun
    // requested during this turn, must be able to post the next event.
    Impl_->Gate->Pending.store(false);
    Impl_->Running = true;
    Impl_->CpuStart = Impl_->Settings.CpuClock();
    Impl_->CpuSample = Impl_->CpuStart;
    Impl_->CpuExhausted = false;
    ++Impl_->Stats.Turns;
    Impl_->Steps = Impl_->StepBudget;
    ++Impl_->Turn;
    Impl_->Context.ExpireDeadlines();
    Impl_->Drain();
    Flush();
}
void TExecutionServices::EndTurn() {
    if (!Impl_->Running) {
        return;
    }
    Impl_->Stats.CpuTimeUs += Impl_->Settings.CpuClock() - Impl_->CpuSample;
    Impl_->Running = false;
}
bool TExecutionServices::TryStep() {
    Y_ENSURE(!Impl_->Closed && Impl_->Turn, "Async execution requires BeginTurn");
    const auto now = Impl_->Settings.CpuClock();
    Impl_->Stats.CpuTimeUs += now - Impl_->CpuSample;
    Impl_->CpuSample = now;
    // Permit one cooperative step even if owner bookkeeping exhausted the time
    // budget. Repeated zero-step turns must never starve runnable work.
    if (Impl_->Steps != Impl_->StepBudget && Impl_->Settings.CpuBudgetUs && now - Impl_->CpuStart >= Impl_->Settings.CpuBudgetUs) {
        if (!Impl_->CpuExhausted) {
            ++Impl_->Stats.CpuBudgetYields;
        }
        Impl_->CpuExhausted = true;
        return false;
    }
    if (!Impl_->Steps) {
        return false;
    }
    --Impl_->Steps;
    ++Impl_->Stats.Steps;
    return true;
}
void TExecutionServices::RequestRerun() {
    Impl_->Gate->Notify(Impl_->Context.GetGeneration());
}
void TExecutionServices::Flush() {
    if (Impl_->Context.Flush([this] { return TryStep(); })) {
        RequestRerun();
    }
}
bool TExecutionServices::CanWaitForMemory() const {
    return Impl_->Settings.Scheduler.Quota || Impl_->Context.GetStats().TrackedOperations || !Impl_->Live.empty();
}
std::shared_ptr<TMemoryReservation> TExecutionServices::TryReserveFrame(ui64 frameBytes) {
    const auto quota = Impl_->Context.GetQuota();
    Y_ENSURE(frameBytes <= quota->GetLimit() && Impl_->AllocatorMemory->GetBytes() <= quota->GetLimit() - frameBytes, "Async frame exceeds memory limit");
    const auto generation = Impl_->Context.GetGeneration();
    return quota->TryReserve(frameBytes, generation, [gate = Impl_->Gate, generation] { gate->Notify(generation); });
}
void TExecutionServices::AccountAllocatorBytes(ui64 bytes) {
    Y_ENSURE(Impl_->AllocatorMemory->Resize(bytes), "Async graph exceeds memory limit");
}
std::optional<TTimePoint> TExecutionServices::NextDeadline() const {
    return Impl_->Context.NextDeadline();
}
TSchedulerStats TExecutionServices::GetSchedulerStats() const {
    return Impl_->Context.GetSchedulerStats();
}
TComputeStats TExecutionServices::GetComputeStats() const {
    return Impl_->Stats;
}
ui64 TExecutionServices::GetTurn() const {
    return Impl_->Turn;
}
TSubmission TExecutionServices::Submit(TStringBuf provider, TRequest request) {
    EnsureCanExecute();
    auto submission = Impl_->Context.Submit(provider, std::move(request));
    if (submission.Status == EAdmission::Accepted) {
        Impl_->Live.insert(submission.Id.Sequence);
    }
    return submission;
}
bool TExecutionServices::IsReady(TOperationId id) const {
    return id.Generation == Impl_->Context.GetGeneration() && Impl_->Ready.contains(id.Sequence);
}
std::optional<TResult> TExecutionServices::TakeResult(TOperationId id) {
    if (!IsReady(id)) {
        return {};
    }
    auto result = std::move(Impl_->Ready.at(id.Sequence));
    Impl_->Ready.erase(id.Sequence);
    Impl_->Live.erase(id.Sequence);
    return result;
}
void TExecutionServices::CancelAndForget(TOperationId id) {
    if (id.Generation != Impl_->Context.GetGeneration()) {
        return;
    }
    Impl_->Context.Cancel(id);
    Impl_->Live.erase(id.Sequence);
    Impl_->Ready.erase(id.Sequence);
    Impl_->Drain();
}
void TExecutionServices::Shutdown() {
    if (Impl_->Closed) {
        return;
    }
    Impl_->Closed = true;
    Impl_->Gate->Closed.store(true);
    Impl_->Context.Shutdown();
    Impl_->SnapshotMemory.reset();
    Impl_->Live.clear();
    Impl_->Ready.clear();
    Impl_->Drain();
}
TStats TExecutionServices::GetStats() const {
    return Impl_->Context.GetStats();
}

void TExecutionServices::RegisterReplayCodec(TString provider, std::shared_ptr<const IReplayCodec> codec) {
    Y_ENSURE(!Impl_->Turn && codec && !codec->Contract().empty(), "Invalid replay codec registration");
    Y_ENSURE(Impl_->Codecs.emplace(std::move(provider), std::move(codec)).second, "Duplicate replay codec");
}
const IReplayCodec& TExecutionServices::GetReplayCodec(TStringBuf provider) const {
    auto it = Impl_->Codecs.find(provider);
    Y_ENSURE(it != Impl_->Codecs.end(), "Replay requires an explicit provider codec");
    return *it->second;
}
ECheckpointMode TExecutionServices::GetCheckpointMode() const {
    return Impl_->Settings.Checkpoint.Mode;
}
ui64 TExecutionServices::GetGeneration() const {
    return Impl_->Context.GetGeneration();
}
const TResult* TExecutionServices::PeekResult(TOperationId id) const {
    return IsReady(id) ? &Impl_->Ready.at(id.Sequence) : nullptr;
}
void TExecutionServices::RegisterParticipant(ui32 id, std::function<bool()> drained) {
    Y_ENSURE(!Impl_->Saving && Impl_->Participants.emplace(id, std::move(drained)).second, "Duplicate checkpoint participant");
}
void TExecutionServices::UnregisterParticipant(ui32 id) {
    Impl_->Participants.erase(id);
}
void TExecutionServices::BeginCheckpoint(ui64 id) {
    EnsureCanExecute();
    Y_ENSURE(GetCheckpointMode() != ECheckpointMode::Disabled && !Impl_->Settings.Checkpoint.PlanFingerprint.empty(), "Async checkpoint is not configured");
    Y_ENSURE(id > Impl_->LastCheckpointId && !Impl_->CheckpointId, "Invalid checkpoint boundary");
    Impl_->LastCheckpointId = Impl_->CheckpointId = id;
    Impl_->InputCut = Impl_->Saved = Impl_->Durable = false;
}
void TExecutionServices::EstablishInputCut(ui64 id) {
    Y_ENSURE(id && id == Impl_->CheckpointId && !Impl_->Restoring, "Invalid input cut");
    Impl_->InputCut = true;
}
bool TExecutionServices::IsInputPaused() const {
    return Impl_->CheckpointId || Impl_->Restoring;
}
ui64 TExecutionServices::OutputProduced() {
    CheckpointProgress();
    return ++Impl_->Produced;
}
void TExecutionServices::OutputAcknowledged(ui64 generation, ui64 sequence) {
    if (generation != GetGeneration()) {
        return;
    }
    Y_ENSURE(sequence == Impl_->Acknowledged + 1 && sequence <= Impl_->Produced, "Invalid downstream acknowledgement");
    Impl_->Acknowledged = sequence;
}
bool TExecutionServices::CanSaveCheckpoint() const {
    return !Impl_->Participants.empty() && !Impl_->Closed && !Impl_->Restoring && !Impl_->Saving && !Impl_->Saved && Impl_->CheckpointId && Impl_->InputCut &&
           Impl_->Produced == Impl_->Acknowledged &&
           (GetCheckpointMode() == ECheckpointMode::Replay || Impl_->Drained());
}
TSnapshotId TExecutionServices::GetSnapshotId() const {
    Y_ENSURE(Impl_->Saved, "Checkpoint snapshot is not saved");
    return {GetGeneration(), Impl_->CheckpointId, Impl_->SnapshotRevision};
}
bool TExecutionServices::SnapshotStored(TSnapshotId id) {
    if (Impl_->Closed || !Impl_->Saved || id != GetSnapshotId()) {
        return false;
    }
    // Caller has transferred snapshot bytes to bounded checkpoint storage.
    Impl_->SnapshotMemory.reset();
    Impl_->Durable = true;
    return true;
}
bool TExecutionServices::CanForwardWatermark() const {
    return !Impl_->Restoring && !Impl_->Closed && Impl_->Produced == Impl_->Acknowledged && Impl_->Drained();
}
bool TExecutionServices::CanForwardBoundary() const {
    return Impl_->CheckpointId && Impl_->Saved && Impl_->Durable && CanForwardWatermark();
}
void TExecutionServices::ReleaseBoundary() {
    Y_ENSURE(CanForwardBoundary(), "Checkpoint cannot overtake pending output");
    Impl_->CheckpointId = 0;
    RequestRerun();
}
void TExecutionServices::EnsureCanExecute() const {
    Y_ENSURE(!Impl_->Restoring && !Impl_->Saving && !Impl_->Closed && !(Impl_->Saved && Impl_->SnapshotMemory),
             "Async execution is suspended for restore/save or closed");
}
void TExecutionServices::CheckpointProgress() {
    EnsureCanExecute();
    // A replay snapshot is a recovery cut, not permission to attach later output
    // to an earlier barrier. Without an engine channel-cut codec, recapture a
    // drained image before forwarding the barrier (conservative Drain fallback).
    Impl_->Saved = Impl_->Durable = false;
}
TString TExecutionServices::BeforeSave() {
    Y_ENSURE(CanSaveCheckpoint(), "Async checkpoint requires a consistent input/output cut and ready participants");
    Impl_->Saving = true;
    ++Impl_->SnapshotRevision;
    const auto bytes = Impl_->Settings.Checkpoint.MaxSnapshotBytes;
    Y_ENSURE(bytes && bytes <= Impl_->Context.GetQuota()->GetLimit() / 3, "Snapshot memory limit");
    Impl_->SnapshotMemory = Impl_->Context.GetQuota()->TryReserve(3 * bytes);
    Y_ENSURE(Impl_->SnapshotMemory, "Snapshot memory limit");
    TString header;
    WriteString(header, "DQ-ASYNC");
    WriteUi32(header, 1);
    WriteString(header, Impl_->Settings.Checkpoint.PlanFingerprint);
    WriteUi32(header, static_cast<ui32>(GetCheckpointMode()));
    WriteUi64(header, Impl_->CheckpointId);
    WriteUi32(header, Impl_->Participants.size());
    for (const auto& [id, _] : Impl_->Participants) {
        WriteUi32(header, id);
    }
    return header;
}
void TExecutionServices::CheckSnapshotSize(ui64 bytes) const {
    Y_ENSURE(bytes <= Impl_->Settings.Checkpoint.MaxSnapshotBytes, "Async snapshot exceeds size limit");
}
void TExecutionServices::AfterSave() {
    Impl_->Saving = false;
    Impl_->Saved = true;
}
void TExecutionServices::AbortSave() noexcept {
    if (!Impl_->Saving) {
        return;
    }
    Impl_->Saving = false;
    Impl_->SnapshotMemory.reset();
}
void TExecutionServices::BeforeLoad(TStringBuf header, ui64 bytes) {
    Y_ENSURE(!Impl_->Turn && Impl_->Participants.empty() && !Impl_->Restoring && !Impl_->Closed && !Impl_->CheckpointId,
             "Restore requires a fresh execution generation before materialization");
    // Poison execution even on a malformed snapshot; discard this graph on error.
    Impl_->Restoring = true;
    CheckSnapshotSize(bytes);
    auto encoded = ReadString(header);
    header = TStringBuf(encoded.data(), encoded.size());
    Y_ENSURE(bytes <= Impl_->Context.GetQuota()->GetLimit() / 3, "Restore memory limit");
    Impl_->SnapshotMemory = Impl_->Context.GetQuota()->TryReserve(3 * bytes);
    Y_ENSURE(Impl_->SnapshotMemory, "Restore memory limit");
    Y_ENSURE(GetCheckpointMode() != ECheckpointMode::Disabled && !Impl_->Settings.Checkpoint.PlanFingerprint.empty(), "Async checkpoint is not configured");
    Y_ENSURE(ReadString(header) == "DQ-ASYNC" && ReadUi32(header) == 1, "Incompatible async checkpoint version");
    Y_ENSURE(ReadString(header) == std::string_view(Impl_->Settings.Checkpoint.PlanFingerprint), "Incompatible async checkpoint plan");
    Y_ENSURE(ReadUi32(header) == static_cast<ui32>(GetCheckpointMode()), "Incompatible async checkpoint mode");
    Impl_->LastCheckpointId = Impl_->CheckpointId = ReadUi64(header);
    Y_ENSURE(Impl_->CheckpointId, "Invalid checkpoint id");
    const auto count = ReadUi32(header);
    Y_ENSURE(count <= header.size(), "Invalid checkpoint participant count");
    for (ui32 i = 0; i < count; ++i) {
        Y_ENSURE(Impl_->ExpectedParticipants.insert(ReadUi32(header)).second, "Duplicate saved participant");
    }
    Y_ENSURE(header.empty(), "Trailing async checkpoint bytes");
}
void TExecutionServices::AfterLoad() {
    Impl_->Parsed = true;
}
void TExecutionServices::CompleteRestore() {
    Y_ENSURE(Impl_->Restoring && Impl_->Parsed, "Graph state has not been restored");
    std::set<ui32> actual;
    for (const auto& [id, _] : Impl_->Participants) {
        actual.insert(id);
    }
    Y_ENSURE(actual == Impl_->ExpectedParticipants, "Checkpoint participants do not match materialized graph");
    Impl_->SnapshotMemory.reset();
    Impl_->Restoring = false;
    Impl_->InputCut = Impl_->Saved = Impl_->Durable = true;
    RequestRerun();
}

} // namespace NKikimr::NMiniKQL::NAsync
