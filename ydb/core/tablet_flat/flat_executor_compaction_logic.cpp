#include "flat_executor_compaction_logic.h"
#include "flat_exec_broker.h"
#include "flat_dbase_scheme.h"
#include "flat_comp_create.h"

#include <ydb/core/base/appdata.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/cast.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TCompactionLogicState::TSnapRequest::~TSnapRequest()
{}

TCompactionLogicState::TTableInfo::~TTableInfo()
{}

TCompactionLogic::TCompactionLogic(NTable::IMemTableMemoryConsumersCollection *memTableMemoryConsumersCollection,
                                   NUtil::ILogger *logger,
                                   NTable::IResourceBroker *broker,
                                   NTable::ICompactionBackend *backend,
                                   TAutoPtr<TCompactionLogicState> state,
                                   TString taskNameSuffix)
    : MemTableMemoryConsumersCollection(memTableMemoryConsumersCollection)
    , Logger(logger)
    , Broker(broker)
    , Backend(backend)
    , Time(TAppData::TimeProvider.Get())
    , State(state)
    , TaskNameSuffix(taskNameSuffix)
{}

TCompactionLogic::~TCompactionLogic()
{}

void TCompactionLogic::Start() {
    auto result = ReflectSchemeChanges();
    Y_ABORT_UNLESS(!result.StrategyChanges);
    State->Snapshots.clear();
}

void TCompactionLogic::Stop() {
    for (auto &kv : State->Tables) {
        StopTable(kv.second);
    }
    State->Tables.clear();
}

TCompactionLogicState::TSnapshotState TCompactionLogic::SnapToLog(ui32 tableId) {
    auto* info = State->Tables.FindPtr(tableId);
    Y_ABORT_UNLESS(info);
    TCompactionLogicState::TSnapshotState ret;
    ret.State = info->Strategy->SnapshotState();
    ret.Strategy = info->StrategyType;
    return ret;
}

void TCompactionLogic::UpdateCompactions()
{
    for (auto &tpr : State->Tables) {
        auto &tableInfo = tpr.second;
        auto &policy = *tableInfo.Policy;
        auto &inMem = tableInfo.InMem;

        if (inMem.State == ECompactionState::PendingBackground) {
            auto priority = tableInfo.ComputeBackgroundPriority(inMem, policy, Time->Now());
            auto oldPriority = inMem.CompactionTask.Priority;

            // Avoid task updates in case of small priority changes.
            if (priority < oldPriority
                && (oldPriority - priority) >= oldPriority / PRIORITY_UPDATE_FACTOR) {
                UpdateCompactionTask(policy.BackgroundSnapshotPolicy.ResourceBrokerTask,
                                     priority, inMem.CompactionTask);
            }
        }

        tableInfo.Strategy->UpdateCompactions();
    }
}

void TCompactionLogic::RequestChanges(ui32 table)
{
    auto *tableInfo = State->Tables.FindPtr(table);
    Y_ABORT_UNLESS(tableInfo);
    tableInfo->ChangesRequested = true;
}

TVector<TTableCompactionChanges> TCompactionLogic::ApplyChanges()
{
    TVector<TTableCompactionChanges> results;

    for (auto& kv : State->Tables) {
        ui32 tableId = kv.first;
        auto *tableInfo = &kv.second;
        if (tableInfo->ChangesRequested) {
            tableInfo->ChangesRequested = false;
            auto& result = results.emplace_back();
            result.Table = tableId;
            result.Changes = tableInfo->Strategy->ApplyChanges();
            result.Strategy = tableInfo->StrategyType;
        }
    }

    return results;
}

void TCompactionLogic::PrepareTableSnapshot(ui32 table, NTable::TSnapEdge edge, TTableSnapshotContext *snapContext) {
    TCompactionLogicState::TTableInfo *tableInfo = State->Tables.FindPtr(table);
    Y_DEBUG_ABORT_UNLESS(tableInfo);
    TCompactionLogicState::TInMem &inMem = tableInfo->InMem;

    Y_ABORT_UNLESS(edge.TxStamp != Max<ui64>(), "TxStamp of snapshot is undefined");

    tableInfo->SnapRequests.emplace_back(TCompactionLogicState::TSnapRequest(edge, snapContext));

    switch (inMem.State) {
    case ECompactionState::Free:
        SubmitCompactionTask(table, 0,
                             tableInfo->Policy->SnapshotResourceBrokerTask,
                             tableInfo->Policy->DefaultTaskPriority,
                             inMem.CompactionTask);
        inMem.State = ECompactionState::SnapshotPending;
        break;
    case ECompactionState::Pending:
        inMem.State = ECompactionState::SnapshotPending;
        break;
    case ECompactionState::PendingBackground:
        // Replace background compaction with regular snapshot task.
        UpdateCompactionTask(tableInfo->Policy->SnapshotResourceBrokerTask,
                             tableInfo->Policy->DefaultTaskPriority,
                             inMem.CompactionTask);
        inMem.State = ECompactionState::SnapshotPending;
        break;
    default:
        break;
    }
}

bool TCompactionLogic::PrepareForceCompaction() {
    bool ok = true;
    for (auto &it : State->Tables) {
        ok &= PrepareForceCompaction(it.first) != 0;
    }
    return ok;
}

ui64 TCompactionLogic::PrepareForceCompaction(ui32 table, EForceCompaction mode) {
    TCompactionLogicState::TTableInfo *tableInfo = State->Tables.FindPtr(table);
    if (!tableInfo)
        return 0;

    if (auto logl = Logger->Log(NUtil::ELnLev::Debug)) {
        logl << "TCompactionLogic PrepareForceCompaction for " << Backend->OwnerTabletId()
            << " table " << table << ", mode " << mode << ", forced state " << tableInfo->ForcedCompactionState
            << ", forced mode " << tableInfo->ForcedCompactionMode;
    }

    if (mode == EForceCompaction::Borrowed) {
        // Note: we also schedule mem table compaction below, because tx status may have borrowed data
        tableInfo->Strategy->ScheduleBorrowedCompaction();
    }

    TCompactionLogicState::TInMem &inMem = tableInfo->InMem;
    switch (tableInfo->ForcedCompactionState) {
        case EForcedCompactionState::None:
            tableInfo->ForcedCompactionState = EForcedCompactionState::PendingMem;
            tableInfo->ForcedCompactionMode = mode;
            ++tableInfo->CurrentForcedMemCompactionId;
            switch (inMem.State) {
            case ECompactionState::Free:
                SubmitCompactionTask(table, 0,
                                    tableInfo->Policy->InMemResourceBrokerTask,
                                    tableInfo->Policy->DefaultTaskPriority,
                                    inMem.CompactionTask);
                inMem.State = ECompactionState::Pending;
                break;
            case ECompactionState::PendingBackground:
                UpdateCompactionTask(tableInfo->Policy->InMemResourceBrokerTask,
                                    tableInfo->Policy->DefaultTaskPriority,
                                    inMem.CompactionTask);
                inMem.State = ECompactionState::Pending;
                break;
            default:
                break;
            }
            break;

        case EForcedCompactionState::PendingMem:
            tableInfo->ForcedCompactionMode = Max(tableInfo->ForcedCompactionMode, mode);
            break;

        default:
            tableInfo->ForcedCompactionQueued = true;
            tableInfo->ForcedCompactionQueuedMode = Max(tableInfo->ForcedCompactionQueuedMode, mode);
            return tableInfo->CurrentForcedMemCompactionId + 1;
    }

    return tableInfo->CurrentForcedMemCompactionId;
}

void TCompactionLogic::TriggerSharedPageCacheMemTableCompaction(ui32 table, ui64 expectedSize) {
    TCompactionLogicState::TTableInfo *tableInfo = State->Tables.FindPtr(table);
    if (!tableInfo)
        return;

    auto &inMem = tableInfo->InMem;
    auto &policy = tableInfo->Policy->BackgroundSnapshotPolicy;

    if (inMem.State == ECompactionState::Free) {
        if (inMem.EstimatedSize >= expectedSize) {
            auto priority = tableInfo->ComputeBackgroundPriority(inMem.CompactionTask, policy, 100, Time->Now());
            SubmitCompactionTask(table, 0, policy.ResourceBrokerTask,
                                priority, inMem.CompactionTask);
            inMem.State = ECompactionState::PendingBackground;
        } else {
            // there was a race and we finished some compaction while our message was waiting
            // so let's notify that we completed it
            Y_DEBUG_ABORT_UNLESS(tableInfo->MemTableMemoryConsumer);
            if (auto& consumer = tableInfo->MemTableMemoryConsumer) {
                MemTableMemoryConsumersCollection->CompactionComplete(consumer);
            }
        }
    }    
}

TFinishedCompactionInfo TCompactionLogic::GetFinishedCompactionInfo(ui32 table) {
    TCompactionLogicState::TTableInfo *tableInfo = State->Tables.FindPtr(table);
    if (!tableInfo || !tableInfo->Strategy)
        return TFinishedCompactionInfo();
    return TFinishedCompactionInfo(
        tableInfo->Strategy->GetLastFinishedForcedCompactionId(),
        tableInfo->Strategy->GetLastFinishedForcedCompactionTs());
}

void TCompactionLogic::AllowBorrowedGarbageCompaction(ui32 table) {
    auto *tableInfo = State->Tables.FindPtr(table);
    Y_ABORT_UNLESS(tableInfo && tableInfo->Strategy, "Cannot AllowBorrowedGarbageCompaction for unexpected table %" PRIu32, table);
    tableInfo->AllowBorrowedGarbageCompaction = true;
    tableInfo->Strategy->AllowBorrowedGarbageCompaction();
}

TReflectSchemeChangesResult TCompactionLogic::ReflectSchemeChanges()
{
    TReflectSchemeChangesResult result;

    const auto& scheme = Backend->DatabaseScheme();

    for (const auto &xpair : scheme.Tables) {
        auto &info = xpair.second;
        auto &table = State->Tables[info.Id];
        table.TableId = info.Id;

        if (auto *policy = table.Policy.Get()) {
            TString err;
            bool ok = NLocalDb::ValidateCompactionPolicyChange(*policy, *info.CompactionPolicy, err);
            Y_ABORT_UNLESS(ok, "table %s id %u: %s", info.Name.data(), info.Id, err.data());
        }

        table.Policy = info.CompactionPolicy;

        auto newStrategyType = scheme.CompactionStrategyFor(info.Id);
        Y_ABORT_UNLESS(newStrategyType != NKikimrSchemeOp::CompactionStrategyUnset);

        if (table.StrategyType != newStrategyType) {
            if (table.StrategyType != NKikimrSchemeOp::CompactionStrategyUnset) {
                result.StrategyChanges.push_back({ info.Id, newStrategyType });
                StrategyChanging(table);
            }

            Y_ABORT_UNLESS(!table.Strategy);
            table.StrategyType = newStrategyType;
            table.Strategy = CreateStrategy(info.Id, newStrategyType);
            Y_ABORT_UNLESS(table.Strategy);

            // Time to start the new strategy
            if (auto* snapshot = State->Snapshots.FindPtr(info.Id)) {
                table.InMem.Steps = snapshot->InMemSteps;
                if (snapshot->Strategy == newStrategyType) {
                    table.Strategy->Start(std::move(snapshot->State));
                } else {
                    // It may happen that the default strategy changes and
                    // no longer matches the snapshot we have. In that case
                    // we should start with an empty state.
                    table.Strategy->Start({ });
                }
            } else {
                table.Strategy->Start({ });
            }

            if (table.AllowBorrowedGarbageCompaction) {
                table.Strategy->AllowBorrowedGarbageCompaction();
            }

            if (!table.MemTableMemoryConsumer) {
                MemTableMemoryConsumersCollection->Register(info.Id);
            }
        } else {
            Y_ABORT_UNLESS(table.Strategy);
            table.Strategy->ReflectSchema();
        }
    }

    for (auto it = State->Tables.begin(), end = State->Tables.end(); it != end; ) {
        if (scheme.Tables.contains(it->first)) {
            ++it;
        } else {
            StopTable(it->second);
            it = State->Tables.erase(it);
        }
    }

    return result;
}

void TCompactionLogic::ProvideMemTableMemoryConsumer(ui32 table, TIntrusivePtr<NMemory::IMemoryConsumer> memTableMemoryConsumer)
{
    auto *tableInfo = State->Tables.FindPtr(table);
    if (tableInfo) {
        memTableMemoryConsumer->SetConsumption(tableInfo->InMem.EstimatedSize);

        Y_DEBUG_ABORT_UNLESS(!tableInfo->MemTableMemoryConsumer);
        tableInfo->MemTableMemoryConsumer = std::move(memTableMemoryConsumer);
    }
}

void TCompactionLogic::ReflectRemovedRowVersions(ui32 table)
{
    auto *tableInfo = State->Tables.FindPtr(table);
    if (tableInfo) {
        tableInfo->Strategy->ReflectRemovedRowVersions();
    }
}

THolder<NTable::ICompactionStrategy> TCompactionLogic::CreateStrategy(
        ui32 tableId,
        NKikimrSchemeOp::ECompactionStrategy strategy)
{
    switch (strategy) {
        case NKikimrSchemeOp::CompactionStrategyGenerational:
            return NTable::CreateGenCompactionStrategy(
                    tableId, Backend, Broker, Time, Logger, TaskNameSuffix);

        default:
            Y_ABORT("Unsupported strategy %s", NKikimrSchemeOp::ECompactionStrategy_Name(strategy).c_str());
    }
}

void TCompactionLogic::StopTable(TCompactionLogicState::TTableInfo &table)
{
    // Note: should be called even without table.SharedPageCacheMemTableRegistration
    MemTableMemoryConsumersCollection->Unregister(table.TableId);

    if (table.Strategy) {
        // Strategy will cancel all pending and running compactions
        table.Strategy->Stop();
        table.Strategy.Reset();
    }

    table.ChangesRequested = false;

    auto &inMem = table.InMem;
    switch (inMem.State) {
        case ECompactionState::Pending:
        case ECompactionState::PendingBackground:
        case ECompactionState::SnapshotPending:
            // We must cancel our own pending task
            Broker->CancelTask(inMem.CompactionTask.TaskId);
            break;

        default:
            break;
    }

    // Everything has already been cancelled
    inMem.CompactingSteps = 0;
    inMem.State = ECompactionState::Free;
    inMem.CompactionTask.TaskId = 0;
    inMem.CompactionTask.CompactionId = 0;
}

void TCompactionLogic::StrategyChanging(TCompactionLogicState::TTableInfo &table)
{
    if (table.Strategy) {
        // Strategy will cancel all pending and running compactions
        table.Strategy->Stop();
        table.Strategy.Reset();
    }

    table.ChangesRequested = false;

    auto &inMem = table.InMem;

    auto resubmit = [&](const TString& type, ui32 priority) {
        if (table.ForcedCompactionState == EForcedCompactionState::CompactingMem) {
            table.ForcedCompactionState = EForcedCompactionState::PendingMem;
        }
        inMem.CompactionTask.TaskId = 0;
        inMem.CompactionTask.CompactionId = 0;
        SubmitCompactionTask(table.TableId, 0, type, priority, inMem.CompactionTask);
    };

    switch (inMem.State) {
        case ECompactionState::Pending:
        case ECompactionState::PendingBackground:
        case ECompactionState::SnapshotPending:
            // Keep currently pending tasks
            break;

        case ECompactionState::Compaction:
            // Compaction has been cancelled, resubmit
            resubmit(
                table.Policy->InMemResourceBrokerTask,
                table.Policy->DefaultTaskPriority);
            inMem.State = ECompactionState::Pending;
            break;

        case ECompactionState::SnapshotCompaction:
            // Compaction has been cancelled, resubmit
            resubmit(
                table.Policy->SnapshotResourceBrokerTask,
                table.Policy->DefaultTaskPriority);
            inMem.State = ECompactionState::SnapshotPending;
            break;

        default:
            break;
    }
}

void TCompactionLogic::UpdateInMemStatsStep(ui32 table, ui32 steps, ui64 size) {
    auto *info = State->Tables.FindPtr(table);
    Y_ABORT_UNLESS(info);
    auto &mem = info->InMem;
    mem.EstimatedSize = size;
    mem.Steps += steps;

    if (auto& consumer = info->MemTableMemoryConsumer) {
        consumer->SetConsumption(size);
        if (steps == 0) {
            MemTableMemoryConsumersCollection->CompactionComplete(consumer);
        }
    }

    CheckInMemStats(table);
}

void TCompactionLogic::CheckInMemStats(ui32 table) {
    auto *info = State->Tables.FindPtr(table);
    Y_ABORT_UNLESS(info);
    auto &mem = info->InMem;

    const auto &policy = *info->Policy;
    const float memLoWatermark = policy.InMemForceSizeToSnapshot * 2.5f; // default 16MB -> 40MB
    const float memHiWatermark = policy.InMemForceSizeToSnapshot * 4.0f; // default 16MB -> 64MB
    mem.OverloadFactor = (float(mem.EstimatedSize) - memLoWatermark) / (memHiWatermark - memLoWatermark);

    if (mem.State != ECompactionState::Free
        && mem.State != ECompactionState::PendingBackground) {
        return;
    }

    if (policy.InMemForceSizeToSnapshot <= mem.EstimatedSize
        || policy.InMemForceStepsToSnapshot <= mem.Steps
        || (policy.InMemSizeToSnapshot <= mem.EstimatedSize && policy.InMemStepsToSnapshot <= mem.Steps))
    {
        // Replace background task or submit a new one.
        if (mem.State == ECompactionState::PendingBackground) {
            UpdateCompactionTask(policy.InMemResourceBrokerTask,
                                 policy.DefaultTaskPriority,
                                 mem.CompactionTask);
        } else {
            SubmitCompactionTask(table, 0,
                                 policy.InMemResourceBrokerTask,
                                 policy.DefaultTaskPriority,
                                 mem.CompactionTask);
        }
        mem.State = ECompactionState::Pending;
    } else if (mem.State == ECompactionState::Free && (mem.EstimatedSize > 0 || mem.Steps > 0)) {
        auto priority = info->ComputeBackgroundPriority(mem, policy, Time->Now());
        if (priority != BAD_PRIORITY) {
            SubmitCompactionTask(table, 0, policy.BackgroundSnapshotPolicy.ResourceBrokerTask,
                                 priority, mem.CompactionTask);
            mem.State = ECompactionState::PendingBackground;
        }
    }
}

void TCompactionLogic::UpdateLogUsage(TArrayRef<const NRedo::TUsage> usage)
{
    for (auto &one : usage) {
        UpdateLogUsage(one);
    }
}

void TCompactionLogic::UpdateLogUsage(const NRedo::TUsage &usage)
{
    auto* tableInfo = State->Tables.FindPtr(usage.Table);
    if (!tableInfo) {
        // Ignore deleted tables
        return;
    }

    auto& inMem = tableInfo->InMem;
    inMem.LogOverheadCount = usage.Items;
    inMem.LogOverheadSize = usage.Bytes;

    if (inMem.State != ECompactionState::Free &&
        inMem.State != ECompactionState::PendingBackground)
    {
        return;
    }

    const auto& policy = *tableInfo->Policy;
    if (policy.LogOverheadSizeToSnapshot > 0 && policy.LogOverheadSizeToSnapshot <= inMem.LogOverheadSize ||
        policy.LogOverheadCountToSnapshot > 0 && policy.LogOverheadCountToSnapshot <= inMem.LogOverheadCount)
    {
        // Replace background task or submit a new one.
        if (inMem.State == ECompactionState::PendingBackground) {
            UpdateCompactionTask(policy.InMemResourceBrokerTask,
                                 policy.DefaultTaskPriority,
                                 inMem.CompactionTask);
        } else {
            SubmitCompactionTask(usage.Table, 0,
                                 policy.InMemResourceBrokerTask,
                                 policy.DefaultTaskPriority,
                                 inMem.CompactionTask);
        }
        inMem.State = ECompactionState::Pending;
    }
}

bool TCompactionLogic::BeginMemTableCompaction(ui64 taskId, ui32 tableId)
{
    TCompactionLogicState::TTableInfo *tableInfo = State->Tables.FindPtr(tableId);
    Y_ABORT_UNLESS(tableInfo,
        "Unexpected BeginMemTableCompaction(%" PRIu64 ", %" PRIu32 ") for a dropped table",
        taskId, tableId);

    TCompactionLogicState::TInMem &inMem = tableInfo->InMem;
    Y_ABORT_UNLESS(taskId == inMem.CompactionTask.TaskId);

    NTable::TSnapEdge edge;

    switch (inMem.State) {
    case ECompactionState::Pending:
    case ECompactionState::PendingBackground:
        inMem.CompactingSteps = inMem.Steps;
        inMem.State = ECompactionState::Compaction;
        edge.Head = NTable::TEpoch::Max();
        break;

    case ECompactionState::SnapshotPending:
        inMem.CompactingSteps = inMem.Steps;
        inMem.State = ECompactionState::SnapshotCompaction;
        edge = tableInfo->SnapRequests.front().Edge;
        break;

    default:
        Y_ABORT("Invalid inMem.State");
    }

    ui64 forcedCompactionId = 0;
    if (edge.Head == NTable::TEpoch::Max() &&
        tableInfo->ForcedCompactionState == EForcedCompactionState::PendingMem)
    {
        tableInfo->ForcedCompactionState = EForcedCompactionState::CompactingMem;
        switch (tableInfo->ForcedCompactionMode) {
            case EForceCompaction::Mem: {
                // FIXME: we probably need to always provide forced compaction id
                break;
            }
            case EForceCompaction::Borrowed: {
                // It's just like a normal mem compaction
                break;
            }
            case EForceCompaction::Full: {
                forcedCompactionId = tableInfo->CurrentForcedMemCompactionId;
                break;
            }
        }
    }

    inMem.CompactionTask.CompactionId = tableInfo->Strategy->BeginMemCompaction(taskId, edge, forcedCompactionId);
    return true;
}

TCompactionLogicState::TTableInfo*
TCompactionLogic::HandleCompaction(
        ui64 compactionId,
        const NTable::TCompactionParams* params,
        TTableCompactionResult* ret)
{
    const ui32 tableId = params->Table;
    const auto edge = params->Edge;

    TCompactionLogicState::TTableInfo *tableInfo = State->Tables.FindPtr(tableId);
    Y_ABORT_UNLESS(tableInfo, "Unexpected CompleteCompaction for a dropped table");

    if (compactionId == tableInfo->InMem.CompactionTask.CompactionId) {
        TCompactionLogicState::TInMem &inMem = tableInfo->InMem;
        Y_ABORT_UNLESS(params->TaskId == inMem.CompactionTask.TaskId);

        switch (inMem.State) {
        case ECompactionState::Compaction:
            inMem.Steps -= std::exchange(inMem.CompactingSteps, 0);
            inMem.State = ECompactionState::Free;
            inMem.CompactionTask.TaskId = 0;
            inMem.CompactionTask.CompactionId = 0;
            break;
        case ECompactionState::SnapshotCompaction:
            Y_ABORT_UNLESS(tableInfo->SnapRequests);
            Y_ABORT_UNLESS(edge == tableInfo->SnapRequests.front().Edge);
            if (ret) {
                ret->CompleteSnapshots.push_back(tableInfo->SnapRequests.front().Context);
                tableInfo->SnapRequests.pop_front();
            }
            inMem.Steps -= std::exchange(inMem.CompactingSteps, 0);
            inMem.State = ECompactionState::Free;
            inMem.CompactionTask.TaskId = 0;
            inMem.CompactionTask.CompactionId = 0;
            break;
        default:
            Y_ABORT("must not happens, state=%d", (int)inMem.State);
        }

        if (tableInfo->ForcedCompactionState == EForcedCompactionState::CompactingMem) {
            tableInfo->ForcedCompactionState = EForcedCompactionState::None;
        }

        if (tableInfo->SnapRequests) {
            SubmitCompactionTask(tableId, 0,
                                 tableInfo->Policy->SnapshotResourceBrokerTask,
                                 tableInfo->Policy->DefaultTaskPriority,
                                 inMem.CompactionTask);
            inMem.State = ECompactionState::SnapshotPending;
        } else if (tableInfo->ForcedCompactionState == EForcedCompactionState::PendingMem) {
            // There is another memory compaction request
            SubmitCompactionTask(tableId, 0,
                                 tableInfo->Policy->InMemResourceBrokerTask,
                                 tableInfo->Policy->DefaultTaskPriority,
                                 inMem.CompactionTask);
            inMem.State = ECompactionState::Pending;
        }

        if (ret) {
            // Signal we need UpdateInMemStats call
            ret->MemCompacted = true;
        } else {
            // Maybe schedule another compaction
            CheckInMemStats(tableId);
        }
    }

    if (tableInfo->ForcedCompactionState == EForcedCompactionState::None) {
        if (tableInfo->ForcedCompactionQueued) {
            tableInfo->ForcedCompactionQueued = false;
            auto mode = std::exchange(tableInfo->ForcedCompactionQueuedMode, EForceCompaction::Mem);
            PrepareForceCompaction(tableId, mode);
        }
    }

    return tableInfo;
}

TTableCompactionResult
TCompactionLogic::CompleteCompaction(
        ui64 compactionId,
        THolder<NTable::TCompactionParams> params,
        THolder<NTable::TCompactionResult> result)
{
    TTableCompactionResult ret;

    auto* tableInfo = HandleCompaction(compactionId, params.Get(), &ret);

    ret.Changes = tableInfo->Strategy->CompactionFinished(compactionId, std::move(params), std::move(result));
    ret.Strategy = tableInfo->StrategyType;

    return ret;
}

void
TCompactionLogic::CancelledCompaction(
        ui64 compactionId,
        THolder<NTable::TCompactionParams> params)
{
    HandleCompaction(compactionId, params.Get(), nullptr);
}

void TCompactionLogic::BorrowedPart(ui32 tableId, NTable::TPartView partView) {
    auto *tableInfo = State->Tables.FindPtr(tableId);
    Y_ABORT_UNLESS(tableInfo);
    tableInfo->Strategy->PartMerged(std::move(partView), 255);
}

void TCompactionLogic::BorrowedPart(ui32 tableId, TIntrusiveConstPtr<NTable::TColdPart> part) {
    auto *tableInfo = State->Tables.FindPtr(tableId);
    Y_ABORT_UNLESS(tableInfo);
    tableInfo->Strategy->PartMerged(std::move(part), 255);
}

ui32 TCompactionLogic::BorrowedPartLevel() {
    return 255;
}

TTableCompactionChanges TCompactionLogic::RemovedParts(ui32 tableId, TArrayRef<const TLogoBlobID> parts) {
    auto *tableInfo = State->Tables.FindPtr(tableId);
    Y_ABORT_UNLESS(tableInfo);
    TTableCompactionChanges ret;
    ret.Table = tableId;
    ret.Changes = tableInfo->Strategy->PartsRemoved(parts);
    ret.Strategy = tableInfo->StrategyType;
    return ret;
}

void TCompactionLogic::SubmitCompactionTask(ui32 table,
                                            ui32 generation,
                                            const TString &type,
                                            ui32 priority,
                                            TCompactionLogicState::TCompactionTask &task)
{
    Y_ABORT_UNLESS(generation == 0, "Unexpected gen %" PRIu32 " in compaction logic", generation);

    task.Priority = priority;
    task.SubmissionTimestamp = Time->Now();

    TString name = Sprintf("gen%" PRIu32 "-table-%" PRIu32 "-%s",
                           generation, table, TaskNameSuffix.data());
    task.TaskId = Broker->SubmitTask(
        std::move(name),
        TResourceParams(type)
            .WithCPU(1)
            .WithPriority(priority),
        [this, table](TTaskId taskId) {
            if (!BeginMemTableCompaction(taskId, table)) {
                Broker->FinishTask(taskId, EResourceStatus::Cancelled);
            }
        });
}

void TCompactionLogic::UpdateCompactionTask(const TString &type,
                                            ui32 priority,
                                            TCompactionLogicState::TCompactionTask &task)
{
    task.Priority = priority;

    Broker->UpdateTask(
        task.TaskId,
        TResourceParams(type)
            .WithCPU(1)
            .WithPriority(priority));
}

ui32 TCompactionLogicState::TTableInfo::ComputeBackgroundPriority(
        const TCompactionLogicState::TCompactionTask &task,
        const TCompactionPolicy::TBackgroundPolicy &policy,
        ui32 percentage,
        TInstant now) const
{
    double priority = policy.PriorityBase;
    priority = priority * 100 / percentage;
    if (task.TaskId) {
        double factor = policy.TimeFactor;
        factor *= log((now - task.SubmissionTimestamp).Seconds());
        priority /= Max(factor, 1.0);
    }

    return priority;
}

ui32 TCompactionLogicState::TTableInfo::ComputeBackgroundPriority(
        const TCompactionLogicState::TInMem &inMem,
        const TCompactionPolicy &policy,
        TInstant now) const
{
    auto &bckgPolicy = policy.BackgroundSnapshotPolicy;
    auto perc = Max((ui32)(inMem.EstimatedSize * 100 / policy.InMemForceSizeToSnapshot),
                    inMem.Steps * 100 / policy.InMemForceStepsToSnapshot);

    if (!perc || perc < bckgPolicy.Threshold)
        return TCompactionLogic::BAD_PRIORITY;

    return ComputeBackgroundPriority(inMem.CompactionTask, bckgPolicy, perc, now);
}

float TCompactionLogic::GetOverloadFactor() const {
    float overloadFactor = 0;
    for (const auto& ti : State->Tables) {
        overloadFactor = Max(overloadFactor, ti.second.InMem.OverloadFactor);
        overloadFactor = Max(overloadFactor, ti.second.Strategy->GetOverloadFactor());
    }
    return overloadFactor;
}

ui64 TCompactionLogic::GetBackingSize() const {
    ui64 size = 0;
    for (const auto& ti : State->Tables) {
        size += ti.second.Strategy->GetBackingSize();
    }
    return size;
}

ui64 TCompactionLogic::GetBackingSize(ui64 ownerTabletId) const {
    ui64 size = 0;
    for (const auto& ti : State->Tables) {
        size += ti.second.Strategy->GetBackingSize(ownerTabletId);
    }
    return size;
}

void TCompactionLogic::OutputHtml(IOutputStream &out, const NTable::TScheme &scheme, const TCgiParameters& cgi) {
    HTML(out) {
        for (const auto &xtable : State->Tables) {
            TAG(TH4) {out << scheme.GetTableInfo(xtable.first)->Name;}

            DIV_CLASS("row") { out
                << "InMem Size: " << xtable.second.InMem.EstimatedSize
                << ", Changes: " << xtable.second.InMem.Steps
                << ", Compaction state: " << xtable.second.InMem.State
                << ", Backing size: " << xtable.second.Strategy->GetBackingSize()
                << ", Log overhead size: " << xtable.second.InMem.LogOverheadSize
                << ", count: " << xtable.second.InMem.LogOverheadCount;
                if (xtable.second.ForcedCompactionState != EForcedCompactionState::None) {
                    out << ", Forced compaction: " << xtable.second.ForcedCompactionState;
                } else if (xtable.second.Strategy->AllowForcedCompaction()) {
                    auto cgiCopy = cgi;
                    cgiCopy.InsertUnescaped("force_compaction", ToString(xtable.first));
                    out << ", <a href=\"executorInternals?" << cgiCopy.Print() << "\">Force compaction</a>";
                }
                if (xtable.second.InMem.CompactionTask.TaskId) {
                    out << ", Task #" << xtable.second.InMem.CompactionTask.TaskId
                        << " (priority " << xtable.second.InMem.CompactionTask.Priority
                        << ") submitted " << xtable.second.InMem.CompactionTask.SubmissionTimestamp.ToStringLocal();
                }
            }

            xtable.second.Strategy->OutputHtml(out);
        }
    }
}

}}
