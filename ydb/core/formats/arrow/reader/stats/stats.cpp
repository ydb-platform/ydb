#include "stats.h"

#include <limits>

namespace NKikimr::NScanStats {
    namespace {
        TStringBuf StorageDataKindName(NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind kind) {
            using TStorageRead = NKqpProto::TColumnShardStepDetails::TStorageRead;
            switch (kind) {
                case TStorageRead::data: return "data";
                case TStorageRead::index: return "index";
                case TStorageRead::metadata: return "metadata";
                case TStorageRead::mixed: return "mixed";
                default: return "unknown";
            }
        }

        bool AddSourceRead(google::protobuf::RepeatedPtrField<NKqpProto::TColumnShardDataSourceRead>& reads,
            TStringBuf kind, TStringBuf name, std::optional<ui64> bytes,
            NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind, bool shared = false) {
            const TStringBuf dataKindName = StorageDataKindName(dataKind);
            if (shared) {
                bytes.reset();
            }
            NKqpProto::TColumnShardDataSourceRead* target = nullptr;
            for (auto& read : reads) {
                if (read.GetSourceKind() == kind && read.GetSourceName() == name) {
                    target = &read;
                    break;
                }
            }
            if (!target) {
                target = reads.Add();
                target->SetSourceKind(kind.data(), kind.size());
                target->SetSourceName(name.data(), name.size());
                target->SetDataKind(dataKindName.data(), dataKindName.size());
            } else if (target->GetDataKind() != dataKindName) {
                target->SetDataKind(target->GetDataKind() == "unknown" || dataKindName == "unknown" ? "unknown" : "mixed");
            }
            if (shared) {
                target->SetShared(true);
            }
            bool overflow = false;
            const auto add = [&](ui64 value, ui64 delta) {
                if (delta > std::numeric_limits<ui64>::max() - value) {
                    overflow = true;
                    return std::numeric_limits<ui64>::max();
                }
                return value + delta;
            };
            target->SetReadCount(add(target->GetReadCount(), 1));
            if (bytes) {
                target->SetReadBytes(add(target->GetReadBytes(), *bytes));
            }
            if (!bytes || overflow) {
                target->SetUnknownByteReads(add(target->GetUnknownByteReads(), 1));
            }
            return !overflow;
        }
    }

    static TString CanonicalStepKind(TStep::EStepKind kind) {
        switch (kind) {
            case TStep::FetchColumns:
                return "Fetching";
            case TStep::MemoryReserve:
                return "Reserve";
            default:
                return TString(TStep::EStepKind_Name(kind));
        }
    }

    static TString DefaultMetaStep(TStep::EStepKind kind) {
        switch (kind) {
            case TStep::SelectPortions:
            case TStep::ParseProgram:
            case TStep::ReaderInit:
                return "Preparation";
            case TStep::Merge:
                return "PKFilter";
            case TStep::Limit:
            case TStep::Ready:
            case TStep::Send:
            case TStep::Ack:
            case TStep::Finish:
                return "Output";
            case TStep::IndexCheck:
                return "IndexFilter";
            case TStep::Filter:
                return "Filter";
            case TStep::Aggregation:
                return "Aggregation";
            case TStep::Projection:
                return "Projection";
            case TStep::BuildResult:
                return "BuildResult";
            default:
                return {};
        }
    }
    namespace {
        thread_local std::shared_ptr<TOperation> ActiveOperation;
        thread_local std::shared_ptr<TOperation> OwnedOperation;
        thread_local std::shared_ptr<TOperation> ContextOperation;
        thread_local std::shared_ptr<TCollector> ActiveCollector;
    } // namespace

    TOperation::TOperation(std::shared_ptr<TCollector> collector, TStep&& record)
        : Collector(std::move(collector))
        , StepId(record.GetStepId())
        , SourceId(record.HasSourceId() ? std::make_optional(record.GetSourceId()) : std::nullopt)
        , Record(std::move(record))
    {
    }

    TOperation::~TOperation() {
        Finish(TStep::cancelled);
    }

    void TOperation::EndActive(TMonotonic now) {
        if (OwnedActiveSince) {
            Record.SetOwnedExecutionDuration(Record.GetOwnedExecutionDuration() + (now - *OwnedActiveSince).MicroSeconds());
            OwnedActiveSince.reset();
        }
        if (ActiveSince) {
            Record.SetIntegralExecutionDuration(Record.GetIntegralExecutionDuration() + (now - *ActiveSince).MicroSeconds());
            ActiveSince.reset();
        }
    }

    void TOperation::EndWait(TMonotonic now) {
        if (WaitingSince) {
            Record.SetIntegralWaitDuration(Record.GetIntegralWaitDuration() + (now - *WaitingSince).MicroSeconds());
            WaitingSince.reset();
        }
    }

    bool TOperation::Resume() {
        std::lock_guard guard(Mutex);
        if (Finalized) {
            return false;
        }
        const bool waiting = WaitingSince.has_value();
        const auto now = TMonotonic::Now();
        EndWait(now);
        if (!ActiveSince) {
            ActiveSince = now;
        }
        if (!OwnedActiveSince && OwnedPauseDepth == 0) {
            Record.SetOwnedExecutionDuration(Record.GetOwnedExecutionDuration());
            OwnedActiveSince = now;
        }
        return waiting;
    }

    void TOperation::Suspend() {
        std::lock_guard guard(Mutex);
        if (!Finalized) {
            const auto now = TMonotonic::Now();
            EndActive(now);
            if (!WaitingSince) {
                WaitingSince = now;
            }
        }
    }

    void TOperation::StopActive() {
        std::lock_guard guard(Mutex);
        if (!Finalized) {
            EndActive(TMonotonic::Now());
        }
    }

    void TOperation::PauseOwnedActive() {
        std::lock_guard guard(Mutex);
        if (Finalized) {
            return;
        }
        ++OwnedPauseDepth;
        if (OwnedActiveSince) {
            Record.SetOwnedExecutionDuration(Record.GetOwnedExecutionDuration() +
                                             (TMonotonic::Now() - *OwnedActiveSince).MicroSeconds());
            OwnedActiveSince.reset();
        }
    }

    void TOperation::ResumeOwnedActive() {
        std::lock_guard guard(Mutex);
        if (Finalized || OwnedPauseDepth == 0) {
            return;
        }
        --OwnedPauseDepth;
        if (OwnedPauseDepth == 0 && ActiveSince) {
            OwnedActiveSince = TMonotonic::Now();
        }
    }

    void TOperation::InheritContext(TStep& child) {
        std::lock_guard guard(Mutex);
        if (child.IndexDescriptorsSize() == 0) {
            child.MutableIndexDescriptors()->CopyFrom(Record.GetIndexDescriptors());
        }
        if (!child.HasApplicationOrder() && Record.HasApplicationOrder()) {
            child.SetApplicationOrder(Record.GetApplicationOrder());
        }
        if (!Record.HasMetaStep()) {
            return;
        }
        if (!child.HasMetaStep() || child.GetStepKind() == TStep::Filter) {
            child.SetMetaStep(Record.GetMetaStep());
        }
        if (child.GetMetaStep() == Record.GetMetaStep()) {
            TString path = Record.GetCanonicalPath();
            if (Record.GetStepKind() != TStep::Reader) {
                if (path) {
                    path += "/";
                }
                path += CanonicalStepKind(Record.GetStepKind());
            }
            child.SetCanonicalPath(path);
        }
    }

    void TOperation::AddRequestedRange(TString rangeIdentity, ui64 bytes) {
        std::lock_guard guard(Mutex);
        if (!Finalized && Record.GetStepKind() == TStep::FetchColumns && RequestedRanges.insert(std::move(rangeIdentity)).second) {
            auto* details = Record.MutableDetails()->MutableFetchColumns();
            details->SetRequestedBytes(details->GetRequestedBytes() + bytes);
        }
    }

    void TOperation::Finish(TStep::EOutcome outcome) {
        TStep finalized;
        {
            std::lock_guard guard(Mutex);
            if (Finalized) {
                return;
            }
            const auto now = TMonotonic::Now();
            EndActive(now);
            EndWait(now);
            if (Record.GetStepKind() == TStep::StorageRead) {
                const auto& details = Record.GetDetails().GetStorageRead();
                using TStorageRead = NKqpProto::TColumnShardStepDetails::TStorageRead;
                TStringBuf kind = "Unknown";
                switch (details.GetBackendKind()) {
                    case TStorageRead::BlobStorage: kind = "BlobStorage"; break;
                    case TStorageRead::ExternalStorage: kind = "ExternalStorage"; break;
                    case TStorageRead::MemoryOrFake: kind = "FakeExternal"; break;
                    default: break;
                }
                const auto bytes = Record.HasReadBytes() && !ReadBytesOverflow
                    ? std::optional<ui64>(Record.GetReadBytes()) : std::nullopt;
                AddSourceRead(*Record.MutableDataSourceReads(), kind, details.GetStorageTier(), bytes, details.GetDataKind());
                Collector->AddStorageRead(kind, details.GetStorageTier(), bytes, details.GetDataKind());
            }
            Record.SetOutcome(outcome);
            finalized.Swap(&Record);
            // Captured callbacks can create children after this operation has finished.
            Record.SetStepKind(finalized.GetStepKind());
            if (finalized.HasParentStepId()) {
                Record.SetParentStepId(finalized.GetParentStepId());
            }
            if (finalized.HasMetaStep()) {
                Record.SetMetaStep(finalized.GetMetaStep());
            }
            if (finalized.HasCanonicalPath()) {
                Record.SetCanonicalPath(finalized.GetCanonicalPath());
            }
            Record.MutableIndexDescriptors()->CopyFrom(finalized.GetIndexDescriptors());
            if (finalized.HasApplicationOrder()) {
                Record.SetApplicationOrder(finalized.GetApplicationOrder());
            }
            Finalized = true;
        }
        Collector->Publish(std::move(finalized));
    }

    void TOperation::AddReadBytes(ui64 bytes) {
        std::lock_guard guard(Mutex);
        if (Finalized) {
            return;
        }
        if (bytes > std::numeric_limits<ui64>::max() - Record.GetReadBytes()) {
            ReadBytesOverflow = true;
            Record.SetReadBytes(std::numeric_limits<ui64>::max());
        } else {
            Record.SetReadBytes(Record.GetReadBytes() + bytes);
        }
    }

    std::optional<ui64> TOperation::GetParentStepId() {
        std::lock_guard guard(Mutex);
        return Record.HasParentStepId() ? std::optional<ui64>(Record.GetParentStepId()) : std::nullopt;
    }

    void TOperation::AddDataSourceRead(TStringBuf kind, TStringBuf name, std::optional<ui64> bytes,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind, bool shared, bool includeInStorage) {
        std::lock_guard guard(Mutex);
        if (Finalized) {
            return;
        }
        AddSourceRead(*Record.MutableDataSourceReads(), kind, name, bytes, dataKind, shared);
        if (includeInStorage) {
            Collector->AddStorageRead(kind, name, bytes, dataKind, shared);
        }
    }

    void TCollector::AddStorageRead(TStringBuf kind, TStringBuf name, std::optional<ui64> bytes,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind, bool shared) {
        std::lock_guard guard(Mutex);
        if (!AddSourceRead(StorageReads, kind, name, bytes, dataKind, shared)) {
            Truncated = true;
        }
    }

    void TCollector::SetTablePath(TString path) {
        std::lock_guard guard(Mutex);
        TablePath = std::move(path);
    }

    TString TCollector::GetTablePath() {
        std::lock_guard guard(Mutex);
        return TablePath;
    }

    void TCollector::AddCacheRead(ui64 bytes, bool cached) {
        std::lock_guard guard(Mutex);
        if (!CacheStats) {
            CacheStats.emplace();
        }
        CacheStats->SetCompletedReadRanges(CacheStats->GetCompletedReadRanges() + 1);
        CacheStats->SetReturnedBytes(CacheStats->GetReturnedBytes() + bytes);
        CacheStats->SetCacheServedReadRanges(CacheStats->GetCacheServedReadRanges() + (cached ? 1 : 0));
        CacheStats->SetCacheServedBytes(CacheStats->GetCacheServedBytes() + (cached ? bytes : 0));
    }

    void RecordDataSourceRead(TStringBuf kind, TStringBuf name, std::optional<ui64> bytes,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind, const TCapture& capture, bool shared, bool includeInStorage) {
        if (!capture.Collector) {
            return;
        }
        auto operation = capture.Collector->Start(TStep::Reader, shared ? "SHARED_READ" : "READ_SOURCE", TString(kind), capture.Operation);
        if (operation) {
            TScope sourceScope(operation, capture.Collector);
            operation->AddDataSourceRead(kind, name, bytes, dataKind, shared, includeInStorage);
            operation->Finish();
        } else if (includeInStorage) {
            capture.Collector->AddStorageRead(kind, name, bytes, dataKind, shared);
        }
    }

    void RecordNonOwnerDataSourceRead(TStringBuf kind, TStringBuf name,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind,
        const TCapture& request, const std::shared_ptr<TOperation>& owner) {
        if (!request.Collector) {
            return;
        }
        const bool sameCollector = owner && owner->GetCollector() == request.Collector;
        const auto requestedParent = request.Operation ? std::optional<ui64>(request.Operation->GetStepId()) : std::nullopt;
        if (sameCollector && owner->GetParentStepId() == requestedParent) {
            return;
        }
        // shortcut: payload shared by collectors needs an unknown allocation until the backend reports per-read ownership.
        RecordDataSourceRead(kind, name, std::nullopt, dataKind, request, true, !sameCollector);
    }

    void TCollector::BindScanId(TString id) {
        std::lock_guard guard(Mutex);
        StatsScanId = std::move(id);
    }

    std::shared_ptr<TOperation> TCollector::Start(TStep::EStepKind kind, TString name, TString operation,
                                                  const std::shared_ptr<TOperation>& parent, std::optional<ui64> sourceId) {
        const ui64 id = ++NextId;
        if (id > MaxRecords) {
            std::lock_guard guard(Mutex);
            Truncated = true;
            return {};
        }
        TStep record;
        record.SetStepId(id);
        record.SetStepKind(kind);
        record.SetStepName(std::move(name));
        record.SetOperation(std::move(operation));
        record.SetIntegralExecutionDuration(0);
        record.SetIntegralWaitDuration(0);
        const TString metaStep = DefaultMetaStep(kind);
        if (metaStep) {
            record.SetMetaStep(metaStep);
            record.SetCanonicalPath("");
        }
        if (parent && parent->GetCollector().get() == this) {
            record.SetParentStepId(parent->GetStepId());
            const bool overview = kind == TStep::Reader && record.GetStepName() == "PROGRAM_EXECUTION";
            if (!overview) {
                parent->InheritContext(record);
            }
        }
        if (!sourceId && parent && parent->GetCollector().get() == this) {
            sourceId = parent->GetSourceId();
        }
        if (sourceId) {
            record.SetSourceId(*sourceId);
        }
        ++ActiveOperations;
        return std::shared_ptr<TOperation>(new TOperation(shared_from_this(), std::move(record)));
    }

    void TCollector::Publish(TStep&& record) {
        const ui64 size = record.ByteSizeLong();
        std::lock_guard guard(Mutex);
        --ActiveOperations;
        if (size > MaxBytes - PublishedBytes) {
            Truncated = true;
            return;
        }
        PublishedBytes += size;
        Completed.emplace_back(std::move(record));
    }

    void TCollector::SetReaderKind(TString value) {
        std::lock_guard guard(Mutex);
        ReaderKind = std::move(value);
    }

    TString TCollector::GetReaderKind() {
        std::lock_guard guard(Mutex);
        return ReaderKind;
    }

    std::unique_ptr<NKqpProto::TColumnShardScanStats> TCollector::Drain(bool finished) {
        auto report = std::make_unique<NKqpProto::TColumnShardScanStats>();
        std::lock_guard guard(Mutex);
        report->SetStatsScanId(StatsScanId);
        report->SetTabletId(TabletId);
        report->SetStorageStatsVersion(1);
        report->MutableStorageReads()->CopyFrom(StorageReads);
        if (TablePath) {
            report->SetTablePath(TablePath);
        }
        if (ReaderKind) {
            report->SetReaderKind(ReaderKind);
        }
        const ui64 pending = ActiveOperations.load();
        report->SetPendingOperations(pending);
        report->SetFinished(finished && pending == 0);
        if (CacheStats) {
            report->MutableCacheStats()->CopyFrom(*CacheStats);
        }
        report->SetTruncated(Truncated);
        for (auto& step : Completed) {
            report->AddSteps()->Swap(&step);
        }
        Completed.clear();
        return report;
    }

    const std::shared_ptr<TOperation>& Current() {
        return ActiveOperation ? ActiveOperation : ContextOperation;
    }

    const std::shared_ptr<TCollector>& CurrentCollector() {
        return ActiveCollector;
    }

    TScope::TScope(std::shared_ptr<TOperation> operation)
        : TScope(std::move(operation), CurrentCollector())
    {
    }

    TCapture::TCapture()
        : Operation(Current())
        , Collector(CurrentCollector())
    {
    }

    TCapture::TCapture(std::shared_ptr<TOperation> operation, std::shared_ptr<TCollector> collector)
        : Operation(std::move(operation))
        , Collector(Operation ? Operation->GetCollector() : std::move(collector))
    {
    }

    TScope::TScope(const TCapture& capture)
    {
        PreviousOwned = std::move(OwnedOperation);
        OwnedOperation = PreviousOwned;
        Previous = std::move(ActiveOperation);
        PreviousCollector = std::move(ActiveCollector);
        PreviousContext = std::move(ContextOperation);
        ContextOperation = capture.Operation;
        ActiveCollector = capture.Operation ? capture.Operation->GetCollector() : capture.Collector;
    }

    TScope::TScope(std::shared_ptr<TOperation> operation, std::shared_ptr<TCollector> collector)
        : Operation(std::move(operation))
    {
        if (Operation && OwnedOperation == Operation && Current() == Operation) {
            Nested = true;
            return;
        }
        PreviousOwned = std::move(OwnedOperation);
        if (Operation && PreviousOwned && Operation != PreviousOwned) {
            PreviousOwned->PauseOwnedActive();
        }
        OwnedOperation = Operation ? Operation : PreviousOwned;
        Previous = std::move(ActiveOperation);
        PreviousCollector = std::move(ActiveCollector);
        PreviousContext = std::move(ContextOperation);
        ActiveOperation = Operation;
        ActiveCollector = Operation ? Operation->GetCollector() : std::move(collector);
        if (Operation && Operation != PreviousOwned) {
            WasWaiting = Operation->Resume();
            OwnsActivation = true;
        }
    }

    TScope::~TScope() {
        if (Nested) {
            return;
        }
        if (OwnsActivation) {
            Operation->StopActive();
            if (WasWaiting) {
                Operation->Suspend();
            }
        }
        if (Operation && PreviousOwned && Operation != PreviousOwned) {
            PreviousOwned->ResumeOwnedActive();
        }
        OwnedOperation = std::move(PreviousOwned);
        ContextOperation = std::move(PreviousContext);
        ActiveOperation = std::move(Previous);
        ActiveCollector = std::move(PreviousCollector);
    }

} // namespace NKikimr::NScanStats
