#pragma once

#include <ydb/core/tx/datashard/buffer_data.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tx/datashard/upload_stats.h>
#include <ydb/core/tx/tx_proxy/erase_rows.h>
#include <ydb/core/base/scan_admission.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/library/actors/core/log.h>
#include <utility>

namespace NKikimr::NDataShard {
using namespace NTableIndex;

class TBatchRowsUploader
{
public:
    enum class EDestinationKind {
        Upload,
        Erase,
    };

    // Accumulates keys to erase from a single table, mirroring TBufferData.
    class TEraseBuffer
    {
    public:
        void AddKey(TSerializedCellVec key) {
            BufferBytes += key.GetBuffer().size();
            Keys->push_back(std::move(key));
        }

        void AddKey(TConstArrayRef<TCell> rowKey) {
            AddKey(TSerializedCellVec{rowKey});
        }

        bool IsEmpty() const {
            return Keys->empty();
        }

        ui64 GetKeys() const {
            return Keys->size();
        }

        ui64 GetBufferBytes() const {
            return BufferBytes;
        }

        std::shared_ptr<NTxProxy::TEraseKeys> GetKeysData() const {
            return Keys;
        }

        void FlushTo(TEraseBuffer& other) {
            Y_ENSURE(this != &other);
            Y_ENSURE(other.IsEmpty());
            other.Keys.swap(Keys);
            other.BufferBytes = std::exchange(BufferBytes, 0);
        }

        void Clear() {
            Keys->clear();
            BufferBytes = 0;
        }

        bool HasReachedLimits(const TIndexBuildScanSettings& scanSettings) const {
            return Keys->size() > scanSettings.GetMaxBatchRows()
                || BufferBytes > scanSettings.GetMaxBatchBytes();
        }

    private:
        std::shared_ptr<NTxProxy::TEraseKeys> Keys = std::make_shared<NTxProxy::TEraseKeys>();
        ui64 BufferBytes = 0;
    };

private:
    struct TUploadAttempt {
        std::shared_ptr<void> Reservation;
        bool Completed = false;
        explicit TUploadAttempt(std::shared_ptr<void> reservation)
            : Reservation(std::move(reservation))
        {}
    };

    struct TDestination {
        TBufferData Buffer;
        TEraseBuffer EraseBuffer;
        TString Table;
        EDestinationKind Kind = EDestinationKind::Upload;
        std::shared_ptr<NTxProxy::TUploadTypes> Types;
        // Last source-table key whose entries for this destination have been
        // durably uploaded. Updated when an upload of this destination acks
        // successfully. Used by GetMinFlushedKey() to compute a cross-
        // destination safe checkpoint for the scan-level LastKeyAck.
        //
        // The accompanying FlushedSeqNo is a monotonic counter sampled at
        // the same moment FlushedKey is sampled. Since the scan reads
        // source rows in ascending key order, capture order == key order;
        // we therefore use SeqNo to compare keys cheaply and type-
        // independently across destinations.
        TSerializedCellVec FlushedKey;
        ui64 FlushedSeqNo = 0;
        // Snapshot of the scan's current source key captured at the moment
        // this destination's buffer was claimed for the in-flight upload.
        // Promoted to FlushedKey when that upload acks.
        TSerializedCellVec PendingFlushKey;
        ui64 PendingFlushSeqNo = 0;
        bool HasFlushedOnce = false;

        operator bool() const {
            return Kind == EDestinationKind::Erase
                ? !EraseBuffer.IsEmpty()
                : !Buffer.IsEmpty();
        }

        bool HasReachedLimits(const TIndexBuildScanSettings& scanSettings) const {
            return Kind == EDestinationKind::Erase
                ? EraseBuffer.HasReachedLimits(scanSettings)
                : Buffer.HasReachedLimits(scanSettings);
        }
    };

public:
    TBatchRowsUploader(const TString& database, const TIndexBuildScanSettings& scanSettings)
        : Database(database)
        , ScanSettings(scanSettings)
    {}

    TBufferData* AddDestination(TString table, std::shared_ptr<NTxProxy::TUploadTypes> types) {
        auto& dst = Destinations[std::make_pair(table, EDestinationKind::Upload)];
        dst.Table = std::move(table);
        dst.Types = std::move(types);
        return &dst.Buffer;
    }

    // Destinations of different kinds are stored separately, so the same table
    // may be used both for uploading rows and for erasing keys.
    TEraseBuffer* AddEraseDestination(TString table) {
        auto& dst = Destinations[std::make_pair(table, EDestinationKind::Erase)];
        dst.Table = std::move(table);
        dst.Kind = EDestinationKind::Erase;
        return &dst.EraseBuffer;
    }

    bool Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        return HandleResponse(EDestinationKind::Upload, ev->Sender, ev->Get()->Status, ev->Get()->Issues);
    }

    bool Handle(TEvTxUserProxy::TEvEraseRowsResponse::TPtr& ev) {
        return HandleResponse(EDestinationKind::Erase, ev->Sender, ev->Get()->Status, ev->Get()->Issues);
    }

    bool ShouldWaitUpload()
    {
        bool hasReachedLimit = false;
        for (auto& [_, dst] : Destinations) {
            if (dst.HasReachedLimits(ScanSettings)) {
                hasReachedLimit = true;
                break;
            }
        }
        if (!hasReachedLimit) {
            return false;
        }

        if (Uploading) {
            return true;
        }
        for (auto& [_, dst] : Destinations) {
            if (TryUpload(dst, true /* by limit */)) {
                break;
            }
        }

        hasReachedLimit = false;
        for (auto& [_, dst] : Destinations) {
            if (dst.HasReachedLimits(ScanSettings)) {
                hasReachedLimit = true;
                break;
            }
        }
        return hasReachedLimit;
    }

    std::optional<TDuration> GetRetryAfter() const {
        if (RetryCount < ScanSettings.GetMaxBatchRetries() && UploadStatus.IsRetriable()) {
            return GetRetryWakeupTimeoutBackoff(RetryCount);
        }
        return {};
    }

    void RetryUpload()
    {
        if (!Uploading) {
            return;
        }

        if (WaitForPreviousAttempt()) {
            return;
        }
        if (!UploadStatus.IsNone() && !UploadStatus.IsSuccess()) {
            ++RetryCount;
        }
        StartUploadInternal();
    }

    bool CanFinish() {
        if (Uploading) {
            return false;
        }

        for (auto& [_, dst] : Destinations) {
            if (TryUpload(dst, false /* not by limit */)) {
                return false;
            }
        }

        return true;
    }

    bool AllFlushed() const {
        if (Uploading) {
            return false;
        }
        for (const auto& [_, dst] : Destinations) {
            if (dst) {
                return false;
            }
        }
        return true;
    }

    ui64 GetUploadBytes() const {
        return UploadBytes;
    }

    void AddIssue(const std::exception& exc) {
        UploadStatus.Issues.AddIssue(NYql::TIssue(TStringBuilder()
            << "Scan failed " << exc.what()));
    }

    template<typename TResponse>
    void Finish(TResponse& response, NTable::EStatus status) {
        if (UploaderId) {
            TlsActivationContext->Send(new IEventHandle(UploaderId, TActorId(), new TEvents::TEvPoison));
            UploaderId = {};
        }

        response.MutableMeteringStats()->SetUploadRows(UploadRows);
        response.MutableMeteringStats()->SetUploadBytes(UploadBytes);

        if (status == NTable::EStatus::Exception) {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        } else if (status != NTable::EStatus::Done) {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else if (UploadStatus.IsNone() || UploadStatus.IsSuccess()) {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
            if (UploadStatus.IsNone()) {
                UploadStatus.Issues.AddIssue(NYql::TIssue("Shard or requested range is empty"));
            }
        } else {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        }

        if constexpr (requires { response.GetRequiredRowBytes(); }) {
            if (response.GetRequiredRowBytes()) {
                response.SetStatus(NKikimrIndexBuilder::ABORTED);
            }
        }
        if (CleanupTimedOut) {
            response.SetStatus(NKikimrIndexBuilder::ABORTED);
            if constexpr (requires { response.SetRetryAfterMs(1000); }) {
                response.SetRetryAfterMs(1000);
            }
        }
        NYql::IssuesToMessage(UploadStatus.Issues, response.MutableIssues());
    }

    const TUploadStatus& GetUploadStatus() const {
        return UploadStatus;
    }

    bool IsSuccess() const {
        return UploadStatus.IsSuccess();
    }

    void SetResourceReservation(std::shared_ptr<void> reservation,
        std::function<std::shared_ptr<void>(ui64)> retain = {}) {
        ResourceReservation = std::move(reservation);
        RetainResource = std::move(retain);
    }

    void SetOwner(const TActorId& owner) {
        Owner = owner;
    }

    // The scan must call this whenever it processes a new source row, before
    // (or right at) AddRow on any destination buffer. The uploader uses the
    // most recent value to snapshot a per-destination "PendingFlushKey" at
    // each TryUpload, which becomes the destination's FlushedKey on a
    // successful upload ack. See GetMinFlushedKey.
    void SetCurrentSourceKey(TSerializedCellVec key) {
        CurrentSourceKey = std::move(key);
    }

    // Returns the latest source-table key K such that, for every destination
    // buffer, all entries this destination would produce for source rows
    // <= K are durably persisted via the upload pipeline. Returns nullopt
    // while any destination has not yet completed a successful upload — in
    // that case there is no cross-destination safe checkpoint and the caller
    // must NOT advance LastKeyAck on the scan-level response.
    //
    // The min across destinations is the safe checkpoint because each
    // destination's FlushedKey reflects only the rows whose entries it has
    // already uploaded; rows past that key may still have entries buffered
    // and would be lost if the scan is resumed past that key.
    std::optional<TSerializedCellVec> GetMinFlushedKey() const {
        if (Destinations.empty()) {
            return std::nullopt;
        }
        const TDestination* minDst = nullptr;
        for (const auto& [_, dst] : Destinations) {
            if (!dst.HasFlushedOnce) {
                return std::nullopt;
            }
            if (!minDst || dst.FlushedSeqNo < minDst->FlushedSeqNo) {
                minDst = &dst;
            }
        }
        return minDst->FlushedKey;
    }

    TString Debug() const {
        TStringBuilder result;

        if (Uploading) {
            result << "UploadTable: " << Uploading.Table;
            if (Uploading.Kind == EDestinationKind::Erase) {
                result << " EraseBuf size: " << Uploading.EraseBuffer.GetBufferBytes()
                    << " Keys: " << Uploading.EraseBuffer.GetKeys();
            } else {
                result << " UploadBuf size: " << Uploading.Buffer.GetBufferBytes();
            }
            result << " RetryCount: " << RetryCount;
        }

        return result;
    }

private:
    bool HandleResponse(EDestinationKind kind, const TActorId& sender,
        Ydb::StatusIds::StatusCode statusCode, const NYql::TIssues& issues)
    {
        Y_ENSURE(UploaderId == sender, "Mismatch"
            << " Uploader: " << UploaderId.ToString()
            << " Sender: " << sender.ToString());
        Y_ENSURE(Uploading);
        Y_ENSURE(Uploading.Kind == kind, "Mismatch"
            << " Uploading kind: " << static_cast<int>(Uploading.Kind)
            << " Response kind: " << static_cast<int>(kind));

        UploaderId = {};

        UploadStatus.StatusCode = statusCode;
        UploadStatus.Issues = issues;
        if (!UploadStatus.IsSuccess()) {
            return false;
        }

        if (auto attempt = PreviousAttempt.lock()) {
            attempt->Completed = true;
        }

        if (kind == EDestinationKind::Erase) {
            UploadBytes += Uploading.EraseBuffer.GetBufferBytes();
            Uploading.EraseBuffer.Clear();
        } else {
            UploadRows += Uploading.Buffer.GetRows();
            UploadBytes += Uploading.Buffer.GetRowCellBytes();
            Uploading.Buffer.Clear();
        }
        RetryCount = 0;

        if (auto it = Destinations.find(std::make_pair(Uploading.Table, Uploading.Kind)); it != Destinations.end()) {
            it->second.FlushedKey = it->second.PendingFlushKey;
            it->second.FlushedSeqNo = it->second.PendingFlushSeqNo;
            it->second.HasFlushedOnce = true;
        }

        for (auto& [_, dst] : Destinations) {
            if (TryUpload(dst, true /* by limit */)) {
                break;
            }
        }

        return true;
    }

    bool TryUpload(TDestination& destination, bool byLimit) {
        if (Y_UNLIKELY(Uploading)) {
            // already uploading something
            return true;
        }

        if (destination && (!byLimit || destination.HasReachedLimits(ScanSettings))) {
            Uploading.Table = destination.Table;
            Uploading.Kind = destination.Kind;
            Uploading.Types = destination.Types;
            if (destination.Kind == EDestinationKind::Erase) {
                destination.EraseBuffer.FlushTo(Uploading.EraseBuffer);
            } else {
                destination.Buffer.FlushTo(Uploading.Buffer);
            }
            // Capture the scan's current source key as the boundary that the
            // upload of this destination's batch covers — i.e. all entries
            // for source rows <= CurrentSourceKey produced by this scan and
            // belonging to this destination are now committed to the upload.
            destination.PendingFlushKey = CurrentSourceKey;
            destination.PendingFlushSeqNo = ++CurrentSourceSeqNo;
            StartUploadInternal();
            return true;
        }

        return false;
    }

    void StartUploadInternal() {
        Y_ENSURE(Uploading);
        Y_ENSURE(!UploaderId);
        Y_ENSURE(Owner);

        if (Uploading.Kind == EDestinationKind::Erase) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::BUILD_INDEX, "Starting batch row erase",
                {"debug", Debug()});

            Y_ENSURE(!Uploading.EraseBuffer.IsEmpty());
            auto actor = NTxProxy::CreateEraseRowsInternal(
                Owner, Database, Uploading.Table, Uploading.EraseBuffer.GetKeysData(),
                true /*writeToTableShadow*/,
                true /*writeToPrivateTable*/,
                true /*writeToIndexImplTable*/);
            UploaderId = TlsActivationContext->Register(actor, Owner, TMailboxType::HTSwap, AppData()->BatchPoolId);
        } else {
            StartUploadRowsInternal();
        }
    }

    void StartUploadRowsInternal() {
        YDB_LOG_DEBUG_COMP(NKikimrServices::BUILD_INDEX, "Starting batch row upload",
            {"debug", Debug()});

        Y_ENSURE(Uploading);
        Y_ENSURE(!Uploading.Buffer.IsEmpty());
        Y_ENSURE(!UploaderId);
        Y_ENSURE(Owner);
        // A timed-out child may leave a protobuf request in a local mailbox.
        // Wait for its allocation owners before making another payload copy.
        if (WaitForPreviousAttempt()) {
            return;
        }
        std::shared_ptr<void> attempt;
        if (ResourceReservation) {
            // Include the canonical rows, protobuf payload, and row/container
            // overhead still owned by this attempt after the scan stops.
            ui64 bytes = 2 * Uploading.Buffer.GetBufferBytes()
                + Uploading.Buffer.GetRows() * 256 + 4_KB;
            for (const auto& [key, _] : *Uploading.Buffer.GetRowsData()) {
                bytes += key.GetCells().size() * sizeof(TCell) * 2;
            }
            for (const auto& [name, type] : *Uploading.Types) {
                bytes += 2 * (name.size() + type.SpaceUsedLong() + 64);
            }
            bytes += 2 * (Database.size() + Uploading.Table.size());
            auto token = std::make_shared<TUploadAttempt>(RetainResource ? RetainResource(bytes) : ResourceReservation);
            PreviousAttempt = token;
            attempt = std::move(token);
        }
        auto actor = NTxProxy::CreateUploadRowsInternal(
            Owner, Database, Uploading.Table, Uploading.Types, Uploading.Buffer.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow,
            true /*writeToPrivateTable*/,
            true /*writeToIndexImplTable*/, false, 0, TBackoff(0), std::move(attempt));

        UploaderId = TlsActivationContext->Register(actor, Owner, TMailboxType::HTSwap, AppData()->BatchPoolId);
    }

    bool WaitForPreviousAttempt() {
        // Completion may race actor/event destruction in another mailbox.
        // Allow one completed attempt to retire while the next one starts;
        // two retained attempts remain the independent bound on overlap.
        if (const auto attempt = PreviousAttempt.lock(); attempt && attempt->Completed && RetiringAttempt.expired()) {
            RetiringAttempt = attempt;
            PreviousAttempt.reset();
        }
        if (PreviousAttempt.expired()) {
            CleanupWaitStarted.reset();
            return false;
        }
        const auto now = TActivationContext::Now();
        if (!CleanupWaitStarted) {
            CleanupWaitStarted = now;
        }
        // Keep the retained payload charged, but do not leave its scan waiting
        // forever for a disconnected pipe or a stalled destination mailbox.
        if (now >= *CleanupWaitStarted + TDuration::Minutes(1)) {
            CleanupTimedOut = true;
            ythrow yexception() << "Timed out waiting for previous upload payload cleanup";
        }
        const auto attempt = PreviousAttempt.lock();
        const auto delay = attempt && attempt->Completed ? TDuration::MilliSeconds(1) : TDuration::MilliSeconds(100);
        TActivationContext::ActorContextFor(Owner).Schedule(delay, new TEvents::TEvWakeup());
        return true;
    }

private:
    std::shared_ptr<void> ResourceReservation;
    std::function<std::shared_ptr<void>(ui64)> RetainResource;
    std::weak_ptr<TUploadAttempt> PreviousAttempt;
    std::weak_ptr<TUploadAttempt> RetiringAttempt;
    std::optional<TInstant> CleanupWaitStarted;
    bool CleanupTimedOut = false;
    const TString Database;
    const TIndexBuildScanSettings ScanSettings;
    TActorId Owner;

    TMap<std::pair<TString, EDestinationKind>, TDestination> Destinations;
    TActorId UploaderId = {};
    TDestination Uploading;
    TUploadStatus UploadStatus = {};
    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;
    ui32 RetryCount = 0;

    // Updated by the scan via SetCurrentSourceKey before AddRow on any
    // destination. Snapshotted into per-destination PendingFlushKey at
    // each TryUpload.
    TSerializedCellVec CurrentSourceKey;
    ui64 CurrentSourceSeqNo = 0;
};

inline void StartScan(TDataShard* dataShard, TAutoPtr<NTable::IScan>&& scan, ui64 id,
    TScanRecord::TSeqNo seqNo, std::optional<TRowVersion> rowVersion, ui32 tableId)
{
    auto& scanManager = dataShard->GetScanManager();

    if (const auto* recCard = scanManager.Get(id)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            return;
        }

        for (auto scanId : recCard->ScanIds) {
            dataShard->CancelScan(tableId, scanId);
        }
        scanManager.Drop(id);
    }

    TScanOptions scanOpts;
    if (rowVersion) {
        scanOpts.SetSnapshotRowVersion(*rowVersion);
    }
    scanOpts.SetResourceBroker("build_index", 10);
    const auto scanId = dataShard->QueueScan(tableId, std::move(scan), 0, scanOpts);
    scanManager.Set(id, seqNo).push_back(scanId);
}

inline ui64 CheckedScanMemoryAdd(ui64 a, ui64 b) {
    Y_ENSURE(b <= Max<ui64>() - a, "Index scan memory estimate overflow");
    return a + b;
}

inline ui64 CheckedScanMemoryMul(ui64 a, ui64 b) {
    Y_ENSURE(!b || a <= Max<ui64>() / b, "Index scan memory estimate overflow");
    return a * b;
}

class TVectorIndexScan : public NTable::IScan {
    std::shared_ptr<void> SnapshotReference;
public:
    void HoldSnapshotReference(std::shared_ptr<void> reference) { SnapshotReference = std::move(reference); }
    virtual void SetRowBytesLimit(ui64 limit) { RowBytesLimit = limit; }
    ui64 GetRowBytesLimit() const { return RowBytesLimit; }

protected:
    template<class TResponse>
    bool CheckRowBytes(TResponse& response, TArrayRef<const TCell> key, TArrayRef<const TCell> row) const {
        return !RowBytesLimit || CheckRowBytes(response, CountRowCellBytes(key, row), RowBytesLimit);
    }

    template<class TResponse>
    bool CheckRowBytes(TResponse& response, ui64 bytes, ui64 limit) const {
        if (!RowBytesLimit || bytes <= limit) {
            return true;
        }
        // Stop before buffering this row. The sender retries with a larger
        // reservation, so a legal source row is not a permanent build error.
        response.SetRequiredRowBytes(bytes);
        response.SetRetryAfterMs(100);
        response.AddIssues()->set_message("Index scan needs a larger row memory reservation");
        return false;
    }

private:
    ui64 RowBytesLimit = 0;
};

template<class TRequest>
ui64 EstimateBuildIndexScanMemory(const TRequest& request, const TUserTable& table, ui64 rowLimit, ui64 requestMemory) {
    ui64 memory = CheckedScanMemoryAdd(4_MB, CheckedScanMemoryMul(requestMemory, 2));
    ui64 clusters = 1;
    if constexpr (requires { request.ClustersSize(); }) {
        clusters = request.ClustersSize();
    } else if constexpr (requires { request.GetK(); }) {
        clusters = request.GetK();
    }
    if constexpr (requires { request.GetSettings(); }) {
        const ui64 dimensions = request.GetSettings().vector_dimension();
        ui64 vectorBytes = CheckedScanMemoryAdd(dimensions ? CheckedScanMemoryMul(dimensions, 4) : rowLimit, 256);
        if constexpr (requires { request.ColumnsSize(); }) {
            vectorBytes = CheckedScanMemoryAdd(vectorBytes, CheckedScanMemoryMul(request.ColumnsSize(), 32));
            vectorBytes = Max(vectorBytes, CheckedScanMemoryAdd(request.GetRequiredRowBytes(),
                CheckedScanMemoryAdd(128, CheckedScanMemoryMul(request.ColumnsSize(), 32))));
            // Samples coexist with the response protobuf and its encoded copy.
            vectorBytes = CheckedScanMemoryMul(vectorBytes, 3);
        }
        memory = CheckedScanMemoryAdd(memory, CheckedScanMemoryMul(clusters, vectorBytes));
        if constexpr (!std::is_same_v<TRequest, NKikimrTxDataShard::TEvReshuffleKMeansRequest> &&
                      !std::is_same_v<TRequest, NKikimrTxDataShard::TEvSampleKRequest>) {
            // Auto-detection must also reserve the largest accumulator dimension
            // accepted by ValidateSettings, including compact bit vectors.
            const ui64 scratchDimensions = dimensions ? dimensions : 65536;
            memory = CheckedScanMemoryAdd(memory, CheckedScanMemoryMul(CheckedScanMemoryMul(clusters, scratchDimensions), 8));
        }
    }
    if constexpr (requires { request.GetOutputName(); }) {
        TIndexBuildScanSettings settings = request.GetScanSettings();
        ui64 destinations = 1;
        ui64 prefixCache = 0;
        if constexpr (requires { request.GetLevelName(); }) {
            ++destinations;
            prefixCache = 1;
        }
        if constexpr (requires { request.GetPrefixName(); }) {
            ++destinations;
        }
        if constexpr (requires { request.GetSkipFirstKey(); }) {
            prefixCache += 3; // first/last key and current filter group
        }
        ui64 overlap = 1;
        if constexpr (requires { request.GetOverlapClusters(); }) {
            overlap = Max<ui64>(1, request.GetOverlapClusters());
        }
        const ui64 rowFanout = CheckedScanMemoryMul(rowLimit, overlap);
        const ui64 buffers = destinations + 2 + prefixCache; // in-flight and protobuf copy
        const ui64 payload = CheckedScanMemoryMul(CheckedScanMemoryAdd(settings.GetMaxBatchBytes(), rowFanout), 2);
        memory = CheckedScanMemoryAdd(memory, CheckedScanMemoryMul(buffers, payload));
        const ui64 rowMetadata = CheckedScanMemoryAdd(128, CheckedScanMemoryMul(table.KeyColumnIds.size() + table.Columns.size(), 32));
        ui64 rows = ui64(settings.GetMaxBatchRows()) + 1;
        if constexpr (requires { request.GetSkipFirstKey(); }) {
            rows = CheckedScanMemoryAdd(rows, CheckedScanMemoryMul(overlap, overlap));
        }
        memory = CheckedScanMemoryAdd(memory, CheckedScanMemoryMul(
            CheckedScanMemoryMul(rows, rowMetadata), buffers + 1));
    }
    // Bounded read-ahead plus the currently materialized row.
    return CheckedScanMemoryAdd(memory, CheckedScanMemoryAdd(1_MB, rowLimit));
}

template<class TRequest>
ui64 FitBuildIndexScanMemory(TRequest& request, const TUserTable& table, ui64 limit, ui64& rowLimit, ui64 requestMemory) {
    if (!limit) {
        return 0;
    }
    ui64 naturalRow = 64_KB;
    if constexpr (requires { request.GetSettings(); }) {
        if (request.GetSettings().vector_dimension()) {
            naturalRow = CheckedScanMemoryAdd(CheckedScanMemoryMul(request.GetSettings().vector_dimension(), 4),
                CheckedScanMemoryAdd(512, CheckedScanMemoryMul(table.Columns.size() + table.KeyColumnIds.size(), 32)));
        }
    }
    const ui64 minimumRow = Max(request.GetRequiredRowBytes(), Min(rowLimit, naturalRow));
    for (;;) {
        const ui64 memory = EstimateBuildIndexScanMemory(request, table, rowLimit, requestMemory);
        if (memory <= limit) {
            return memory;
        }
        // Preserve a useful per-row allowance while reducing batch buffers.
        // Keep moderate batches before resorting to smaller rows, rather than
        // reducing both to tiny values on every unsuccessful estimate.
        if constexpr (requires { request.MutableScanSettings(); }) {
            auto& settings = *request.MutableScanSettings();
            if (settings.GetMaxBatchBytes() > 64_KB || settings.GetMaxBatchRows() > 128) {
                if (settings.GetMaxBatchBytes() > 64_KB) {
                    settings.SetMaxBatchBytes(Max<ui64>(64_KB, settings.GetMaxBatchBytes() / 2));
                }
                if (settings.GetMaxBatchRows() > 128) {
                    settings.SetMaxBatchRows(Max<ui32>(128, settings.GetMaxBatchRows() / 2));
                }
                continue;
            }
        }
        if (rowLimit > minimumRow) {
            rowLimit = Max(minimumRow, rowLimit / 2);
            continue;
        }
        if constexpr (requires { request.MutableScanSettings(); }) {
            auto& settings = *request.MutableScanSettings();
            if (settings.GetMaxBatchBytes() || settings.GetMaxBatchRows() > 1) {
                settings.SetMaxBatchBytes(settings.GetMaxBatchBytes() / 2);
                settings.SetMaxBatchRows(Max<ui32>(1, settings.GetMaxBatchRows() / 2));
                continue;
            }
        }
        return 0;
    }
}

template<class TRequest, class TResponse>
void ReplyBuildIndexScanFailure(TDataShard* self, TActorId recipient, const TRequest& request,
    THolder<TResponse>& response, const TString& reason, bool retry)
{
    if (!response) {
        response = MakeHolder<TResponse>();
    }
    auto& record = response->Record;
    record.SetId(request.GetId());
    record.SetTabletId(self->TabletID());
    record.SetRequestSeqNoGeneration(request.GetSeqNoGeneration());
    record.SetRequestSeqNoRound(request.GetSeqNoRound());
    record.SetStatus(retry ? NKikimrIndexBuilder::ABORTED : NKikimrIndexBuilder::BAD_REQUEST);
    record.ClearRetryAfterMs();
    if (retry) {
        record.SetRetryAfterMs(1000);
    }
    record.AddIssues()->set_message(reason);
    TlsActivationContext->Send(new IEventHandle(recipient, self->SelfId(), response.Release()));
}

// The request and response are moved, never copied into an unbounded closure.
// Only admission-aware senders may receive the new retry hint; old nodes keep
// their existing protocol while rolling upgrades are in progress.
template<class TRequest, class TResponse, class TFactory>
void StartDeferredBuildIndexScan(TDataShard* self, typename TRequest::TPtr& ev,
    THolder<TResponse>&& response, TRowVersion rowVersion, ui32 tableId, TFactory makeScan)
{
    auto& request = ev->Get()->Record;
    if (request.GetAllowMemoryAdmission()) {
        if constexpr (requires { request.MutableScanSettings(); }) {
            auto* settings = request.MutableScanSettings();
            const ui64 cap = AppData()->DataShardConfig.GetBuildIndexBatchRowsLimit();
            settings->SetMaxBatchRows(Min<ui64>(Max<ui64>(1, settings->GetMaxBatchRows()), Max<ui64>(1, cap)));
            // Materialize the default before sizing the protobuf. Fitting may
            // reduce these scalar values, but cannot increase their encoding.
            settings->SetMaxBatchBytes(settings->GetMaxBatchBytes());
        }
    }
    const ui64 id = request.GetId();
    const TScanRecord::TSeqNo seqNo{request.GetSeqNoGeneration(), request.GetSeqNoRound()};
    const auto pathId = TPathId::FromProto(request.GetPathId());
    const auto tableIt = self->GetUserTables().find(pathId.LocalPathId);
    Y_ENSURE(tableIt != self->GetUserTables().end());
    const auto& table = *tableIt->second;
    if (!request.GetAllowMemoryAdmission()) {
        auto scan = makeScan(table, request, ev->Sender, std::move(response));
        StartScan(self, std::move(scan), id, seqNo, rowVersion, tableId);
        return;
    }

    auto reject = [&](const TString& reason, bool retry) {
        ReplyBuildIndexScanFailure(self, ev->Sender, request, response, reason, retry);
    };
    auto& manager = self->GetScanManager();
    if (const auto* previous = manager.Get(id)) {
        if (previous->SeqNo == seqNo) {
            return;
        }
        if (previous->SeqNo > seqNo) {
            reject("Stale index scan request sequence number", false);
            return;
        }
        for (ui64 serial : previous->ScanIds) {
            self->CancelScan(tableId, serial);
        }
        manager.Drop(id);
    }

    const auto& config = AppData()->DataShardConfig;
    ui64 rowLimit = Max(config.GetBuildIndexRowBytesLimit(), request.GetRequiredRowBytes());
    if (!rowLimit) {
        reject("BuildIndexRowBytesLimit must be nonzero for memory admission", false);
        return;
    }
    const ui64 serializedBytes = request.ByteSizeLong();
    const ui64 requestMemory = request.SpaceUsedLong();
    if (serializedBytes > config.GetBuildIndexRequestBytesLimit()) {
        reject("Index scan request exceeds BuildIndexRequestBytesLimit", false);
        return;
    }
    const ui64 requestBytes = CheckedScanMemoryAdd(requestMemory, serializedBytes);
    const ui64 pendingBytes = CheckedScanMemoryAdd(requestBytes, 4_KB);
    if (pendingBytes > config.GetBuildIndexPendingBytesLimit()) {
        reject("Index scan request exceeds BuildIndexPendingBytesLimit", false);
        return;
    }
    auto& admission = *AppData()->BuildIndexScanAdmission;
    const ui64 scanLimit = Min(config.GetBuildIndexScanMemoryLimit(),
        admission.GetActiveByteLimit(config.GetBuildIndexPendingBytesLimit()));
    // Batch sizes and the initial row allowance are upper bounds, not a reason
    // to retry forever when a smaller scan fits. Actual larger rows retain the
    // RequiredRowBytes floor discovered by the existing row-reservation retry.
    const ui64 memory = FitBuildIndexScanMemory(request, table, scanLimit, rowLimit, requestMemory);
    if (!memory) {
        const bool retry = FitBuildIndexScanMemory(request, table,
            config.GetBuildIndexScanMemoryLimit(), rowLimit, requestMemory) != 0;
        reject("Minimum index scan reservation exceeds the current memory budget", retry);
        return;
    }
    const ui64 pendingLimit = admission.GetByteLimit(config.GetBuildIndexPendingBytesLimit());
    if (pendingBytes > pendingLimit) {
        reject("Index scan request exceeds the current node pending byte budget", true);
        return;
    }
    if (!config.GetBuildIndexPendingScansLimit()) {
        reject("Node index scan pending queue capacity is zero", false);
        return;
    }
    auto counters = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "build_index_admission");
    auto lease = admission.TryAcquire(pendingBytes, config.GetBuildIndexPendingScansLimit(),
        config.GetBuildIndexPendingBytesLimit(), counters);
    if (!lease) {
        reject("Node index scan pending queue is full", true);
        return;
    }

    std::shared_ptr<void> snapshotReference;
    if constexpr (requires { request.HasSnapshotStep(); }) {
        if (request.HasSnapshotStep() || request.HasSnapshotTxId()) {
            const TSnapshotKey key(pathId, request.GetSnapshotStep(), request.GetSnapshotTxId());
            snapshotReference = self->AcquireIndexScanSnapshot(key);
            if (!snapshotReference) {
                reject("Requested snapshot is unavailable before scan admission", false);
                return;
            }
        }
    }

    struct TPending {
        // Destruction order: release the quota after freeing retained messages.
        std::shared_ptr<TScanAdmissionControl::TLease> Lease;
        typename TRequest::TPtr Event;
        THolder<TResponse> Response;
        std::shared_ptr<void> SnapshotReference;
        bool AcceptedSent = false;
        bool Replied = false;
    };
    auto pending = std::make_shared<TPending>();
    pending->Lease = lease;
    pending->Event = std::move(ev);
    pending->Response = std::move(response);
    pending->SnapshotReference = std::move(snapshotReference);
    const ui64 schemaVersion = table.GetTableSchemaVersion();
    auto fail = [self, pending, id, seqNo](const TString& reason, bool retry) {
        if (pending->Replied) {
            return;
        }
        pending->Replied = true;
        ReplyBuildIndexScanFailure(self, pending->Event->Sender, pending->Event->Get()->Record,
            pending->Response, reason, retry);
        if (const auto* scan = self->GetScanManager().Get(id); scan && scan->SeqNo == seqNo) {
            self->GetScanManager().Drop(id);
        }
    };
    TScanOptions options;
    options.SetResourceBroker("build_index", 10, memory);
    options.SetReadAhead(0, 1_MB);
    options.SetSnapshotRowVersion(rowVersion);
    options.PendingReservation = lease;
    options.QueueTimeout = TDuration::MilliSeconds(Max<ui32>(1, config.GetBuildIndexQueueTimeoutMs()));
    options.OnAdmissionFailure = fail;
    options.OnMemoryLimitExceeded = [quota = AppData()->BuildIndexScanAdmission,
        generation = admission.GetBrokerLimitGeneration()](ui64 limit) {
        quota->RestrictBrokerLimit(limit, generation);
    };
    options.OnAdmissionTimeout = [self, pending, memory, pendingBytes, quota = AppData()->BuildIndexScanAdmission]() {
        const auto& config = AppData()->DataShardConfig;
        if (memory > Min(config.GetBuildIndexScanMemoryLimit(),
                quota->GetActiveByteLimit(config.GetBuildIndexPendingBytesLimit())) ||
            pendingBytes > quota->GetByteLimit(config.GetBuildIndexPendingBytesLimit())) {
            // A smaller budget needs a new fitting pass, rather than waiting
            // forever with an oversized reservation left in the broker queue.
            return false;
        }
        if (std::exchange(pending->AcceptedSent, true)) {
            // Recheck changing budgets without accumulating heartbeat messages
            // in a pipe to an unavailable requester.
            return true;
        }
        auto accepted = MakeHolder<TResponse>();
        auto& record = accepted->Record;
        const auto& request = pending->Event->Get()->Record;
        record.SetId(request.GetId());
        record.SetTabletId(self->TabletID());
        record.SetRequestSeqNoGeneration(request.GetSeqNoGeneration());
        record.SetRequestSeqNoRound(request.GetSeqNoRound());
        record.SetStatus(NKikimrIndexBuilder::ACCEPTED);
        TlsActivationContext->Send(new IEventHandle(pending->Event->Sender, self->SelfId(), accepted.Release()));
        return true;
    };
    auto readVersion = std::make_shared<TRowVersion>(TRowVersion::Max());
    options.OnAdmissionReady = [self, readVersion, explicitSnapshot = bool(pending->SnapshotReference), chosen = false](
        TScanOptions& opts, std::unique_ptr<IEventHandle> resume) mutable {
        if (!chosen) {
            if (!explicitSnapshot) {
                opts.SetSnapshotRowVersion(self->GetMvccTxVersion(EMvccTxMode::ReadOnly));
            }
            const auto* version = std::get_if<TScanOptions::TSnapshotByRowVersion>(&opts.Snapshot);
            Y_ENSURE(version);
            *readVersion = version->RowVersion;
            chosen = true;
        }
        if (self->GetVolatileTxManager().HasVolatileTxsAtSnapshot(*readVersion)) {
            self->GetVolatileTxManager().AttachWaitingSnapshotEvent(*readVersion, std::move(resume));
            return false;
        }
        return true;
    };
    options.Factory = [self, pending, pathId, tableId, schemaVersion, rowLimit, memory, makeScan, fail](TScanOptions& opts) mutable -> TAutoPtr<NTable::IScan> {
        try {
            const auto found = self->GetUserTables().find(pathId.LocalPathId);
            if (!self->IsStateActive() || found == self->GetUserTables().end() ||
                found->second->LocalTid != tableId || found->second->GetTableSchemaVersion() != schemaVersion) {
                fail("Table changed while waiting for scan admission", true);
                return {};
            }
            const auto& config = AppData()->DataShardConfig;
            if (memory > Min(config.GetBuildIndexScanMemoryLimit(),
                    AppData()->BuildIndexScanAdmission->GetActiveByteLimit(config.GetBuildIndexPendingBytesLimit()))) {
                // Admission may have been granted before a long volatile wait
                // or before the controller applied a smaller memory budget.
                fail("Index scan memory budget changed before scan construction", true);
                return {};
            }
            const auto& req = pending->Event->Get()->Record;
            bool explicitSnapshot = false;
            if constexpr (requires { req.HasSnapshotStep(); }) {
                explicitSnapshot = req.HasSnapshotStep() || req.HasSnapshotTxId();
                if (explicitSnapshot) {
                    Y_ENSURE(pending->SnapshotReference, "Explicit scan snapshot was not pinned");
                }
            }
            const auto* version = std::get_if<TScanOptions::TSnapshotByRowVersion>(&opts.Snapshot);
            Y_ENSURE(version);
            auto scan = makeScan(*found->second, req, pending->Event->Sender, std::move(pending->Response));
            auto* vectorScan = CheckedCast<TVectorIndexScan*>(scan.Get());
            vectorScan->SetRowBytesLimit(rowLimit);
            vectorScan->HoldSnapshotReference(pending->SnapshotReference);
            return scan;
        } catch (const std::exception& exc) {
            fail(TStringBuilder() << "Cannot initialize admitted index scan: " << exc.what(), false);
            return {};
        }
    };
    const ui64 serial = self->QueueScan(tableId, {}, 0, options);
    manager.Set(id, seqNo).push_back(serial);
    manager.HoldReadVersion(id, seqNo, readVersion);
}

template<typename TResponse>
void FillScanResponseCommonFields(TResponse& response, ui64 scanId, ui64 tabletId, TScanRecord::TSeqNo seqNo)
{
    auto& rec = response.Record;
    rec.SetId(scanId);
    rec.SetTabletId(tabletId);
    rec.SetRequestSeqNoGeneration(seqNo.Generation);
    rec.SetRequestSeqNoRound(seqNo.Round);
}

template<typename TResponse>
inline void FailScan(ui64 scanId, ui64 tabletId, TActorId sender, TScanRecord::TSeqNo seqNo, const std::exception& exc, const TString& logScanType)
{
    YDB_LOG_ERROR_COMP(NKikimrServices::BUILD_INDEX, "Unhandled exception in build index scan",
        {"scanType", logScanType},
        {"tabletId", tabletId},
        {"exceptionType", TypeName(exc)},
        {"exceptionMessage", exc.what()},
        {"backtrace", TBackTrace::FromCurrentException().PrintToString()});

    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_scan_broken", true)->Inc();

    auto response = MakeHolder<TResponse>();
    FillScanResponseCommonFields(*response, scanId, tabletId, seqNo);
    response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);

    auto* issue = response->Record.AddIssues();
    issue->set_severity(NYql::TSeverityIds::S_ERROR);
    issue->set_message(TStringBuilder() << "Scan failed " << exc.what());

    TlsActivationContext->Send(new IEventHandle(sender, TActorId(), response.Release()));
}

}
