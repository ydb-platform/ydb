#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <library/cpp/time_provider/monotonic.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace NKikimr::NScanStats {

    using TStep = NKqpProto::TColumnShardStepStats;
    class TCollector;

    class TOperation {
        friend class TCollector;
        const std::shared_ptr<TCollector> Collector;
        const ui64 StepId;
        const std::optional<ui64> SourceId;
        std::mutex Mutex;
        TStep Record;
        std::optional<TMonotonic> ActiveSince;
        std::optional<TMonotonic> OwnedActiveSince;
        ui32 OwnedPauseDepth = 0;
        std::optional<TMonotonic> WaitingSince;
        bool Finalized = false;
    bool ReadBytesOverflow = false;
        THashSet<TString> RequestedRanges;

        TOperation(std::shared_ptr<TCollector> collector, TStep&& record);
        void EndActive(TMonotonic now);
        void EndWait(TMonotonic now);

    public:
        ~TOperation();
        ui64 GetStepId() const {
            return StepId;
        }
        const std::optional<ui64>& GetSourceId() const {
            return SourceId;
        }
        const std::shared_ptr<TCollector>& GetCollector() const {
            return Collector;
        }

        template <class TCallback>
        void Update(TCallback&& callback) {
            std::lock_guard guard(Mutex);
            if (!Finalized) {
                callback(Record);
            }
        }

        bool Resume();
        void Suspend();
        void StopActive();
        void PauseOwnedActive();
        void ResumeOwnedActive();
        void InheritContext(TStep& child);
        void Finish(TStep::EOutcome outcome = TStep::completed);
        void AddRequestedRange(TString rangeIdentity, ui64 bytes);
    void AddDataSourceRead(TStringBuf kind, TStringBuf name, std::optional<ui64> bytes,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::unknown,
        bool shared = false, bool includeInStorage = true);

    std::optional<ui64> GetParentStepId();

    void AddReadBytes(ui64 bytes);
    };

    class TCollector: public std::enable_shared_from_this<TCollector> {
        friend class TOperation;
        // Bound total raw data, including records already drained to KQP.
        static constexpr ui64 MaxRecords = 16384;
        static constexpr ui64 MaxBytes = 8 << 20;
        const ui64 TabletId;
        TString StatsScanId;
        TString ReaderKind;
        std::atomic<ui64> NextId = 0;
        std::mutex Mutex;
        std::vector<TStep> Completed;
        ui64 PublishedBytes = 0;
        std::atomic<ui64> ActiveOperations = 0;
        std::optional<NKqpProto::TColumnShardCacheStats> CacheStats;
    google::protobuf::RepeatedPtrField<NKqpProto::TColumnShardDataSourceRead> StorageReads;
    TString TablePath;
        bool Truncated = false;

        void Publish(TStep&& record);

    public:
        explicit TCollector(ui64 tabletId)
            : TabletId(tabletId)
        {
        }
        void BindScanId(TString id);
        void AddCacheRead(ui64 bytes, bool cached);
    void AddStorageRead(TStringBuf kind, TStringBuf name, std::optional<ui64> bytes,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::unknown,
        bool shared = false);
    void SetTablePath(TString path);
    TString GetTablePath();
        std::shared_ptr<TOperation> Start(TStep::EStepKind kind, TString name, TString operation,
                                          const std::shared_ptr<TOperation>& parent = {}, std::optional<ui64> sourceId = {});
        std::unique_ptr<NKqpProto::TColumnShardScanStats> Drain(bool finished);
        void SetReaderKind(TString value);
        TString GetReaderKind();
    };

    const std::shared_ptr<TOperation>& Current();
    const std::shared_ptr<TCollector>& CurrentCollector();

    struct TCapture {
        std::shared_ptr<TOperation> Operation;
        std::shared_ptr<TCollector> Collector;

        TCapture();
        TCapture(std::shared_ptr<TOperation> operation, std::shared_ptr<TCollector> collector);
    };

    void RecordDataSourceRead(TStringBuf kind, TStringBuf name, std::optional<ui64> bytes,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::unknown,
        const TCapture& capture = TCapture(), bool shared = false, bool includeInStorage = true);

    void RecordNonOwnerDataSourceRead(TStringBuf kind, TStringBuf name,
        NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind dataKind,
        const TCapture& request, const std::shared_ptr<TOperation>& owner);

    class TScope {
        const std::shared_ptr<TOperation> Operation;
        std::shared_ptr<TOperation> Previous;
        std::shared_ptr<TOperation> PreviousOwned;
        std::shared_ptr<TOperation> PreviousContext;
        std::shared_ptr<TCollector> PreviousCollector;
        bool WasWaiting = false;
        bool OwnsActivation = false;
        bool Nested = false;

    public:
        explicit TScope(std::shared_ptr<TOperation> operation);
        TScope(std::shared_ptr<TOperation> operation, std::shared_ptr<TCollector> collector);
        // Propagating async context does not resume the captured consumer's timer.
        explicit TScope(const TCapture& capture);
        ~TScope();
        TScope(const TScope&) = delete;
        TScope& operator=(const TScope&) = delete;
    };

} // namespace NKikimr::NScanStats
