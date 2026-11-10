#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <atomic>
#include <mutex>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace NKikimr::NDataShard {

    inline constexpr ui32 DefaultHnswM = 16;

    // Omitted proto3 parameters use the documented server-side default.
    ui64 GetHnswDeltaRows(const Ydb::Table::VectorIndexSettings& settings);

    ui64 GetHnswMinRows(const Ydb::Table::VectorIndexSettings& settings);
    bool AreHnswIndexSettingsCompatible(
        const Ydb::Table::VectorIndexSettings& cached,
        const Ydb::Table::VectorIndexSettings& requested);

    // Result of an HNSW search: pairs of (serialized primary key, distance).
    struct THnswSearchResult {
        std::vector<std::pair<TString, float>> Results;
        bool Covered = true;
    };

    // Owned by the tablet mailbox. Generations share committed history, never
    // transaction-local intents. Build workers only access the immutable graph.
    class THnswIndexChanges {
    public:
        struct TVersion {
            TRowVersion Version;
            std::optional<TString> Vector;
            std::shared_ptr<void> MemoryReservation;
        };
        using THistory = std::vector<TVersion>;

        void Set(TString key, TRowVersion version, std::optional<TString> vector,
                 std::shared_ptr<void> reservation = {});
        static const TVersion* Find(const THistory& history, TRowVersion version, TRowVersion base);
        const TVersion* Find(TStringBuf key, TRowVersion version, TRowVersion base) const;
        size_t CountAfter(TRowVersion base) const;
        // Every remaining generation already contains these versions in its base.
        void PruneThrough(TRowVersion base);
        // Keep each pinned snapshot, the predecessor at the floor, and newer versions.
        // Future reads below this floor deliberately use the exact table scan.
        void Compact(TRowVersion floor, const std::vector<TRowVersion>& pins = {});
        void AdvanceRetention(TRowVersion floor, const std::vector<TRowVersion>& pins);
        // Advance on commit without scanning other keys or open readers. Leases
        // keep their snapshot's predecessor when Set compacts the changed key.
        void AdvanceRetentionFloor(TRowVersion floor);
        std::shared_ptr<void> PinReadVersion(TRowVersion version);
        bool Covers(TRowVersion version) const;
        void Clear();
        static size_t EstimateBytes(TStringBuf key, size_t vectorBytes);

        size_t GetVersionCount() const {
            return VersionCount;
        }
        size_t GetEstimatedBytes() const {
            return EstimatedBytes;
        }

        THashMap<TString, THistory> Rows;
        bool Valid = true;
        TRowVersion LowerReadVersion = TRowVersion::Min();

    private:
        void UpdateCounts(std::optional<TRowVersion> before, std::optional<TRowVersion> after);
        mutable std::mutex CountsMutex;
        mutable TMap<TRowVersion, size_t> CountsByBase;
        void CompactHistory(TStringBuf key, THistory& history);
        std::vector<TRowVersion> PinnedVersions;
        bool RetentionInitialized = false;
        struct TReadPins {
            std::mutex Mutex;
            TMap<TRowVersion, size_t> Versions;
        };
        std::shared_ptr<TReadPins> ReadPins = std::make_shared<TReadPins>();
        size_t VersionCount = 0;
        size_t EstimatedBytes = 0;
    };

    // In-memory HNSW index over a single Float vector column, backed by
    // ydb/library/nmslib. The graph is immutable; committed changes live in a
    // versioned journal shared by the tablet's retained graph generations.
    class THnswIndex {
    public:
        ~THnswIndex();

        THnswIndex(const THnswIndex&) = delete;
        THnswIndex& operator=(const THnswIndex&) = delete;

        // Builds an index from (serialized primary key, raw vector bytes) pairs.
        // Vector bytes are in the KNN UDF wire format: elements followed by a
        // trailing 1-byte format tag; only FloatVector is supported here.
        // Returns nullptr and sets `error` if the settings/data are not eligible
        // (e.g. non-float vector type, invalid vector bytes, empty input unless
        // allowEmpty is set for a rebuild) or if
        // the estimated memory to hold the index would exceed maxMemoryBytes.
        static std::unique_ptr<THnswIndex> Build(
            const Ydb::Table::VectorIndexSettings& settings,
            const std::vector<std::pair<TString, TString>>& keysAndVectors,
            ui64 maxMemoryBytes,
            TString& error, bool allowEmpty = false,
            const std::atomic<bool>* cancelled = nullptr);

        // Returns up to k nearest neighbors of targetVector (same wire format as
        // build-time vectors), ordered from closest to farthest.
        THnswSearchResult Search(TStringBuf targetVector, size_t k,
                                 TRowVersion readVersion = TRowVersion::Max(), ui32 efSearch = 15) const;

        // Resolves the visible vector; base vectors are reconstructed from the
        // wrapper-owned NMSLIB Objects, and delta vectors retain their wire format.
        bool GetVector(TStringBuf key, TString& result,
                       TRowVersion readVersion = TRowVersion::Max()) const;

        // Applies a posting-table change on top of the immutable HNSW graph.
        // Updated vectors are searched exhaustively and shadow the graph entry;
        // erased keys are filtered from graph results.
        bool Upsert(TString key, TString vector, TRowVersion version = TRowVersion::Max());
        void Erase(TStringBuf key, TRowVersion version = TRowVersion::Max());
        bool HasDelta(TStringBuf key) const;
        bool HasChanges() const;
        size_t ChangeCount() const;

        void SetSnapshot(TRowVersion base, std::shared_ptr<THnswIndexChanges> changes,
                         TRowVersion upper = TRowVersion::Max());
        TRowVersion GetBaseVersion() const;
        std::shared_ptr<void> PinReadVersion(TRowVersion version);
        bool CanRead(TRowVersion version) const;
        // The limit counts distinct changed rows, independently of graph size.
        bool NeedsRebuild(ui64 deltaRows) const;
        static bool IsValidVector(TStringBuf vector, size_t dimension);

        size_t Size() const;
        size_t Dimension() const;

        // Estimated resident memory of this index, in bytes, using the same
        // formula as the build-time budget check.
        size_t EstimatedMemoryBytes() const;

        // Estimates the memory required to hold an HNSW index over `rowCount`
        // vectors of `dimension` float elements, without building anything.
        // Used to decide up front whether a build should even be attempted.
        static size_t EstimateMemoryBytes(size_t rowCount, size_t dimension, ui32 connectivity = DefaultHnswM,
                                          size_t serializedKeyBytes = 0);

    private:
        class TImpl;

        explicit THnswIndex(std::unique_ptr<TImpl> impl);

        std::unique_ptr<TImpl> Impl;
    };

} // namespace NKikimr::NDataShard
