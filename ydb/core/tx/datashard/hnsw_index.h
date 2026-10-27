#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <iosfwd>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace NKikimr::NDataShard {

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
        static size_t EstimateBytes(TStringBuf key, size_t vectorBytes);

        size_t GetVersionCount() const {
            return VersionCount;
        }
        size_t GetEstimatedBytes() const {
            return EstimatedBytes;
        }

        THashMap<TString, THistory> Rows;
        bool Valid = true;

    private:
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
            TString& error, bool allowEmpty = false);

        // MVCC state is private to each tablet view; immutable graph storage
        // and its memory reservation live until the last view is released.
        std::unique_ptr<THnswIndex> CreateView(TRowVersion base,
            std::shared_ptr<THnswIndexChanges> changes,
            TRowVersion upper = TRowVersion::Max()) const;
        bool SharesGraphWith(const THnswIndex& other) const;
        bool HasGraphReservation() const;
        size_t GraphUseCount() const;
        void SetGraphReservation(std::shared_ptr<void> reservation);

        // Portable graph-only snapshot. Journals and transaction-local changes
        // are separate. Loading restores topology without running construction.
        void SaveGraph(std::ostream& output) const;
        static std::unique_ptr<THnswIndex> LoadGraph(std::istream& input,
            const Ydb::Table::VectorIndexSettings& settings,
            ui64 maxMemoryBytes, TString& error);

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
        static size_t EstimateMemoryBytes(size_t rowCount, size_t dimension, ui32 connectivity = 16,
                                          size_t serializedKeyBytes = 0);

    private:
        class TGraph;
        class TImpl;

        explicit THnswIndex(std::unique_ptr<TImpl> impl);

        std::unique_ptr<TImpl> Impl;
    };

} // namespace NKikimr::NDataShard
