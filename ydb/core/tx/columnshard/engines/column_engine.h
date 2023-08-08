#pragma once
#include "changes/abstract.h"
#include "granules_table.h"
#include "portions/portion_info.h"
#include "scheme/snapshot_scheme.h"
#include "predicate/filter.h"
#include "changes/compaction_info.h"
#include <ydb/core/tx/columnshard/common/reverse_accessor.h>

namespace NKikimr::NColumnShard {
class TTiersManager;
class TTtl;
}

namespace NKikimr::NOlap {
class TInsertColumnEngineChanges;
class TCompactColumnEngineChanges;
class TTTLColumnEngineChanges;
class TCleanupColumnEngineChanges;
struct TSelectInfo {
    struct TStats {
        size_t Granules{};
        size_t Portions{};
        size_t Records{};
        size_t Blobs{};
        size_t Rows{};
        size_t Bytes{};

        const TStats& operator += (const TStats& stats) {
            Granules += stats.Granules;
            Portions += stats.Portions;
            Records += stats.Records;
            Blobs += stats.Blobs;
            Rows += stats.Rows;
            Bytes += stats.Bytes;
            return *this;
        }
    };

    std::vector<TGranuleRecord> Granules; // ordered by key (ascending)
    std::vector<TPortionInfo> Portions;

    NColumnShard::TContainerAccessorWithDirection<std::vector<TPortionInfo>> GetPortionsOrdered(const bool reverse) const {
        return NColumnShard::TContainerAccessorWithDirection<std::vector<TPortionInfo>>(Portions, reverse);
    }

    NColumnShard::TContainerAccessorWithDirection<std::vector<TGranuleRecord>> GetGranulesOrdered(const bool reverse) const {
        return NColumnShard::TContainerAccessorWithDirection<std::vector<TGranuleRecord>>(Granules, reverse);
    }

    size_t NumRecords() const {
        size_t records = 0;
        for (auto& portionInfo : Portions) {
            records += portionInfo.NumRecords();
        }
        return records;
    }

    TStats Stats() const {
        TStats out;
        out.Granules = Granules.size();
        out.Portions = Portions.size();

        THashSet<TUnifiedBlobId> uniqBlob;
        for (auto& portionInfo : Portions) {
            out.Records += portionInfo.NumRecords();
            out.Rows += portionInfo.NumRows();
            for (auto& rec : portionInfo.Records) {
                uniqBlob.insert(rec.BlobRange.BlobId);
            }
        }
        out.Blobs += uniqBlob.size();
        for (auto blobId : uniqBlob) {
            out.Bytes += blobId.BlobSize();
        }
        return out;
    }

    friend IOutputStream& operator << (IOutputStream& out, const TSelectInfo& info) {
        if (info.Granules.size()) {
            out << "granules:";
            for (auto& rec : info.Granules) {
                out << " " << rec;
            }
            out << "; ";
        }
        if (info.Portions.size()) {
            out << "portions:";
            for (auto& portionInfo : info.Portions) {
                out << portionInfo;
            }
        }
        return out;
    }
};

class TColumnEngineStats {
private:
    static constexpr const ui64 NUM_KINDS = 5;
    static_assert(NUM_KINDS == NOlap::TPortionMeta::EProduced::EVICTED, "NUM_KINDS must match NOlap::TPortionMeta::EProduced enum");
public:
    class TPortionsStats {
    private:
        template <class T>
        T SumVerifiedPositive(const T v1, const T v2) const {
            T result = v1 + v2;
            Y_VERIFY(result >= 0);
            return result;
        }
    public:

        i64 Portions = 0;
        i64 Blobs = 0;
        i64 Rows = 0;
        i64 Bytes = 0;
        i64 RawBytes = 0;
        THashMap<ui32, size_t> BytesByColumn;
        THashMap<ui32, size_t> RawBytesByColumn;

        TString DebugString() const {
            return TStringBuilder() << "portions=" << Portions << ";blobs=" << Blobs << ";rows=" << Rows << ";bytes=" << Bytes << ";raw_bytes=" << RawBytes << ";";
        }

        TPortionsStats operator+(const TPortionsStats& item) const {
            TPortionsStats result = *this;
            return result += item;
        }

        TPortionsStats operator*(const i32 kff) const {
            TPortionsStats result;
            result.Portions = kff * Portions;
            result.Blobs = kff * Blobs;
            result.Rows = kff * Rows;
            result.Bytes = kff * Bytes;
            result.RawBytes = kff * RawBytes;

            for (auto&& i : BytesByColumn) {
                result.BytesByColumn[i.first] = kff * i.second;
            }

            for (auto&& i : RawBytesByColumn) {
                result.RawBytesByColumn[i.first] = kff * i.second;
            }
            return result;
        }

        TPortionsStats& operator-=(const TPortionsStats& item) {
            return *this += item * -1;
        }

        TPortionsStats& operator+=(const TPortionsStats& item) {
            Portions = SumVerifiedPositive(Portions, item.Portions);
            Blobs = SumVerifiedPositive(Blobs, item.Blobs);
            Rows = SumVerifiedPositive(Rows, item.Rows);
            Bytes = SumVerifiedPositive(Bytes, item.Bytes);
            RawBytes = SumVerifiedPositive(RawBytes, item.RawBytes);
            for (auto&& i : item.BytesByColumn) {
                auto& v = BytesByColumn[i.first];
                v = SumVerifiedPositive(v, i.second);
            }

            for (auto&& i : item.RawBytesByColumn) {
                auto& v = RawBytesByColumn[i.first];
                v = SumVerifiedPositive(v, i.second);
            }
            return *this;
        }
    };

    i64 Tables{};
    i64 Granules{};
    i64 EmptyGranules{};
    i64 OverloadedGranules{};
    i64 ColumnRecords{};
    i64 ColumnMetadataBytes{};
    THashMap<TPortionMeta::EProduced, TPortionsStats> StatsByType;

    std::vector<ui32> GetKinds() const {
        std::vector<ui32> result;
        for (auto&& i : GetEnumAllValues<NOlap::TPortionMeta::EProduced>()) {
            if (i == NOlap::TPortionMeta::EProduced::UNSPECIFIED) {
                continue;
            }
            result.emplace_back(i);
        }
        return result;
    }

    template <class T, class TAccessor>
    std::vector<T> GetValues(const TAccessor accessor) const {
        std::vector<T> result;
        for (auto&& i : GetEnumAllValues<NOlap::TPortionMeta::EProduced>()) {
            if (i == NOlap::TPortionMeta::EProduced::UNSPECIFIED) {
                continue;
            }
            result.emplace_back(accessor(GetStats(i)));
        }
        return result;
    }

    template <class T, class TAccessor>
    T GetValuesSum(const TAccessor accessor) const {
        T result = 0;
        for (auto&& i : GetEnumAllValues<NOlap::TPortionMeta::EProduced>()) {
            if (i == NOlap::TPortionMeta::EProduced::UNSPECIFIED) {
                continue;
            }
            result += accessor(GetStats(i));
        }
        return result;
    }

    static ui32 GetRecordsCount() {
        return NUM_KINDS;
    }

    ui64 GetPortionsCount() const {
        const auto accessor = [](const TPortionsStats& stats) {
            return stats.Portions;
        };
        return GetValuesSum<ui64>(accessor);
    }

    template <class T>
    std::vector<T> GetConstValues(const T constValue) const {
        const auto accessor = [constValue](const TPortionsStats& /*stats*/) {
            return constValue;
        };
        return GetValues<T>(accessor);
    }

    std::vector<uint64_t> GetRowsValues() const {
        const auto accessor = [](const TPortionsStats& stats) {
            return stats.Rows;
        };
        return GetValues<uint64_t>(accessor);
    }

    std::vector<uint64_t> GetBytesValues() const {
        const auto accessor = [](const TPortionsStats& stats) {
            return stats.Bytes;
        };
        return GetValues<uint64_t>(accessor);
    }

    std::vector<uint64_t> GetRawBytesValues() const {
        const auto accessor = [](const TPortionsStats& stats) {
            return stats.RawBytes;
        };
        return GetValues<uint64_t>(accessor);
    }

    std::vector<uint64_t> GetPortionsValues() const {
        const auto accessor = [](const TPortionsStats& stats) {
            return stats.Portions;
        };
        return GetValues<uint64_t>(accessor);
    }

    std::vector<uint64_t> GetBlobsValues() const {
        const auto accessor = [](const TPortionsStats& stats) {
            return stats.Blobs;
        };
        return GetValues<uint64_t>(accessor);
    }

    const TPortionsStats& GetStats(const TPortionMeta::EProduced type) const {
        auto it = StatsByType.find(type);
        if (it == StatsByType.end()) {
            return Default<TPortionsStats>();
        } else {
            return it->second;
        }
    }

    const TPortionsStats& GetInsertedStats() const {
        return GetStats(TPortionMeta::EProduced::INSERTED);
    }

    const TPortionsStats& GetCompactedStats() const {
        return GetStats(TPortionMeta::EProduced::COMPACTED);
    }

    const TPortionsStats& GetSplitCompactedStats() const {
        return GetStats(TPortionMeta::EProduced::SPLIT_COMPACTED);
    }

    const TPortionsStats& GetInactiveStats() const {
        return GetStats(TPortionMeta::EProduced::INACTIVE);
    }

    const TPortionsStats& GetEvictedStats() const {
        return GetStats(TPortionMeta::EProduced::EVICTED);
    }

    TPortionsStats Active() const {
        return GetInsertedStats() + GetCompactedStats() + GetSplitCompactedStats();
    }

    void Clear() {
        *this = {};
    }
};

class TVersionedIndex {
    std::map<TSnapshot, ISnapshotSchema::TPtr> Snapshots;
    std::shared_ptr<arrow::Schema> IndexKey;
    std::map<ui64, ISnapshotSchema::TPtr> SnapshotByVersion;
    ui64 LastSchemaVersion = 0;
public:
    ISnapshotSchema::TPtr GetSchema(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        return it == SnapshotByVersion.end() ? nullptr : it->second;
    }

    ISnapshotSchema::TPtr GetSchemaUnsafe(const ui64 version) const {
        auto it = SnapshotByVersion.find(version);
        Y_VERIFY(it != SnapshotByVersion.end());
        return it->second;
    }

    ISnapshotSchema::TPtr GetSchema(const TSnapshot& version) const {
        for (auto it = Snapshots.rbegin(); it != Snapshots.rend(); ++it) {
            if (it->first <= version) {
                return it->second;
            }
        }
        Y_VERIFY(!Snapshots.empty());
        Y_VERIFY(version.IsZero());
        return Snapshots.begin()->second; // For old compaction logic compatibility
    }

    ISnapshotSchema::TPtr GetLastSchema() const {
        Y_VERIFY(!Snapshots.empty());
        return Snapshots.rbegin()->second;
    }

    const std::shared_ptr<arrow::Schema>& GetIndexKey() const noexcept {
        return IndexKey;
    }

    void AddIndex(const TSnapshot& version, TIndexInfo&& indexInfo) {
        if (Snapshots.empty()) {
            IndexKey = indexInfo.GetIndexKey();
        } else {
            Y_VERIFY(IndexKey->Equals(indexInfo.GetIndexKey()));
        }
        auto it = Snapshots.emplace(version, std::make_shared<TSnapshotSchema>(std::move(indexInfo), version));
        Y_VERIFY(it.second);
        auto newVersion = it.first->second->GetVersion();

        if (SnapshotByVersion.contains(newVersion)) {
            Y_VERIFY_S(LastSchemaVersion != 0, TStringBuilder() << "Last: " << LastSchemaVersion);
            Y_VERIFY_S(LastSchemaVersion == newVersion, TStringBuilder() << "Last: " << LastSchemaVersion << ";New: " << newVersion);
        }

        SnapshotByVersion[newVersion] = it.first->second;
        LastSchemaVersion = newVersion;
    }
};


class IColumnEngine {
public:
    virtual ~IColumnEngine() = default;

    virtual const TVersionedIndex& GetVersionedIndex() const = 0;
    virtual const std::shared_ptr<arrow::Schema>& GetReplaceKey() const { return GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetReplaceKey(); }
    virtual const std::shared_ptr<arrow::Schema>& GetSortingKey() const { return GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetSortingKey(); }
    virtual const std::shared_ptr<arrow::Schema>& GetIndexKey() const { return GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetIndexKey(); }
    virtual const THashSet<ui64>* GetOverloadedGranules(const ui64 pathId) const = 0;
    bool HasOverloadedGranules(const ui64 pathId) const {
        return GetOverloadedGranules(pathId) != nullptr;
    }

    virtual TString SerializeMark(const NArrow::TReplaceKey& key) const = 0;
    virtual NArrow::TReplaceKey DeserializeMark(const TString& key, std::optional<ui32> markNumKeys) const = 0;

    virtual bool Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop = {}) = 0;

    virtual std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                                const THashSet<ui32>& columnIds,
                                                const TPKRangesFilter& pkRangesFilter) const = 0;
    virtual std::unique_ptr<TCompactionInfo> Compact(const TCompactionLimits& limits, const THashSet<ui64>& busyGranuleIds) = 0;
    virtual std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept = 0;
    virtual std::shared_ptr<TCompactColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                                  const TCompactionLimits& limits) noexcept = 0;
    virtual std::shared_ptr<TCleanupColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, THashSet<ui64>& pathsToDrop,
                                                               ui32 maxRecords) noexcept = 0;
    virtual std::shared_ptr<TTTLColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction,
                                                           ui64 maxBytesToEvict = TCompactionLimits::DEFAULT_EVICTION_BYTES) noexcept = 0;
    virtual bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) noexcept = 0;
    virtual void UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) = 0;
    //virtual void UpdateTableSchema(ui64 pathId, const TSnapshot& snapshot, TIndexInfo&& info) = 0; // TODO
    virtual const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const = 0;
    virtual const TColumnEngineStats& GetTotalStats() = 0;
    virtual ui64 MemoryUsage() const { return 0; }
    virtual TSnapshot LastUpdate() const { return TSnapshot::Zero(); }
    virtual void OnTieringModified(std::shared_ptr<NColumnShard::TTiersManager> manager, const NColumnShard::TTtl& ttl) = 0;
};

}
