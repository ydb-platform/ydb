#pragma once
#include "defs.h"
#include "tier_info.h"
#include "index_info.h"
#include "portion_info.h"
#include "db_wrapper.h"
#include "columns_table.h"
#include "compaction_info.h"
#include "granules_table.h"
#include "predicate/filter.h"
#include "insert_table/data.h"

#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

struct TPredicate;

struct TCompactionLimits {
    static constexpr const ui64 MIN_GOOD_BLOB_SIZE = 256 * 1024; // some BlobStorage constant
    static constexpr const ui64 MAX_BLOB_SIZE = 8 * 1024 * 1024; // some BlobStorage constant
    static constexpr const ui64 EVICT_HOT_PORTION_BYTES = 1 * 1024 * 1024;
    static constexpr const ui64 DEFAULT_EVICTION_BYTES = 64 * 1024 * 1024;
    static constexpr const ui64 MAX_BLOBS_TO_DELETE = 10000;
    static constexpr const ui64 OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID = 1024 * MAX_BLOB_SIZE;

    ui32 GoodBlobSize{MIN_GOOD_BLOB_SIZE};
    ui32 GranuleBlobSplitSize{MAX_BLOB_SIZE};
    ui32 GranuleExpectedSize{5 * MAX_BLOB_SIZE};
    ui32 GranuleOverloadSize{20 * MAX_BLOB_SIZE};
    ui32 InGranuleCompactInserts{100}; // Trigger in-granule compaction to reduce count of portions' records
    ui32 InGranuleCompactSeconds{2 * 60}; // Trigger in-granule comcation to guarantee no PK intersections
};

class TMark {
public:
    struct TCompare {
        using is_transparent = void;

        bool operator() (const TMark& a, const TMark& b) const {
            return a < b;
        }

        bool operator() (const arrow::Scalar& a, const TMark& b) const {
            return a < b;
        }

        bool operator() (const TMark& a, const arrow::Scalar& b) const {
            return a < b;
        }
    };

    explicit TMark(const NArrow::TReplaceKey& key)
        : Border(key)
    {}

    TMark(const TMark& m) = default;
    TMark& operator = (const TMark& m) = default;

    bool operator == (const TMark& m) const {
        return Border == m.Border;
    }

    std::partial_ordering operator <=> (const TMark& m) const {
        return Border <=> m.Border;
    }

    bool operator == (const arrow::Scalar& firstKey) const {
        // TODO: avoid ToScalar()
        return NArrow::ScalarCompare(*NArrow::TReplaceKey::ToScalar(Border, 0), firstKey) == 0;
    }

    std::partial_ordering operator <=> (const arrow::Scalar& firstKey) const {
        // TODO: avoid ToScalar()
        const int cmp = NArrow::ScalarCompare(*NArrow::TReplaceKey::ToScalar(Border, 0), firstKey);
        if (cmp < 0) {
            return std::partial_ordering::less;
        } else if (cmp > 0) {
            return std::partial_ordering::greater;
        } else {
            return std::partial_ordering::equivalent;
        }
    }

    const NArrow::TReplaceKey& GetBorder() const noexcept {
        return Border;
    }

    ui64 Hash() const {
        return Border.Hash();
    }

    operator size_t () const {
        return Hash();
    }

    operator bool () const = delete;

    static TString SerializeScalar(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema);
    static NArrow::TReplaceKey DeserializeScalar(const TString& key, const std::shared_ptr<arrow::Schema>& schema);

    static TString SerializeComposite(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema);
    static NArrow::TReplaceKey DeserializeComposite(const TString& key, const std::shared_ptr<arrow::Schema>& schema);

    static NArrow::TReplaceKey MinBorder(const std::shared_ptr<arrow::Schema>& schema);
    static NArrow::TReplaceKey ExtendBorder(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema);

    std::string ToString() const;

private:
    /// @note It's possible to share columns in TReplaceKey between multiple marks:
    /// read all marks as a batch; create TMark for each row
    NArrow::TReplaceKey Border;

    static std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type);
};

struct TPortionEvictionFeatures {
    const TString TargetTierName;
    const ui64 PathId;      // portion path id for cold-storage-key construct
    bool NeedExport = false;
    bool DataChanges = true;

    TPortionEvictionFeatures(const TString& targetTierName, const ui64 pathId, bool needExport)
        : TargetTierName(targetTierName)
        , PathId(pathId)
        , NeedExport(needExport)
    {}
};

class TColumnEngineChanges {
public:
    enum EType {
        UNSPECIFIED,
        INSERT,
        COMPACTION,
        CLEANUP,
        TTL,
    };

    virtual ~TColumnEngineChanges() = default;

    TColumnEngineChanges(const EType type, const TCompactionLimits& limits)
        : Type(type)
        , Limits(limits)
    {}

    void SetBlobs(THashMap<TBlobRange, TString>&& blobs) {
        Y_VERIFY(!blobs.empty());
        Blobs = std::move(blobs);
    }

    EType Type;
    TCompactionLimits Limits;
    TSnapshot InitSnapshot = TSnapshot::Zero();
    std::unique_ptr<TCompactionInfo> CompactionInfo;
    std::vector<NOlap::TInsertedData> DataToIndex;
    std::vector<TPortionInfo> SwitchedPortions; // Portions that would be replaced by new ones
    std::vector<TPortionInfo> AppendedPortions; // New portions after indexing or compaction
    std::vector<TPortionInfo> PortionsToDrop;
    std::vector<std::pair<TPortionInfo, TPortionEvictionFeatures>> PortionsToEvict; // {portion, TPortionEvictionFeatures}
    std::vector<TColumnRecord> EvictedRecords;
    std::vector<std::pair<TPortionInfo, ui64>> PortionsToMove; // {portion, new granule}
    THashMap<TBlobRange, TString> Blobs;
    THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> CachedBlobs;
    bool NeedRepeat{false};

    bool IsInsert() const noexcept { return Type == INSERT; }
    bool IsCompaction() const noexcept { return Type == COMPACTION; }
    bool IsCleanup() const noexcept { return Type == CLEANUP; }
    bool IsTtl() const noexcept { return Type == TTL; }

    const char * TypeString() const {
        switch (Type) {
            case UNSPECIFIED:
                break;
            case INSERT:
                return "insert";
            case COMPACTION:
                return CompactionInfo
                    ? (CompactionInfo->InGranule() ? "compaction in granule" : "compaction split granule" )
                    : "compaction";
            case CLEANUP:
                return "cleanup";
            case TTL:
                return "ttl";
        }
        return "";
    }

    ui64 TotalBlobsSize() const {
        ui64 size = 0;
        for (const auto& [_, blob] : Blobs) {
            size += blob.size();
        }
        return size;
    }

    /// Returns blob-ranges grouped by blob-id.
    static THashMap<TUnifiedBlobId, std::vector<TBlobRange>>
    GroupedBlobRanges(const std::vector<TPortionInfo>& portions) {
        Y_VERIFY(portions.size());

        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> sameBlobRanges;
        for (const auto& portionInfo : portions) {
            Y_VERIFY(!portionInfo.Empty());

            for (const auto& rec : portionInfo.Records) {
                sameBlobRanges[rec.BlobRange.BlobId].push_back(rec.BlobRange);
            }
        }
        return sameBlobRanges;
    }

    /// Returns blob-ranges grouped by blob-id.
    static THashMap<TUnifiedBlobId, std::vector<TBlobRange>>
    GroupedBlobRanges(const std::vector<std::pair<TPortionInfo, TPortionEvictionFeatures>>& portions) {
        Y_VERIFY(portions.size());

        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> sameBlobRanges;
        for (const auto& [portionInfo, _] : portions) {
            Y_VERIFY(!portionInfo.Empty());

            for (const auto& rec : portionInfo.Records) {
                sameBlobRanges[rec.BlobRange.BlobId].push_back(rec.BlobRange);
            }
        }
        return sameBlobRanges;
    }
};

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

    std::vector<ui64> GranulesOrder(bool rev = false) const {
        size_t size = Granules.size();
        std::vector<ui64> order(size);
        if (rev) {
            size_t pos = size - 1;
            for (size_t i = 0; i < size; ++i, --pos) {
                order[i] = Granules[pos].Granule;
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                order[i] = Granules[i].Granule;
            }
        }
        return order;
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
    static_assert(NUM_KINDS == NOlap::TPortionMeta::EVICTED, "NUM_KINDS must match NOlap::TPortionMeta::EProduced enum");
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
            if (i == NOlap::TPortionMeta::UNSPECIFIED) {
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
            if (i == NOlap::TPortionMeta::UNSPECIFIED) {
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
            if (i == NOlap::TPortionMeta::UNSPECIFIED) {
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
public:
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
        Snapshots.emplace(version, std::make_shared<TSnapshotSchema>(std::move(indexInfo), version));
    }
};


class IColumnEngine {
public:
    virtual ~IColumnEngine() = default;

    virtual const TIndexInfo& GetIndexInfo() const = 0;
    virtual const TVersionedIndex& GetVersionedIndex() const = 0;
    virtual const std::shared_ptr<arrow::Schema>& GetReplaceKey() const { return GetIndexInfo().GetReplaceKey(); }
    virtual const std::shared_ptr<arrow::Schema>& GetSortingKey() const { return GetIndexInfo().GetSortingKey(); }
    virtual const std::shared_ptr<arrow::Schema>& GetIndexKey() const { return GetIndexInfo().GetIndexKey(); }
    virtual const THashSet<ui64>* GetOverloadedGranules(ui64 /*pathId*/) const { return nullptr; }
    bool HasOverloadedGranules(const ui64 pathId) const {
        return GetOverloadedGranules(pathId) != nullptr;
    }

    virtual TString SerializeMark(const NArrow::TReplaceKey& key) const = 0;
    virtual NArrow::TReplaceKey DeserializeMark(const TString& key, std::optional<ui32> markNumKeys) const = 0;

    virtual bool Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop = {}) = 0;

    virtual std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                                const THashSet<ui32>& columnIds,
                                                const TPKRangesFilter& pkRangesFilter) const = 0;
    virtual std::unique_ptr<TCompactionInfo> Compact(const TCompactionLimits& limits) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartInsert(const TCompactionLimits& limits, std::vector<TInsertedData>&& dataToIndex) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                                  const TSnapshot& outdatedSnapshot, const TCompactionLimits& limits) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, const TCompactionLimits& limits, THashSet<ui64>& pathsToDrop,
                                                               ui32 maxRecords) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<arrow::Schema>& schema,
                                                           ui64 maxBytesToEvict = TCompactionLimits::DEFAULT_EVICTION_BYTES) = 0;
    virtual bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) = 0;
    virtual void FreeLocks(std::shared_ptr<TColumnEngineChanges> changes) = 0;
    virtual void UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) = 0;
    //virtual void UpdateTableSchema(ui64 pathId, const TSnapshot& snapshot, TIndexInfo&& info) = 0; // TODO
    virtual const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const = 0;
    virtual const TColumnEngineStats& GetTotalStats() = 0;
    virtual ui64 MemoryUsage() const { return 0; }
    virtual TSnapshot LastUpdate() const { return TSnapshot::Zero(); }
};

}
