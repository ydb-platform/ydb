#pragma once
#include "defs.h"
#include "tier_info.h"
#include "index_info.h"
#include "portion_info.h"
#include "db_wrapper.h"
#include "insert_table.h"
#include "columns_table.h"
#include "granules_table.h"
#include "predicate/filter.h"

#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

struct TPredicate;

struct TCompactionLimits {
    static constexpr const ui32 MIN_GOOD_BLOB_SIZE = 256 * 1024; // some BlobStorage constant
    static constexpr const ui32 MAX_BLOB_SIZE = 8 * 1024 * 1024; // some BlobStorage constant
    static constexpr const ui64 EVICT_HOT_PORTION_BYTES = 1 * 1024 * 1024;
    static constexpr const ui64 DEFAULT_EVICTION_BYTES = 64 * 1024 * 1024;
    static constexpr const ui64 MAX_BLOBS_TO_DELETE = 10000;

    ui32 GoodBlobSize{MIN_GOOD_BLOB_SIZE};
    ui32 GranuleBlobSplitSize{MAX_BLOB_SIZE};
    ui32 GranuleExpectedSize{5 * MAX_BLOB_SIZE};
    ui32 GranuleOverloadSize{20 * MAX_BLOB_SIZE};
    ui32 InGranuleCompactInserts{100}; // Trigger in-granule compaction to reduce count of portions' records
    ui32 InGranuleCompactSeconds{2 * 60}; // Trigger in-granule comcation to guarantee no PK intersections
};

struct TMark {
    /// @note It's possible to share columns in TReplaceKey between multiple marks:
    /// read all marks as a batch; create TMark for each row
    NArrow::TReplaceKey Border;

    explicit TMark(const NArrow::TReplaceKey& key)
        : Border(key)
    {}

    explicit TMark(const std::shared_ptr<arrow::Schema>& schema)
        : Border(MinBorder(schema))
    {}

    TMark(const TString& key, const std::shared_ptr<arrow::Schema>& schema)
        : Border(Deserialize(key, schema))
    {}

    TMark(const TMark& m) = default;
    TMark& operator = (const TMark& m) = default;

    bool operator == (const TMark& m) const {
        return Border == m.Border;
    }

    std::partial_ordering operator <=> (const TMark& m) const {
        return Border <=> m.Border;
    }

    ui64 Hash() const {
        return Border.Hash();
    }

    operator size_t () const {
        return Hash();
    }

    operator bool () const = delete;

    static TString Serialize(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema);
    static NArrow::TReplaceKey Deserialize(const TString& key, const std::shared_ptr<arrow::Schema>& schema);
    std::string ToString() const;

private:
    static std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type);
    static NArrow::TReplaceKey MinBorder(const std::shared_ptr<arrow::Schema>& schema);
};

struct TCompactionInfo {
    TSet<ui64> Granules;
    bool InGranule{false};

    bool Empty() const { return Granules.empty(); }
    bool Good() const { return Granules.size() == 1; }

    friend IOutputStream& operator << (IOutputStream& out, const TCompactionInfo& info) {
        if (info.Good() == 1) {
            ui64 granule = *info.Granules.begin();
            out << (info.InGranule ? "in granule" : "split granule") << " compaction of granule " << granule;
        } else {
            out << "wrong compaction of " << info.Granules.size() << " granules";
        }
        return out;
    }
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

    explicit TColumnEngineChanges(EType type)
        : Type(type)
    {}

    void SetBlobs(THashMap<TBlobRange, TString>&& blobs) {
        Y_VERIFY(!blobs.empty());
        Blobs = std::move(blobs);
    }

    EType Type{UNSPECIFIED};
    TCompactionLimits Limits;
    TSnapshot InitSnapshot = TSnapshot::Zero();
    TSnapshot ApplySnapshot = TSnapshot::Zero();
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
                return "compaction";
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

struct TColumnEngineStats {
    struct TPortionsStats {
        i64 Portions{};
        i64 Blobs{};
        i64 Rows{};
        i64 Bytes{};
        i64 RawBytes{};
    };

    i64 Tables{};
    i64 Granules{};
    i64 EmptyGranules{};
    i64 OverloadedGranules{};
    i64 ColumnRecords{};
    i64 ColumnMetadataBytes{};
    TPortionsStats Inserted{};
    TPortionsStats Compacted{};
    TPortionsStats SplitCompacted{};
    TPortionsStats Inactive{};
    TPortionsStats Evicted{};

    TPortionsStats Active() const {
        return TPortionsStats {
            .Portions = ActivePortions(),
            .Blobs = ActiveBlobs(),
            .Rows = ActiveRows(),
            .Bytes = ActiveBytes(),
            .RawBytes = ActiveRawBytes()
        };
    }

    i64 ActivePortions() const { return Inserted.Portions + Compacted.Portions + SplitCompacted.Portions; }
    i64 ActiveBlobs() const { return Inserted.Blobs + Compacted.Blobs + SplitCompacted.Blobs; }
    i64 ActiveRows() const { return Inserted.Rows + Compacted.Rows + SplitCompacted.Rows; }
    i64 ActiveBytes() const { return Inserted.Bytes + Compacted.Bytes + SplitCompacted.Bytes; }
    i64 ActiveRawBytes() const { return Inserted.RawBytes + Compacted.RawBytes + SplitCompacted.RawBytes; }

    void Clear() {
        *this = {};
    }
};

class TVersionedIndex {
    std::map<TSnapshot, ISnapshotSchema::TPtr> Snapshots;
public:
    ISnapshotSchema::TPtr GetSchema(const TSnapshot& version) const {
        Y_UNUSED(version);
        return GetLastSchema();
        /*
            for (auto it = Snapshots.rbegin(); it != Snapshots.rend(); ++it) {
                if (it->first <= version) {
                    return it->second;
                }
            }
            Y_VERIFY(false);
            return nullptr;
        */
    }

    ISnapshotSchema::TPtr GetLastSchema() const {
        Y_VERIFY(!Snapshots.empty());
        return Snapshots.rbegin()->second;
    }

    void AddIndex(const TSnapshot& version, TIndexInfo&& indexInfo) {
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
    virtual bool HasOverloadedGranules() const { return false; }

    virtual TString SerializeMark(const NArrow::TReplaceKey& key) const = 0;
    virtual NArrow::TReplaceKey DeserializeMark(const TString& key) const = 0;

    virtual bool Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop = {}) = 0;

    virtual std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                                const THashSet<ui32>& columnIds,
                                                const TPKRangesFilter& pkRangesFilter) const = 0;
    virtual std::unique_ptr<TCompactionInfo> Compact(ui64& lastCompactedGranule) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                                  const TSnapshot& outdatedSnapshot) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, THashSet<ui64>& pathsToDrop,
                                                               ui32 maxRecords) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<arrow::Schema>& schema,
                                                           ui64 maxBytesToEvict = TCompactionLimits::DEFAULT_EVICTION_BYTES) = 0;
    virtual bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) = 0;
    virtual void FreeLocks(std::shared_ptr<TColumnEngineChanges> changes) = 0;
    virtual void UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) = 0;
    //virtual void UpdateTableSchema(ui64 pathId, const TSnapshot& snapshot, TIndexInfo&& info) = 0; // TODO
    virtual void UpdateCompactionLimits(const TCompactionLimits& limits) = 0;
    virtual const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const = 0;
    virtual const TColumnEngineStats& GetTotalStats() = 0;
    virtual ui64 MemoryUsage() const { return 0; }
    virtual TSnapshot LastUpdate() const { return TSnapshot::Zero(); }
};

}
