#pragma once
#include "defs.h"
#include "index_info.h"
#include "portion_info.h"
#include "db_wrapper.h"
#include "insert_table.h"
#include "columns_table.h"
#include "granules_table.h"

#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

struct TPredicate;

struct TCompactionLimits {
    static constexpr const ui32 MIN_GOOD_BLOB_SIZE = 256 * 1024; // some BlobStorage constant
    static constexpr const ui32 MAX_BLOB_SIZE = 8 * 1024 * 1024; // some BlobStorage constant
    static constexpr const ui64 DEFAULT_EVICTION_BYTES = 64 * 1024 * 1024;
    static constexpr const ui64 MAX_BLOBS_TO_DELETE = 10000;

    ui32 GoodBlobSize{MIN_GOOD_BLOB_SIZE};
    ui32 GranuleBlobSplitSize{MAX_BLOB_SIZE};
    ui32 GranuleExpectedSize{5 * MAX_BLOB_SIZE};
    ui32 GranuleOverloadSize{20 * MAX_BLOB_SIZE};
    ui32 InGranuleCompactInserts{100}; // Trigger in-granule compaction to reduce count of portions' records
    ui32 InGranuleCompactSeconds{2 * 60}; // Trigger in-granule comcation to guarantee no PK intersections
};

struct TCompactionInfo {
    TSet<ui64> Granules;
    bool InGranule{false};

    bool Empty() const { return Granules.empty(); }
    bool Good() const { return Granules.size() == 1; }

    ui64 ChooseOneGranule(ui64 lastGranule) {
        Y_VERIFY(Granules.size());

        auto it = Granules.upper_bound(lastGranule);
        if (it == Granules.end()) {
            it = Granules.begin();
        }

        ui64 granule = *it;
        Granules.clear();
        Granules.insert(granule);
        return granule;
    }

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

struct TTiersInfo {
    struct TTierTimeBorder {
        TString TierName;
        TInstant EvictBorder;

        TTierTimeBorder(TString tierName, TInstant evictBorder)
            : TierName(tierName)
            , EvictBorder(evictBorder)
        {}

        std::shared_ptr<arrow::Scalar> ToTimestamp() const {
            if (Scalar) {
                return Scalar;
            }

            Scalar = std::make_shared<arrow::TimestampScalar>(
                EvictBorder.MicroSeconds(), arrow::timestamp(arrow::TimeUnit::MICRO));
            return Scalar;
        }

    private:
        mutable std::shared_ptr<arrow::Scalar> Scalar;
    };

    TString Column;
    std::vector<TTierTimeBorder> TierBorders; // Ordered tiers from hottest to coldest

    TTiersInfo(const TString& column, TInstant border = {}, const TString& tierName = {})
        : Column(column)
    {
        if (border) {
            AddTier(tierName, border);
        }
    }

    void AddTier(const TString& tierName, TInstant border) {
        TierBorders.emplace_back(TTierTimeBorder(tierName, border));
    }
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

    TColumnEngineChanges(EType type)
        : Type(type)
    {}

    void SetBlobs(THashMap<TBlobRange, TString>&& blobs) {
        Y_VERIFY(!blobs.empty());
        Blobs = std::move(blobs);
    }

    EType Type{UNSPECIFIED};
    TCompactionLimits Limits;
    TSnapshot InitSnapshot;
    TSnapshot ApplySnapshot;
    std::unique_ptr<TCompactionInfo> CompactionInfo;
    TVector<NOlap::TInsertedData> DataToIndex;
    TVector<TPortionInfo> SwitchedPortions; // Portions that would be replaced by new ones
    TVector<TPortionInfo> AppendedPortions; // New portions after indexing or compaction
    TVector<TPortionInfo> PortionsToDrop;
    TVector<std::pair<TPortionInfo, TString>> PortionsToEvict; // {portion, target tier name}
    TVector<TColumnRecord> EvictedRecords;
    TVector<std::pair<TPortionInfo, ui64>> PortionsToMove; // {portion, new granule}
    THashMap<TBlobRange, TString> Blobs;
    bool NeedRepeat{false};

    bool IsInsert() const { return Type == INSERT; }
    bool IsCompaction() const { return Type == COMPACTION; }
    bool IsCleanup() const { return Type == CLEANUP; }
    bool IsTtl() const { return Type == TTL; }

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
        for (auto& [blobId, blob] : Blobs) {
            size += blob.size();
        }
        return size;
    }

    static THashMap<TUnifiedBlobId, std::vector<TBlobRange>>
    GroupedBlobRanges(const TVector<TPortionInfo>& portions) {
        Y_VERIFY(portions.size());

        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> sameBlobRanges;
        for (auto& portionInfo : portions) {
            Y_VERIFY(!portionInfo.Empty());

            for (auto& rec : portionInfo.Records) {
                sameBlobRanges[rec.BlobRange.BlobId].push_back(rec.BlobRange);
            }
        }
        return sameBlobRanges;
    }

    static THashMap<TUnifiedBlobId, std::vector<TBlobRange>>
    GroupedBlobRanges(const TVector<std::pair<TPortionInfo, TString>>& portions) {
        Y_VERIFY(portions.size());

        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> sameBlobRanges;
        for (auto& [portionInfo, _] : portions) {
            Y_VERIFY(!portionInfo.Empty());

            for (auto& rec : portionInfo.Records) {
                sameBlobRanges[rec.BlobRange.BlobId].push_back(rec.BlobRange);
            }
        }
        return sameBlobRanges;
    }

    friend IOutputStream& operator << (IOutputStream& out, const TColumnEngineChanges& changes) {
        if (ui32 switched = changes.SwitchedPortions.size()) {
            out << "switch " << switched << " portions";
            for (auto& portionInfo : changes.SwitchedPortions) {
                out << portionInfo;
            }
            out << "; ";
        }
        if (ui32 added = changes.AppendedPortions.size()) {
            out << "add " << added << " portions";
            for (auto& portionInfo : changes.AppendedPortions) {
                out << portionInfo;
            }
            out << "; ";
        }
        if (ui32 moved = changes.PortionsToMove.size()) {
            out << "move " << moved << " portions";
            for (auto& [portionInfo, granule] : changes.PortionsToMove) {
                out << portionInfo << " (to " << granule << ")";
            }
            out << "; ";
        }
        if (ui32 evicted = changes.PortionsToEvict.size()) {
            out << "evict " << evicted << " portions";
            for (auto& [portionInfo, tier] : changes.PortionsToEvict) {
                out << portionInfo << " (to " << tier << ")";
            }
            out << "; ";
        }
        if (ui32 dropped = changes.PortionsToDrop.size()) {
            out << "drop " << dropped << " portions";
            for (auto& portionInfo : changes.PortionsToDrop) {
                out << portionInfo;
            }
        }
        return out;
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

    TVector<TGranuleRecord> Granules; // oredered by key (asc)
    TVector<TPortionInfo> Portions;

    TVector<ui64> GranulesOrder(bool rev = false) const {
        size_t size = Granules.size();
        TVector<ui64> order(size);
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
        ui64 Portions{};
        ui64 Blobs{};
        ui64 Rows{};
        ui64 Bytes{};
        ui64 RawBytes{};
    };

    ui64 Tables{};
    ui64 Granules{};
    ui64 EmptyGranules{};
    ui64 OverloadedGranules{};
    ui64 ColumnRecords{};
    ui64 ColumnMetadataBytes{};
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

    ui64 ActivePortions() const { return Inserted.Portions + Compacted.Portions + SplitCompacted.Portions; }
    ui64 ActiveBlobs() const { return Inserted.Blobs + Compacted.Blobs + SplitCompacted.Blobs; }
    ui64 ActiveRows() const { return Inserted.Rows + Compacted.Rows + SplitCompacted.Rows; }
    ui64 ActiveBytes() const { return Inserted.Bytes + Compacted.Bytes + SplitCompacted.Bytes; }
    ui64 ActiveRawBytes() const { return Inserted.RawBytes + Compacted.RawBytes + SplitCompacted.RawBytes; }

    void Clear() {
        *this = {};
    }
};

class IColumnEngine {
public:
    virtual ~IColumnEngine() = default;

    virtual const TIndexInfo& GetIndexInfo() const = 0;
    virtual const std::shared_ptr<arrow::Schema>& GetReplaceKey() const { return GetIndexInfo().GetReplaceKey(); }
    virtual const std::shared_ptr<arrow::Schema>& GetSortingKey() const { return GetIndexInfo().GetSortingKey(); }
    virtual const std::shared_ptr<arrow::Schema>& GetIndexKey() const { return GetIndexInfo().GetIndexKey(); }
    virtual const THashSet<ui64>* GetOverloadedGranules(ui64 /*pathId*/) const { return nullptr; }
    virtual bool HasOverloadedGranules() const { return false; }

    virtual bool Load(IDbWrapper& db, const THashSet<ui64>& pathsToDrop = {}) = 0;

    virtual std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                                const THashSet<ui32>& columnIds,
                                                std::shared_ptr<TPredicate> from,
                                                std::shared_ptr<TPredicate> to) const = 0;
    virtual std::unique_ptr<TCompactionInfo> Compact() = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartInsert(TVector<TInsertedData>&& dataToIndex) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                                  const TSnapshot& outdatedSnapshot) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCleanup(const TSnapshot& snapshot,
                                                               THashSet<ui64>& pathsToDrop) = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartTtl(const THashMap<ui64, TTiersInfo>& pathTtls,
                                                           ui64 maxBytesToEvict = TCompactionLimits::DEFAULT_EVICTION_BYTES) = 0;
    virtual bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) = 0;
    virtual void UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) = 0;
    //virtual void UpdateTableSchema(ui64 pathId, const TSnapshot& snapshot, TIndexInfo&& info) = 0; // TODO
    virtual void UpdateCompactionLimits(const TCompactionLimits& limits) = 0;
    virtual const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const = 0;
    virtual const TColumnEngineStats& GetTotalStats() = 0;
    virtual ui64 MemoryUsage() const { return 0; }
    virtual TSnapshot LastUpdate() const { return {}; }
};

}
