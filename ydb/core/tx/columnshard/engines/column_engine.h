#pragma once
#include "db_wrapper.h"

#include "changes/abstract/compaction_info.h"
#include "changes/abstract/settings.h"
#include "predicate/filter.h"
#include "scheme/snapshot_scheme.h"
#include "scheme/versions/versioned_index.h"

#include <ydb/core/tx/columnshard/common/reverse_accessor.h>

namespace NKikimr::NColumnShard {
class TTiersManager;
class TTtl;
}   // namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {
class TInsertColumnEngineChanges;
class TCompactColumnEngineChanges;
class TColumnEngineChanges;
class TTTLColumnEngineChanges;
class TCleanupPortionsColumnEngineChanges;
class TCleanupTablesColumnEngineChanges;
class TPortionInfo;
namespace NDataLocks {
class TManager;
}

struct TSelectInfo {
    struct TStats {
        size_t Portions{};
        size_t Records{};
        size_t Blobs{};
        size_t Rows{};
        size_t Bytes{};

        const TStats& operator+=(const TStats& stats) {
            Portions += stats.Portions;
            Records += stats.Records;
            Blobs += stats.Blobs;
            Rows += stats.Rows;
            Bytes += stats.Bytes;
            return *this;
        }
    };

    std::vector<std::shared_ptr<TPortionInfo>> PortionsOrderedPK;

    NColumnShard::TContainerAccessorWithDirection<std::vector<std::shared_ptr<TPortionInfo>>> GetPortionsOrdered(const bool reverse) const {
        return NColumnShard::TContainerAccessorWithDirection<std::vector<std::shared_ptr<TPortionInfo>>>(PortionsOrderedPK, reverse);
    }

    size_t NumChunks() const;

    TStats Stats() const;

    void DebugStream(IOutputStream& out);
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
            Y_ABORT_UNLESS(result >= 0);
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
    i64 ColumnRecords{};
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

class IColumnEngine {
protected:
    virtual void DoRegisterTable(const ui64 pathId) = 0;

public:
    static ui64 GetMetadataLimit();

    virtual ~IColumnEngine() = default;

    virtual const TVersionedIndex& GetVersionedIndex() const = 0;
    virtual std::shared_ptr<TVersionedIndex> CopyVersionedIndexPtr() const = 0;
    virtual const std::shared_ptr<arrow::Schema>& GetReplaceKey() const;

    virtual bool HasDataInPathId(const ui64 pathId) const = 0;
    virtual bool ErasePathId(const ui64 pathId) = 0;
    virtual bool Load(IDbWrapper& db) = 0;
    void RegisterTable(const ui64 pathId) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "RegisterTable")("path_id", pathId);
        return DoRegisterTable(pathId);
    }
    virtual bool IsOverloadedByMetadata(const ui64 limit) const = 0;
    virtual std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot, const TPKRangesFilter& pkRangesFilter) const = 0;
    virtual std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCompaction(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept = 0;
    virtual std::shared_ptr<TCleanupPortionsColumnEngineChanges> StartCleanupPortions(const TSnapshot& snapshot, const THashSet<ui64>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept = 0;
    virtual std::shared_ptr<TCleanupTablesColumnEngineChanges> StartCleanupTables(THashSet<ui64>& pathsToDrop) noexcept = 0;
    virtual std::vector<std::shared_ptr<TTTLColumnEngineChanges>> StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const ui64 memoryUsageLimit) noexcept = 0;
    virtual bool ApplyChangesOnTxCreate(std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) noexcept = 0;
    virtual bool ApplyChangesOnExecute(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) noexcept = 0;
    virtual void RegisterSchemaVersion(const TSnapshot& snapshot, TIndexInfo&& info) = 0;
    virtual void RegisterSchemaVersion(const TSnapshot& snapshot, const NKikimrSchemeOp::TColumnTableSchema& schema) = 0;
    virtual const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const = 0;
    virtual const TColumnEngineStats& GetTotalStats() = 0;
    virtual ui64 MemoryUsage() const {
        return 0;
    }
    virtual TSnapshot LastUpdate() const {
        return TSnapshot::Zero();
    }
    virtual void OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& manager, const NColumnShard::TTtl& ttl, const std::optional<ui64> pathId) = 0;
};

}   // namespace NKikimr::NOlap
