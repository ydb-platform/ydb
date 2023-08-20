#pragma once
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <util/stream/output.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

struct TPortionMeta {
private:
    void AddMinMax(ui32 columnId, const std::shared_ptr<arrow::Array>& column, bool sorted);
    std::shared_ptr<arrow::RecordBatch> ReplaceKeyEdges; // first and last PK rows
    YDB_ACCESSOR_DEF(TString, TierName);
public:
    using EProduced = NPortion::EProduced;

    std::optional<NArrow::TReplaceKey> IndexKeyStart;
    std::optional<NArrow::TReplaceKey> IndexKeyEnd;
    EProduced Produced{EProduced::UNSPECIFIED};
    ui32 FirstPkColumn = 0;

    bool DeserializeFromProto(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo);
    
    std::optional<NKikimrTxColumnShard::TIndexPortionMeta> SerializeToProto(const ui32 columnId, const ui32 chunk) const;

    void FillBatchInfo(const std::shared_ptr<arrow::RecordBatch> batch, const TIndexInfo& indexInfo);

    EProduced GetProduced() const {
        return Produced;
    }

    TString DebugString() const;

    friend IOutputStream& operator << (IOutputStream& out, const TPortionMeta& info) {
        out << info.DebugString();
        return out;
    }
};

class TPortionAddress {
private:
    YDB_READONLY(ui64, GranuleId, 0);
    YDB_READONLY(ui64, PortionId, 0);
public:
    TPortionAddress(const ui64 granuleId, const ui64 portionId)
        : GranuleId(granuleId)
        , PortionId(portionId)
    {

    }

    TString DebugString() const;

    bool operator<(const TPortionAddress& item) const {
        return std::tie(GranuleId, PortionId) < std::tie(item.GranuleId, item.PortionId);
    }

    bool operator==(const TPortionAddress& item) const {
        return std::tie(GranuleId, PortionId) == std::tie(item.GranuleId, item.PortionId);
    }
};

} // namespace NKikimr::NOlap

template<>
struct THash<NKikimr::NOlap::TPortionAddress> {
    inline ui64 operator()(const NKikimr::NOlap::TPortionAddress& x) const noexcept {
        return CombineHashes(x.GetPortionId(), x.GetGranuleId());
    }
};

