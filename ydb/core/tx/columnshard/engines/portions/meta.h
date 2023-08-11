#pragma once
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <util/stream/output.h>

namespace NKikimr::NOlap {

struct TPortionMeta {
    using EProduced = NPortion::EProduced;

    struct TColumnMeta {
        ui32 NumRows{0};
        ui32 RawBytes{0};
        std::shared_ptr<arrow::Scalar> Min;
        std::shared_ptr<arrow::Scalar> Max;

        bool HasMinMax() const noexcept {
            return Min.get() && Max.get();
        }
    };

    EProduced GetProduced() const {
        return Produced;
    }

    EProduced Produced{EProduced::UNSPECIFIED};
    THashMap<ui32, TColumnMeta> ColumnMeta;
    ui32 FirstPkColumn = 0;
    std::shared_ptr<arrow::RecordBatch> ReplaceKeyEdges; // first and last PK rows
    std::optional<NArrow::TReplaceKey> IndexKeyStart;
    std::optional<NArrow::TReplaceKey> IndexKeyEnd;

    TString DebugString() const {
        return TStringBuilder() <<
            "produced:" << Produced << ";"
            ;
    }

    bool HasMinMax(ui32 columnId) const {
        if (!ColumnMeta.contains(columnId)) {
            return false;
        }
        return ColumnMeta.find(columnId)->second.HasMinMax();
    }

    bool HasPkMinMax() const {
        return HasMinMax(FirstPkColumn);
    }

    ui32 NumRows() const {
        if (FirstPkColumn) {
            Y_VERIFY(ColumnMeta.contains(FirstPkColumn));
            return ColumnMeta.find(FirstPkColumn)->second.NumRows;
        }
        return 0;
    }

    friend IOutputStream& operator << (IOutputStream& out, const TPortionMeta& info) {
        out << "reason" << (ui32)info.Produced;
        for (const auto& [_, meta] : info.ColumnMeta) {
            if (meta.NumRows) {
                out << " " << meta.NumRows << " rows";
                break;
            }
        }
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

    bool operator<(const TPortionAddress& item) const {
        return std::tie(GranuleId, PortionId) < std::tie(item.GranuleId, item.PortionId);
    }

    bool operator==(const TPortionAddress& item) const {
        return std::tie(GranuleId, PortionId) == std::tie(item.GranuleId, item.PortionId);
    }
};

} // namespace NKikimr::NOlap
