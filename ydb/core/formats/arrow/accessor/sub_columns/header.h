#pragma once
#include "stats.h"

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TReadRange {
private:
    YDB_READONLY(ui32, Offset, 0);
    YDB_READONLY(ui32, Size, 0);

public:
    TReadRange(const ui32 offset, const ui32 size)
        : Offset(offset)
        , Size(size) {
    }
};

class TSubColumnsHeader {
private:
    TDictStats ColumnStats;
    TDictStats OtherStats;
    NKikimrArrowAccessorProto::TSubColumnsAccessor AddressesProto;
    const ui32 HeaderSize;
    ui32 ColumnsSize = 0;
    ui32 OthersSize = 0;

public:

    bool HasSubColumn(const TString& subColumnName) const {
        return ColumnStats.GetKeyIndexOptional(std::string_view(subColumnName.data(), subColumnName.size())) ||
               OtherStats.GetKeyIndexOptional(std::string_view(subColumnName.data(), subColumnName.size()));
    }

    TConstructorContainer GetAccessorConstructor(const ui32 colIndex) const {
        return ColumnStats.GetAccessorConstructor(colIndex);
    }

    std::shared_ptr<arrow::Field> GetField(const ui32 colIndex) const {
        return ColumnStats.GetField(colIndex);
    }

    TReadRange GetColumnReadRange(const ui32 idx) const {
        AFL_VERIFY(idx < (ui32)AddressesProto.GetKeyColumns().size())("count", AddressesProto.GetKeyColumns().size())("idx", idx);
        ui32 idxLocal = 0;
        ui32 predShift = 0;
        for (auto&& i : AddressesProto.GetKeyColumns()) {
            if (idxLocal == idx) {
                return TReadRange(HeaderSize + predShift, i.GetSize());
            }
            ++idxLocal;
            predShift += i.GetSize();
        }
        AFL_VERIFY(false);
        return TReadRange(0, 0);
    }

    TReadRange GetOthersReadRange() const {
        return TReadRange(HeaderSize + ColumnsSize, OthersSize);
    }

    const TDictStats& GetColumnStats() const {
        return ColumnStats;
    }
    const TDictStats& GetOtherStats() const {
        return OtherStats;
    }

    const NKikimrArrowAccessorProto::TSubColumnsAccessor& GetAddressesProto() const {
        return AddressesProto;
    }

    ui32 GetHeaderSize() const {
        return HeaderSize;
    }

    ui32 GetColumnsSize() const {
        return ColumnsSize;
    }

    ui32 GetOthersSize() const {
        return OthersSize;
    }

    TSubColumnsHeader(
        TDictStats&& columnStats, TDictStats&& otherStats, NKikimrArrowAccessorProto::TSubColumnsAccessor&& proto, const ui32 headerSize)
        : ColumnStats(std::move(columnStats))
        , OtherStats(std::move(otherStats))
        , AddressesProto(std::move(proto))
        , HeaderSize(headerSize) {
        ColumnsSize = 0;
        for (ui32 i = 0; i < (ui32)AddressesProto.GetKeyColumns().size(); ++i) {
            ColumnsSize += AddressesProto.GetKeyColumns(i).GetSize();
        }
        OthersSize = 0;
        for (ui32 i = 0; i < (ui32)AddressesProto.GetOtherColumns().size(); ++i) {
            OthersSize += AddressesProto.GetOtherColumns(i).GetSize();
        }
    }

    static TSubColumnsHeader BuildEmpty() {
        static TSubColumnsHeader result =
            TSubColumnsHeader(TDictStats::BuildEmpty(), TDictStats::BuildEmpty(), NKikimrArrowAccessorProto::TSubColumnsAccessor(), 0);
        return result;

    }

    static TConclusion<TSubColumnsHeader> ReadHeader(const TString& originalData, const TChunkConstructionData& externalInfo);
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
